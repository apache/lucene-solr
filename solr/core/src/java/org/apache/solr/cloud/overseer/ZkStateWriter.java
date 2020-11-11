/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.cloud.overseer;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.BoundedTreeSet;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;

public class ZkStateWriter {
  // private static final long MAX_FLUSH_INTERVAL = TimeUnit.NANOSECONDS.convert(Overseer.STATE_UPDATE_DELAY, TimeUnit.MILLISECONDS);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ZkStateReader reader;

  /**
   * Represents a no-op {@link ZkWriteCommand} which will result in no modification to cluster state
   */

  protected volatile Stats stats;

  AtomicReference<Exception> lastFailedException = new AtomicReference<>();
  private final Map<String,Integer> trackVersions = new ConcurrentHashMap<>();

  Map<String,DocCollection> failedUpdates = new ConcurrentHashMap<>();

  private volatile ClusterState cs;
  private boolean dirty;
  private Set<String> collectionsToWrite = new HashSet<>();

  protected final ReentrantLock ourLock = new ReentrantLock(true);
  protected final ReentrantLock writeLock = new ReentrantLock(true);

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats) {
    assert zkStateReader != null;

    this.reader = zkStateReader;
    this.stats = stats;

    zkStateReader.forciblyRefreshAllClusterStateSlow();
    cs = zkStateReader.getClusterState();
  }

  public void enqueueUpdate(ClusterState clusterState, ZkNodeProps message, boolean stateUpdate) throws Exception {

    if (log.isDebugEnabled()) log.debug("enqueue update stateUpdate={}", stateUpdate);
    ourLock.lock();
    try {
      AtomicBoolean changed = new AtomicBoolean();

      if (!stateUpdate) {
        if (clusterState == null) {
          throw new NullPointerException("clusterState cannot be null");
        }
        changed.set(true);
        clusterState.forEachCollection(collection -> {
          DocCollection currentCollection = cs.getCollectionOrNull(collection.getName());

          for (Slice slice : collection) {
            if (currentCollection != null) {
              Slice currentSlice = currentCollection.getSlice(slice.getName());
              if (currentSlice != null) {
                slice.setState(currentSlice.getState());
                Replica leader = currentSlice.getLeader();
                slice.setLeader(currentSlice.getLeader());
                if (leader != null) {
                  leader.setState(Replica.State.ACTIVE);
                  leader.getProperties().put("leader", "true");
                }
              }
            }

            for (Replica replica : slice) {
              if (currentCollection != null) {
                Replica currentReplica = currentCollection.getReplica(replica.getName());
                if (currentReplica != null) {
                  replica.setState(currentReplica.getState());
                }
              }
              Object removed = replica.getProperties().remove("numShards");
            }
          }
        });

        collectionsToWrite.addAll(clusterState.getCollectionsMap().keySet());
        Collection<DocCollection> collections = cs.getCollectionsMap().values();
        for (DocCollection collection : collections) {
          if (clusterState.getCollectionOrNull(collection.getName()) == null) {
            clusterState = clusterState.copyWith(collection.getName(), collection);
          }
        }

        this.cs = clusterState;
      } else {
        final String operation = message.getStr(Overseer.QUEUE_OPERATION);
        OverseerAction overseerAction = OverseerAction.get(operation);
        if (overseerAction == null) {
          throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
        }
        switch (overseerAction) {
          case STATE:
            log.info("state cmd");
            String collection = message.getStr("collection");
            DocCollection docColl = cs.getCollectionOrNull(collection);
            if (docColl != null) {
              Replica replica = docColl.getReplica(message.getStr(ZkStateReader.CORE_NAME_PROP));
              if (replica != null) {
                Replica.State state = Replica.State.getState((String) message.get(ZkStateReader.STATE_PROP));
                log.info("set state {} {}", state, replica);
                if (state != replica.getState()) {
                  replica.setState(state);
                  changed.set(true);
                  collectionsToWrite.add(collection);
                }
              }
            }
            break;
          case LEADER:
            log.info("leader cmd");
            collection = message.getStr("collection");
            docColl = cs.getCollectionOrNull(collection);
            if (docColl != null) {
              Slice slice = docColl.getSlice(message.getStr("shard"));
              if (slice != null) {
                Replica replica = docColl.getReplica(message.getStr(ZkStateReader.CORE_NAME_PROP));
                if (replica != null) {
                  log.info("set leader {} {}", message.getStr(ZkStateReader.CORE_NAME_PROP), replica);
                  slice.setLeader(replica);
                  replica.setState(Replica.State.ACTIVE);
                  replica.getProperties().put("leader", "true");
                  Collection<Replica> replicas = slice.getReplicas();
                  for (Replica r : replicas) {
                    if (r != replica) {
                      r.getProperties().remove("leader");
                    }
                  }
                  changed.set(true);
                  collectionsToWrite.add(collection);
                }
              }
            }
            break;
//          case ADDROUTINGRULE:
//            return new SliceMutator(cloudManager).addRoutingRule(clusterState, message);
//          case REMOVEROUTINGRULE:
//            return new SliceMutator(cloudManager).removeRoutingRule(clusterState, message);
          case UPDATESHARDSTATE:
            collection = message.getStr("collection");
            message.getProperties().remove("collection");
            message.getProperties().remove("operation");

              docColl = cs.getCollectionOrNull(collection);
              if (docColl != null) {
                for (Map.Entry<String,Object> entry : message.getProperties().entrySet()) {
                  Slice slice = docColl.getSlice(entry.getKey());
                  if (slice != null) {
                    Slice.State state = Slice.State.getState((String) entry.getValue());
                    if (slice.getState() != state) {
                      slice.setState(state);
                      changed.set(true);
                      collectionsToWrite.add(collection);
                    }
                  }
                }
              }
            break;
          case DOWNNODE:
            collection = message.getStr("collection");
            docColl = cs.getCollectionOrNull(collection);
            if (docColl != null) {
              List<Replica> replicas = docColl.getReplicas();
              for (Replica replica : replicas) {
                if (replica.getState() != Replica.State.DOWN) {
                  replica.setState(Replica.State.DOWN);
                  changed.set(true);
                  collectionsToWrite.add(collection);
                }
              }
            }
            break;
          default:
            throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());

        }

      }

      if (stateUpdate) {
        //      log.error("isStateUpdate, final cs: {}", this.cs);
        //      log.error("isStateUpdate, submitted state: {}", clusterState);
        if (!changed.get()) {
          log.warn("Published state that changed nothing");
          return;
        }
      }
      dirty = true;
    } finally {
      ourLock.unlock();
    }
  }

  public Integer lastWrittenVersion(String collection) {
    return trackVersions.get(collection);
  }

  /**
   * Writes all pending updates to ZooKeeper and returns the modified cluster state
   *
   */
  public void writePendingUpdates() {

    writeLock.lock();
    try {
      ourLock.lock();
      try {
        if (!dirty) {
          return;
        }

        if (log.isDebugEnabled()) {
          log.debug("writePendingUpdates {}", cs);
        }

        if (failedUpdates.size() > 0) {
          log.warn("Some collection updates failed {} logging last exception", failedUpdates, lastFailedException); // nocommit expand
          failedUpdates.clear();
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, lastFailedException.get());
        }
      } finally {
        ourLock.unlock();
      }

      // wait to see our last publish version has propagated TODO don't wait on collections not hosted on overseer?
      cs.forEachCollection(collection -> {
        if (collectionsToWrite.contains(collection.getName())) {
          Integer v = null;
          try {
            //System.out.println("waiting to see state " + prevVersion);
            v = trackVersions.get(collection.getName());
            if (v == null) v = 0;
            if (v == 0) return;
            Integer version = v;
            try {
              log.debug("wait to see last published version for collection {} {}", collection.getName(), v);
              reader.waitForState(collection.getName(), 5, TimeUnit.SECONDS, (l, col) -> {
                if (col == null) {
                  return true;
                }
                //                          if (col != null) {
                //                            log.info("the version " + col.getZNodeVersion());
                //                          }
                if (col != null && col.getZNodeVersion() >= version) {
                  if (log.isDebugEnabled()) log.debug("Waited for ver: {}", col.getZNodeVersion() + 1);
                  // System.out.println("found the version");
                  return true;
                }
                return false;
              });
            } catch (InterruptedException e) {
              ParWork.propagateInterrupt(e);
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
            }
          } catch (TimeoutException e) {
            log.warn("Timeout waiting to see written cluster state come back " + v);
          }
        }

      });

      ourLock.lock();
      AtomicInteger lastVersion = new AtomicInteger();
      try {
        cs.forEachCollection(collection -> {
          if (collectionsToWrite.contains(collection.getName())) {
            String name = collection.getName();
            String path = ZkStateReader.getCollectionPath(collection.getName());
            if (log.isDebugEnabled()) log.debug("process {}", collection);
            Stat stat = new Stat();
            boolean success = false;
            try {

              byte[] data = Utils.toJSON(singletonMap(name, collection));

              if (log.isDebugEnabled()) log.debug("Write state.json prevVersion={} bytes={} col={}", collection.getZNodeVersion(), data.length, collection);

              try {
              
                Integer v = trackVersions.get(collection.getName());
                Integer version;
                if (v != null) {
                  version = v;
                  lastVersion.set(version);
                  reader.getZkClient().setData(path, data, version, true);
                } else {
                  Stat existsStat = reader.getZkClient().exists(path, null);
                  if (existsStat == null) {
                    version = 0;
                    lastVersion.set(version);
                    reader.getZkClient().create(path, data, CreateMode.PERSISTENT, true);
                  } else {
                    version = stat.getVersion();
                    lastVersion.set(version);
                    reader.getZkClient().setData(path, data, version, true);
                  }
                }
                trackVersions.put(collection.getName(), version + 1);
              } catch (KeeperException.NoNodeException e) {
                if (log.isDebugEnabled()) log.debug("No node found for state.json", e);
                trackVersions.remove(collection.getName());
                // likely deleted
              } catch (KeeperException.BadVersionException bve) {
                //lastFailedException.set(bve);
                //failedUpdates.put(collection.getName(), collection);
                stat = reader.getZkClient().exists(path, null);
                trackVersions.put(collection.getName(), stat.getVersion());
                // this is a tragic error, we must disallow usage of this instance
                log.warn("Tried to update the cluster state using version={} but we where rejected, found {}", lastVersion.get(), stat.getVersion(), bve);
              }
              if (log.isDebugEnabled()) log.debug("Set version for local collection {} to {}", collection.getName(), collection.getZNodeVersion() + 1);
            } catch (InterruptedException | AlreadyClosedException e) {
              log.info("We have been closed or one of our resources has, bailing {}", e.getClass().getSimpleName() + ":" + e.getMessage());
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
            } catch (KeeperException.SessionExpiredException e) {
              log.error("", e);
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
            } catch (Exception e) {
              log.error("Failed processing update=" + collection, e);
            }
          }
        });

        dirty = false;
        collectionsToWrite.clear();
      } finally {
        ourLock.unlock();
      }
    } finally {
      writeLock.unlock();
    }
    // nocommit - harden against failures and exceptions

    //    if (log.isDebugEnabled()) {
    //      log.debug("writePendingUpdates() - end - New Cluster State is: {}", newClusterState);
    //    }

  }

  public ClusterState getClusterstate(boolean stateUpdate) {
    ourLock.lock();
    try {
      return cs;
    } finally {
      ourLock.unlock();
    }
  }

}

