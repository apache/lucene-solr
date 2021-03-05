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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.StatePublisher;
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
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;

public class ZkStateWriter {
  // private static final long MAX_FLUSH_INTERVAL = TimeUnit.NANOSECONDS.convert(Overseer.STATE_UPDATE_DELAY, TimeUnit.MILLISECONDS);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final ZkStateReader reader;
  private final Overseer overseer;

  /**
   * Represents a no-op {@link ZkWriteCommand} which will result in no modification to cluster state
   */

  protected volatile Stats stats;

  AtomicReference<Exception> lastFailedException = new AtomicReference<>();
  private final Map<String,Integer> trackVersions = new ConcurrentHashMap<>();

  private final Map<String, ZkNodeProps> stateUpdates = new HashMap<>();

  Map<String,DocCollection> failedUpdates = new ConcurrentHashMap<>();

  Map<Long,String> idToCollection = new ConcurrentHashMap<>();

  private volatile ClusterState cs;

  protected final ReentrantLock ourLock = new ReentrantLock();

  private final ActionThrottle throttle = new ActionThrottle("ZkStateWriter", Integer.getInteger("solr.zkstatewriter.throttle", 100), new TimeSource.NanoTimeSource(){
    public void sleep(long ms) throws InterruptedException {
      ourLock.newCondition().await(ms, TimeUnit.MILLISECONDS);
    }
  });

  private static AtomicLong ID = new AtomicLong();

  private Set<String> dirtyStructure = new HashSet<>();
  private Set<String> dirtyState = new HashSet<>();

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats, Overseer overseer) {
    this.overseer = overseer;
    this.reader = zkStateReader;
    this.stats = stats;

    cs = zkStateReader.getClusterState();

    long[] highId = new long[1];
    cs.forEachCollection(collection -> {
      if (collection.getId() > highId[0]) {
        highId[0] = collection.getId();
      }

      idToCollection.put(collection.getId(), collection.getName());
//      String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(collection.getName());
//      if (log.isDebugEnabled()) log.debug("clear state updates on new overseer for collection {}", collection.getName());
//      try {
//        reader.getZkClient().setData(stateUpdatesPath, Utils.toJSON(new ZkNodeProps()), -1, true);
//      } catch (KeeperException e) {
//        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//      } catch (InterruptedException e) {
//        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//      }
    });

    ID.set(highId[0]);

    if (log.isDebugEnabled()) log.debug("zkStateWriter starting with cs {}", cs);
  }

  public void enqueueUpdate(ClusterState clusterState, ZkNodeProps message, boolean stateUpdate) throws Exception {

    if (log.isDebugEnabled()) log.debug("enqueue update stateUpdate={} cs={}", stateUpdate, clusterState);
    //log.info("Get our write lock for enq");
    ourLock.lock();
    //log.info("Got our write lock for enq");
    try {

      if (!stateUpdate) {
        if (clusterState == null) {
          throw new NullPointerException("clusterState cannot be null");
        }

        clusterState.forEachCollection(collection -> {
          if (trackVersions.get(collection.getName()) == null) {
            reader.forciblyRefreshClusterStateSlow(collection.getName());
            DocCollection latestColl = reader.getClusterState().getCollectionOrNull(collection.getName());

            if (latestColl == null) {
              //log.info("no node exists, using version 0");
              trackVersions.remove(collection.getName());
            } else {
              cs.getCollectionStates().put(latestColl.getName(), new ClusterState.CollectionRef(latestColl));
              //log.info("got version from zk {}", existsStat.getVersion());
              int version = latestColl.getZNodeVersion();
              log.info("Updating local tracked version to {} for {}", version, collection.getName());
              trackVersions.put(collection.getName(), version);
            }
          }


          DocCollection currentCollection = cs.getCollectionOrNull(collection.getName());
          collection.getProperties().remove("pullReplicas");
          collection.getProperties().remove("replicationFactor");
          collection.getProperties().remove("maxShardsPerNode");
          collection.getProperties().remove("nrtReplicas");
          collection.getProperties().remove("tlogReplicas");
          for (Slice slice : collection) {
            if (currentCollection != null) {
              Slice currentSlice = currentCollection.getSlice(slice.getName());
              if (currentSlice != null) {
                slice.setState(currentSlice.getState());
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
          dirtyStructure.add(collection.getName());
        });

        Collection<DocCollection> collections = cs.getCollectionsMap().values();
        for (DocCollection collection : collections) {
          if (clusterState.getCollectionOrNull(collection.getName()) == null) {
            clusterState = clusterState.copyWith(collection.getName(), collection);
          }
        }

        this.cs = clusterState;
      } else {
        final String operation = message.getStr(StatePublisher.OPERATION);
        OverseerAction overseerAction = OverseerAction.get(operation);
        if (overseerAction == null) {
          throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
        }

        switch (overseerAction) {
          case STATE:
            if (log.isDebugEnabled()) log.debug("state cmd {}", message);
            message.getProperties().remove(StatePublisher.OPERATION);

            for (Map.Entry<String,Object> entry : message.getProperties().entrySet()) {
              if (OverseerAction.DOWNNODE.equals(OverseerAction.get(entry.getKey()))) {
                if (log.isDebugEnabled()) {
                  log.debug("state cmd entry {} asOverseerCmd={}", entry, OverseerAction.get(entry.getKey()));
                }
                nodeOperation(entry, Replica.State.getShortState(Replica.State.DOWN));
              } else if (OverseerAction.RECOVERYNODE.equals(OverseerAction.get(entry.getKey()))) {
                if (log.isDebugEnabled()) {
                  log.debug("state cmd entry {} asOverseerCmd={}", entry, OverseerAction.get(entry.getKey()));
                }
                nodeOperation(entry, Replica.State.getShortState(Replica.State.RECOVERING));
              }
            }

            for (Map.Entry<String,Object> entry : message.getProperties().entrySet()) {
              if (OverseerAction.DOWNNODE.equals(OverseerAction.get(entry.getKey()))) {
                continue;
              } else if (OverseerAction.RECOVERYNODE.equals(OverseerAction.get(entry.getKey()))) {
                continue;
              } else {
                if (log.isDebugEnabled()) log.debug("state cmd entry {} asOverseerCmd={}", entry, OverseerAction.get(entry.getKey()));
                String id = entry.getKey();

                String stateString = (String) entry.getValue();
                if (log.isDebugEnabled()) {
                  log.debug("stateString={}", stateString);
                }

                long collectionId = Long.parseLong(id.split("-")[0]);
                String collection = reader.getClusterState().getCollection(collectionId);

                if (collection == null) {
                  continue;
                }

                String setState = Replica.State.shortStateToState(stateString).toString();

                if (trackVersions.get(collection) == null) {
                  reader.forciblyRefreshClusterStateSlow(collection);
                  DocCollection latestColl = reader.getClusterState().getCollectionOrNull(collection);

                  if (latestColl == null) {
                    //log.info("no node exists, using version 0");
                    trackVersions.remove(collection);
                  } else {
                    cs.getCollectionStates().put(latestColl.getName(), new ClusterState.CollectionRef(latestColl));
                    //log.info("got version from zk {}", existsStat.getVersion());
                    int version = latestColl.getZNodeVersion();
                    log.info("Updating local tracked version to {} for {}", version, collection);
                    trackVersions.put(collection, version);
                  }
                }

                ZkNodeProps updates = stateUpdates.get(collection);
                if (updates == null) {
                  updates = new ZkNodeProps();
                  stateUpdates.put(collection, updates);
                }
                Integer ver = trackVersions.get(collection);
                if (ver == null) {
                  ver = 0;
                }
                updates.getProperties().put("_cs_ver_", ver.toString());

                DocCollection docColl = cs.getCollectionOrNull(collection);
                if (docColl != null) {
                  Replica replica = docColl.getReplicaById(id);
                  if (replica != null) {
                    if (setState.equals("leader")) {
                      if (log.isDebugEnabled()) {
                        log.debug("set leader {}", replica);
                      }
                      Slice slice = docColl.getSlice(replica.getSlice());
                      slice.setLeader(replica);
                      replica.setState(Replica.State.ACTIVE);
                      replica.getProperties().put("leader", "true");
                      Collection<Replica> replicas = slice.getReplicas();
                      for (Replica r : replicas) {
                        if (r != replica) {
                          r.getProperties().remove("leader");
                        }
                      }
                      updates.getProperties().put(replica.getId(), "l");
                      dirtyState.add(collection);
                    } else {
                      Replica.State state = Replica.State.getState(setState);
                      Replica existingLeader = docColl.getSlice(replica).getLeader();
                      if (existingLeader != null && existingLeader.getName().equals(replica.getName())) {
                        docColl.getSlice(replica).setLeader(null);
                      }
                      updates.getProperties().put(replica.getId(), Replica.State.getShortState(state));
                      // log.info("set state {} {}", state, replica);
                      replica.setState(state);
                      dirtyState.add(collection);
                    }
                  }
                }
              }
            }

            break;
            // MRM TODO:
//          case ADDROUTINGRULE:
//            return new SliceMutator(cloudManager).addRoutingRule(clusterState, message);
//          case REMOVEROUTINGRULE:
//            return new SliceMutator(cloudManager).removeRoutingRule(clusterState, message);
          case UPDATESHARDSTATE:  // MRM TODO: look at how we handle this and make it so it can use StatePublisher
            String collection = message.getStr("collection");
            message.getProperties().remove("collection");
            message.getProperties().remove(StatePublisher.OPERATION);

            DocCollection docColl = cs.getCollectionOrNull(collection);
            if (docColl != null) {
              for (Map.Entry<String,Object> e : message.getProperties().entrySet()) {
                Slice slice = docColl.getSlice(e.getKey());
                if (slice != null) {
                  Slice.State state = Slice.State.getState((String) e.getValue());
                  slice.setState(state);
                  dirtyStructure.add(collection);
                }
              }
            }
            break;
          default:
            throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());

        }

      }

    } catch (Exception e) {
      log.error("Exception while queuing update", e);
      throw e;
    }  finally {
      ourLock.unlock();
    }
  }

  private void nodeOperation(Map.Entry<String,Object> entry, String operation) {
    log.info("set operation {} for {}", operation, entry.getValue());
    cs.forEachCollection(docColl -> {

      if (trackVersions.get(docColl.getName()) == null) {
        reader.forciblyRefreshClusterStateSlow(docColl.getName());
        DocCollection latestColl = reader.getClusterState().getCollectionOrNull(docColl.getName());

        if (latestColl == null) {
          //log.info("no node exists, using version 0");
          trackVersions.remove(docColl.getName());
        } else {
          cs.getCollectionStates().put(latestColl.getName(), new ClusterState.CollectionRef(latestColl));
          //log.info("got version from zk {}", existsStat.getVersion());
          int version = latestColl.getZNodeVersion();
          log.info("Updating local tracked version to {} for {}", version, docColl.getName());
          trackVersions.put(docColl.getName(), version);
        }
      }

      ZkNodeProps updates = stateUpdates.get(docColl.getName());
      if (updates == null) {
        updates = new ZkNodeProps();
        stateUpdates.put(docColl.getName(), updates);
      }
      Integer ver = trackVersions.get(docColl.getName());
      if (ver == null) {
        //   ver = docColl.getZNodeVersion();
        if (ver == null) {
          ver = 0;
        }
      }
      updates.getProperties().put("_cs_ver_", ver.toString());
      List<Replica> replicas = docColl.getReplicas();
      for (Replica replica : replicas) {
        if (!Replica.State.getShortState(replica.getState()).equals(operation) && replica.getNodeName().equals(entry.getValue())) {
          if (log.isDebugEnabled()) log.debug("set {} for replica {}", operation, replica);
          // MRM TODO:
          Slice slice = docColl.getSlice(replica.getSlice());
          slice.setLeader(null);
          replica.setState(Replica.State.shortStateToState(operation));
          updates.getProperties().put(replica.getId(), operation);
          dirtyState.add(docColl.getName());
        }
      }
    });
  }

  public Integer lastWrittenVersion(String collection) {
    return trackVersions.get(collection);
  }

  /**
   * Writes all pending updates to ZooKeeper and returns the modified cluster state
   *
   */

  // if additional updates too large, publish structure changew
  public void writePendingUpdates() {

    do {
      try {
        write();
        break;
      } catch (KeeperException.BadVersionException e) {

      } catch (Exception e) {
        log.error("write pending failed", e);
        break;
      }

    } while (!overseer.isClosed());

  }

  private void write() throws KeeperException.BadVersionException {
    // writeLock.lock();
    // try {
    //   log.info("Get our write lock");
    if (log.isDebugEnabled()) {
      log.debug("writePendingUpdates {}", cs);
    }

    ourLock.lock();
    try {
 //     log.info("Got our write lock");

      throttle.minimumWaitBetweenActions();
      throttle.markAttemptingAction();

      if (failedUpdates.size() > 0) {
        Exception lfe = lastFailedException.get();
        log.warn("Some collection updates failed {} logging last exception", failedUpdates, lfe); // MRM TODO: expand
        failedUpdates.clear();
        lfe = null;
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, lfe);
      }
//      } finally {
//        ourLock.unlock();
//      }

    // wait to see our last publish version has propagated TODO don't wait on collections not hosted on overseer?
    // waitForStateWePublishedToComeBack();

 //   ourLock.lock();
    AtomicInteger lastVersion = new AtomicInteger();
    AtomicReference<KeeperException.BadVersionException> badVersionException = new AtomicReference();
    List<String> removeCollections = new ArrayList<>();
    //log.info("writing out state, looking at collections count={} toWrite={} {} : {}", cs.getCollectionsMap().size(), collectionsToWrite.size(), cs.getCollectionsMap().keySet(), collectionsToWrite);
    //try {
      cs.forEachCollection(collection -> {
       // log.info("check collection {}", collection);
        Integer version = null;
        if (dirtyStructure.contains(collection.getName()) || dirtyState.contains(collection.getName())) {
        //  log.info("process collection {}", collection);
          String name = collection.getName();
          String path = ZkStateReader.getCollectionPath(collection.getName());
          String pathSCN = ZkStateReader.getCollectionSCNPath(collection.getName());
         // log.info("process collection {} path {}", collection.getName(), path);
          Stat existsStat = null;
          if (log.isTraceEnabled()) log.trace("process {}", collection);
          try {
           // log.info("get data for {}", name);
            byte[] data = Utils.toJSON(singletonMap(name, collection));
          //  log.info("got data for {} {}", name, data.length);

            try {
              Integer v = trackVersions.get(collection.getName());

              if (v != null) {
                //log.info("got version from cache {}", v);
                version = v;
              } else {
                version = 0;
              }
              lastVersion.set(version);
              if (log.isDebugEnabled()) log.debug("Write state.json prevVersion={} bytes={} col={}", version, data.length, collection);

              reader.getZkClient().setData(path, data, version, true, true);
              trackVersions.put(collection.getName(), version + 1);
              if (dirtyStructure.contains(collection.getName())) {
                if (log.isDebugEnabled()) log.debug("structure change in {}", collection.getName());
                dirtyStructure.remove(collection.getName());
                reader.getZkClient().setData(pathSCN, null, -1, true, true);

                ZkNodeProps updates = stateUpdates.get(collection.getName());
                if (updates != null) {
                  updates.getProperties().clear();
                 String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(collection.getName());
                 if (log.isDebugEnabled()) log.debug("write state updates for collection {} {}", collection.getName(), updates);
                  try {
                    reader.getZkClient().setData(stateUpdatesPath, Utils.toJSON(updates), -1, true, true);
                  } catch (KeeperException.NoNodeException e) {
                    if (log.isDebugEnabled()) log.debug("No node found for " + stateUpdatesPath, e);
                    lastVersion.set(-1);
                    trackVersions.remove(collection.getName());
                    // likely deleted
                  }
                }
              }

            } catch (KeeperException.NoNodeException e) {
              if (log.isDebugEnabled()) log.debug("No node found for state.json", e);

              lastVersion.set(-1);
              trackVersions.remove(collection.getName());
              // likely deleted

            } catch (KeeperException.BadVersionException bve) {
              //lastFailedException.set(bve);
              //failedUpdates.put(collection.getName(), collection);
             // Stat estate = reader.getZkClient().exists(path, null);
              trackVersions.remove(collection.getName());
              Stat stat = reader.getZkClient().exists(path, null, false, false);
              log.info("Tried to update state.json ({}) with bad version {} \n {}", collection, version, stat != null ? stat.getVersion() : "null");

              if (!overseer.isClosed() && stat != null) {
                trackVersions.put(collection.getName(), stat.getVersion());
              } else {
                removeCollections.add(collection.getName());
              }

              throw bve;
            }

            if (dirtyState.contains(collection.getName())) {
              ZkNodeProps updates = stateUpdates.get(collection.getName());
              if (updates != null) {
                writeStateUpdates(lastVersion, collection, updates);
              }
            }

          } catch (KeeperException.BadVersionException bve) {
            badVersionException.set(bve);
          } catch (InterruptedException | AlreadyClosedException e) {
            log.info("We have been closed or one of our resources has, bailing {}", e.getClass().getSimpleName() + ":" + e.getMessage());

          } catch (Exception e) {
            log.error("Failed processing update=" + collection, e);
          }
        }

      });

      removeCollections.forEach(c ->  removeCollection(c));

      if (badVersionException.get() != null) {
        throw badVersionException.get();
      }

      //log.info("Done with successful cluster write out");

    } finally {
      ourLock.unlock();
    }
    //    } finally {
    //      writeLock.unlock();
    //    }
    // MRM TODO: - harden against failures and exceptions

    //    if (log.isDebugEnabled()) {
    //      log.debug("writePendingUpdates() - end - New Cluster State is: {}", newClusterState);
    //    }
  }

  private void writeStateUpdates(AtomicInteger lastVersion, DocCollection collection, ZkNodeProps updates) throws KeeperException, InterruptedException {
    String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(collection.getName());
    if (log.isDebugEnabled()) log.debug("write state updates for collection {} {}", collection.getName(), updates);
    dirtyState.remove(collection.getName());
    try {
      reader.getZkClient().setData(stateUpdatesPath, Utils.toJSON(updates), -1, true, true);
    } catch (KeeperException.NoNodeException e) {
      if (log.isDebugEnabled()) log.debug("No node found for state.json", e);
      lastVersion.set(-1);
      trackVersions.remove(collection.getName());
      // likely deleted
    }
  }

  private void waitForStateWePublishedToComeBack() {
    cs.forEachCollection(collection -> {
      if (dirtyStructure.contains(collection.getName()) || dirtyState.contains(collection.getName())) {
        Integer v = null;
        try {
          //System.out.println("waiting to see state " + prevVersion);
          v = trackVersions.get(collection.getName());
          if (v == null) v = 0;
          if (v == 0) return;
          Integer version = v;
          try {
            log.info("wait to see last published version for collection {} {}", collection.getName(), v);
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
  }

  public ClusterState getClusterstate(boolean stateUpdate) {
    ourLock.lock();
    try {
      return ClusterState.getRefCS(cs.getCollectionsMap(), -2);
    } finally {
      ourLock.unlock();
    }
  }

  public void removeCollection(String collection) {
    log.info("Removing collection from zk state {}", collection);
    ourLock.lock();
    try {
      stateUpdates.remove(collection);
      cs.getCollectionStates().remove(collection);
      trackVersions.remove(collection);
      reader.getZkClient().deleteAsync(ZkStateReader.getCollectionSCNPath(collection), -1);
      reader.getZkClient().deleteAsync(ZkStateReader.getCollectionStateUpdatesPath(collection), -1);
    } catch (Exception e) {
      log.error("", e);
    } finally {
      ourLock.unlock();
    }
  }

  public long getHighestId() {
    return ID.incrementAndGet();
  }
}

