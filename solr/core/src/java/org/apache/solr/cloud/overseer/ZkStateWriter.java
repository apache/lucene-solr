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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.api.collections.Assign;
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

  private final Map<String,Integer> trackVersions = new ConcurrentHashMap<>(128, 0.75f, 16);

  private final Map<String, ZkNodeProps> stateUpdates = new ConcurrentHashMap<>();

  Map<String,DocCollection> failedUpdates = new ConcurrentHashMap<>();

  Map<Long,String> idToCollection = new ConcurrentHashMap<>(128, 0.75f, 16);

  private Map<String,DocAssign> assignMap = new ConcurrentHashMap<>(128, 0.75f, 16);

  private Map<String,ColState> collLocks = new ConcurrentHashMap<>(128, 0.75f, 16);

  private final Map<String,DocCollection> cs = new ConcurrentHashMap<>(128, 0.75f, 16);

  private static class ColState {
    ReentrantLock collLock = new ReentrantLock(true);
    ActionThrottle throttle = new ActionThrottle("ZkStateWriter", Integer.getInteger("solr.zkstatewriter.throttle", 0), new TimeSource.NanoTimeSource());
  }


  private AtomicLong ID = new AtomicLong();

  private Set<String> dirtyStructure = ConcurrentHashMap.newKeySet();
  private Set<String> dirtyState = ConcurrentHashMap.newKeySet();

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats, Overseer overseer) {
    this.overseer = overseer;
    this.reader = zkStateReader;
    this.stats = stats;

  }

  public Future enqueueUpdate(DocCollection docCollection, Map<String,List<ZkStateWriter.StateUpdate>> collStateUpdates, boolean stateUpdate) throws Exception {
    return ParWork.getRootSharedExecutor().submit(() -> {

      try {
        if (log.isDebugEnabled()) log.debug("enqueue update stateUpdate={} docCollection={} cs={}", stateUpdate, docCollection, cs);
        if (!stateUpdate) {

          String collectionName = docCollection.getName();

          ColState collState = collLocks.compute(collectionName, (s, colState) -> {
            if (colState == null) {
              ColState cState = new ColState();
              return cState;
            }
            return colState;
          });
          collState.collLock.lock();

          try {

            DocCollection currentCollection = cs.get(docCollection.getName());
            log.debug("zkwriter collection={}", docCollection);
            log.debug("zkwriter currentCollection={}", currentCollection);

            idToCollection.putIfAbsent(docCollection.getId(), docCollection.getName());

            //          if (currentCollection != null) {
            //            if (currentCollection.getId() != collection.getId()) {
            //              removeCollection(collection.getName());
            //            }
            //          }

            if (currentCollection != null) {

              currentCollection.getProperties().keySet().retainAll(docCollection.getProperties().keySet());
              List<String> removeSlices = new ArrayList();
              for (Slice slice : docCollection) {
                Slice currentSlice = currentCollection.getSlice(slice.getName());
                if (currentSlice != null) {
                  if (currentSlice.get("remove") != null || slice.getProperties().get("remove") != null) {
                    removeSlices.add(slice.getName());
                  } else {
                    currentCollection.getSlicesMap().put(slice.getName(), slice.update(currentSlice));
                  }
                } else {
                  if (slice.getProperties().get("remove") != null) {
                    continue;
                  }
                  Set<String> remove = new HashSet<>();

                  for (Replica replica : slice) {

                    if (replica.get("remove") != null) {
                      remove.add(replica.getName());
                    }
                  }
                  for (String removeReplica : remove) {
                    slice.getReplicasMap().remove(removeReplica);
                  }
                  currentCollection.getSlicesMap().put(slice.getName(), slice);
                }
              }
              for (String removeSlice : removeSlices) {
                currentCollection.getSlicesMap().remove(removeSlice);
              }
              cs.put(currentCollection.getName(), currentCollection);

            } else {
              docCollection.getProperties().remove("pullReplicas");
              docCollection.getProperties().remove("replicationFactor");
              docCollection.getProperties().remove("maxShardsPerNode");
              docCollection.getProperties().remove("nrtReplicas");
              docCollection.getProperties().remove("tlogReplicas");
              List<String> removeSlices = new ArrayList();
              for (Slice slice : docCollection) {
                Slice currentSlice = docCollection.getSlice(slice.getName());
                if (currentSlice != null) {
                  if (slice.getProperties().get("remove") != null) {
                    removeSlices.add(slice.getName());
                  }
                }
              }
              for (String removeSlice : removeSlices) {
                docCollection.getSlicesMap().remove(removeSlice);
              }

              cs.put(docCollection.getName(), docCollection);
            }

            dirtyStructure.add(collectionName);

          } finally {
            collState.collLock.unlock();
          }
        } else {

          for (Map.Entry<String,List<StateUpdate>> entry : collStateUpdates.entrySet()) {

            ColState collState = collLocks.compute(entry.getKey(), (s, reentrantLock) -> {
              if (reentrantLock == null) {
                ColState colState = new ColState();
                return colState;
              }
              return reentrantLock;
            });

            collState.collLock.lock();
            try {
              String collectionId = entry.getKey();
              String collection = idToCollection.get(Long.parseLong(collectionId));
              if (collection == null) {
                log.error("Collection not found by id={} collections={}", collectionId, idToCollection);
                throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Collection not found by id=" + collectionId);
              }

              ZkNodeProps updates = stateUpdates.get(collection);
              if (updates == null) {
                updates = new ZkNodeProps();
                stateUpdates.put(collection, updates);
              }

              for (StateUpdate state : entry.getValue()) {

                Integer ver = trackVersions.get(collection);
                if (ver == null) {
                  ver = 0;
                }
                updates.getProperties().put("_cs_ver_", ver.toString());

                log.debug("version for state updates {}", ver.toString());

                DocCollection docColl = cs.get(collection);
                if (docColl != null) {

                  if (state.sliceState != null) {
                    Slice slice = docColl.getSlice(state.sliceName);
                    if (slice != null) {
                      slice.setState(Slice.State.getState(state.sliceState));
                    }
                    dirtyStructure.add(docColl.getName());
                    continue;
                  }

                  Replica replica = docColl.getReplicaById(state.id);
                  log.debug("found existing collection name={}, look for replica={} found={}", collection, state.id, replica);
                  if (replica != null) {
                    String setState = Replica.State.shortStateToState(state.state).toString();
                    //                        if (blockedNodes.contains(replica.getNodeName())) {
                    //                          continue;
                    //                        }
                    log.debug("zkwriter publish state={} replica={}", state.state, replica.getName());
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
                      updates.getProperties().put(replica.getInternalId(), "l");
                      dirtyState.add(collection);
                    } else {
                      Replica.State s = Replica.State.getState(setState);
                      Replica existingLeader = docColl.getSlice(replica).getLeader();
                      if (existingLeader != null && existingLeader.getName().equals(replica.getName())) {
                        docColl.getSlice(replica).setLeader(null);
                      }
                      updates.getProperties().put(replica.getInternalId(), Replica.State.getShortState(s));
                      log.debug("set state {} {}", state, replica);
                      replica.setState(s);
                      dirtyState.add(collection);
                    }
                  } else {
                    log.debug("Could not find replica id={} in {} {}", state.id, docColl.getReplicaByIds(), docColl.getReplicas());
                  }
                } else {
                  log.debug("Could not find existing collection name={}", collection);
                  String setState = Replica.State.shortStateToState(state.state).toString();
                  if (setState.equals("leader")) {
                    updates.getProperties().put(state.id.substring(state.id.indexOf('-') + 1), "l");
                    dirtyState.add(collection);
                  } else {
                    Replica.State s = Replica.State.getState(setState);
                    updates.getProperties().put(state.id.substring(state.id.indexOf('-') + 1), Replica.State.getShortState(s));
                    dirtyState.add(collection);
                  }
                }
              }

              String coll = entry.getKey();
              dirtyState.add(coll);
              Integer ver = trackVersions.get(coll);
              if (ver == null) {
                ver = 0;
              }
              updates.getProperties().put("_cs_ver_", ver.toString());
              for (StateUpdate theUpdate : entry.getValue()) {
                updates.getProperties().put(theUpdate.id.substring(theUpdate.id.indexOf("-") + 1), theUpdate.state);
              }

            } finally {
              collState.collLock.unlock();
            }
          }
        }
        
      } catch (Exception e) {
        log.error("Exception while queuing update", e);
        throw e;
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

  // if additional updates too large, publish structure change
  public void writePendingUpdates(String collection) {

    do {
      try {
        write(collection);
        break;
      } catch (KeeperException.BadVersionException e) {

      } catch (Exception e) {
        log.error("write pending failed", e);
        break;
      }

    } while (!overseer.isClosed());

  }

  private void write(String coll) throws KeeperException.BadVersionException {

    if (log.isDebugEnabled()) {
      log.debug("writePendingUpdates {}", cs);
    }

    AtomicInteger lastVersion = new AtomicInteger();
    AtomicReference<KeeperException.BadVersionException> badVersionException = new AtomicReference();

    DocCollection collection = cs.get(coll);

    if (collection == null) {
      return;
    }

    if (log.isDebugEnabled()) log.debug("check collection {} {} {}", collection, dirtyStructure, dirtyState);
    Integer version = null;
    if (dirtyStructure.contains(collection.getName())) {
      log.info("process collection {}", collection);
      ColState collState = collLocks.compute(Long.toString(collection.getId()), (s, reentrantLock) -> {
        if (reentrantLock == null) {
          ColState colState = new ColState();
          return colState;
        }
        return reentrantLock;
      });

      collState.collLock.lock();
      try {
        collState.throttle.minimumWaitBetweenActions();
        collState.throttle.markAttemptingAction();
        String name = collection.getName();
        String path = ZkStateReader.getCollectionPath(collection.getName());
        String pathSCN = ZkStateReader.getCollectionSCNPath(collection.getName());
        // log.info("process collection {} path {}", collection.getName(), path);
        Stat existsStat = null;
        if (log.isTraceEnabled()) log.trace("process {}", collection);
        try {
          // log.info("get data for {}", name);

          //  log.info("got data for {} {}", name, data.length);

          try {

            if (log.isDebugEnabled()) log.debug("structure change in {}", collection.getName());
            byte[] data = Utils.toJSON(singletonMap(name, collection));
            Integer v = trackVersions.get(collection.getName());

            if (v != null) {
              //log.info("got version from cache {}", v);
              version = v;
            } else {
              version = 0;
            }
            lastVersion.set(version);
            if (log.isDebugEnabled()) log.debug("Write state.json prevVersion={} bytes={} col={}", version, data.length, collection);

            reader.getZkClient().setData(path, data, version, true, false);
            if (log.isDebugEnabled()) log.debug("set new version {} {}", collection.getName(), version + 1);

            reader.getZkClient().setData(pathSCN, null, -1, true, false);

            dirtyStructure.remove(collection.getName());

            ZkNodeProps updates = stateUpdates.get(collection.getName());
            if (updates != null) {
              updates.getProperties().clear();
              //(collection, updates);
              //dirtyState.remove(collection.getName());
            }

            trackVersions.put(collection.getName(), version + 1);

          } catch (KeeperException.NoNodeException e) {
            if (log.isDebugEnabled()) log.debug("No node found for state.json", e);

            lastVersion.set(-1);
            trackVersions.remove(collection.getName());
            stateUpdates.remove(collection.getName());
            cs.remove(collection);
            // likely deleted

          } catch (KeeperException.BadVersionException bve) {
            log.info("Tried to update state.json ({}) with bad version", collection);
            //lastFailedException.set(bve);
            //failedUpdates.put(collection.getName(), collection);
            // Stat estate = reader.getZkClient().exists(path, null);
            trackVersions.remove(collection.getName());
            Stat stat = reader.getZkClient().exists(path, null, false, false);
            log.info("Tried to update state.json ({}) with bad version {} \n {}", collection, version, stat != null ? stat.getVersion() : "null");

            if (!overseer.isClosed() && stat != null) {
              trackVersions.put(collection.getName(), stat.getVersion());
            }
            throw bve;
          }

        } catch (KeeperException.BadVersionException bve) {
          badVersionException.set(bve);
        } catch (InterruptedException | AlreadyClosedException e) {
          log.info("We have been closed or one of our resources has, bailing {}", e.getClass().getSimpleName() + ":" + e.getMessage());

        } catch (Exception e) {
          log.error("Failed processing update=" + collection, e);
        }
      } finally {
        collState.collLock.unlock();
      }
    }

    if (dirtyState.contains(collection.getName())) { //&& !dirtyStructure.contains(collection.getName())
      ZkNodeProps updates = stateUpdates.get(collection.getName());
      if (updates != null) {
        try {
          writeStateUpdates(collection, updates);
        } catch (Exception e) {
          log.error("exception writing state updates", e);
        }
      }
    }

    //removeCollections.forEach(c ->  removeCollection(c));

    if (badVersionException.get() != null) {
      throw badVersionException.get();
    }

    //log.info("Done with successful cluster write out");

    //    } finally {
    //      writeLock.unlock();
    //    }
    // MRM TODO: - harden against failures and exceptions

    //    if (log.isDebugEnabled()) {
    //      log.debug("writePendingUpdates() - end - New Cluster State is: {}", newClusterState);
    //    }
  }

  private void writeStateUpdates(DocCollection collection, ZkNodeProps updates) throws KeeperException, InterruptedException {
    String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(collection.getName());
    if (log.isDebugEnabled()) log.debug("write state updates for collection {} {}", collection.getName(), updates);
    try {
      reader.getZkClient().setData(stateUpdatesPath, Utils.toJSON(updates), -1, true, false);
    } catch (KeeperException.NoNodeException e) {
      if (log.isDebugEnabled()) log.debug("No node found for state.json", e);
      // likely deleted
    }
    dirtyState.remove(collection.getName());
  }

  public ClusterState getClusterstate() {
    return ClusterState.getRefCS(cs, -2);
  }

  public Map<String,DocCollection>  getCS() {
    return cs;
  }

  public Set<String> getDirtyStateCollections() {
    return dirtyState;
  }


  public void removeCollection(String collection) {
    log.info("Removing collection from zk state {}", collection);
    ColState collState = collLocks.compute(collection, (s, reentrantLock) -> {
      if (reentrantLock == null) {
        ColState colState = new ColState();
        return colState;
      }
      return reentrantLock;
    });
    collState.collLock.lock();
    try {
      stateUpdates.remove(collection);
      cs.remove(collection);
      assignMap.remove(collection);
      trackVersions.remove(collection);
      dirtyStructure.remove(collection);
      dirtyState.remove(collection);
      ZkNodeProps message = new ZkNodeProps("name", collection);
      cs.remove(collection);
      Long id = null;
      for (Map.Entry<Long, String> entry : idToCollection.entrySet()) {
        if (entry.getValue().equals(collection)) {
          id = entry.getKey();
          break;
        }
      }
      if (id != null) {
        idToCollection.remove(id);
      }
    } catch (Exception e) {
      log.error("", e);
    } finally {
      collState.collLock.unlock();
    }
  }

  public long getHighestId() {
    return ID.incrementAndGet();
  }

  public synchronized int getReplicaAssignCnt(String collection, String shard) {
    DocAssign docAssign = assignMap.get(collection);
    if (docAssign == null) {
      docAssign = new DocAssign();
      docAssign.name = collection;
      assignMap.put(docAssign.name, docAssign);


      int id = docAssign.replicaAssignCnt.incrementAndGet();
      log.info("assign id={} for collection={} slice={}", id, collection, shard);
      return id;
    }

    int id = docAssign.replicaAssignCnt.incrementAndGet();
    log.info("assign id={} for collection={} slice={}", id, collection, shard);
    return id;
  }

  public void init() {
    reader.forciblyRefreshAllClusterStateSlow();
    ClusterState readerState = reader.getClusterState();
    if (readerState != null) {
      cs.putAll(readerState.copy().getCollectionsMap());
    }

    long[] highId = new long[1];
    cs.values().forEach(collection -> {
      if (collection.getId() > highId[0]) {
        highId[0] = collection.getId();
      }

      idToCollection.put(collection.getId(), collection.getName());

      trackVersions.put(collection.getName(), collection.getZNodeVersion());

      DocAssign docAssign = new DocAssign();
      docAssign.name = collection.getName();
      assignMap.put(docAssign.name, docAssign);
      int max = 1;
      Collection<Slice> slices = collection.getSlices();
      for (Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();

        for (Replica replica : replicas) {
          Matcher matcher = Assign.pattern.matcher(replica.getName());
          if (matcher.matches()) {
            int val = Integer.parseInt(matcher.group(1));
            max = Math.max(max, val);
          }
        }
      }
      docAssign.replicaAssignCnt.set(max);
    });

    ID.set(highId[0]);

    if (log.isDebugEnabled()) log.debug("zkStateWriter starting with cs {}", cs);
  }

  private static class DocAssign {
    String name;
    private AtomicInteger replicaAssignCnt = new AtomicInteger();
  }

  public static class StateUpdate {
    public String id;
    public String state;
    public String sliceState;
    public String sliceName;
  }

}

