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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.StatePublisher;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
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

  private final Map<String,Integer> trackVersions = new ConcurrentHashMap<>();

  private final Map<String, ZkNodeProps> stateUpdates = new ConcurrentHashMap<>();

  Map<String,DocCollection> failedUpdates = new ConcurrentHashMap<>();

  Map<Long,String> idToCollection = new ConcurrentHashMap<>();

  private Map<String,DocAssign> assignMap = new ConcurrentHashMap<>();

  private Map<String,ColState> collLocks = new ConcurrentHashMap<>();

  private final Map<String,DocCollection> cs = new ConcurrentHashMap<>();

  private static class ColState {
    ReentrantLock collLock = new ReentrantLock(true);
    ActionThrottle throttle = new ActionThrottle("ZkStateWriter", Integer.getInteger("solr.zkstatewriter.throttle", 50), new TimeSource.NanoTimeSource());
  }


  private AtomicLong ID = new AtomicLong();

  private Set<String> dirtyStructure = ConcurrentHashMap.newKeySet();
  private Set<String> dirtyState = ConcurrentHashMap.newKeySet();

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats, Overseer overseer) {
    this.overseer = overseer;
    this.reader = zkStateReader;
    this.stats = stats;

  }

  public Future enqueueUpdate(DocCollection docCollection, ZkNodeProps message, boolean stateUpdate) throws Exception {
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
          final String operation = message.getStr(StatePublisher.OPERATION);
          OverseerAction overseerAction = OverseerAction.get(operation);
          if (overseerAction == null) {
            throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
          }

          switch (overseerAction) {
            case STATE:
              if (log.isDebugEnabled()) log.debug("state cmd {}", message);
              message.getProperties().remove(StatePublisher.OPERATION);

              Map<String,List<StateUpdate>> collStateUpdates = new HashMap<>();
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
                  String collection = idToCollection.get(collectionId);
                  if (collection == null) {
                    log.info("collection for id={} is null", collectionId);
                    continue;
                  }

                  List<StateUpdate> updates = collStateUpdates.get(collection);
                  if (updates == null) {
                    updates = new ArrayList<>();
                    collStateUpdates.put(collection, updates);
                  }

                  StateUpdate update = new StateUpdate();
                  update.id = id;
                  update.state = stateString;
                  updates.add(update);

                }
              }

              for (Map.Entry<String,List<StateUpdate>> entry : collStateUpdates.entrySet()) {
                if (OverseerAction.DOWNNODE.equals(OverseerAction.get(entry.getKey()))) {
                  continue;
                } else if (OverseerAction.RECOVERYNODE.equals(OverseerAction.get(entry.getKey()))) {
                  continue;
                } else {

                  ColState collState = collLocks.compute(entry.getKey(), (s, reentrantLock) -> {
                    if (reentrantLock == null) {
                      ColState colState = new ColState();
                      return colState;
                    }
                    return reentrantLock;
                  });

                  collState.collLock.lock();
                  try {
                    //                if (collection == null) {
                    //                  Collection<ClusterState.CollectionRef> colls = cs.getCollectionStates().values();
                    //                  log.info("look for collection for id={} in {}}", id, cs.getCollectionStates().keySet());
                    //
                    //                  for (ClusterState.CollectionRef docCollectionRef : colls) {
                    //                    DocCollection docCollection = docCollectionRef.get();
                    //                    if (docCollection == null) {
                    //                      log.info("docCollection={}", docCollection);
                    //                    }
                    //                    if (docCollection.getId() == collectionId) {
                    //                      collection = docCollection.getName();
                    //                      break;
                    //                    }
                    //                  }
                    //                  if (collection == null) {
                    //                    continue;
                    //                  }
                    //                }
                    String collection = entry.getKey();
                    for (StateUpdate state : entry.getValue()) {

                      String setState = Replica.State.shortStateToState(state.state).toString();

                      if (trackVersions.get(collection) == null) {
                        // reader.forciblyRefreshClusterStateSlow(collection);
                        DocCollection latestColl = null; //reader.getClusterState().getCollectionOrNull(collection);

                        if (latestColl == null) {
                          //log.info("no node exists, using version 0");
                          trackVersions.remove(collection);
                        } else {
                          //  cs.getCollectionStates().put(latestColl.getName(), new ClusterState.CollectionRef(latestColl));
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

                      log.debug("version for state updates {}", ver.toString());

                      DocCollection docColl = cs.get(collection);
                      if (docColl != null) {
                        Replica replica = docColl.getReplicaById(state.id);
                        log.debug("found existing collection name={}, look for replica={} found={}", collection, state.id, replica);
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
                            Replica.State s = Replica.State.getState(setState);
                            Replica existingLeader = docColl.getSlice(replica).getLeader();
                            if (existingLeader != null && existingLeader.getName().equals(replica.getName())) {
                              docColl.getSlice(replica).setLeader(null);
                            }
                            updates.getProperties().put(replica.getId(), Replica.State.getShortState(s));
                            log.debug("set state {} {}", state, replica);
                            replica.setState(s);
                            dirtyState.add(collection);
                          }
                        } else {
                          log.debug("Could not find replica id={} in {} {}", state.id, docColl.getReplicaByIds(), docColl.getReplicas());
                        }
                      } else {
                        log.debug("Could not find existing collection name={}", collection);
                        if (setState.equals("leader")) {
                          updates.getProperties().put(state.id, "l");
                          dirtyState.add(collection);
                        } else {
                          Replica.State s = Replica.State.getState(setState);
                          updates.getProperties().put(state.id, Replica.State.getShortState(s));
                          dirtyState.add(collection);
                        }
                      }
                    }
                  } finally {
                    collState.collLock.unlock();
                  }
                }
              }

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

              ColState collState = collLocks.compute(collection, (s, reentrantLock) -> {
                if (reentrantLock == null) {
                  ColState colState = new ColState();
                  return colState;
                }
                return reentrantLock;
              });

              collState.collLock.lock();
              try {
                DocCollection docColl = cs.get(collection);
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
              } finally {
                collState.collLock.unlock();
              }
              break;

            default:
              throw new RuntimeException("unknown operation:" + operation + " contents:" + message.getProperties());
          }

        }




      } catch (Exception e) {
        log.error("Exception while queuing update", e);
        throw e;
      }
    });
  }

  private void nodeOperation(Map.Entry<String,Object> entry, String operation) {
    log.debug("zkwriter set node operation {} for {} cs={}}", operation, entry.getValue(), cs);


    //
    //    if (cs.getCollectionStates().size() == 0) {
    //        reader.forciblyRefreshAllClusterStateSlow();
    //        clusterState = reader.getClusterState().copy();
    //       log.debug("set operation try again {} for {} cs={}}", operation, entry.getValue(), clusterState);
    //       // cs = clusterState;
    //    }

    cs.values().forEach(docColl -> {
      ColState collState = collLocks.compute(docColl.getName(), (s, reentrantLock) -> {
        if (reentrantLock == null) {
          ColState colState = new ColState();
          return colState;
        }
        return reentrantLock;
      });

      collState.collLock.lock();
      try {
        if (trackVersions.get(docColl.getName()) == null) {
          // reader.forciblyRefreshClusterStateSlow(docColl.getName());

          //log.info("got version from zk {}", existsStat.getVersion());
          int version = docColl.getZNodeVersion();
          log.info("Updating local tracked version to {} for {}", version, docColl.getName());
          trackVersions.put(docColl.getName(), version);
          idToCollection.put(docColl.getId(), docColl.getName());

        }

        ZkNodeProps updates = stateUpdates.get(docColl.getName());
        if (updates == null) {
          updates = new ZkNodeProps();
          stateUpdates.put(docColl.getName(), updates);
        }
        Integer ver = trackVersions.get(docColl.getName());
        if (ver == null) {
          ver = 1;
        }
        updates.getProperties().put("_cs_ver_", ver.toString());
        //     dirtyState.add(docColl.getName());
        //   dirtyStructure.add(docColl.getName());
        List<Replica> replicas = docColl.getReplicas();
        log.debug("update replicas with node operation {} reps={}", operation, replicas.size());
        for (Replica replica : replicas) {
          if (!Replica.State.getShortState(replica.getState()).equals(operation) && replica.getNodeName().equals(entry.getValue())) {
            if (log.isDebugEnabled()) log.debug("set node operation {} for replica {}", operation, replica);
            // MRM TODO:
            //   Slice slice = docColl.getSlice(replica.getSlice());
            //          Replica leaderReplica = slice.getLeader();
            //          if (leaderReplica != null && replica == leaderReplica) {
            //            leaderReplica.getProperties().remove("leader");
            //            slice.setLeader(null);
            //          }
            replica.setState(Replica.State.shortStateToState(operation));
            updates.getProperties().put(replica.getId(), operation);
            dirtyState.add(docColl.getName());
          }
        }
      } finally {
        collState.collLock.unlock();
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
    if (dirtyStructure.contains(collection.getName()) || dirtyState.contains(collection.getName())) {
      log.info("process collection {}", collection);
      ColState collState = collLocks.compute(collection.getName(), (s, reentrantLock) -> {
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
          byte[] data = Utils.toJSON(singletonMap(name, collection));
          //  log.info("got data for {} {}", name, data.length);

          try {

            if (dirtyStructure.contains(collection.getName())) {
              if (log.isDebugEnabled()) log.debug("structure change in {}", collection.getName());

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
              trackVersions.put(collection.getName(), version + 1);

              reader.getZkClient().setData(pathSCN, null, -1, true, false);
              dirtyStructure.remove(collection.getName());

              ZkNodeProps updates = stateUpdates.get(collection.getName());
              if (updates != null) {
                updates.getProperties().clear();
              }
            }

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

          if (dirtyState.contains(collection.getName())) { //&& !dirtyStructure.contains(collection.getName())
            ZkNodeProps updates = stateUpdates.get(collection.getName());
            if (updates != null) {
              writeStateUpdates(collection, updates);
            }
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

  private static class StateUpdate {
    String id;
    String state;
  }

}

