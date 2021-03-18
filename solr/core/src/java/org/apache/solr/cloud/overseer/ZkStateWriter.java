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

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
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

  private final Map<String, ConcurrentHashMap> stateUpdates = new ConcurrentHashMap<>();

  Map<Long,String> idToCollection = new ConcurrentHashMap<>(128, 0.75f, 16);

  private Map<String,DocAssign> assignMap = new ConcurrentHashMap<>(128, 0.75f, 16);

  private Map<String,ColState> collLocks = new ConcurrentHashMap<>(128, 0.75f, 16);

  private final Map<String,DocCollection> cs = new ConcurrentHashMap<>(128, 0.75f, 16);

  private static class ColState {
    ReentrantLock collLock = new ReentrantLock(true);
  }

  private AtomicLong ID = new AtomicLong();

  private Set<String> dirtyStructure = ConcurrentHashMap.newKeySet();
  private Set<String> dirtyState = ConcurrentHashMap.newKeySet();

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats, Overseer overseer) {
    this.overseer = overseer;
    this.reader = zkStateReader;
    this.stats = stats;

  }

  public void enqueueUpdate(DocCollection docCollection, Map<String,ConcurrentHashMap<String,ZkStateWriter.StateUpdate>> collStateUpdates, boolean stateUpdate) throws Exception {

    try {

      if (!stateUpdate) {
        if (log.isDebugEnabled()) log.debug("enqueue structure change docCollection={}", docCollection);

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
          dirtyStructure.add(docCollection.getName());
          idToCollection.putIfAbsent(docCollection.getId(), docCollection.getName());

          if (currentCollection != null) {
            docCollection.setZnodeVersion(currentCollection.getZNodeVersion());
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

        } finally {
          collState.collLock.unlock();
        }
      } else {
        log.trace("enqueue state change states={}", collStateUpdates);
        for (Map.Entry<String,ConcurrentHashMap<String,ZkStateWriter.StateUpdate>> entry : collStateUpdates.entrySet()) {

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

              continue;
              //throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Collection not found by id=" + collectionId);
            }

            ConcurrentHashMap updates = stateUpdates.get(collection);
            if (updates == null) {
              updates = new ConcurrentHashMap();
              stateUpdates.put(collection, updates);
            }

            DocCollection docColl = cs.get(collection);
            String csVersion;
            if (docColl != null) {
              csVersion = Integer.toString(docColl.getZNodeVersion());
              for (StateUpdate state : entry.getValue().values()) {
                if (state.sliceState != null) {
                  Slice slice = docColl.getSlice(state.sliceName);
                  if (slice != null) {
                    slice.setState(Slice.State.getState(state.sliceState));
                    slice.getProperties().put("state", state.sliceState);
                  }
                  dirtyStructure.add(collection);
                  continue;
                }

                Replica replica = docColl.getReplicaById(state.id);
                log.trace("found existing collection name={}, look for replica={} found={}", collection, state.id, replica);
                if (replica != null) {

                  log.trace("zkwriter publish state={} replica={}", state.state, replica.getName());
                  if (state.state.equals("l")) {

                    log.trace("set leader {}", replica);

                    Slice slice = docColl.getSlice(replica.getSlice());
                    Map<String,Replica> replicasMap = slice.getReplicasCopy();
                    Map properties = new HashMap(replica.getProperties());

                    properties.put("leader",  "true");
                    properties.put("state", Replica.State.ACTIVE);
                   // properties.put(replica.getInternalId(), "l");
                    for (Replica r : replicasMap.values()) {
                      if (replica.getName().equals(r.getName())) {
                        continue;
                      }
                      log.trace("process non leader {} {}", r, r.getProperty(ZkStateReader.LEADER_PROP));
                      if ("true".equals(r.getProperties().get(ZkStateReader.LEADER_PROP))) {
                        log.debug("remove leader prop {}", r);
                        Map<String,Object> props = new HashMap<>(r.getProperties());
                        props.remove(ZkStateReader.LEADER_PROP);
                        Replica newReplica = new Replica(r.getName(), props, collection, docColl.getId(), r.getSlice(), overseer.getZkStateReader());
                        replicasMap.put(r.getName(), newReplica);
                      }
                    }

                    Replica newReplica = new Replica(replica.getName(), properties, collection, docColl.getId(), replica.getSlice(), overseer.getZkStateReader());

                    replicasMap.put(replica.getName(), newReplica);

                    Slice newSlice = new Slice(slice.getName(), replicasMap, slice.getProperties(), collection, docColl.getId(), overseer.getZkStateReader());

                    Map<String,Slice> newSlices = docColl.getSlicesCopy();
                    newSlices.put(slice.getName(), newSlice);

                    log.trace("add new slice leader={} {} {}", newSlice.getLeader(), newSlice, docColl);

                    DocCollection newDocCollection = new DocCollection(collection, newSlices, docColl.getProperties(), docColl.getRouter(), docColl.getZNodeVersion(), docColl.getStateUpdates());
                    cs.put(collection, newDocCollection);
                    docColl = newDocCollection;
                    updates.put(replica.getInternalId(), "l");
                    dirtyState.add(collection);
                  } else {
                    String setState = Replica.State.shortStateToState(state.state).toString();
                    Replica.State s = Replica.State.getState(setState);

                    log.trace("set state {} {}", state, replica);

                    Slice slice = docColl.getSlice(replica.getSlice());
                    Map<String,Replica> replicasMap = slice.getReplicasCopy();
                    Map properties = new HashMap(replica.getProperties());

                    properties.put("state", s);
                    properties.remove(ZkStateReader.LEADER_PROP);

                    Replica newReplica = new Replica(replica.getName(), properties, collection, docColl.getId(), replica.getSlice(), overseer.getZkStateReader());

                    replicasMap.put(replica.getName(), newReplica);

                    Slice newSlice = new Slice(slice.getName(), replicasMap, slice.getProperties(), collection, docColl.getId(), overseer.getZkStateReader());

                    Map<String,Slice> newSlices = docColl.getSlicesCopy();
                    newSlices.put(slice.getName(), newSlice);

                    log.trace("add new slice leader={} {}", newSlice.getLeader(), newSlice);

                    DocCollection newDocCollection = new DocCollection(collection, newSlices, docColl.getProperties(), docColl.getRouter(), docColl.getZNodeVersion(), docColl.getStateUpdates());
                    cs.put(collection, newDocCollection);
                    docColl = newDocCollection;
                    updates.put(replica.getInternalId(), state.state);
                    dirtyState.add(collection);
                  }
                } else {
                  log.debug("Could not find replica id={} in {} {}", state.id, docColl.getReplicaByIds(), docColl.getReplicas());
                }
              }
            } else {
              for (StateUpdate state : entry.getValue().values()) {
                log.debug("Could not find existing collection name={}", collection);
                String setState = Replica.State.shortStateToState(state.state).toString();
                if (setState.equals("l")) {
                  updates.put(state.id.substring(state.id.indexOf('-') + 1), "l");
                  dirtyState.add(collection);
                } else {
                  Replica.State s = Replica.State.getState(setState);
                  updates.put(state.id.substring(state.id.indexOf('-') + 1), Replica.State.getShortState(s));
                  dirtyState.add(collection);
                }
              }
              log.debug("version for state updates 0");
              csVersion = "0";
            }

            if (dirtyState.contains(collection)) {
              updates.put("_cs_ver_", csVersion);
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
  }

  public Integer lastWrittenVersion(String collection) {
    DocCollection col = cs.get(collection);
    if (col == null) {
      return 0;
    }
    return col.getZNodeVersion();
  }

  /**
   * Writes all pending updates to ZooKeeper and returns the modified cluster state
   *
   */

  public Future writePendingUpdates(String collection) {
    return ParWork.getRootSharedExecutor().submit(() -> {
      do {
        try {
          write(collection);
          break;
        } catch (KeeperException.BadVersionException e) {

        } catch (Exception e) {
          log.error("write pending failed", e);
          break;
        }

      } while (!overseer.isClosed() && !overseer.getZkStateReader().getZkClient().isClosed());
    });
  }

  private void write(String coll) throws KeeperException.BadVersionException {

    if (log.isDebugEnabled()) {
      log.debug("writePendingUpdates {}", coll);
    }

    AtomicReference<KeeperException.BadVersionException> badVersionException = new AtomicReference();

    DocCollection collection = cs.get(coll);

    if (collection == null) {
      return;
    }

    log.debug("process collection {}", coll);
    ColState collState = collLocks.compute(Long.toString(collection.getId()), (s, reentrantLock) -> {
      if (reentrantLock == null) {
        ColState colState = new ColState();
        return colState;
      }
      return reentrantLock;
    });
    collState.collLock.lock();
    try {
      collection = cs.get(coll);

      if (collection == null) {
        return;
      }

      if (log.isTraceEnabled()) log.trace("check collection {} {} {}", collection, dirtyStructure, dirtyState);

      //  collState.throttle.minimumWaitBetweenActions();
      //  collState.throttle.markAttemptingAction();
      String name = collection.getName();
      String path = ZkStateReader.getCollectionPath(collection.getName());
      String pathSCN = ZkStateReader.getCollectionSCNPath(collection.getName());

      if (log.isTraceEnabled()) log.trace("process {}", collection);
      try {

        if (dirtyStructure.contains(name)) {
          if (log.isDebugEnabled()) log.debug("structure change in {}", collection.getName());

          byte[] data = Utils.toJSON(singletonMap(name, collection));

          if (log.isDebugEnabled()) log.debug("Write state.json prevVersion={} bytes={} col={}", collection.getZNodeVersion(), data.length, collection);

          Integer finalVersion = collection.getZNodeVersion();
          dirtyStructure.remove(collection.getName());
          if (reader == null) {
            log.error("read not initialized in zkstatewriter");
          }
          if (reader.getZkClient() == null) {
            log.error("zkclient not initialized in zkstatewriter");
          }

          Stat stat;
          try {

            stat = reader.getZkClient().setData(path, data, finalVersion, true, false);
            collection.setZnodeVersion(finalVersion + 1);

            if (log.isDebugEnabled()) log.debug("set new version {} {}", collection.getName(), stat.getVersion());
          } catch (KeeperException.NoNodeException e) {
            log.debug("No node found for state.json", e);

          } catch (KeeperException.BadVersionException bve) {
            stat = reader.getZkClient().exists(path, null, false, false);
            log.info("Tried to update state.json ({}) with bad version {} \n {}", collection, finalVersion, stat != null ? stat.getVersion() : "null");

            throw bve;
          }

          reader.getZkClient().setData(pathSCN, null, -1, true, false);

          ConcurrentHashMap updates = stateUpdates.get(collection.getName());
          if (updates != null) {
            updates.clear();
            writeStateUpdates(collection, updates);
          }

        } else if (dirtyState.contains(collection.getName())) {
          ConcurrentHashMap updates = stateUpdates.get(collection.getName());
          if (updates != null) {
            try {
              writeStateUpdates(collection, updates);
            } catch (Exception e) {
              log.error("exception writing state updates", e);
            }
          }
        }

      } catch (KeeperException.BadVersionException bve) {
        badVersionException.set(bve);
      } catch (InterruptedException | AlreadyClosedException e) {
        log.info("We have been closed or one of our resources has, bailing {}", e.getClass().getSimpleName() + ":" + e.getMessage());
        throw new AlreadyClosedException(e);

      } catch (Exception e) {
        log.error("Failed processing update=" + collection, e);
      }

      if (badVersionException.get() != null) {
        throw badVersionException.get();
      }

    } finally {
      collState.collLock.unlock();
    }
  }

  private void writeStateUpdates(DocCollection collection, ConcurrentHashMap updates) throws KeeperException, InterruptedException {
    if (updates.size() == 0) {
      return;
    }
    String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(collection.getName());
    log.trace("write state updates for collection {} ver={} {}", collection.getName(), updates.get("_cs_ver_"), updates);
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
    log.debug("Removing collection from zk state {}", collection);
    try {
      ColState collState = collLocks.compute(collection, (s, reentrantLock) -> {
        if (reentrantLock == null) {
          ColState colState = new ColState();
          return colState;
        }
        return reentrantLock;
      });
      collState.collLock.lock();
      try {
        Long id = null;
        for (Map.Entry<Long,String> entry : idToCollection.entrySet()) {
          if (entry.getValue().equals(collection)) {
            id = entry.getKey();
            break;
          }
        }
        if (id != null) {
          idToCollection.remove(id);
        }
        stateUpdates.remove(collection);
        DocCollection doc = cs.get(collection);

        if (doc != null) {
          List<Replica> replicas = doc.getReplicas();
          for (Replica replica : replicas) {
            overseer.getCoreContainer().getZkController().clearCachedState(replica.getName());
          }
        }

        cs.remove(collection);
        assignMap.remove(collection);
        dirtyStructure.remove(collection);
        dirtyState.remove(collection);

      } finally {
        collState.collLock.unlock();
      }
    } catch (Exception e) {
      log.error("Exception removing collection", e);

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
      log.debug("assign id={} for collection={} slice={}", id, collection, shard);
      return id;
    }

    int id = docAssign.replicaAssignCnt.incrementAndGet();
    log.debug("assign id={} for collection={} slice={}", id, collection, shard);
    return id;
  }

  public void init() {
    try {
      overseer.getCoreContainer().getZkController().clearStatePublisher();
      ClusterState readerState = reader.getClusterState();
      if (readerState != null) {
        reader.forciblyRefreshAllClusterStateSlow();
        cs.putAll(readerState.copy().getCollectionsMap());
      }

      long[] highId = new long[1];
      cs.values().forEach(collection -> {
        String collectionName = collection.getName();
        ColState collState = collLocks.compute(collectionName, (s, colState) -> {
          if (colState == null) {
            ColState cState = new ColState();
            return cState;
          }
          return colState;
        });
        collState.collLock.lock();
        try {

          if (collection.getId() > highId[0]) {
            highId[0] = collection.getId();
          }

          idToCollection.put(collection.getId(), collection.getName());

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
        } finally {
          collState.collLock.unlock();
        }
      });

      ID.set(highId[0]);

      if (log.isDebugEnabled()) log.debug("zkStateWriter starting with cs {}", cs);
    } catch (Exception e) {
      log.error("Exception in ZkStateWriter init", e);
    }
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

