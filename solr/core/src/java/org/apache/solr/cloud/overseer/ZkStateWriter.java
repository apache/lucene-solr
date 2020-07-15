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

import static java.util.Collections.singletonMap;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.cloud.Stats;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;


// nocommit - need to allow for a configurable flush interval again
public class ZkStateWriter {
  // pleeeease leeeeeeeeeeets not - THERE HAS TO BE  BETTER WAY
  // private static final long MAX_FLUSH_INTERVAL = TimeUnit.NANOSECONDS.convert(Overseer.STATE_UPDATE_DELAY, TimeUnit.MILLISECONDS);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Represents a no-op {@link ZkWriteCommand} which will result in no modification to cluster state
   */
  public static ZkWriteCommand NO_OP = ZkWriteCommand.noop();

  //protected final ZkStateReader reader;
  protected volatile Stats stats;

  protected final Map<String, DocCollection> updates = new HashMap<>();
  private int numUpdates = 0;

  // / protected boolean isClusterStateModified = false;
  protected long lastUpdatedTime = 0;


  private final ZkStateReader reader;

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats) {
    assert zkStateReader != null;

    this.reader = zkStateReader;
    this.stats = stats;
  }

  /**
   * Applies the given {@link ZkWriteCommand} on the <code>prevState</code>. The modified
   * {@link ClusterState} is returned and it is expected that the caller will use the returned
   * cluster state for the subsequent invocation of this method.
   * <p>
   * The modified state may be buffered or flushed to ZooKeeper depending on the internal buffering
   * logic of this class. The {@link #hasPendingUpdates()} method may be used to determine if the
   * last enqueue operation resulted in buffered state. The method {@link #writePendingUpdates(ClusterState)} can
   * be used to force an immediate flush of pending cluster state changes.
   *
   * @param prevState the cluster state information on which the given <code>cmd</code> is applied
   * @param cmds       the list of {@link ZkWriteCommand} which specifies the change to be applied to cluster state in atomic
   * @param callback  a {@link org.apache.solr.cloud.overseer.ZkStateWriter.ZkWriteCallback} object to be used
   *                  for any callbacks
   * @return modified cluster state created after applying <code>cmd</code> to <code>prevState</code>. If
   * <code>cmd</code> is a no-op ({@link #NO_OP}) then the <code>prevState</code> is returned unmodified.
   * @throws IllegalStateException if the current instance is no longer usable. The current instance must be
   *                               discarded.
   * @throws Exception             on an error in ZK operations or callback. If a flush to ZooKeeper results
   *                               in a {@link org.apache.zookeeper.KeeperException.BadVersionException} this instance becomes unusable and
   *                               must be discarded
   */
  public ClusterState enqueueUpdate(ClusterState prevState, List<ZkWriteCommand> cmds, ZkWriteCallback callback) throws IllegalStateException, Exception {
    if (log.isDebugEnabled()) {
      log.debug("enqueueUpdate(ClusterState prevState={}, List<ZkWriteCommand> cmds={}, ZkWriteCallback callback={}) - start", prevState, cmds, callback);
    }

// nocommit - all this
    for (ZkWriteCommand cmd : cmds) {
      updates.put(cmd.name, cmd.collection);
      numUpdates++;
    }

    // if (maybeFlushAfter()) {
    ClusterState state;
    while (true) {
      try {
        state = writePendingUpdates(reader.getClusterState());
      } catch (KeeperException.BadVersionException e) {
        prevState = reader.getClusterState();
        stats = new Stats();
        numUpdates = 0;
        lastUpdatedTime = 0;
        continue;
//        log.info("BadVersion");
//        throw new AlreadyClosedException();
      }
      break;
    }

    if (callback != null) {
      callback.onWrite();
    }

    if (log.isDebugEnabled()) {
      log.debug("enqueueUpdate(ClusterState, List<ZkWriteCommand>, ZkWriteCallback) - end");
    }
    return state;
    // }

//    if (log.isDebugEnabled()) {
//      log.debug("enqueueUpdate(ClusterState, List<ZkWriteCommand>, ZkWriteCallback) - end");
//    }
//    return clusterState;
  }

  public boolean hasPendingUpdates() {
    if (log.isDebugEnabled()) {
      log.debug("hasPendingUpdates() - start");
    }

    boolean returnboolean = numUpdates != 0;
    if (log.isDebugEnabled()) {
      log.debug("hasPendingUpdates() - end");
    }
    return returnboolean;
  }

  /**
   * Writes all pending updates to ZooKeeper and returns the modified cluster state
   *
   * @return the modified cluster state
   * @throws IllegalStateException if the current instance is no longer usable and must be discarded
   * @throws KeeperException       if any ZooKeeper operation results in an error
   * @throws InterruptedException  if the current thread is interrupted
   */
  public ClusterState writePendingUpdates(ClusterState prevState) throws IllegalStateException, KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("writePendingUpdates() - start updates.size={}", updates.size());
    }
    assert prevState != null;
    Timer.Context timerContext = stats.time("update_state");
    boolean success = false;
    ClusterState newClusterState = null;
    int prevVersion = -1;
    try {
      // if (!updates.isEmpty()) {
      for (Map.Entry<String,DocCollection> entry : updates.entrySet()) {
        String name = entry.getKey();
        String path = ZkStateReader.getCollectionPath(name);
        DocCollection c = entry.getValue();
        Stat stat = new Stat();

        try {

          if (c == null) {
            // let's clean up the state.json of this collection only, the rest should be clean by delete collection cmd
            if (log.isDebugEnabled()) {
              log.debug("going to delete state.json {}", path);
            }
            reader.getZkClient().clean(path);
          } else if (prevState.getCollectionsMap().containsKey(name)) {
            if (log.isDebugEnabled()) {
              log.debug("writePendingUpdates() - going to update_collection {} version: {}", path,
                      prevState.getZNodeVersion());
            }

           // assert c.getStateFormat() > 1;
            // stat = reader.getZkClient().getCurator().checkExists().forPath(path);

            prevVersion = prevState.getCollection(c.getName()).getZNodeVersion();
            Map<String,Slice> existingSlices = prevState.getCollection(c.getName()).getSlicesMap();

            Map<String,Slice> newSliceMap = new HashMap<>(existingSlices.size() + 1);

            if (log.isDebugEnabled()) {
              log.debug("Existing slices {}", existingSlices);
            }

            existingSlices.forEach((sliceId, slice) -> {
              newSliceMap.put(sliceId, slice);
            });

            if (log.isDebugEnabled()) {
              log.debug("Add collection {}", c);
            }

            prevState.getCollection(c.getName()).getSlicesMap().forEach((sliceId, slice) -> {
              Collection<Replica> replicas = slice.getReplicas();

              Map<String,Replica> newReplicas = new HashMap<>();

              Map<String,Object> newProps = new HashMap<>();

              newProps.putAll(slice.getProperties());

              Slice existingSlice = newSliceMap.get(sliceId);
              if (existingSlice != null) {
                existingSlice.getReplicas().forEach((replica) -> {
                  newReplicas.put(replica.getName(), replica);
                });
              }

              replicas.forEach((replica) -> newReplicas.put(replica.getName(), replica));

              c.getSlice(sliceId).getReplicas().forEach((replica) -> {
                newReplicas.put(replica.getName(), replica);
              });

              Slice newSlice = new Slice(sliceId, newReplicas, newProps, c.getName());
              newSliceMap.put(sliceId, newSlice);

            });

            if (log.isDebugEnabled()) {
              log.debug("New Slice Map after combining {}", newSliceMap);
            }

            DocCollection newCollection = new DocCollection(name, newSliceMap, c.getProperties(), c.getRouter(),
                    prevState.getZNodeVersion(), path);
            LinkedHashMap collStates = new LinkedHashMap<>(prevState.getCollectionsMap());
            collStates.put(name, new ClusterState.CollectionRef(newCollection));
            newClusterState = new ClusterState(prevState.getLiveNodes(), collStates, prevVersion);

            byte[] data = Utils.toJSON(singletonMap(c.getName(), newCollection));

            if (log.isDebugEnabled()) {
              log.debug("Write state.json bytes={} cs={}", data.length, newClusterState);
            }
           // stat = reader.getZkClient().getCurator().setData().withVersion(prevVersion).forPath(path, data);
            stat =  reader.getZkClient().setData(path, data, prevVersion, true);
          } else {
            if (log.isDebugEnabled()) {
              log.debug("writePendingUpdates() - going to create_collection {}", path);
            }
         //   assert c.getStateFormat() > 1;
            DocCollection newCollection = new DocCollection(name, c.getSlicesMap(), c.getProperties(), c.getRouter(),
                    0, path);

            LinkedHashMap collStates = new LinkedHashMap<>(prevState.getCollectionStates());
            collStates.put(name, new ClusterState.CollectionRef(newCollection));
            newClusterState = new ClusterState(prevState.getLiveNodes(), collStates, prevState.getZNodeVersion());

            byte[] data = Utils.toJSON(singletonMap(c.getName(), newCollection));
            // reader.getZkClient().getCurator().create().storingStatIn(stat).forPath(path, data); // nocommit look at
            // async updates
            if (log.isDebugEnabled()) {
              log.debug("Write state.json bytes={} cs={}", data.length, newClusterState);
            }
            try {
              prevVersion = 0;
              reader.getZkClient().create(path, data, CreateMode.PERSISTENT, true);
            } catch(KeeperException.NodeExistsException e) {
              stat =  reader.getZkClient().setData(path, data, -1, true);
            }
          }

        } catch (Exception e) {
          if (e instanceof KeeperException.BadVersionException) {
            // nocommit invalidState = true;
            if (log.isDebugEnabled()) log.debug("Tried to update the cluster state using version={} but we where rejected, currently at {}", prevVersion, ((KeeperException.BadVersionException) e).getMessage(), e);
            throw (KeeperException.BadVersionException) e;
          }
          ParWork.propegateInterrupt(e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed processing update=" + entry, e) {
          };
        }
        // }

        updates.clear();
        // numUpdates = 0;
        try {
          reader.waitForState(c.getName(), 5, TimeUnit.SECONDS,
                  (l, col) -> {
                    if (col != null && col.getZNodeVersion() > prevState.getZNodeVersion()) {
                      if (log.isDebugEnabled()) log.debug("Waited for ver: {}", col.getZNodeVersion());
                      return true;
                    }
                    return false;
                  });
        } catch (TimeoutException e) {
          throw new RuntimeException(e);
        }

      }

      // assert newClusterState.getZNodeVersion() >= 0;
      // byte[] data = Utils.toJSON(newClusterState);
      // Stat stat = reader.getZkClient().setData(ZkStateReader.CLUSTER_STATE, data, newClusterState.getZNodeVersion(),
      // true);
      //
      //
      //

      lastUpdatedTime = System.nanoTime();
      success = true;
    } catch (KeeperException.BadVersionException bve) {
      // this is a tragic error, we must disallow usage of this instance
      //  log.error("Tried to update the cluster state using version={} but we where rejected as the version is {}", newClusterState.getZNodeVersion(), bve.getMessage(), bve);
      // nocommit invalidState = true;
      throw bve;
    } finally {
      timerContext.stop();
      if (success) {
        stats.success("update_state");
      } else {
        stats.error("update_state");
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("writePendingUpdates() - end - New Cluster State is: {}", newClusterState);
    }
    if (newClusterState == null) {
      newClusterState = prevState;
    }
    assert newClusterState != null;
    return newClusterState;
  }

  public interface ZkWriteCallback {
    /**
     * Called by ZkStateWriter if state is flushed to ZK
     */
    void onWrite() throws Exception;
  }
}

