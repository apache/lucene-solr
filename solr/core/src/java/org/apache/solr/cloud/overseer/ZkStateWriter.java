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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;

/**
 * ZkStateWriter is responsible for writing updates to the cluster state stored in ZooKeeper for
 * both stateFormat=1 collection (stored in shared /clusterstate.json in ZK) and stateFormat=2 collections
 * each of which get their own individual state.json in ZK.
 *
 * Updates to the cluster state are specified using the
 * {@link #enqueueUpdate(ClusterState, List, ZkWriteCallback)} method. The class buffers updates
 * to reduce the number of writes to ZK. The buffered updates are flushed during <code>enqueueUpdate</code>
 * automatically if necessary. The {@link #writePendingUpdates()} can be used to force flush any pending updates.
 *
 * If either {@link #enqueueUpdate(ClusterState, List, ZkWriteCallback)} or {@link #writePendingUpdates()}
 * throws a {@link org.apache.zookeeper.KeeperException.BadVersionException} then the internal buffered state of the
 * class is suspect and the current instance of the class should be discarded and a new instance should be created
 * and used for any future updates.
 */
public class ZkStateWriter {
  private static final long MAX_FLUSH_INTERVAL = TimeUnit.NANOSECONDS.convert(Overseer.STATE_UPDATE_DELAY, TimeUnit.MILLISECONDS);
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Represents a no-op {@link ZkWriteCommand} which will result in no modification to cluster state
   */
  public static ZkWriteCommand NO_OP = ZkWriteCommand.noop();

  protected final ZkStateReader reader;
  protected final Stats stats;

  protected Map<String, ZkWriteCommand> updates = new HashMap<>();
  private int numUpdates = 0;
  protected ClusterState clusterState = null;
  protected boolean isClusterStateModified = false;
  protected long lastUpdatedTime = 0;

  /**
   * Set to true if we ever get a BadVersionException so that we can disallow future operations
   * with this instance
   */
  protected boolean invalidState = false;

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats) {
    assert zkStateReader != null;

    this.reader = zkStateReader;
    this.stats = stats;
    this.clusterState = zkStateReader.getClusterState();
  }

  /**
   * Applies the given {@link ZkWriteCommand} on the <code>prevState</code>. The modified
   * {@link ClusterState} is returned and it is expected that the caller will use the returned
   * cluster state for the subsequent invocation of this method.
   * <p>
   * The modified state may be buffered or flushed to ZooKeeper depending on the internal buffering
   * logic of this class. The {@link #hasPendingUpdates()} method may be used to determine if the
   * last enqueue operation resulted in buffered state. The method {@link #writePendingUpdates()} can
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
    if (invalidState) {
      throw new IllegalStateException("ZkStateWriter has seen a tragic error, this instance can no longer be used");
    }
    if (cmds.isEmpty()) return prevState;
    if (isNoOps(cmds)) return prevState;

    boolean forceFlush = false;
    if (cmds.size() == 1) {
      //most messages result in only one command. let's deal with it right away
      ZkWriteCommand cmd = cmds.get(0);
      if (cmd.collection != null && cmd.collection.isPerReplicaState()) {
        //we do not wish to batch any updates for collections with per-replica state because
        // these changes go to individual ZK nodes and there is zero advantage to batching
        //now check if there are any updates for the same collection already present
        if (updates.containsKey(cmd.name)) {
          //this should not happen
          // but let's get those updates out anyway
          writeUpdate(updates.remove(cmd.name));
        }
        //now let's write the current message
        try {
          return writeUpdate(cmd);
        } finally {
          if (callback !=null) callback.onWrite();
        }
      }
    } else {
      //there are more than one commands created as a result of this message
      for (ZkWriteCommand cmd : cmds) {
        if (cmd.collection != null && cmd.collection.isPerReplicaState()) {
          // we don't try to optimize for this case. let's flush out all after this
          forceFlush = true;
          break;
        }
      }
    }


    for (ZkWriteCommand cmd : cmds) {
      if (cmd == NO_OP) continue;
      if (!isClusterStateModified && clusterStateGetModifiedWith(cmd, prevState)) {
        isClusterStateModified = true;
      }
      prevState = prevState.copyWith(cmd.name, cmd.collection);
      if (cmd.collection == null || cmd.collection.getStateFormat() != 1) {
        updates.put(cmd.name, cmd);
        numUpdates++;
      }
    }
    clusterState = prevState;

    if (forceFlush || maybeFlushAfter()) {
      ClusterState state = writePendingUpdates();
      if (callback != null) {
        callback.onWrite();
      }
      return state;
    }

    return clusterState;
  }

  private boolean isNoOps(List<ZkWriteCommand> cmds) {
    for (ZkWriteCommand cmd : cmds) {
      if (cmd != NO_OP) return false;
    }
    return true;
  }

  /**
   * Check whether {@value ZkStateReader#CLUSTER_STATE} (for stateFormat = 1) get changed given command
   */
  private boolean clusterStateGetModifiedWith(ZkWriteCommand command, ClusterState state) {
    DocCollection previousCollection = state.getCollectionOrNull(command.name);
    boolean wasPreviouslyStateFormat1 = previousCollection != null && previousCollection.getStateFormat() == 1;
    boolean isCurrentlyStateFormat1 = command.collection != null && command.collection.getStateFormat() == 1;
    return wasPreviouslyStateFormat1 || isCurrentlyStateFormat1;
  }
  /**
   * Logic to decide a flush after processing a list of ZkWriteCommand
   *
   * @return true if a flush to ZK is required, false otherwise
   */
  private boolean maybeFlushAfter() {
    return System.nanoTime() - lastUpdatedTime > MAX_FLUSH_INTERVAL || numUpdates > Overseer.STATE_UPDATE_BATCH_SIZE;
  }

  public boolean hasPendingUpdates() {
    return numUpdates != 0 || isClusterStateModified;
  }
  public ClusterState writeUpdate(ZkWriteCommand command) throws IllegalStateException, KeeperException, InterruptedException {
    Map<String, ZkWriteCommand> commands = new HashMap<>();
    commands.put(command.name, command);
    return writePendingUpdates(commands);
  }
  public ClusterState writePendingUpdates() throws KeeperException, InterruptedException {
    return writePendingUpdates(updates);

  }
  /**
   * Writes all pending updates to ZooKeeper and returns the modified cluster state
   *
   * @return the modified cluster state
   * @throws IllegalStateException if the current instance is no longer usable and must be discarded
   * @throws KeeperException       if any ZooKeeper operation results in an error
   * @throws InterruptedException  if the current thread is interrupted
   */
  public ClusterState writePendingUpdates(Map<String, ZkWriteCommand> updates) throws IllegalStateException, KeeperException, InterruptedException {
    if (invalidState) {
      throw new IllegalStateException("ZkStateWriter has seen a tragic error, this instance can no longer be used");
    }
    if ((updates == this.updates)
        && !hasPendingUpdates()) {
      return clusterState;
    }
    Timer.Context timerContext = stats.time("update_state");
    boolean success = false;
    try {
      if (!updates.isEmpty()) {
        for (Map.Entry<String, ZkWriteCommand> entry : updates.entrySet()) {
          String name = entry.getKey();
          String path = ZkStateReader.getCollectionPath(name);
          ZkWriteCommand cmd = entry.getValue();
          DocCollection c = cmd.collection;

          if (cmd.ops != null && cmd.ops.isPreOp()) {
            cmd.ops.persist(path, reader.getZkClient());
            clusterState = clusterState.copyWith(name,
                  cmd.collection.copyWith(PerReplicaStates.fetch(cmd.collection.getZNode(), reader.getZkClient(), null)));
          }
          if (!cmd.persistCollState) continue;
          if (c == null) {
            // let's clean up the state.json of this collection only, the rest should be clean by delete collection cmd
            log.debug("going to delete state.json {}", path);
            reader.getZkClient().clean(path);
          } else if (c.getStateFormat() > 1) {
            byte[] data = Utils.toJSON(singletonMap(c.getName(), c));
            if (reader.getZkClient().exists(path, true)) {
              if (log.isDebugEnabled()) {
                log.debug("going to update_collection {} version: {}", path, c.getZNodeVersion());
              }
              Stat stat = reader.getZkClient().setData(path, data, c.getZNodeVersion(), true);
              DocCollection newCollection = new DocCollection(name, c.getSlicesMap(), c.getProperties(), c.getRouter(), stat.getVersion(), path);
              clusterState = clusterState.copyWith(name, newCollection);
            } else {
              log.debug("going to create_collection {}", path);
              reader.getZkClient().create(path, data, CreateMode.PERSISTENT, true);
              DocCollection newCollection = new DocCollection(name, c.getSlicesMap(), c.getProperties(), c.getRouter(), 0, path);
              clusterState = clusterState.copyWith(name, newCollection);
            }
          } else if (c.getStateFormat() == 1) {
            isClusterStateModified = true;
          }
          if (cmd.ops != null && !cmd.ops.isPreOp()) {
            cmd.ops.persist(path, reader.getZkClient());
            DocCollection currentCollState = clusterState.getCollection(cmd.name);
            if ( currentCollState != null) {
              clusterState = clusterState.copyWith(name,
                  currentCollState.copyWith(PerReplicaStates.fetch(currentCollState.getZNode(), reader.getZkClient(), null)));
            }
          }
        }

        updates.clear();
        numUpdates = 0;
      }

      if (isClusterStateModified) {
        assert clusterState.getZkClusterStateVersion() >= 0;
        byte[] data = Utils.toJSON(clusterState);
        Stat stat = reader.getZkClient().setData(ZkStateReader.CLUSTER_STATE, data, clusterState.getZkClusterStateVersion(), true);
        Map<String, DocCollection> collections = clusterState.getCollectionsMap();
        // use the reader's live nodes because our cluster state's live nodes may be stale
        clusterState = new ClusterState(stat.getVersion(), reader.getClusterState().getLiveNodes(), collections);
        isClusterStateModified = false;
      }
      lastUpdatedTime = System.nanoTime();
      success = true;
    } catch (KeeperException.BadVersionException bve) {
      // this is a tragic error, we must disallow usage of this instance
      invalidState = true;
      throw bve;
    } finally {
      timerContext.stop();
      if (success) {
        stats.success("update_state");
      } else {
        stats.error("update_state");
      }
    }

    log.trace("New Cluster State is: {}", clusterState);
    return clusterState;
  }

  /**
   * @return the most up-to-date cluster state until the last enqueueUpdate operation
   */
  public ClusterState getClusterState() {
    return clusterState;
  }

  public interface ZkWriteCallback {
    /**
     * Called by ZkStateWriter if state is flushed to ZK
     */
    void onWrite() throws Exception;
  }
}
