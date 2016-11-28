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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Timer;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
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
 * {@link #enqueueUpdate(ClusterState, ZkWriteCommand, ZkWriteCallback)} method. The class buffers updates
 * to reduce the number of writes to ZK. The buffered updates are flushed during <code>enqueueUpdate</code>
 * automatically if necessary. The {@link #writePendingUpdates()} can be used to force flush any pending updates.
 *
 * If either {@link #enqueueUpdate(ClusterState, ZkWriteCommand, ZkWriteCallback)} or {@link #writePendingUpdates()}
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
  protected final Overseer.Stats stats;

  protected Map<String, DocCollection> updates = new HashMap<>();
  protected ClusterState clusterState = null;
  protected boolean isClusterStateModified = false;
  protected long lastUpdatedTime = 0;

  // state information which helps us batch writes
  protected int lastStateFormat = -1; // sentinel value
  protected String lastCollectionName = null;

  /**
   * Set to true if we ever get a BadVersionException so that we can disallow future operations
   * with this instance
   */
  protected boolean invalidState = false;

  public ZkStateWriter(ZkStateReader zkStateReader, Overseer.Stats stats) {
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
   * @param cmd       the {@link ZkWriteCommand} which specifies the change to be applied to cluster state
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
  public ClusterState enqueueUpdate(ClusterState prevState, ZkWriteCommand cmd, ZkWriteCallback callback) throws IllegalStateException, Exception {
    if (invalidState) {
      throw new IllegalStateException("ZkStateWriter has seen a tragic error, this instance can no longer be used");
    }
    if (cmd == NO_OP) return prevState;

    if (maybeFlushBefore(cmd)) {
      // we must update the prev state to the new one
      prevState = clusterState = writePendingUpdates();
      if (callback != null) {
        callback.onWrite();
      }
    }

    if (callback != null) {
      callback.onEnqueue();
    }

    /*
    We need to know if the collection has moved from stateFormat=1 to stateFormat=2 (as a result of MIGRATECLUSTERSTATE)
     */
    DocCollection previousCollection = prevState.getCollectionOrNull(cmd.name);
    boolean wasPreviouslyStateFormat1 = previousCollection != null && previousCollection.getStateFormat() == 1;
    boolean isCurrentlyStateFormat1 = cmd.collection != null && cmd.collection.getStateFormat() == 1;

    if (cmd.collection == null) {
      if (wasPreviouslyStateFormat1) {
        isClusterStateModified = true;
      }
      clusterState = prevState.copyWith(cmd.name, null);
      updates.put(cmd.name, null);
    } else {
      if (!isCurrentlyStateFormat1) {
        updates.put(cmd.name, cmd.collection);
      }
      if (isCurrentlyStateFormat1 || wasPreviouslyStateFormat1) {
        isClusterStateModified = true;
      }
      clusterState = prevState.copyWith(cmd.name, cmd.collection);
    }

    if (maybeFlushAfter(cmd)) {
      ClusterState state = writePendingUpdates();
      if (callback != null) {
        callback.onWrite();
      }
      return state;
    }

    return clusterState;
  }

  /**
   * Logic to decide a flush before processing a ZkWriteCommand
   *
   * @param cmd the ZkWriteCommand instance
   * @return true if a flush is required, false otherwise
   */
  protected boolean maybeFlushBefore(ZkWriteCommand cmd) {
    if (lastUpdatedTime == 0) {
      // first update, make sure we go through
      return false;
    }
    if (cmd.collection == null) {
      return false;
    }
    if (cmd.collection.getStateFormat() != lastStateFormat) {
      return true;
    }
    return cmd.collection.getStateFormat() > 1 && !cmd.name.equals(lastCollectionName);
  }

  /**
   * Logic to decide a flush after processing a ZkWriteCommand
   *
   * @param cmd the ZkWriteCommand instance
   * @return true if a flush to ZK is required, false otherwise
   */
  protected boolean maybeFlushAfter(ZkWriteCommand cmd) {
    if (cmd.collection == null)
      return false;
    lastCollectionName = cmd.name;
    lastStateFormat = cmd.collection.getStateFormat();
    return System.nanoTime() - lastUpdatedTime > MAX_FLUSH_INTERVAL;
  }

  public boolean hasPendingUpdates() {
    return !updates.isEmpty() || isClusterStateModified;
  }

  /**
   * Writes all pending updates to ZooKeeper and returns the modified cluster state
   *
   * @return the modified cluster state
   * @throws IllegalStateException if the current instance is no longer usable and must be discarded
   * @throws KeeperException       if any ZooKeeper operation results in an error
   * @throws InterruptedException  if the current thread is interrupted
   */
  public ClusterState writePendingUpdates() throws IllegalStateException, KeeperException, InterruptedException {
    if (invalidState) {
      throw new IllegalStateException("ZkStateWriter has seen a tragic error, this instance can no longer be used");
    }
    if (!hasPendingUpdates()) return clusterState;
    Timer.Context timerContext = stats.time("update_state");
    boolean success = false;
    try {
      if (!updates.isEmpty()) {
        for (Map.Entry<String, DocCollection> entry : updates.entrySet()) {
          String name = entry.getKey();
          String path = ZkStateReader.getCollectionPath(name);
          DocCollection c = entry.getValue();

          if (c == null) {
            // let's clean up the collections path for this collection
            log.debug("going to delete_collection {}", path);
            reader.getZkClient().clean("/collections/" + name);
          } else if (c.getStateFormat() > 1) {
            byte[] data = Utils.toJSON(singletonMap(c.getName(), c));
            if (reader.getZkClient().exists(path, true)) {
              log.debug("going to update_collection {} version: {}", path, c.getZNodeVersion());
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
        }

        updates.clear();
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

    return clusterState;
  }

  /**
   * @return time returned by System.nanoTime at which the main cluster state was last written to ZK or 0 if
   * never
   */
  public long getLastUpdatedTime() {
    return lastUpdatedTime;
  }

  /**
   * @return the most up-to-date cluster state until the last enqueueUpdate operation
   */
  public ClusterState getClusterState() {
    return clusterState;
  }

  public interface ZkWriteCallback {
    /**
     * Called by ZkStateWriter if a ZkWriteCommand is queued
     */
    public void onEnqueue() throws Exception;

    /**
     * Called by ZkStateWriter if state is flushed to ZK
     */
    public void onWrite() throws Exception;
  }
}

