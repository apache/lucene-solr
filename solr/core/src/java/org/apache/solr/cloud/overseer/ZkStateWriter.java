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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.codahale.metrics.Timer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeOut;
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

  /**
   * Represents a no-op {@link ZkWriteCommand} which will result in no modification to cluster state
   */
  public static ZkWriteCommand NO_OP = ZkWriteCommand.noop();

  //protected final ZkStateReader reader;
  protected volatile Stats stats;

  //protected final Deque<DocCollection> updates = new ArrayDeque<>();
  Map<String,DocCollection> updatesToWrite = new LinkedHashMap<>();


  // / protected boolean isClusterStateModified = false;
  protected long lastUpdatedTime = 0;

  private Map<String,Integer> trackVersions = new HashMap<>();

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
   * last enqueue operation resulted in buffered state. The method writePendingUpdates(ClusterState) can
   * be used to force an immediate flush of pending cluster state changes.
   *
   * @param state the cluster state information on which the given <code>cmd</code> is applied
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
  public ClusterState enqueueUpdate(ClusterState state, List<ZkWriteCommand> cmds, ZkWriteCallback callback) throws IllegalStateException, Exception {
    if (log.isDebugEnabled()) {
      // nocommit trace?
      log.debug("enqueueUpdate(ClusterState prevState={}, List<ZkWriteCommand> cmds={}, updates={}, ZkWriteCallback callback={}) - start", state, cmds, updatesToWrite, callback);
    }
    Map<String,DocCollection> updateCmds = new LinkedHashMap<>(cmds.size());

    for (ZkWriteCommand cmd : cmds) {
        updateCmds.put(cmd.name, cmd.collection);
    }

    if (updateCmds.isEmpty()) {
      return state;
    }
    ClusterState prevState = state;
    Set<Map.Entry<String,DocCollection>> entries = updateCmds.entrySet();
    for (Map.Entry<String,DocCollection> entry : entries) {
      DocCollection c = entry.getValue();

      String name = entry.getKey();
      String path = ZkStateReader.getCollectionPath(name);

      try {

        if (c == null) {
          // let's clean up the state.json of this collection only, the rest should be clean by delete collection cmd
          if (log.isDebugEnabled()) {
            log.debug("going to delete state.json {}", path);
          }

          updatesToWrite.remove(name);
          trackVersions.remove(name);

          reader.getZkClient().clean(path);

          TimeOut timeout = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
          timeout.waitFor("", () -> {
            DocCollection rc = reader.getClusterState().getCollectionOrNull(name);
            if (rc == null) return true;
            return false;
          });

          LinkedHashMap<String,ClusterState.CollectionRef> collStates = new LinkedHashMap<>(prevState.getCollectionStates());
          collStates.remove(name);
          prevState = new ClusterState(prevState.getLiveNodes(), collStates, prevState.getZNodeVersion());
        } else if (updatesToWrite.get(name) != null) {
          if (log.isDebugEnabled()) {
            log.debug("enqueueUpdate() - going to update_collection {} version: {}", path, c.getZNodeVersion());
          }


          if (log.isDebugEnabled()) {
            log.debug("The new collection {}", c);
          }
          updatesToWrite.put(name, c);
          LinkedHashMap<String,ClusterState.CollectionRef> collStates = new LinkedHashMap<>(prevState.getCollectionStates());
          collStates.put(name, new ClusterState.CollectionRef(c));
          prevState = new ClusterState(prevState.getLiveNodes(), collStates, prevState.getZNodeVersion());
        } else {
          if (log.isDebugEnabled()) {
            log.debug("enqueueUpdate() - going to create_collection {}", path);
          }
          //   assert c.getStateFormat() > 1;
          int version = 0;
          DocCollection prevCol = prevState.getCollectionOrNull(c.getName());
          if (prevCol != null) {
            version = prevCol.getZNodeVersion();
          }

          DocCollection newCollection = new DocCollection(name, c.getSlicesMap(), c.getProperties(), c.getRouter(), version);

          LinkedHashMap<String,ClusterState.CollectionRef> collStates = new LinkedHashMap<>(prevState.getCollectionStates());
          collStates.put(name, new ClusterState.CollectionRef(newCollection));
          prevState = new ClusterState(prevState.getLiveNodes(), collStates, prevState.getZNodeVersion());
          updatesToWrite.put(name, newCollection);
        }
      } catch (InterruptedException | AlreadyClosedException e) {
        ParWork.propagateInterrupt(e);
        throw e;
      } catch (KeeperException.SessionExpiredException e) {
        throw e;
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        if (e instanceof KeeperException.BadVersionException) {
          log.warn("Tried to update the cluster state using but we where rejected, currently at {}", c == null ? "null" : c.getZNodeVersion(), e);
          throw e;
        }
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed processing update=" + c + "\n" + prevState, e);
      }
    }

    if (callback != null) {
      callback.onWrite();
    }

    if (log.isDebugEnabled()) {
      log.debug("enqueueUpdate(ClusterState, List<ZkWriteCommand>, ZkWriteCallback) - end");
    }
    return prevState;
  }

  public boolean hasPendingUpdates() {
    if (log.isDebugEnabled()) {
      log.debug("hasPendingUpdates() - start");
    }

    boolean returnboolean = updatesToWrite.size() > 0;
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
  public ClusterState writePendingUpdates(ClusterState state) throws IllegalStateException, KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("writePendingUpdates() - start updates.size={}", updatesToWrite.size());
    }
    Timer.Context timerContext = stats.time("update_state");
    boolean success = false;
    try {

      // assert newClusterState.getZNodeVersion() >= 0;
      // byte[] data = Utils.toJSON(newClusterState);
      // Stat stat = reader.getZkClient().setData(ZkStateReader.CLUSTER_STATE, data, newClusterState.getZNodeVersion(),
      // true);
      //
      //
      //
      Map<String,DocCollection> failedUpdates = new LinkedHashMap<>();
      Exception lastFailedException = null;
      for (DocCollection c : updatesToWrite.values()) {
        String name = c.getName();
        String path = ZkStateReader.getCollectionPath(c.getName());

        Stat stat = new Stat();

        try {

         // if (reader.getClusterState().getCollectionOrNull(c.getName()) != null) {

            byte[] data = Utils.toJSON(singletonMap(c.getName(), c));

            //if (log.isDebugEnabled()) {
            if (log.isDebugEnabled()) log.debug("Write state.json prevVersion={} bytes={} cs={}", c.getZNodeVersion(), data.length, c);
            //}
            // stat = reader.getZkClient().getCurator().setData().withVersion(prevVersion).forPath(path, data);
            try {
              int version = c.getZNodeVersion();
              Integer v = trackVersions.get(c.getName());
              if (v != null) {
                version = v;
                trackVersions.put(c.getName(), v + 1);
              } else {
                trackVersions.put(c.getName(), version + 1);
              }

              reader.getZkClient().setData(path, data, version, true);
            } catch (KeeperException.BadVersionException bve) {
              lastFailedException = bve;
              failedUpdates.put(c.getName(), c);
              stat = reader.getZkClient().exists(path, null);
              // this is a tragic error, we must disallow usage of this instance
              log.warn("Tried to update the cluster state using version={} but we where rejected, found {}", c.getZNodeVersion(), stat.getVersion(), bve);
            }
            if (log.isDebugEnabled()) log.debug("Set version for local collection {} to {}", c.getName(), c.getZNodeVersion() + 1);
        } catch (InterruptedException | AlreadyClosedException e) {
          ParWork.propagateInterrupt(e);
          throw e;
        } catch (KeeperException.SessionExpiredException e) {
          throw e;
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Failed processing update=" + c, e);
        }
      }



        for (DocCollection c : updatesToWrite.values()) {
          if (c != null && !failedUpdates.containsKey(c.getName())) {
            try {
              //System.out.println("waiting to see state " + prevVersion);
              Integer v = trackVersions.get(c.getName());
              int version = v - 1;
              
              Integer finalPrevVersion = version;
              reader.waitForState(c.getName(), 15, TimeUnit.SECONDS, (l, col) -> {

                //              if (col != null) {
                //                System.out.println("the version " + col.getZNodeVersion());
                //              }

                if (col != null && col.getZNodeVersion() > finalPrevVersion) {
                  if (log.isDebugEnabled()) log.debug("Waited for ver: {}", col.getZNodeVersion() + 1);
                  // System.out.println("found the version");
                  return true;
                }
                return false;
              });
            } catch (TimeoutException e) {
              log.warn("Timeout waiting to see written cluster state come back");
            }
          }
        }


      lastUpdatedTime = System.nanoTime();



      if (failedUpdates.size() > 0) {
        log.warn("Some collection updates failed {} logging last exception", failedUpdates, lastFailedException);
        failedUpdates.clear();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, lastFailedException);
      }

      success = true;
    } finally {
      timerContext.stop();
      if (success) {
        stats.success("update_state");
      } else {
        stats.error("update_state");
      }
    }

//    if (log.isDebugEnabled()) {
//      log.debug("writePendingUpdates() - end - New Cluster State is: {}", newClusterState);
//    }

    try {
      return new ClusterState(state.getLiveNodes(), updatesToWrite);
    } finally {
      updatesToWrite.clear();
    }
  }

  public Map<String,DocCollection>  getUpdatesToWrite() {
    return updatesToWrite;
  }

  public interface ZkWriteCallback {
    /**
     * Called by ZkStateWriter if state is flushed to ZK
     */
    void onWrite() throws Exception;
  }
}

