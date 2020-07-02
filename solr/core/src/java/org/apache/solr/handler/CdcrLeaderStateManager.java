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
package org.apache.solr.handler;

import java.lang.invoke.MethodHandles;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Manage the leader state of the CDCR nodes.
 * </p>
 * <p>
 * It takes care of notifying the {@link CdcrReplicatorManager} in case
 * of a leader state change.
 * </p>
 * @deprecated since 8.6
 */
@Deprecated
class CdcrLeaderStateManager extends CdcrStateManager {

  private boolean amILeader = false;

  private LeaderStateWatcher wrappedWatcher;
  private Watcher watcher;

  private SolrCore core;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  CdcrLeaderStateManager(final SolrCore core) {
    this.core = core;

    // Fetch leader state and register the watcher at startup
    try {
      SolrZkClient zkClient = core.getCoreContainer().getZkController().getZkClient();
      ClusterState clusterState = core.getCoreContainer().getZkController().getClusterState();

      watcher = this.initWatcher(zkClient);
      // if the node does not exist, it means that the leader was not yet registered. This can happen
      // when the cluster is starting up. The core is not yet fully loaded, and the leader election process
      // is waiting for it.
      if (this.isLeaderRegistered(zkClient, clusterState)) {
        this.checkIfIAmLeader();
      }
    } catch (KeeperException | InterruptedException e) {
      log.warn("Failed fetching initial leader state and setting watch", e);
    }
  }

  /**
   * Checks if the leader is registered. If it is not registered, we are probably at the
   * initialisation phase of the cluster. In this case, we must attach a watcher to
   * be notified when the leader is registered.
   */
  private boolean isLeaderRegistered(SolrZkClient zkClient, ClusterState clusterState)
      throws KeeperException, InterruptedException {
    // First check if the znode exists, and register the watcher at the same time
    return zkClient.exists(this.getZnodePath(), watcher, true) != null;
  }

  /**
   * SolrZkClient does not guarantee that a watch object will only be triggered once for a given notification
   * if we does not wrap the watcher - see SOLR-6621.
   */
  private Watcher initWatcher(SolrZkClient zkClient) {
    wrappedWatcher = new LeaderStateWatcher();
    return zkClient.wrapWatcher(wrappedWatcher);
  }

  private void checkIfIAmLeader() throws KeeperException, InterruptedException {
    SolrZkClient zkClient = core.getCoreContainer().getZkController().getZkClient();
    ZkNodeProps props = ZkNodeProps.load(zkClient.getData(CdcrLeaderStateManager.this.getZnodePath(), null, null, true));
    if (props != null) {
      CdcrLeaderStateManager.this.setAmILeader(props.get("core").equals(core.getName()));
    }
  }

  private String getZnodePath() {
    String myShardId = core.getCoreDescriptor().getCloudDescriptor().getShardId();
    String myCollection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    return "/collections/" + myCollection + "/leaders/" + myShardId + "/leader";
  }

  void setAmILeader(boolean amILeader) {
    if (this.amILeader != amILeader) {
      this.amILeader = amILeader;
      this.callback(); // notify the observers of a state change
    }
  }

  boolean amILeader() {
    return amILeader;
  }

  void shutdown() {
    if (wrappedWatcher != null) {
      wrappedWatcher.cancel(); // cancel the watcher to avoid spurious warn messages during shutdown
    }
  }

  private class LeaderStateWatcher implements Watcher {

    private boolean isCancelled = false;

    /**
     * Cancel the watcher to avoid spurious warn messages during shutdown.
     */
    void cancel() {
      isCancelled = true;
    }

    @Override
    public void process(WatchedEvent event) {
      if (isCancelled) return; // if the watcher is cancelled, do nothing.
      String collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
      String shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();

      log.debug("The leader state has changed: {} @ {}:{}", event, collectionName, shard);
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }

      try {
        log.info("Received new leader state @ {}:{}", collectionName, shard);
        SolrZkClient zkClient = core.getCoreContainer().getZkController().getZkClient();
        ClusterState clusterState = core.getCoreContainer().getZkController().getClusterState();
        if (CdcrLeaderStateManager.this.isLeaderRegistered(zkClient, clusterState)) {
          CdcrLeaderStateManager.this.checkIfIAmLeader();
        }
      } catch (KeeperException | InterruptedException e) {
        log.warn("Failed updating leader state and setting watch @ {}: {}", collectionName, shard, e);
      }
    }

  }

}

