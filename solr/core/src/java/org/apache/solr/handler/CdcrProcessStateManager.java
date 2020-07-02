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

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Manage the life-cycle state of the CDCR process. It is responsible of synchronising the state
 * through Zookeeper. The state of the CDCR process is stored in the zk node defined by {@link #getZnodePath()}.
 * </p>
 * <p>
 * It takes care of notifying the {@link CdcrReplicatorManager} in case
 * of a process state change.
 * </p>
 * @deprecated since 8.6
 */
@Deprecated
class CdcrProcessStateManager extends CdcrStateManager {

  private CdcrParams.ProcessState state = DEFAULT_STATE;

  private ProcessStateWatcher wrappedWatcher;
  private Watcher watcher;

  private SolrCore core;

  /**
   * The default state must be STOPPED. See comments in
   * {@link #setState(org.apache.solr.handler.CdcrParams.ProcessState)}.
   */
  static CdcrParams.ProcessState DEFAULT_STATE = CdcrParams.ProcessState.STOPPED;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  CdcrProcessStateManager(final SolrCore core) {
    this.core = core;

    // Ensure that the status znode exists
    this.createStateNode();

    // Register the watcher at startup
    try {
      SolrZkClient zkClient = core.getCoreContainer().getZkController().getZkClient();
      watcher = this.initWatcher(zkClient);
      this.setState(CdcrParams.ProcessState.get(zkClient.getData(this.getZnodePath(), watcher, null, true)));
    } catch (KeeperException | InterruptedException e) {
      log.warn("Failed fetching initial state", e);
    }
  }

  /**
   * SolrZkClient does not guarantee that a watch object will only be triggered once for a given notification
   * if we does not wrap the watcher - see SOLR-6621.
   */
  private Watcher initWatcher(SolrZkClient zkClient) {
    wrappedWatcher = new ProcessStateWatcher();
    return zkClient.wrapWatcher(wrappedWatcher);
  }

  private String getZnodeBase() {
    return "/collections/" + core.getCoreDescriptor().getCloudDescriptor().getCollectionName() + "/cdcr/state";
  }

  private String getZnodePath() {
    return getZnodeBase() + "/process";
  }

  void setState(CdcrParams.ProcessState state) {
    if (this.state != state) {
      this.state = state;
      this.callback(); // notify the observers of a state change
    }
  }

  CdcrParams.ProcessState getState() {
    return state;
  }

  /**
   * Synchronise the state to Zookeeper. This method must be called only by the handler receiving the
   * action.
   */
  void synchronize() {
    SolrZkClient zkClient = core.getCoreContainer().getZkController().getZkClient();
    try {
      zkClient.setData(this.getZnodePath(), this.getState().getBytes(), true);
      // check if nobody changed it in the meantime, and set a new watcher
      this.setState(CdcrParams.ProcessState.get(zkClient.getData(this.getZnodePath(), watcher, null, true)));
    } catch (KeeperException | InterruptedException e) {
      log.warn("Failed synchronising new state", e);
    }
  }

  private void createStateNode() {
    SolrZkClient zkClient = core.getCoreContainer().getZkController().getZkClient();
    try {
      if (!zkClient.exists(this.getZnodePath(), true)) {
        if (!zkClient.exists(this.getZnodeBase(), true)) { // Should be a no-op if the node exists
          zkClient.makePath(this.getZnodeBase(), null, CreateMode.PERSISTENT, null, false, true);
        }
        zkClient.create(this.getZnodePath(), DEFAULT_STATE.getBytes(), CreateMode.PERSISTENT, true);
        if (log.isInfoEnabled()) {
          log.info("Created znode {}", this.getZnodePath());
        }
      }
    } catch (KeeperException.NodeExistsException ne) {
      // Someone got in first and created the node.
    } catch (KeeperException | InterruptedException e) {
      log.warn("Failed to create CDCR process state node", e);
    }
  }

  void shutdown() {
    if (wrappedWatcher != null) {
      wrappedWatcher.cancel(); // cancel the watcher to avoid spurious warn messages during shutdown
    }
  }

  private class ProcessStateWatcher implements Watcher {

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

      log.info("The CDCR process state has changed: {} @ {}:{}", event, collectionName, shard);
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      SolrZkClient zkClient = core.getCoreContainer().getZkController().getZkClient();
      try {
        CdcrParams.ProcessState state = CdcrParams.ProcessState.get(zkClient.getData(CdcrProcessStateManager.this.getZnodePath(), watcher, null, true));
        log.info("Received new CDCR process state from watcher: {} @ {}:{}", state, collectionName, shard);
        CdcrProcessStateManager.this.setState(state);
      } catch (KeeperException | InterruptedException e) {
        log.warn("Failed synchronising new state @ {}: {}", collectionName, shard, e);
      }
    }

  }

}

