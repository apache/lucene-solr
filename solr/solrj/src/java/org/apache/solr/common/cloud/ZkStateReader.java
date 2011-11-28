package org.apache.solr.common.cloud;

/**
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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.common.SolrException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkStateReader {
  private static Logger log = LoggerFactory.getLogger(ZkStateReader.class);
  
  public static final String COLLECTIONS_ZKNODE = "/collections";
  public static final String URL_PROP = "url";
  public static final String NODE_NAME_PROP = "node_name";
  public static final String ROLES_PROP = "roles";
  public static final String STATE_PROP = "state";
  public static final String LIVE_NODES_ZKNODE = "/live_nodes";
  public static final String CLUSTER_STATE = "/clusterstate.xml";

  public static final String RECOVERING = "recovering";
  public static final String ACTIVE = "active";
  
  private volatile CloudState cloudState = new CloudState();

  private static final long CLOUD_UPDATE_DELAY = Long.parseLong(System.getProperty("CLOUD_UPDATE_DELAY", "5000"));

  public static final String LEADER_ELECT_ZKNODE = "/leader_elect";


  private static class ZKTF implements ThreadFactory {
    private static ThreadGroup tg = new ThreadGroup("ZkStateReader");
    @Override
    public Thread newThread(Runnable r) {
      Thread td = new Thread(tg, r);
      td.setDaemon(true);
      return td;
    }
  }
  private ScheduledExecutorService updateCloudExecutor = Executors.newScheduledThreadPool(1, new ZKTF());

  private boolean cloudStateUpdateScheduled;

  private SolrZkClient zkClient;
  
  private boolean closeClient = false;
  
  public ZkStateReader(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }
  
  public ZkStateReader(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) throws InterruptedException, TimeoutException, IOException {
    closeClient = true;
    zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
        // on reconnect, reload cloud info
        new OnReconnect() {

          public void command() {
            try {
            	ZkStateReader.this.createClusterStateWatchersAndUpdate();
            } catch (KeeperException e) {
              log.error("", e);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                  "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.error("", e);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                  "", e);
            }

          }
        });
  }
  
  // load and publish a new CollectionInfo
  public void updateCloudState(boolean immediate) throws KeeperException, InterruptedException,
      IOException {
    updateCloudState(immediate, false);
  }
  
  // load and publish a new CollectionInfo
  public void updateLiveNodes() throws KeeperException, InterruptedException,
      IOException {
    updateCloudState(true, true);
  }
  
  public synchronized void createClusterStateWatchersAndUpdate() throws KeeperException,
      InterruptedException {
    // We need to fetch the current cluster state and the set of live nodes
    
    if (!zkClient.exists(CLUSTER_STATE)) {
      try {
        zkClient.create(CLUSTER_STATE, null, CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        // if someone beats us to creating this ignore it
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          throw e;
        }
      }
    }
    
    CloudState clusterState;
    
    log.info("Updating cluster state from ZooKeeper... ");
    zkClient.exists(CLUSTER_STATE, new Watcher() {
      
      @Override
      public void process(WatchedEvent event) {
        log.info("A cluster state change has occurred");
        try {
          byte[] data = zkClient.getData(CLUSTER_STATE, this, null);
          // delayed approach
          // ZkStateReader.this.updateCloudState(false, false);
          synchronized (ZkStateReader.this.getUpdateLock()) {
            CloudState clusterState = CloudState.load(zkClient, ZkStateReader.this.cloudState
                .getLiveNodes());
            // update volatile
            cloudState = clusterState;
          }
        } catch (KeeperException e) {
          if (e.code() == KeeperException.Code.SESSIONEXPIRED
              || e.code() == KeeperException.Code.CONNECTIONLOSS) {
            log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
            return;
          }
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        } catch (InterruptedException e) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
          log.warn("", e);
          return;
        } /*
           * catch(IOException e){ log.error("", e); throw new
           * ZooKeeperException( SolrException.ErrorCode.SERVER_ERROR, "", e); }
           */
      }
      
    });
    
    
    List<String> liveNodes = zkClient.getChildren(LIVE_NODES_ZKNODE,
        new Watcher() {
          
          @Override
          public void process(WatchedEvent event) {
            log.info("Updating live nodes");
            try {
              // delayed approach
              // ZkStateReader.this.updateCloudState(false, true);
              synchronized (ZkStateReader.this.getUpdateLock()) {
                List<String> liveNodes = zkClient.getChildren(
                    LIVE_NODES_ZKNODE, this);
                Set<String> liveNodesSet = new HashSet<String>();
                liveNodesSet.addAll(liveNodes);
                CloudState clusterState = new CloudState(liveNodesSet, cloudState.getCollectionStates());
                // update volatile
                cloudState = clusterState;
              }
            } catch (KeeperException e) {
              if (e.code() == KeeperException.Code.SESSIONEXPIRED
                  || e.code() == KeeperException.Code.CONNECTIONLOSS) {
                log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
                return;
              }
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.warn("", e);
              return;
            }
          }
          
        });
    Set<String> liveNodeSet = new HashSet<String>();
    liveNodeSet.addAll(liveNodes);
    clusterState = CloudState.load(zkClient, liveNodeSet);
    this.cloudState = clusterState;
    
  }
  
  
  // load and publish a new CollectionInfo
  private synchronized void updateCloudState(boolean immediate,
      final boolean onlyLiveNodes) throws KeeperException,
      InterruptedException, IOException {
    log.info("Manual update of cluster state initiated");
    // build immutable CloudInfo
    
    if (immediate) {
      CloudState clusterState;
      List<String> liveNodes = zkClient.getChildren(LIVE_NODES_ZKNODE, null);
      Set<String> liveNodesSet = new HashSet<String>();
      liveNodesSet.addAll(liveNodes);
      if (!onlyLiveNodes) {
        log.info("Updating cloud state from ZooKeeper... ");

        clusterState = CloudState.load(zkClient, liveNodesSet);
      } else {
        log.info("Updating live nodes from ZooKeeper... ");
        clusterState = cloudState;
      }
 
      // update volatile
      this.cloudState = clusterState;
    } else {
      if (cloudStateUpdateScheduled) {
        log.info("Cloud state update for ZooKeeper already scheduled");
        return;
      }
      log.info("Scheduling cloud state update from ZooKeeper...");
      cloudStateUpdateScheduled = true;
      updateCloudExecutor.schedule(new Runnable() {
        
        public void run() {
          log.info("Updating cluster state from ZooKeeper...");
          synchronized (getUpdateLock()) {
            cloudStateUpdateScheduled = false;
            CloudState clusterState;
            try {
              List<String> liveNodes = zkClient.getChildren(LIVE_NODES_ZKNODE,
                  null);
              Set<String> liveNodesSet = new HashSet<String>();
              liveNodesSet.addAll(liveNodes);
              
              if (!onlyLiveNodes) {
                log.info("Updating cloud state from ZooKeeper... ");
                
                clusterState = CloudState.load(zkClient, liveNodesSet);
              } else {
                log.info("Updating live nodes from ZooKeeper... ");
                clusterState = new CloudState(liveNodesSet, ZkStateReader.this.cloudState.getCollectionStates());
              }
              
              // update volatile
              ZkStateReader.this.cloudState = clusterState;
              
            } catch (KeeperException e) {
              if (e.code() == KeeperException.Code.SESSIONEXPIRED
                  || e.code() == KeeperException.Code.CONNECTIONLOSS) {
                log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
                return;
              }
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            }
            // update volatile
            ZkStateReader.this.cloudState = cloudState;
          }
        }
      }, CLOUD_UPDATE_DELAY, TimeUnit.MILLISECONDS);
    }
    
  }
   
  /**
   * @return information about the cluster from ZooKeeper
   */
  public CloudState getCloudState() {
    return cloudState;
  }
  
  public Object getUpdateLock() {
    return this;
  }

  public void close() {
    if (closeClient) {
      try {
        zkClient.close();
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      }
    }
  }
  
  abstract class RunnableWatcher implements Runnable {
		Watcher watcher;
		public RunnableWatcher(Watcher watcher){
			this.watcher = watcher;
		}

	}
  
  // nocommit TODO: do this with cloud state or something along those lines
  // and if we find out we cannot talk to zk anymore, we should probably realize we are not
  // a leader anymore - we shouldn't accept updates at all??
  public String getLeader(String collection, String shard) throws Exception {
    
    String url = null;
    int tries = 30;
    while (true) {
      if (!zkClient
          .exists("/collections/" + collection + "/leader_elect/" + shard + "/leader")) {
        if (tries-- == 0) {
          throw new RuntimeException("No registered leader was found");
        }
        Thread.sleep(1000);
        continue;
      }
      String leaderPath = "/collections/" + collection + "/leader_elect/" + shard + "/leader";
      List<String> leaderChildren = zkClient.getChildren(
          leaderPath, null);
      if (leaderChildren.size() > 0) {
        String leader = leaderChildren.get(0);
        byte[] data = zkClient.getData(leaderPath + "/" + leader, null, null);
        ZkNodeProps props = ZkNodeProps.load(data);
        url = props.get(ZkStateReader.URL_PROP);
        break;
      } else {
        if (tries-- == 0) {
          throw new RuntimeException("No registered leader was found");
        }
        Thread.sleep(1000);
      }
    }
    return url;
  }
  
}
