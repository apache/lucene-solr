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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.common.SolrException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkStateReader {
  private static Logger log = LoggerFactory.getLogger(ZkStateReader.class);
  
  public static final String COLLECTIONS_ZKNODE = "/collections";
  public static final String URL_PROP = "url";
  public static final String NODE_NAME = "node_name";
  public static final String SHARDS_ZKNODE = "/shards";
  public static final String LIVE_NODES_ZKNODE = "/live_nodes";
  
  private volatile CloudState cloudState  = new CloudState(new HashSet<String>(0), new HashMap<String,Map<String,Slice>>(0));
  
  private static final long CLOUD_UPDATE_DELAY = Long.parseLong(System.getProperty("CLOUD_UPDATE_DELAY", "5000"));

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
              makeCollectionsNodeWatches();
              makeShardsWatches(true);
              updateCloudState(false);
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
            } catch (IOException e) {
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
  
  // load and publish a new CollectionInfo
  private synchronized void updateCloudState(boolean immediate, final boolean onlyLiveNodes) throws KeeperException, InterruptedException,
      IOException {

    // TODO: - possibly: incremental update rather than reread everything
    
    // build immutable CloudInfo
    
    if(immediate) {
      if(!onlyLiveNodes) {
        log.info("Updating cloud state from ZooKeeper... ");
      } else {
        log.info("Updating live nodes from ZooKeeper... ");
      }
      CloudState cloudState;
      cloudState = CloudState.buildCloudState(zkClient, this.cloudState, onlyLiveNodes);
      // update volatile
      this.cloudState = cloudState;
    } else {
      if(cloudStateUpdateScheduled) {
        log.info("Cloud state update for ZooKeeper already scheduled");
        return;
      }
      log.info("Scheduling cloud state update from ZooKeeper...");
      cloudStateUpdateScheduled = true;
      updateCloudExecutor.schedule(new Runnable() {
        
        public void run() {
          log.info("Updating cloud state from ZooKeeper...");
          synchronized (getUpdateLock()) {
            cloudStateUpdateScheduled = false;
            CloudState cloudState;
            try {
              cloudState = CloudState.buildCloudState(zkClient,
                  ZkStateReader.this.cloudState, onlyLiveNodes);
            } catch (KeeperException e) {
              if(e.code() == KeeperException.Code.SESSIONEXPIRED || e.code() == KeeperException.Code.CONNECTIONLOSS) {
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
            } catch (IOException e) {
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
  
  public void makeShardZkNodeWatches(boolean makeWatchesForReconnect) throws KeeperException, InterruptedException {
    CloudState cloudState = getCloudState();
    
    Set<String> knownCollections = cloudState.getCollections();
    List<String> collections = zkClient.getChildren(COLLECTIONS_ZKNODE, null);

    for(final String collection : collections) {
      if(makeWatchesForReconnect || !knownCollections.contains(collection)) {
        log.info("Found new collection:" + collection);
        Watcher watcher = new Watcher() {
          public void process(WatchedEvent event) {
            log.info("Detected changed ShardId in collection:" + collection);
            try {
              makeShardsWatches(collection, false);
              updateCloudState(false);
            } catch (KeeperException e) {
              if(e.code() == KeeperException.Code.SESSIONEXPIRED || e.code() == KeeperException.Code.CONNECTIONLOSS) {
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
            } catch (IOException e) {
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            }
          }
        };
        boolean madeWatch = true;
        String shardZkNode = COLLECTIONS_ZKNODE + "/" + collection
            + SHARDS_ZKNODE;
        for (int i = 0; i < 5; i++) {
          try {
            zkClient.getChildren(shardZkNode, watcher);
          } catch (KeeperException.NoNodeException e) {
            // most likely, the collections node has been created, but not the
            // shards node yet -- pause and try again
            madeWatch = false;
            if (i == 4) {
              log.error("Could not set shards zknode watch, because the zknode does not exist:" + shardZkNode);
              break;
            }
            Thread.sleep(100);
          }
          if (madeWatch) {
            log.info("Made shard watch:" + shardZkNode);
            break;
          }
        }
      }
    }
  }
  
  public void makeShardsWatches(final String collection, boolean makeWatchesForReconnect) throws KeeperException,
      InterruptedException {
    if (zkClient.exists(COLLECTIONS_ZKNODE + "/" + collection + SHARDS_ZKNODE)) {
      List<String> shardIds = zkClient.getChildren(COLLECTIONS_ZKNODE + "/"
          + collection + SHARDS_ZKNODE, null);
      CloudState cloudState = getCloudState();
      Set<String> knownShardIds;
      Map<String,Slice> slices = cloudState.getSlices(collection);
      if (slices != null) {
        knownShardIds = slices.keySet();
      } else {
        knownShardIds = new HashSet<String>(0);
      }
      for (final String shardId : shardIds) {
        if (makeWatchesForReconnect || !knownShardIds.contains(shardId)) {
          zkClient.getChildren(COLLECTIONS_ZKNODE + "/" + collection
              + SHARDS_ZKNODE + "/" + shardId, new Watcher() {

            public void process(WatchedEvent event) {
              log.info("Detected a shard change under ShardId:" + shardId + " in collection:" + collection);
              try {
                updateCloudState(false);
              } catch (KeeperException e) {
                if(e.code() == KeeperException.Code.SESSIONEXPIRED || e.code() == KeeperException.Code.CONNECTIONLOSS) {
                  log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
                  return;
                }
                log.error("", e);
                throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                    "", e);
              } catch (InterruptedException e) {
                // Restore the interrupted status
                Thread.currentThread().interrupt();
                log.error("", e);
                throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                    "", e);
              } catch (IOException e) {
                log.error("", e);
                throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                    "", e);
              }
            }
          });
        }
      }
    }
  }
  
  /**
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makeShardsWatches(boolean makeWatchesForReconnect) throws KeeperException, InterruptedException {
    List<String> collections = zkClient.getChildren(COLLECTIONS_ZKNODE, null);
    for (final String collection : collections) {
      makeShardsWatches(collection, makeWatchesForReconnect);
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

  public void makeCollectionsNodeWatches() throws KeeperException, InterruptedException {
    log.info("Start watching collections zk node for changes");
    zkClient.getChildren(ZkStateReader.COLLECTIONS_ZKNODE, new Watcher(){

      public void process(WatchedEvent event) {
          try {

            log.info("Detected a new or removed collection");
            synchronized (getUpdateLock()) {
              makeShardZkNodeWatches(false);
              updateCloudState(false);
            }
            // re-watch
            String path = event.getPath();
            if (path != null) {
              zkClient.getChildren(path, this);
            }
          } catch (KeeperException e) {
            if(e.code() == KeeperException.Code.SESSIONEXPIRED || e.code() == KeeperException.Code.CONNECTIONLOSS) {
              log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
              return;
            }
            log.error("", e);
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          } catch (InterruptedException e) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
            log.error("", e);
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          } catch (IOException e) {
            log.error("", e);
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          }

      }});
    
    zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE, new Watcher(){

      public void process(WatchedEvent event) {
        if(event.getType() !=  EventType.NodeDataChanged) {
          return;
        }
        log.info("Notified of CloudState change");
        try {
          synchronized (getUpdateLock()) {
            makeShardZkNodeWatches(false);
            updateCloudState(false);
          }
          zkClient.exists(ZkStateReader.COLLECTIONS_ZKNODE, this);
        } catch (KeeperException e) {
          if(e.code() == KeeperException.Code.SESSIONEXPIRED || e.code() == KeeperException.Code.CONNECTIONLOSS) {
            log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
            return;
          }
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        } catch (InterruptedException e) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        } catch (IOException e) {
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        }
        
      }});
    
  }
}
