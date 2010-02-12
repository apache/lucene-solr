package org.apache.solr.cloud;

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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.cloud.SolrZkClient.OnReconnect;
import org.apache.solr.common.SolrException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkStateReader {
  private static Logger log = LoggerFactory.getLogger(ZkStateReader.class);
  
  private volatile CloudState cloudState  = new CloudState(new HashSet<String>(0), new HashMap<String,Map<String,Slice>>(0));
  
  private static final long CLOUD_UPDATE_DELAY = Long.parseLong(System.getProperty("CLOUD_UPDATE_DELAY", "5000"));

  private ScheduledExecutorService updateCloudExecutor = Executors.newScheduledThreadPool(1);

  private boolean cloudStateUpdateScheduled;

  private SolrZkClient zkClient;
  
  public ZkStateReader(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }
  
  public ZkStateReader(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) throws InterruptedException, TimeoutException, IOException {
    zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
        // on reconnect, reload cloud info
        new OnReconnect() {

          public void command() {
            try {
              // nocommit: recreate watches ????
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

    // TODO: - incremental update rather than reread everything
    
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
  
  /**
   * @return information about the cluster from ZooKeeper
   */
  public CloudState getCloudState() {
    return cloudState;
  }
  
  public Object getUpdateLock() {
    return this;
  }
}
