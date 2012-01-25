package org.apache.solr.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A watcher for shard leader.
 */
public class ShardLeaderWatcher implements Watcher {
  private static Logger logger = LoggerFactory.getLogger(ShardLeaderWatcher.class); 
  static interface ShardLeaderListener {
    void announceLeader(String collection, String shardId, ZkCoreNodeProps props);
  }
  
  private final String shard;
  private final String collection;
  private final String path;
  private final SolrZkClient zkClient;
  private volatile boolean closed = false;
  private final ShardLeaderListener listener;
  
  public ShardLeaderWatcher(String shard, String collection,
      SolrZkClient zkClient, ShardLeaderListener listener) throws KeeperException, InterruptedException {
    this.shard = shard;
    this.collection = collection;
    this.path = ZkStateReader.getShardLeadersPath(collection, shard);
    this.zkClient = zkClient;
    this.listener = listener;
    processLeaderChange();
  }
  
  private void processLeaderChange() throws KeeperException, InterruptedException {
    if(closed) return;
    try {
      byte[] data = zkClient.getData(path, this, null, true);
      if (data != null) {
        final ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(ZkNodeProps.load(data));
        listener.announceLeader(collection, shard, leaderProps);
      }
    } catch (KeeperException ke) {
      //check if we lost connection or the node was gone
      if (ke.code() != Code.CONNECTIONLOSS && ke.code() != Code.SESSIONEXPIRED
          && ke.code() != Code.NONODE) {
        throw ke;
      }
    }
  }

  @Override
  public void process(WatchedEvent event) {
    try {
      processLeaderChange();
    } catch (KeeperException e) {
      logger.warn("Shard leader watch triggered but Solr cannot talk to zk.");
    } catch (InterruptedException e) {
      Thread.interrupted();
      logger.warn("Shard leader watch triggered but Solr cannot talk to zk.");
    }
  }
  
  public void close() {
    closed = true;
  }

}
