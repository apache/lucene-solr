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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;

public class AssignShard {
  private SolrZkClient client;
  
  public AssignShard(SolrZkClient client) {
    this.client = client;
  }
  
  /**
   * Assign a new unique id up to slices count - then add replicas evenly.
   * 
   * @param collection
   * 
   * @param slices
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public String assignShard(String collection, int slices)
      throws KeeperException, InterruptedException {
    // we want the collection lock
    ZkCollectionLock lock = new ZkCollectionLock(client, collection);
    lock.lock();
    String returnShardId = null;
    try {
      // lets read the current shards - we want to read straight from zk, and we
      // assume we have some kind
      // of collection level lock
      String shardIdPaths = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
          + ZkStateReader.SHARDS_ZKNODE;
      
      List<String> shardIdNames = client.getChildren(shardIdPaths, null);
      
      if (shardIdNames.size() == 0) {
        return "shard1";
      }
      
      if (shardIdNames.size() < slices) {
        return "shard" + (shardIdNames.size() + 1);
      }
      
      // else figure out which shard needs more replicas
      final Map<String,Integer> map = new HashMap<String,Integer>();
      for (String shardId : shardIdNames) {
        int cnt = client.getChildren(shardIdPaths + "/" + shardId, null).size();
        map.put(shardId, cnt);
      }

      Collections.sort(shardIdNames, new Comparator<String>() {
        
        @Override
        public int compare(String o1, String o2) {
          Integer one = map.get(o1);
          Integer two = map.get(o2);
          return one.compareTo(two);
        }
      });

      returnShardId = shardIdNames.get(0);
    } finally {
      lock.unlock();
    }
    return returnShardId;
  }
}
