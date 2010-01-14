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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Information about the Collection.
 * 
 */
public final class CollectionState {

  protected static final Logger log = LoggerFactory
      .getLogger(CollectionState.class);
  
  static final String SHARD_LIST_PROP = "shard_list";
  static final String URL_PROP = "url";
  static final String ROLE_PROP = "role";
  
  // maps shard name to the shard addresses and roles
  private final Map<String,Properties> shards;
  private final long updateTime;

  private final List<String> nodes;



  public CollectionState(Map<String,Properties> shards, List<String> nodes) {
    //nocommit: defensive copy?
    this.shards = shards;
    this.updateTime = System.currentTimeMillis();
    this.nodes = nodes;
  }
  
//  public CollectionInfo(SolrZkClient client, String path) throws KeeperException, InterruptedException, IOException {
//    //nocommit: 
//    // build immutable CollectionInfo
//    shardNameToShardInfoList = readShardInfo(client, path);
//    nodes = client.getChildren(path, null);
//    this.updateTime = System.currentTimeMillis();
//  }
  
  /**
   * Read info on the available Shards and Nodes.
   * @param zkClient 
   * 
   * @param path to the shards zkNode
   * @return Map from shard name to a {@link ShardInfoList}
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   */
  public Map<String,ShardInfoList> readShardInfo(SolrZkClient zkClient, String path)
      throws KeeperException, InterruptedException, IOException {
    // for now, just reparse everything
    HashMap<String,ShardInfoList> shardNameToShardList = new HashMap<String,ShardInfoList>();

    if (zkClient.exists(path, null) == null) {
      throw new IllegalStateException("Cannot find zk shards node that should exist:"
          + path);
    }


    for (String zkNodeName : nodes) {
      byte[] data = zkClient.getData(path + "/" + zkNodeName, null,
          null);

      Properties props = new Properties();
      props.load(new ByteArrayInputStream(data));

      String url = (String) props.get(URL_PROP);
      String shardNameList = (String) props.get(SHARD_LIST_PROP);
      String[] shardsNames = shardNameList.split(",");
      for (String shardName : shardsNames) {
        ShardInfoList sList = shardNameToShardList.get(shardName);
        List<ShardInfo> shardList;
        if (sList == null) {
          shardList = new ArrayList<ShardInfo>(1);
        } else {
          List<ShardInfo> oldShards = sList.getShards();
          shardList = new ArrayList<ShardInfo>(oldShards.size() + 1);
          shardList.addAll(oldShards);
        }

        ShardInfo shard = new ShardInfo(url);
        shardList.add(shard);
        ShardInfoList list = new ShardInfoList(shardList);

        shardNameToShardList.put(shardName, list);
      }

    }

    return Collections.unmodifiableMap(shardNameToShardList);
  }

  /**
   * //nocommit
   * 
   * @return
   */
//  public List<String> getSearchShards() {
//    List<String> nodeList = new ArrayList<String>();
//    for (ShardInfoList nodes : shardNameToShardInfoList.values()) {
//      nodeList.add(nodes.getShardUrl());
//    }
//    return nodeList;
//  }
  
  public List<String> getNodes() {
    return Collections.unmodifiableList(nodes);
  }

  /**
   * @return last time info was updated.
   */
  public long getUpdateTime() {
    return updateTime;
  }

}
