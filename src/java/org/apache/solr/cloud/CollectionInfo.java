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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Information about the Collection.
 * 
 */
public final class CollectionInfo {

  // maps shard name to the shard addresses and roles
  private final Map<String,ShardInfoList> shardNameToShardInfoList;
  private final long updateTime;

  public CollectionInfo(Map<String,ShardInfoList> shardNameToShardInfoList) {
    //nocommit: defensive copy?
    this.shardNameToShardInfoList = shardNameToShardInfoList;
    this.updateTime = System.currentTimeMillis();
  }

  /**
   * //nocommit
   * 
   * @return
   */
  public List<String> getSearchShards() {
    List<String> nodeList = new ArrayList<String>();
    for (ShardInfoList nodes : shardNameToShardInfoList.values()) {
      nodeList.add(nodes.getShardUrl());
    }
    return nodeList;
  }

  public ShardInfoList getShardInfoList(String shardName) {
    return shardNameToShardInfoList.get(shardName);
  }
  

  /**
   * @return last time info was updated.
   */
  public long getUpdateTime() {
    return updateTime;
  }

}
