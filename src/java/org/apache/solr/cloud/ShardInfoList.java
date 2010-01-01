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

import java.util.Collections;
import java.util.List;

/**
 * List of Shards (URL + Role)
 *
 */
public final class ShardInfoList {

  private final List<ShardInfo> shards;

  public ShardInfoList(List<ShardInfo> shards) {
    //nocommit: defensive copy?
    this.shards = shards;
  }
  
  public List<ShardInfo> getShards() {
    return Collections.unmodifiableList(shards);
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    ShardInfo shardInfo;
    int sz = shards.size();
    for(int i = 0; i < sz; i++) {
      shardInfo = shards.get(i);
      sb.append(shardInfo.getUrl() + ", " + shardInfo.getRole());
      if(i != sz-1) {
        sb.append(" : ");
      }
    }
    return sb.toString();
  }
  
  // nocommit: return one URL from list of shards
  public String getShardUrl() {
    // nocommit
    for (ShardInfo shard : shards) {
      System.out.println("getNode:" + shard.getUrl());
      if (shard.getRole() != ShardInfo.Role.MASTER) {
        return shard.getUrl();
      }
    }
    throw new IllegalStateException("No slaves for shard");
  }
}
