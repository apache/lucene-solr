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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.KeeperException;

public class AssignShard {

  /**
   * Assign a new unique id up to slices count - then add replicas evenly.
   * 
   * @param collection
   * @param state
   * @return
   */
  public static String assignShard(String collection, CloudState state) {

    int shards = Integer.getInteger(ZkStateReader.NUM_SHARDS_PROP,1);

    String returnShardId = null;
    Map<String, Slice> sliceMap = state.getSlices(collection);

    if (sliceMap == null) {
      return "shard1";
    }

    List<String> shardIdNames = new ArrayList<String>(sliceMap.keySet());

    if (shardIdNames.size() < shards) {
      return "shard" + (shardIdNames.size() + 1);
    }

    // else figure out which shard needs more replicas
    final Map<String, Integer> map = new HashMap<String, Integer>();
    for (String shardId : shardIdNames) {
      int cnt = sliceMap.get(shardId).getShards().size();
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
    return returnShardId;
  }
}
