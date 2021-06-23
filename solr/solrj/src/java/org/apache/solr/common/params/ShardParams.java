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
package org.apache.solr.common.params;

import org.apache.solr.common.util.StrUtils;

/**
 * Parameters used for distributed search.
 * 
 * When adding a new parameter here, please also add the corresponding
 * one-line test case in the ShardParamsTest class.
 * 
 */
public interface ShardParams {
  /** the shards to use (distributed configuration) */
  String SHARDS = "shards";
  
  /** per-shard start and rows */
  String SHARDS_ROWS = "shards.rows";
  String SHARDS_START = "shards.start";
  
  /** IDs of the shard documents */
  String IDS = "ids";
  
  /** whether the request goes to a shard */
  String IS_SHARD = "isShard";
  
  /** The requested URL for this shard */
  String SHARD_URL = "shard.url";

  /** The requested shard name */
  String SHARD_NAME = "shard.name";

  /** The Request Handler for shard requests */
  String SHARDS_QT = "shards.qt";
  
  /** Request detailed match info for each shard (true/false) */
  String SHARDS_INFO = "shards.info";

  /** Should things fail if there is an error? (true/false/{@value #REQUIRE_ZK_CONNECTED}) */
  String SHARDS_TOLERANT = "shards.tolerant";
  
  /** query purpose for shard requests */
  String SHARDS_PURPOSE = "shards.purpose";

  /** Shards sorting rules */
  String SHARDS_PREFERENCE = "shards.preference";

  /** Replica type sort rule */
  String SHARDS_PREFERENCE_REPLICA_TYPE = "replica.type";

  /** Replica location sort rule */
  String SHARDS_PREFERENCE_REPLICA_LOCATION = "replica.location";

  /** Replica leader status sort rule, value= true/false */
  String SHARDS_PREFERENCE_REPLICA_LEADER = "replica.leader";

  /** Node with same system property sort rule */
  String SHARDS_PREFERENCE_NODE_WITH_SAME_SYSPROP = "node.sysprop";

  /** Replica base/fallback sort rule */
  String SHARDS_PREFERENCE_REPLICA_BASE = "replica.base";

  /** Value denoting local replicas */
  String REPLICA_LOCAL = "local";

  /** Value denoting randomized replica sort */
  String REPLICA_RANDOM = "random";

  /** Value denoting stable replica sort */
  String REPLICA_STABLE = "stable";

  /** configure dividend param for stable replica sort */
  String ROUTING_DIVIDEND = "dividend";

  /** configure hash param for stable replica sort */
  String ROUTING_HASH = "hash";

  String _ROUTE_ = "_route_";

  /** Force a single-pass distributed query? (true/false) */
  String DISTRIB_SINGLE_PASS = "distrib.singlePass";
  
  /**
   * Throw an error from search requests when the {@value #SHARDS_TOLERANT} param
   * has this value and ZooKeeper is not connected. 
   * 
   * @see #getShardsTolerantAsBool(SolrParams) 
   */
  String REQUIRE_ZK_CONNECTED = "requireZkConnected";

  /**
   * Parse the {@value #SHARDS_TOLERANT} param from <code>params</code> as a boolean;
   * accepts {@value #REQUIRE_ZK_CONNECTED} as a valid value indicating <code>false</code>.
   * 
   * By default, returns <code>false</code> when {@value #SHARDS_TOLERANT} is not set
   * in <code>params</code>.
   */
  static boolean getShardsTolerantAsBool(SolrParams params) {
    String shardsTolerantValue = params.get(SHARDS_TOLERANT);
    if (null == shardsTolerantValue || shardsTolerantValue.equals(REQUIRE_ZK_CONNECTED)) {
      return false;
    } else {
      return StrUtils.parseBool(shardsTolerantValue); // throw an exception if non-boolean
    }
  }
}
