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

/**
 * Parameters used for distributed search.
 * 
 * When adding a new parameter here, please also add the corresponding
 * one-line test case in the ShardParamsTest class.
 * 
 */
public interface ShardParams {
  /** the shards to use (distributed configuration) */
  public static final String SHARDS = "shards";
  
  /** per-shard start and rows */
  public static final String SHARDS_ROWS = "shards.rows";
  public static final String SHARDS_START = "shards.start";
  
  /** IDs of the shard documents */
  public static final String IDS = "ids";
  
  /** whether the request goes to a shard */
  public static final String IS_SHARD = "isShard";
  
  /** The requested URL for this shard */
  public static final String SHARD_URL = "shard.url";
  
  /** The Request Handler for shard requests */
  public static final String SHARDS_QT = "shards.qt";
  
  /** Request detailed match info for each shard (true/false) */
  public static final String SHARDS_INFO = "shards.info";

  /** Should things fail if there is an error? (true/false) */
  public static final String SHARDS_TOLERANT = "shards.tolerant";
  
  /** query purpose for shard requests */
  public static final String SHARDS_PURPOSE = "shards.purpose";

  public static final String _ROUTE_ = "_route_";

  /** Force a single-pass distributed query? (true/false) */
  public static final String DISTRIB_SINGLE_PASS = "distrib.singlePass";
}
