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

package org.apache.solr.cluster.api;

import org.apache.solr.common.cloud.Replica;

/** replica of a shard */
public interface ShardReplica {
  /** Name of this replica */
  String name();

  /** The shard which it belongs to */
  String shard();

  /** collection which it belongs to */
  String collection();

  /** Name of the node where this replica is present */
  String node();

  /** Name of the core where this is hosted */
  String core();

  /** type of the replica */
  Replica.Type type();

  /** Is the replica alive now */
  boolean alive();

  /**Size of the index in bytes. Keep in mind that this may result in a network call.
   * Also keep in mind that the value that you get is at best an approximation.
   * The exact size may vary from replica to replica
   */
  long indexSize();

  /**Is this replica the leader */
  boolean isLeader();

  /**Baseurl for this replica
   */
  String url(ApiType type);
}
