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

public interface CommonAdminParams
{

  /** Async or not? **/
  String ASYNC = "async";
  /** Wait for final state of the operation. */
  String WAIT_FOR_FINAL_STATE = "waitForFinalState";
  /** Allow in-place move of replicas that use shared filesystems. */
  String IN_PLACE_MOVE = "inPlaceMove";
  /** Method to use for shard splitting. */
  String SPLIT_METHOD = "splitMethod";
  /** Check distribution of documents to prefixes in shard to determine how to split */
  String SPLIT_BY_PREFIX = "splitByPrefix";
  /** Number of sub-shards to create. **/
  String NUM_SUB_SHARDS = "numSubShards";
  /** Timeout for replicas to become active. */
  String TIMEOUT = "timeout";
  /** Inexact shard splitting factor. */
  String SPLIT_FUZZ = "splitFuzz";
}
