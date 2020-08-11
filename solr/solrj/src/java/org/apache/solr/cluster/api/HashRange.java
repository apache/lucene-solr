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

/**
 * A range of hash that is stored in a shard
 */
public interface HashRange {

  /** minimum value (inclusive) */
  int min();

  /** maximum value (inclusive) */
  int max();

  /** Check if a given hash falls in this range */
  default boolean includes(int hash) {
    return hash >= min() && hash <= max();
  }

  /** Check if another range is a subset of this range */
  default boolean isSubset(HashRange subset) {
    return min() <= subset.min() && max() >= subset.max();
  }

}
