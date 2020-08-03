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

package org.apache.solr.cluster.placement;

import java.util.Set;

/**
 * Represents a Collection in SolrCloud. Although naming this class "Collection" is possible it would be confusing.
 */
public interface SolrCollection {
  /**
   * The collection name (value passed to {@link Cluster#getCollection(String)}).
   */
  String getName();

  /**
   * The {@link Shard}'s over which the data of this {@link SolrCollection} is distributed.
   */
  Set<Shard> getShards();
}
