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

import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;

/**
 * <p>Placement decision for a single {@link Replica}. Note this placement decision is used as part of a {@link PlacementPlan},
 * it does not directly lead to the plugin code getting a corresponding {@link Replica} instance, nor does it require the
 * plugin to provide a {@link Shard} instance (the plugin code gets such instances for existing replicas and shards in the
 * cluster but does not create them directly for adding new replicas for new or existing shards).
 *
 * <p>Captures the {@link SolrCollection}, {@link Shard} (via the shard name), {@link Node} and {@link org.apache.solr.cluster.Replica.ReplicaType}
 * of a Replica to be created.
 */
public interface ReplicaPlacement {

  /**
   * @return the {@link SolrCollection} for which the replica should be created
   */
  SolrCollection getCollection();

  /**
   * @return the name of the {@link Shard} for which the replica should be created. Note that only the name of the shard
   * is returned and not a {@link Shard} instance because the shard might not yet exist when the placement request is made.
   */
  String getShardName();

  /**
   * @return the {@link Node} on which the replica should be created
   */
  Node getNode();

  /**
   * @return the type of the replica to be created
   */
  Replica.ReplicaType getReplicaType();
}
