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
import org.apache.solr.cluster.SolrCollection;

import java.util.Set;

/**
 * Allows plugins to create {@link PlacementPlan}s telling the Solr layer where to create replicas following the processing of
 * a {@link PlacementRequest}. The Solr layer can (and will) check that the {@link PlacementPlan} conforms to the {@link PlacementRequest} (and
 * if it does not, the requested operation will fail).
 */
public interface PlacementPlanFactory {
  /**
   * <p>Creates a {@link PlacementPlan} for adding replicas to a given shard(s) of an existing collection. Note this is also
   * used for creating new collections since such a creation first creates the collection, then adds the replicas.
   *
   * <p>This is in support (directly or indirectly) of {@link org.apache.solr.cloud.api.collections.AddReplicaCmd},
   * {@link org.apache.solr.cloud.api.collections.CreateShardCmd}, {@link org.apache.solr.cloud.api.collections.ReplaceNodeCmd},
   * {@link org.apache.solr.cloud.api.collections.MoveReplicaCmd}, {@link org.apache.solr.cloud.api.collections.SplitShardCmd},
   * {@link org.apache.solr.cloud.api.collections.RestoreCmd}, {@link org.apache.solr.cloud.api.collections.MigrateCmd}
   * as well as of {@link org.apache.solr.cloud.api.collections.CreateCollectionCmd}.
   */
  PlacementPlan createPlacementPlan(PlacementRequest request, Set<ReplicaPlacement> replicaPlacements);

  /**
   * <p>Creates a {@link ReplicaPlacement} to be passed to {@link PlacementPlan} factory methods.
   *
   * <p>Note the plugin can also build its own instances implementing {@link ReplicaPlacement} instead of using this call
   * (but using this method makes it easier).
   */
  ReplicaPlacement createReplicaPlacement(SolrCollection solrCollection, String shardName, Node node, Replica.ReplicaType replicaType);
}