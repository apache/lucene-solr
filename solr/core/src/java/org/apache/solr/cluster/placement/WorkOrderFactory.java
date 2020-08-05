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
 * Allows plugins to create {@link WorkOrder}s telling the Solr layer where to create replicas following the processing of
 * a {@link Request}. The Solr layer can (and will) check that the {@link WorkOrder} conforms to the {@link Request} (and
 * if it does not, the requested operation will fail).
 */
public interface WorkOrderFactory {
  /**
   * <p>Creates a {@link WorkOrder} for adding a new collection and its replicas.
   *
   * <p>This is in support of {@link org.apache.solr.cloud.api.collections.CreateCollectionCmd}.
   */
  WorkOrder createWorkOrderNewCollection(CreateNewCollectionRequest request, String CollectionName, Set<ReplicaPlacement> replicaPlacements);

  /**
   * <p>Creates a {@link WorkOrder} for adding replicas to a given shard of an existing collection.
   *
   * <p>This is in support (directly or indirectly) of {@link org.apache.solr.cloud.api.collections.AddReplicaCmd},
   * {@link org.apache.solr.cloud.api.collections.CreateShardCmd}, {@link org.apache.solr.cloud.api.collections.ReplaceNodeCmd},
   * {@link org.apache.solr.cloud.api.collections.MoveReplicaCmd}, {@link org.apache.solr.cloud.api.collections.SplitShardCmd},
   * {@link org.apache.solr.cloud.api.collections.RestoreCmd} and {@link org.apache.solr.cloud.api.collections.MigrateCmd}.
   * (as well as of {@link org.apache.solr.cloud.api.collections.CreateCollectionCmd} in the specific case of
   * {@link org.apache.solr.common.params.CollectionAdminParams#WITH_COLLECTION} but this should be removed shortly and
   * the section in parentheses of this comment should be removed when the {@code withCollection} javadoc link appears broken).
   */
  WorkOrder createWorkOrderAddReplicas(AddReplicasRequest request, String CollectionName, Set<ReplicaPlacement> replicaPlacements);

  /**
   * Creates a {@link ReplicaPlacement} needed to be passed to some/all {@link WorkOrder} factory methods.
   */
  ReplicaPlacement createReplicaPlacement(String shardName, Node node, Replica.ReplicaType replicaType);
}