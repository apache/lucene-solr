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

public interface WorkOrderFactory {
  /**
   * <p>Creates a {@link WorkOrder} for adding a new collection and its replicas.
   *
   * <p>This is in support of {@link org.apache.solr.cloud.api.collections.CreateCollectionCmd}.
   */
  WorkOrder createWorkOrderNewCollection(CreateCollectionRequest request, String CollectionName, Set<ReplicaPlacement> replicaPlacements);

//  /**
//   * Creates a {@link WorkOrder} for adding a replica to an existing shard of an existing collection.
//   */
//  WorkOrder createWorkOrderCreateReplica(Request request, String CollectionName, ReplicaPlacement replicaPlacement);

  /**
   * Creates a {@link ReplicaPlacement} needed to be passed to some/all {@link WorkOrder} factory methods.
   */
  ReplicaPlacement createReplicaPlacement(String shardName, Node node, Replica.ReplicaType replicaType);
}