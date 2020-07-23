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

package org.apache.solr.cloud.gumi;

import java.util.Set;

public interface WorkOrderFactory {
  /**
   * Creates a {@link WorkOrder} for adding a new collection (does not add any replica)
   */
  NewCollectionWorkOrder createWorkOrderNewCollection(Request request, String CollectionName, Set<String> shardNames);

  /**
   * Creates a {@link WorkOrder} for adding a replica to an existing shard of an existing collection or a to a yet to
   * exist shard for a yet to exit collection for which a {@link NewCollectionWorkOrder} was already added (+ possibly other
   * combinations: the collection might exist but the shard not yet b.
   *
   * @param shardName 0 based shard number. TODO: likely needs to be reworked when looking at adding replicas to existing collection/shard
   */
  CreateReplicaWorkOrder createWorkOrderCreateReplica(Request request, ReplicaType replicaType, String CollectionName,
                                                      String shardName, Node targetNode);
}