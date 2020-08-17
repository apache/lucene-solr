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

package org.apache.solr.cluster.placement.impl;

import java.util.Set;

import org.apache.solr.cluster.placement.AddReplicasPlacementRequest;
import org.apache.solr.cluster.placement.Node;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlanFactory;
import org.apache.solr.cluster.placement.Replica;
import org.apache.solr.cluster.placement.ReplicaPlacement;

class PlacementPlanFactoryImpl implements PlacementPlanFactory {
  @Override
  public PlacementPlan createPlacementPlanAddReplicas(AddReplicasPlacementRequest request, Set<ReplicaPlacement> replicaPlacements) {
    return new PlacementPlanAddReplicasImpl(request, replicaPlacements);
  }

  @Override
  public ReplicaPlacement createReplicaPlacement(String shardName, Node node, Replica.ReplicaType replicaType) {
    return new ReplicaPlacementImpl(shardName, node, replicaType);
  }
}
