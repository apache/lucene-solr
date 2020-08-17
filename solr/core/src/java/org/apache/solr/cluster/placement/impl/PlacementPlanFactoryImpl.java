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
import org.apache.solr.cluster.placement.CreateNewCollectionPlacementRequest;
import org.apache.solr.cluster.placement.Node;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlanFactory;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.cluster.placement.Replica;
import org.apache.solr.cluster.placement.ReplicaPlacement;

class PlacementPlanFactoryImpl implements PlacementPlanFactory {
  @Override
  public PlacementPlan createPlacementPlanNewCollection(CreateNewCollectionPlacementRequest request, Set<ReplicaPlacement> replicaPlacements) {
    return internalCreatePlacementPlan(request, replicaPlacements);
  }

  @Override
  public PlacementPlan createPlacementPlanAddReplicas(AddReplicasPlacementRequest request, Set<ReplicaPlacement> replicaPlacements) {
    return internalCreatePlacementPlan(request, replicaPlacements);
  }

  @Override
  public ReplicaPlacement createReplicaPlacement(String shardName, Node node, Replica.ReplicaType replicaType) {
    return new ReplicaPlacementImpl(shardName, node, replicaType);
  }

  /**
   * TODO: The two methods above end up doing the same thing. As suggested by AB in https://github.com/apache/lucene-solr/pull/1684#discussion_r468377374 we can make do with a single one. For now keeping the two in the interface but eventually will remove and only keep this one. Decide before merge!
   */
  private PlacementPlan internalCreatePlacementPlan(PlacementRequest request, Set<ReplicaPlacement> replicaPlacements) {
    return new PlacementPlanImpl(request, replicaPlacements);
  }
}
