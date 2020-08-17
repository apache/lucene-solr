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

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cluster.placement.Cluster;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlanFactory;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PropertyKeyFactory;
import org.apache.solr.common.cloud.ReplicaPosition;

/**
 * This assign strategy delegates placement computation to "plugin" code.
 */
public class PlacementPluginAssignStrategy implements Assign.AssignStrategy {

  private static final PlacementPlanFactory PLACEMENT_PLAN_FACTORY = new PlacementPlanFactoryImpl();
  private static final PropertyKeyFactory PROPERTY_KEY_FACTORY = new PropertyKeyFactoryImpl();

  private final PlacementPlugin plugin;
  public PlacementPluginAssignStrategy(PlacementPlugin plugin) {
    this.plugin = plugin;
  }

  public List<ReplicaPosition> assign(SolrCloudManager solrCloudManager, Assign.AssignRequest assignRequest)
      throws Assign.AssignmentException, IOException, InterruptedException {

    Cluster cluster = new ClusterImpl(solrCloudManager);

    // TODO create from assignRequest
    PlacementRequest placementRequest = null;

    final PlacementPlan placementPlan;
    try {
      // TODO Implement factories, likely keep instances around...
      placementPlan = plugin.computePlacement(cluster, placementRequest, PROPERTY_KEY_FACTORY, null, PLACEMENT_PLAN_FACTORY);
    } catch (PlacementException pe) {
      throw new Assign.AssignmentException(pe);
    }

    return ReplicaPlacementImpl.toReplicaPositions(placementPlan.getReplicaPlacements());
  }
}
