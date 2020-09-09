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

package org.apache.solr.cluster.placement.plugins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.solr.cluster.placement.*;

/**
 * Implements random placement for new collection creation while preventing two replicas of same shard from being placed on same node.
 *
 * TODO: code not tested and never run, there are no implementation yet for used interfaces
 */
public class SamplePluginRandomPlacement implements PlacementPlugin {

  private final PlacementPluginConfig config;

  private SamplePluginRandomPlacement(PlacementPluginConfig config) {
    this.config = config;
  }

  static public class Factory implements PlacementPluginFactory {
    @Override
    public PlacementPlugin createPluginInstance(PlacementPluginConfig config) {
      return new SamplePluginRandomPlacement(config);
    }
  }

  public PlacementPlan computePlacement(Cluster cluster, PlacementRequest request, AttributeFetcher attributeFetcher,
                                        PlacementPlanFactory placementPlanFactory) throws PlacementException {
    final int totalReplicasPerShard = request.getCountNrtReplicas() +
        request.getCountTlogReplicas() + request.getCountPullReplicas();

    if (cluster.getLiveNodes().size() < totalReplicasPerShard) {
      throw new PlacementException("Cluster size too small for number of replicas per shard");
    }

    Set<ReplicaPlacement> replicaPlacements = new HashSet<>(totalReplicasPerShard * request.getShardNames().size());

    // Now place randomly all replicas of all shards on available nodes
    for (String shardName : request.getShardNames()) {
      // Shuffle the nodes for each shard so that replicas for a shard are placed on distinct yet random nodes
      ArrayList<Node> nodesToAssign = new ArrayList<>(cluster.getLiveNodes());
      Collections.shuffle(nodesToAssign, new Random());

      placeForReplicaType(nodesToAssign, placementPlanFactory, replicaPlacements,
          shardName, request.getCountNrtReplicas(), Replica.ReplicaType.NRT);
      placeForReplicaType(nodesToAssign, placementPlanFactory, replicaPlacements,
          shardName, request.getCountTlogReplicas(), Replica.ReplicaType.TLOG);
      placeForReplicaType(nodesToAssign, placementPlanFactory, replicaPlacements,
          shardName, request.getCountPullReplicas(), Replica.ReplicaType.PULL);
    }

    return placementPlanFactory.createPlacementPlan(request, replicaPlacements);
  }

  private void placeForReplicaType(ArrayList<Node> nodesToAssign, PlacementPlanFactory placementPlanFactory,
                                   Set<ReplicaPlacement> replicaPlacements,
                                   String shardName, int countReplicas, Replica.ReplicaType replicaType) {
    for (int replica = 0; replica < countReplicas; replica++) {
      Node node = nodesToAssign.remove(0);

      replicaPlacements.add(placementPlanFactory.createReplicaPlacement(shardName, node, replicaType));
    }
  }
}
