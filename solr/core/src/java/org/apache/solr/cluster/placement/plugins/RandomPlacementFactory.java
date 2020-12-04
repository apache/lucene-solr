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

import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.*;

/**
 * <p>Factory for creating {@link RandomPlacementPlugin}, a placement plugin implementing random placement for new
 * collection creation while preventing two replicas of same shard from being placed on same node..</p>
 *
 * <p>See {@link AffinityPlacementFactory} for a more realistic example and documentation.</p>
 */
public class RandomPlacementFactory implements PlacementPluginFactory<PlacementPluginFactory.NoConfig> {

  @Override
  public PlacementPlugin createPluginInstance() {
    return new RandomPlacementPlugin();
  }

  public static class RandomPlacementPlugin implements PlacementPlugin {
    private final Random replicaPlacementRandom = new Random(); // ok even if random sequence is predictable.

    private RandomPlacementPlugin() {
      // We make things reproducible in tests by using test seed if any
      String seed = System.getProperty("tests.seed");
      if (seed != null) {
        replicaPlacementRandom.setSeed(seed.hashCode());
      }
    }

    public PlacementPlan computePlacement(Cluster cluster, PlacementRequest request, AttributeFetcher attributeFetcher,
                                          PlacementPlanFactory placementPlanFactory) throws PlacementException {
      int totalReplicasPerShard = 0;
      for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
        totalReplicasPerShard += request.getCountReplicasToCreate(rt);
      }

      if (cluster.getLiveNodes().size() < totalReplicasPerShard) {
        throw new PlacementException("Cluster size too small for number of replicas per shard");
      }

      Set<ReplicaPlacement> replicaPlacements = new HashSet<>(totalReplicasPerShard * request.getShardNames().size());

      // Now place randomly all replicas of all shards on available nodes
      for (String shardName : request.getShardNames()) {
        // Shuffle the nodes for each shard so that replicas for a shard are placed on distinct yet random nodes
        ArrayList<Node> nodesToAssign = new ArrayList<>(cluster.getLiveNodes());
        Collections.shuffle(nodesToAssign, replicaPlacementRandom);

        for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
          placeForReplicaType(request.getCollection(), nodesToAssign, placementPlanFactory, replicaPlacements, shardName, request, rt);
        }
      }

      return placementPlanFactory.createPlacementPlan(request, replicaPlacements);
    }

    private void placeForReplicaType(SolrCollection solrCollection, ArrayList<Node> nodesToAssign, PlacementPlanFactory placementPlanFactory,
                                     Set<ReplicaPlacement> replicaPlacements,
                                     String shardName, PlacementRequest request, Replica.ReplicaType replicaType) {
      for (int replica = 0; replica < request.getCountReplicasToCreate(replicaType); replica++) {
        Node node = nodesToAssign.remove(0);

        replicaPlacements.add(placementPlanFactory.createReplicaPlacement(solrCollection, shardName, node, replicaType));
      }
    }
  }
}
