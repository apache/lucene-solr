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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.*;
import org.apache.solr.common.util.SuppressForbidden;

/**
 * <p>Factory for creating {@link MinimizeCoresPlacementPlugin}, a Placement plugin implementing placing replicas
 * to minimize number of cores per {@link Node}, while not placing two replicas of the same shard on the same node.
 * This code is meant as an educational example of a placement plugin.</p>
 *
 * <p>See {@link AffinityPlacementFactory} for a more realistic example and documentation.</p>
 */
public class MinimizeCoresPlacementFactory implements PlacementPluginFactory<PlacementPluginFactory.NoConfig> {

  @Override
  public PlacementPlugin createPluginInstance() {
    return new MinimizeCoresPlacementPlugin();
  }

  static private class MinimizeCoresPlacementPlugin implements PlacementPlugin {

    @SuppressForbidden(reason = "Ordering.arbitrary() has no equivalent in Comparator class. Rather reuse than copy.")
    public PlacementPlan computePlacement(Cluster cluster, PlacementRequest request, AttributeFetcher attributeFetcher,
                                          PlacementPlanFactory placementPlanFactory) throws PlacementException {
      int totalReplicasPerShard = 0;
      for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
        totalReplicasPerShard += request.getCountReplicasToCreate(rt);
      }

      if (cluster.getLiveNodes().size() < totalReplicasPerShard) {
        throw new PlacementException("Cluster size too small for number of replicas per shard");
      }

      // Get number of cores on each Node
      TreeMultimap<Integer, Node> nodesByCores = TreeMultimap.create(Comparator.naturalOrder(), Ordering.arbitrary());

      Set<Node> nodes = request.getTargetNodes();

      attributeFetcher.requestNodeCoreCount();
      attributeFetcher.fetchFrom(nodes);
      AttributeValues attrValues = attributeFetcher.fetchAttributes();


      // Get the number of cores on each node and sort the nodes by increasing number of cores
      for (Node node : nodes) {
        if (attrValues.getCoresCount(node).isEmpty()) {
          throw new PlacementException("Can't get number of cores in " + node);
        }
        nodesByCores.put(attrValues.getCoresCount(node).get(), node);
      }

      Set<ReplicaPlacement> replicaPlacements = new HashSet<>(totalReplicasPerShard * request.getShardNames().size());

      // Now place all replicas of all shards on nodes, by placing on nodes with the smallest number of cores and taking
      // into account replicas placed during this computation. Note that for each shard we must place replicas on different
      // nodes, when moving to the next shard we use the nodes sorted by their updated number of cores (due to replica
      // placements for previous shards).
      for (String shardName : request.getShardNames()) {
        // Assign replicas based on the sort order of the nodesByCores tree multimap to put replicas on nodes with less
        // cores first. We only need totalReplicasPerShard nodes given that's the number of replicas to place.
        // We assign based on the passed nodeEntriesToAssign list so the right nodes get replicas.
        ArrayList<Map.Entry<Integer, Node>> nodeEntriesToAssign = new ArrayList<>(totalReplicasPerShard);
        Iterator<Map.Entry<Integer, Node>> treeIterator = nodesByCores.entries().iterator();
        for (int i = 0; i < totalReplicasPerShard; i++) {
          nodeEntriesToAssign.add(treeIterator.next());
        }

        // Update the number of cores each node will have once the assignments below got executed so the next shard picks the
        // lowest loaded nodes for its replicas.
        for (Map.Entry<Integer, Node> e : nodeEntriesToAssign) {
          int coreCount = e.getKey();
          Node node = e.getValue();
          nodesByCores.remove(coreCount, node);
          nodesByCores.put(coreCount + 1, node);
        }

        for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
          placeReplicas(request.getCollection(), nodeEntriesToAssign, placementPlanFactory, replicaPlacements, shardName, request, replicaType);
        }
      }

      return placementPlanFactory.createPlacementPlan(request, replicaPlacements);
    }

    private void placeReplicas(SolrCollection solrCollection, ArrayList<Map.Entry<Integer, Node>> nodeEntriesToAssign,
                               PlacementPlanFactory placementPlanFactory, Set<ReplicaPlacement> replicaPlacements,
                               String shardName, PlacementRequest request, Replica.ReplicaType replicaType) {
      for (int replica = 0; replica < request.getCountReplicasToCreate(replicaType); replica++) {
        final Map.Entry<Integer, Node> entry = nodeEntriesToAssign.remove(0);
        final Node node = entry.getValue();

        replicaPlacements.add(placementPlanFactory.createReplicaPlacement(solrCollection, shardName, node, replicaType));
      }
    }
  }
}
