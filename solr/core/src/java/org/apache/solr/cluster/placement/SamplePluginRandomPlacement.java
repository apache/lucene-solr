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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Implements random placement for new collection creation while preventing two replicas of same shard from being placed on same node.
 *
 * TODO: code not tested and never run, there are no implementation yet for used interfaces
 */
public class SamplePluginRandomPlacement implements PlacementPlugin {

  public WorkOrder computePlacement(Topo clusterTopo, Request placementRequest, PropertyKeyFactory propertyFactory,
                                          PropertyValueFetcher propertyFetcher, WorkOrderFactory workOrderFactory) throws PlacementException {
    // This plugin only supports Creating a collection, and only one collection. Real code would be different...
    if (!(placementRequest instanceof CreateNewCollectionRequest)) {
      throw new PlacementException("This plugin only supports creating collections");
    }

    CreateNewCollectionRequest reqCreateCollection = (CreateNewCollectionRequest) placementRequest;

    final int totalReplicasPerShard = reqCreateCollection.getNrtReplicationFactor() +
        reqCreateCollection.getTlogReplicationFactor() + reqCreateCollection.getPullReplicationFactor();

    if (clusterTopo.getLiveNodes().size() < totalReplicasPerShard) {
      throw new PlacementException("Cluster size too small for number of replicas per shard");
    }

    Set<ReplicaPlacement> replicaPlacements = new HashSet<>(totalReplicasPerShard * reqCreateCollection.getShardNames().size());

    // Now place randomly all replicas of all shards on available nodes
    for (String shardName : reqCreateCollection.getShardNames()) {
      // Shuffle the nodes for each shard so that replicas for a shard are placed on distinct yet random nodes
      ArrayList<Node> nodesToAssign = new ArrayList<>(clusterTopo.getLiveNodes());
      Collections.shuffle(nodesToAssign, new Random());

      placeForReplicaType(nodesToAssign, workOrderFactory, replicaPlacements,
          shardName, reqCreateCollection.getNrtReplicationFactor(), Replica.ReplicaType.NRT);
      placeForReplicaType(nodesToAssign, workOrderFactory, replicaPlacements,
          shardName, reqCreateCollection.getTlogReplicationFactor(), Replica.ReplicaType.TLOG);
      placeForReplicaType(nodesToAssign, workOrderFactory, replicaPlacements,
          shardName, reqCreateCollection.getPullReplicationFactor(), Replica.ReplicaType.PULL);
    }

    WorkOrder newCollectionWO = workOrderFactory.createWorkOrderNewCollection(
        reqCreateCollection, reqCreateCollection.getCollectionName(), replicaPlacements);

    return newCollectionWO;
  }

  private void placeForReplicaType(ArrayList<Node> nodesToAssign, WorkOrderFactory workOrderFactory,
                                   Set<ReplicaPlacement> replicaPlacements,
                                   String shardName, int countReplicas, Replica.ReplicaType replicaType) {
    for (int replica = 0; replica < countReplicas; replica++) {
      Node node = nodesToAssign.remove(0);

      replicaPlacements.add(workOrderFactory.createReplicaPlacement(shardName, node, replicaType));
    }
  }
}
