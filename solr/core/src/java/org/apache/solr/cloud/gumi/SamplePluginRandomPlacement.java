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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implements random placement for new collection creation while preventing two replicas of same shard from being placed on same node.
 *
 * TODO: code not tested and never run, there are no implementation yet for used interfaces
 */
public class SamplePluginRandomPlacement implements GumiPlugin {

  public List<WorkOrder> computePlacement(Topo clusterTopo, List<Request> placementRequests, PropertyKeyFactory propertyFactory,
                                          WorkOrderFactory workOrderFactory) throws GumiException {
    // This plugin only supports Creating a collection, and only one collection. Real code would be different...
    if (placementRequests.size() != 1 ||  !(placementRequests.get(0) instanceof CreateCollectionRequest)) {
      throw new GumiException("This plugin only supports creating collections");
    }

    CreateCollectionRequest reqCreateCollection = (CreateCollectionRequest) placementRequests.get(0);

    final int totalReplicasPerShard = reqCreateCollection.getNRTReplicationFactor() +
        reqCreateCollection.getTLOGReplicationFactor() + reqCreateCollection.getPULLReplicationFactor();

    if (clusterTopo.getLiveNodes().size() < totalReplicasPerShard) {
      throw new GumiException("Cluster size too small for number of replicas per shard");
    }

    // Will have one WorkOrder for creating the collection and then one per replica
    List<WorkOrder> workToDo = new ArrayList<>(1 + totalReplicasPerShard * reqCreateCollection.getShardCount());

    // First need a work order to create the collection
    NewCollectionWorkOrder newCollection = workOrderFactory.createWorkOrderNewCollection(reqCreateCollection,
        reqCreateCollection.getCollectionName(), reqCreateCollection.getShardCount());
    workToDo.add(newCollection);

    // Now place randomly all replicas of all shards on available nodes
    for (int shardIndex = 0; shardIndex < reqCreateCollection.getShardCount(); shardIndex++) {
      // Shuffle the nodes for each shard so that replicas for a shard are placed on distinct yet random nodes
      ArrayList<Node> nodesToAssign = new ArrayList<>(clusterTopo.getLiveNodes());
      Collections.shuffle(nodesToAssign);

      placeForReplicaType(reqCreateCollection, nodesToAssign, workOrderFactory, workToDo,
          shardIndex, reqCreateCollection.getNRTReplicationFactor(), ReplicaType.NRT);
      placeForReplicaType(reqCreateCollection, nodesToAssign, workOrderFactory, workToDo,
          shardIndex, reqCreateCollection.getTLOGReplicationFactor(), ReplicaType.TLOG);
      placeForReplicaType(reqCreateCollection, nodesToAssign, workOrderFactory, workToDo,
          shardIndex, reqCreateCollection.getPULLReplicationFactor(), ReplicaType.PULL);
    }

    return workToDo;
  }

  /**
   * Pulled out of {@link #computePlacement} to avoid repeating code.
   */
  private void placeForReplicaType(CreateCollectionRequest reqCreateCollection, ArrayList<Node> nodesToAssign,
                                   WorkOrderFactory workOrderFactory, List<WorkOrder> workToDo,
                                   int shardIndex, int countReplicas, ReplicaType replicaType) {
    for (int replica = 0; replica < countReplicas; replica++) {
      Node node = nodesToAssign.remove(0);
      CreateReplicaWorkOrder createReplica = workOrderFactory.createWorkOrderCreateReplica(reqCreateCollection,
          replicaType, reqCreateCollection.getCollectionName(), shardIndex, node);

      workToDo.add(createReplica);
    }
  }
}
