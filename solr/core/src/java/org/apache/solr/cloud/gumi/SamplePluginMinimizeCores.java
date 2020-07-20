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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

/**
 * Implements placing replicas to minimize number of cores per {@link Node}, while not placing two replicas of the same
 * shard on the same node.
 *
 * TODO: code not tested and never run, there are no implementation yet for used interfaces
 */
public class SamplePluginMinimizeCores implements GumiPlugin {

  public List<WorkOrder> computePlacement(Topo clusterTopo, List<Request> placementRequests, PropertyKeyFactory propertyFactory,
                                          WorkOrderFactory workOrderFactory) throws GumiException {
    // This plugin only supports Creating a collection, and only one collection. Real code would be different...
    if (placementRequests.size() != 1 ||  !(placementRequests.get(0) instanceof CreateCollectionRequest)) {
      throw new GumiException("This plugin only supports creating collections");
    }

    final CreateCollectionRequest reqCreateCollection = (CreateCollectionRequest) placementRequests.get(0);

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

    // Get number of cores on each Node
    TreeMultimap<Integer, Node> nodesByCores = TreeMultimap.create(Comparator.naturalOrder(), Ordering.arbitrary());

    // Get the number of cores on each node and sort the nodes by increasing number of cores
    final CoresCountPropertyKey coresCountPropertyKey = propertyFactory.createCoreCountKey();
    for (Node node : clusterTopo.getLiveNodes()) {
      Map<PropertyKey, PropertyValue> propMap = node.getProperties(Collections.singleton(coresCountPropertyKey));
      PropertyValue returnedValue = propMap.get(coresCountPropertyKey);
      if (returnedValue == null) {
        throw new GumiException("Can't get number of cores in " + node);
      }
      CoresCountPropertyValue coresCountPropertyValue = (CoresCountPropertyValue) returnedValue;
      nodesByCores.put(coresCountPropertyValue.getCoresCount(), node);
    }

    // Now place all replicas of all shards on nodes, by placing on nodes with the smallest number of cores and taking
    // into account replicas placed during this computation. Note for each shard we must place on different nodes but
    // when moving to the next shard should use the newly sorted order.
    for (int shardIndex = 0; shardIndex < reqCreateCollection.getShardCount(); shardIndex++) {
      // Capture a list for assignment order based on the sort order of the treemap to put replicas on nodes with less
      // cores first. We only need totalReplicasPerShard nodes. Need to capture the pair in order to update the treemap
      // as we go. TODO I guess and hope there's a more elegant way to do that but it's getting late here.
      ArrayList<Map.Entry<Integer, Node>> nodeEntriesToAssign = new ArrayList<>(totalReplicasPerShard);
      Iterator<Map.Entry<Integer, Node>> treeIterator = nodesByCores.entries().iterator();
      for (int i = 0; i < totalReplicasPerShard; i++) {
        nodeEntriesToAssign.add(treeIterator.next());
      }

      placeForReplicaType(reqCreateCollection, nodeEntriesToAssign, workOrderFactory, workToDo,
          shardIndex, reqCreateCollection.getNRTReplicationFactor(), ReplicaType.NRT, nodesByCores);
      placeForReplicaType(reqCreateCollection, nodeEntriesToAssign, workOrderFactory, workToDo,
          shardIndex, reqCreateCollection.getTLOGReplicationFactor(), ReplicaType.TLOG, nodesByCores);
      placeForReplicaType(reqCreateCollection, nodeEntriesToAssign, workOrderFactory, workToDo,
          shardIndex, reqCreateCollection.getPULLReplicationFactor(), ReplicaType.PULL, nodesByCores);
    }

    return workToDo;
  }

  /**
   * Pulled out of {@link #computePlacement} to avoid repeating code.
   */
  private void placeForReplicaType(CreateCollectionRequest reqCreateCollection, ArrayList<Map.Entry<Integer, Node>> nodeEntriesToAssign,
                                   WorkOrderFactory workOrderFactory, List<WorkOrder> workToDo,
                                   int shardIndex, int countReplicas, ReplicaType replicaType,
                                   TreeMultimap<Integer, Node> nodesByCores) {
    for (int replica = 0; replica < countReplicas; replica++) {
      final Map.Entry<Integer, Node> entry = nodeEntriesToAssign.remove(0);
      final Node node = entry.getValue();
      final Integer coreCount = entry.getKey();

      CreateReplicaWorkOrder createReplica = workOrderFactory.createWorkOrderCreateReplica(reqCreateCollection,
          replicaType, reqCreateCollection.getCollectionName(), shardIndex, node);

      workToDo.add(createReplica);

      // Update the entry in the treemap, there's one more core on the node. This does not impact the iteration order
      // for the current shard, we're working off a copy of the treemap.
      nodesByCores.remove(coreCount, node);
      nodesByCores.put(coreCount + 1, node);
    }
  }
}
