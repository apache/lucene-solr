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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import org.apache.solr.cluster.placement.AddReplicasPlacementRequest;
import org.apache.solr.cluster.placement.Cluster;
import org.apache.solr.cluster.placement.CoresCountPropertyValue;
import org.apache.solr.cluster.placement.Node;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.PropertyKey;
import org.apache.solr.cluster.placement.PropertyKeyFactory;
import org.apache.solr.cluster.placement.PropertyValue;
import org.apache.solr.cluster.placement.PropertyValueFetcher;
import org.apache.solr.cluster.placement.Replica;
import org.apache.solr.cluster.placement.ReplicaPlacement;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlanFactory;
import org.apache.solr.common.util.SuppressForbidden;

/**
 * Implements placing replicas to minimize number of cores per {@link Node}, while not placing two replicas of the same
 * shard on the same node.
 *
 * TODO: code not tested and never run, there are no implementation yet for used interfaces
 */
public class SamplePluginMinimizeCores implements PlacementPlugin {

  private final PlacementPluginConfig config;

  private SamplePluginMinimizeCores(PlacementPluginConfig config) {
    this.config = config;
  }

  static public class Factory implements PlacementPluginFactory {

    /**
     * Empty public constructor is used to instantiate this factory based on configuration in solr.xml, element
     * {@code <placementPluginFactory>} in element {@code <solrcloud>}.
     */
    public Factory() {
    }

    @Override
    public PlacementPlugin createPluginInstance(PlacementPluginConfig config) {
      return new SamplePluginMinimizeCores(config);
    }
  }

  @SuppressForbidden(reason = "Ordering.arbitrary() has no equivalent in Comparator class. Rather reuse than copy.")
  public PlacementPlan computePlacement(Cluster cluster, PlacementRequest placementRequest, PropertyKeyFactory propertyFactory,
                                        PropertyValueFetcher propertyFetcher, PlacementPlanFactory placementPlanFactory) throws PlacementException {
    // This plugin only supports Creating a collection.
    if (!(placementRequest instanceof AddReplicasPlacementRequest)) {
      throw new PlacementException("This toy plugin only supports adding replicas");
    }

    final AddReplicasPlacementRequest reqAddReplicas = (AddReplicasPlacementRequest) placementRequest;

    final int totalReplicasPerShard = reqAddReplicas.getCountNrtReplicas() +
        reqAddReplicas.getCountTlogReplicas() + reqAddReplicas.getCountPullReplicas();

    if (cluster.getLiveNodes().size() < totalReplicasPerShard) {
      throw new PlacementException("Cluster size too small for number of replicas per shard");
    }

    // Get number of cores on each Node
    TreeMultimap<Integer, Node> nodesByCores = TreeMultimap.create(Comparator.naturalOrder(), Ordering.arbitrary());

    // Get the number of cores on each node and sort the nodes by increasing number of cores
    for (Node node : cluster.getLiveNodes()) {
      // TODO: redo this. It is potentially less efficient to call propertyFetcher.getProperties() multiple times rather than once
      final PropertyKey coresCountPropertyKey = propertyFactory.createCoreCountKey(node);
      Map<PropertyKey, PropertyValue> propMap = propertyFetcher.fetchProperties(Collections.singleton(coresCountPropertyKey));
      PropertyValue returnedValue = propMap.get(coresCountPropertyKey);

      if (returnedValue == null) {
        throw new PlacementException("Can't get number of cores in " + node);
      }
      CoresCountPropertyValue coresCountPropertyValue = (CoresCountPropertyValue) returnedValue;
      nodesByCores.put(coresCountPropertyValue.getCoresCount(), node);
    }

    Set<ReplicaPlacement> replicaPlacements = new HashSet<>(totalReplicasPerShard * reqAddReplicas.getShardNames().size());

    // Now place all replicas of all shards on nodes, by placing on nodes with the smallest number of cores and taking
    // into account replicas placed during this computation. Note that for each shard we must place replicas on different
    // nodes, when moving to the next shard we use the nodes sorted by their updated number of cores (due to replica
    // placements for previous shards).
    for (String shardName : reqAddReplicas.getShardNames()) {
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

      placeReplicas(nodeEntriesToAssign, placementPlanFactory, replicaPlacements, shardName, reqAddReplicas.getCountNrtReplicas(), Replica.ReplicaType.NRT);
      placeReplicas(nodeEntriesToAssign, placementPlanFactory, replicaPlacements, shardName, reqAddReplicas.getCountTlogReplicas(), Replica.ReplicaType.TLOG);
      placeReplicas(nodeEntriesToAssign, placementPlanFactory, replicaPlacements, shardName, reqAddReplicas.getCountPullReplicas(), Replica.ReplicaType.PULL);
    }

    return placementPlanFactory.createPlacementPlanAddReplicas(reqAddReplicas, replicaPlacements);
  }

  private void placeReplicas(ArrayList<Map.Entry<Integer, Node>> nodeEntriesToAssign,
                             PlacementPlanFactory placementPlanFactory, Set<ReplicaPlacement> replicaPlacements,
                             String shardName, int countReplicas, Replica.ReplicaType replicaType) {
    for (int replica = 0; replica < countReplicas; replica++) {
      final Map.Entry<Integer, Node> entry = nodeEntriesToAssign.remove(0);
      final Node node = entry.getValue();

      replicaPlacements.add(placementPlanFactory.createReplicaPlacement(shardName, node, replicaType));
    }
  }
}
