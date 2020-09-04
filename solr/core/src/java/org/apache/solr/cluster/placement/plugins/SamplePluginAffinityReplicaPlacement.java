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

import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import org.apache.solr.cluster.placement.*;
import org.apache.solr.common.util.SuppressForbidden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;

/**
 * <p>Implements placing replicas in a way that replicate past Autoscaling config defined
 * <a href="https://github.com/lucidworks/fusion-cloud-native/blob/master/policy.json#L16">here</a>.</p>
 *
 * <p>This specification is doing the following:
 * <p><i>Spread replicas per shard as evenly as possible across multiple availability zones (given by a sys prop),
 * assign replicas based on replica type to specific kinds of nodes (another sys prop), and avoid having more than
 * one replica per shard on the same node.<br>
 * Only after these constraints are satisfied do minimize cores per node or disk usage.</i></p>
 *
 * <p>Overall strategy of this plugin:</p>
 * <ul><li>
 *     The set of nodes in the cluster is obtained and transformed into 3 independent sets (that can overlap) of nodes
 *     accepting each of the three replica types.
 * </li><li>
 *     For each shard on which placing replicas is required and then for each replica type to place (starting with NRT, then TLOG then PULL): <ul>
 *         <li>The set of candidates nodes corresponding to the replica type is used and from that set are removed nodes
 *         that already have a replica (of any type) for that shard</li>
 *         <li>If there are not enough nodes, either an error is thrown or the replica(s) in excess are not added. Likely something
 *         to be governed by per replica type configuration (i.e. throw error if NRT can't be created but
 *         skip "silently" if PULL can't be created? TODO Do we need a soft error reporting mechanism?)<br>
 *         This check likely happens in the following steps but called out separately here.</li>
 *         <li>The number of (already existing) replicas of the current type on each Availability Zone is collected.</li>
 *         <li>Separate the set of available nodes to as many subsets (possibly some are empty) as there are Availability Zones
 *         defined for the candidate nodes</li>
 *         <li>In each AZ nodes subset, sort the nodes by increasing total number of cores count, with possibly a condition
 *         that pushes nodes with low disk space to the end of the list? Or a weighted combination of the relative
 *         importance of these two factors? Some randomization? Marking as non available nodes with not enough disk space?
 *         These and other are likely aspects to be played with once the plugin is tested or observed to be running in prod,
 *         don't expect the initial code drop(s) to do all of that.</li>
 *         <li>Iterate over the number of replicas to place (for the current replica type for the current shard):
 *         <ul>
 *             <li>Based on the number of replicas per AZ collected previously, pick the non empty set of nodes having the
 *             lowest number of replicas. Then pick the first node in that set. That's the node the replica is placed one.
 *             Remove the node from the set of available nodes for the given AZ and increase the number of replicas placed
 *             on that AZ.</li>
 *         </ul></li>
 *         <li>During this process, the number of cores on the nodes in general is tracked to take into account placement
 *         decisions so that not all shards decide to put their replicas on the same nodes (they might though if these are
 *         the less loaded nodes).</li>
 *     </ul>
 * </li>
 * </ul>
 *
 * TODO: disclaimer: code not tested and never really run
 */
public class SamplePluginAffinityReplicaPlacement implements PlacementPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * <p>Name of the system property on a node indicating which (public cloud) Availability Zone that node is in. The value
   * is any string, different strings denote different availability zones.
   *
   * <p>Nodes on which this system property is not defined are considered being in the same Availability Zone.
   */
  public static final String AVAILABILITY_ZONE_SYSPROP = "availability_zone";

  /**
   * <p>Name of the system property on a node indicating the type of replicas allowed on that node.
   * The value of that system property is a comma separated list or a single string of value names of
   * {@link org.apache.solr.cluster.placement.Replica.ReplicaType} (case insensitive). If that property is not defined, that node is
   * considered accepting all replica types (i.e. undefined is equivalent to {@code "NRT,Pull,tlog"}).
   *
   * <p>See {@link #getNodesPerReplicaType}.
   */
  public static final String REPLICA_TYPE_SYSPROP = "replica_type";

  private final PlacementPluginConfig config;

  private SamplePluginAffinityReplicaPlacement(PlacementPluginConfig config) {
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
      return new SamplePluginAffinityReplicaPlacement(config);
    }
  }

  @SuppressForbidden(reason = "Ordering.arbitrary() has no equivalent in Comparator class. Rather reuse than copy.")
  public PlacementPlan computePlacement(Cluster cluster, PlacementRequest placementRequest, AttributeFetcher attributeFetcher,
                                        PlacementPlanFactory placementPlanFactory) throws PlacementException {
    if (!(placementRequest instanceof AddReplicasPlacementRequest)) {
      throw new PlacementException("This plugin only supports adding replicas, no support for " + placementRequest.getClass().getName());
    }

    final AddReplicasPlacementRequest request = (AddReplicasPlacementRequest) placementRequest;

    Set<Node> nodes = request.getTargetNodes();
    SolrCollection solrCollection = request.getCollection();

    // Request all needed attributes
    attributeFetcher.requestNodeSystemProperty(AVAILABILITY_ZONE_SYSPROP).requestNodeSystemProperty(REPLICA_TYPE_SYSPROP);
    attributeFetcher.requestNodeCoreCount().requestNodeFreeDisk();
    attributeFetcher.fetchFrom(nodes);
    AttributeValues attrValues = attributeFetcher.fetchAttributes();

    // Split the set of nodes into 3 sets of nodes accepting each replica type (sets can overlap if nodes accept multiple replica types)
    // These subsets sets are actually maps, because we capture the number of cores (of any replica type) present on each node.
    // The EnumMap below maps a replica type to a map of nodes -> number of cores on the node
    EnumMap<Replica.ReplicaType, Map<Node, Integer>> replicaTypeToNodesAndCounts = getNodesPerReplicaType(nodes, attrValues);

    // Build the replic placement decisions here
    Set<ReplicaPlacement> replicaPlacements = new HashSet<>();

    // Let's now iterate on all shards to create replicas for and start finding home sweet homes for the replicas
    for (String shardName : request.getShardNames()) {
      // Iterate on the replica types in the enum order. We place more strategic replicas first
      // (NRT is more strategic than TLOG more strategic than PULL). This is in case we eventually decide that less
      // strategic replica placement impossibility is not a problem that should lead to replica placement computation
      // failure. Current code does fail if placement is impossible (constraint is at most one replica of a shard on any node).
      for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
        makePlacementDecisions(solrCollection, shardName, replicaType, request.getCountReplicasToCreate(replicaType),
                attrValues, replicaTypeToNodesAndCounts, replicaPlacements);
      }
    }



    // WIP - continue here.







    final int totalReplicasPerShard = request.getCountNrtReplicas() +
        request.getCountTlogReplicas() + request.getCountPullReplicas();

    if (cluster.getLiveNodes().size() < totalReplicasPerShard) {
      throw new PlacementException("Cluster size too small for number of replicas per shard");
    }

    // Get number of cores on each Node
    TreeMultimap<Integer, Node> nodesByCores = TreeMultimap.create(Comparator.naturalOrder(), Ordering.arbitrary());

    // Get the number of cores on each node and sort the nodes by increasing number of cores
    for (Node node : cluster.getLiveNodes()) {
      if (attrValues.getCoresCount(node).isEmpty()) {
        throw new PlacementException("Can't get number of cores in " + node);
      }
      nodesByCores.put(attrValues.getCoresCount(node).get(), node);
    }


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

      placeReplicas(nodeEntriesToAssign, placementPlanFactory, replicaPlacements, shardName, request.getCountNrtReplicas(), Replica.ReplicaType.NRT);
      placeReplicas(nodeEntriesToAssign, placementPlanFactory, replicaPlacements, shardName, request.getCountTlogReplicas(), Replica.ReplicaType.TLOG);
      placeReplicas(nodeEntriesToAssign, placementPlanFactory, replicaPlacements, shardName, request.getCountPullReplicas(), Replica.ReplicaType.PULL);
    }

    return placementPlanFactory.createPlacementPlanAddReplicas(request, replicaPlacements);
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

  /**
   * Given the set of all nodes on which to do placement and fetched attributes, builds the maps representing
   * candidate nodes for placement of replicas of each replica type. The map values are the number of cores on these nodes.
   * These maps are packaged and returned in an EnumMap keyed by replica type.
   * Nodes for which the number of cores is not available for whatever reason are excluded from acceptable candidate nodes
   * as it would not be possible to make any meaningful placement decisions.
   * @param nodes all nodes on which this plugin should compute placement
   * @param attrValues attributes fetched for the nodes. This method uses system property {@link #REPLICA_TYPE_SYSPROP} as
   *                   well as the number of cores on each node.
   */
  private EnumMap<Replica.ReplicaType, Map<Node, Integer>> getNodesPerReplicaType(Set<Node> nodes, AttributeValues attrValues) {
    EnumMap<Replica.ReplicaType, Map<Node, Integer>> replicaTypeToNodesAndCounts = new EnumMap<>(Replica.ReplicaType.class);
    for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
      replicaTypeToNodesAndCounts.put(replicaType, Maps.newHashMap());
    }

    for (Node node : nodes) {
      if (attrValues.getCoresCount(node).isEmpty()) {
        if (log.isWarnEnabled()) {
          log.warn("Unknown number of cores on node {}, skipping for placement decisions.", node.getName());
        }
        continue;
      }

      Integer coresCount = attrValues.getCoresCount(node).get();

      String supportedReplicaTypes = attrValues.getSystemProperty(node, REPLICA_TYPE_SYSPROP).isPresent() ? attrValues.getSystemProperty(node, REPLICA_TYPE_SYSPROP).get() : null;
      // If property not defined or is only whitespace on a node, assuming node can take any replica type
      if (supportedReplicaTypes == null || supportedReplicaTypes.isBlank()) {
        for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
          replicaTypeToNodesAndCounts.get(rt).put(node, coresCount);
        }
      } else {
        Set<String> acceptedTypes = Arrays.stream(supportedReplicaTypes.split(",")).map(String::trim).map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
        for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
          if (acceptedTypes.contains(rt.name().toLowerCase(Locale.ROOT))) {
            replicaTypeToNodesAndCounts.get(rt).put(node, coresCount);
          }
        }
      }
    }
    return replicaTypeToNodesAndCounts;
  }

  /**
   * <p>Picks nodes from {@code targetNodes} for placing {@code numReplicas} replicas.
   *
   * <p>The criteria used in this method are, in this order:
   * <ol>
   *     <li>No more than one replica of a given shard on a given node (strictly enforced)</li>
   *     <li>Balance as much as possible the number of replicas of the given {@link org.apache.solr.cluster.placement.Replica.ReplicaType} over available AZ's.
   *     This balancing takes into account existing replicas of the corresponding type, if any.</li>
   *     <li>Place replicas on nodes having more than a certain amount of free disk space (note that nodes with a too small
   *     amount of free disk space were eliminated as placement targets earlier, see TODO</li>
   *     <li>Place replicas on nodes having a smaller number of cores (the number of cores considered
   *     for this decision includes decisions made during the processing of the placement request)</li>
   * </ol>
   */
  private void makePlacementDecisions(SolrCollection solrCollection, String shardName, Replica.ReplicaType replicaType,
                                      int numReplicas, AttributeValues attrValues,
                                      EnumMap<Replica.ReplicaType, Map<Node, Integer>> replicaTypeToNodesAndCounts,
                                      Set<ReplicaPlacement> replicaPlacements) {
    // Build the set of candidate nodes, i.e. nodes not having (yet) a replica of the given shard
    Set<Node> candidateNodes = new HashSet<>(replicaTypeToNodesAndCounts.get(replicaType).keySet());
    Shard shard = solrCollection.getShard(shardName);
    if (shard != null) {
      // shard is non null if we're adding replicas to an already existing collection.
      // If we're creating the collection, the shards do not exist yet.
      for (Replica replica : shard.replicas()) {
        candidateNodes.remove(replica.getNode());
      }
    }

    // We now have the set of real candidate nodes, we've enforced "No more than one replica of a given shard on a given node"

    // WIP - continue here

  }
}
