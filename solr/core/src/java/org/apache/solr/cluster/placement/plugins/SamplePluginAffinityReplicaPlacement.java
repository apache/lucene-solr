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

import com.google.common.collect.*;
import org.apache.solr.cluster.*;
import org.apache.solr.cluster.placement.*;
import org.apache.solr.common.util.Pair;
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
 *         <li>If there are not enough nodes, an error is thrown (this is checked further down during processing).</li>
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
 * <p>This code is a realistic placement computation, based on a few assumptions. The code is written in such a way to
 * make it relatively easy to adapt it to (somewhat) different assumptions. Configuration options could be introduced
 * to allow configuration base option selection as well...</p>
 *
 * <p>In order to configure this plugin to be used for placement decisions, the following {@code curl} command (or something
 * equivalent) has to be executed once the cluster is already running in order to set
 * the appropriate Zookeeper stored configuration. Replace {@code localhost:8983} by one of your servers' IP address and port.</p>
 *
 * <pre>
 *
  curl -X POST -H 'Content-type:application/json' -d '{
    "set-placement-plugin": {
      "class": "org.apache.solr.cluster.placement.plugins.SamplePluginAffinityReplicaPlacement$Factory",
      "minimalFreeDiskGB": 10,
      "deprioritizedFreeDiskGB": 50
    }
  }' http://localhost:8983/api/cluster
 * </pre>
 *
 * <p>The consequence will be the creation of an element in the Zookeeper file {@code /clusterprops.json} as follows:</p>
 *
 * <pre>
 *
 * "placement-plugin":{
 *     "class":"org.apache.solr.cluster.placement.plugins.SamplePluginAffinityReplicaPlacement$Factory",
 *     "minimalFreeDiskGB":10,
 *     "deprioritizedFreeDiskGB":50}
 * </pre>
 *
 * <p>In order to delete the placement-plugin section from {@code /clusterprops.json} (and to fallback to either Legacy
 * or rule based placement if configured for a collection), execute:</p>
 *
 * <pre>
 *
  curl -X POST -H 'Content-type:application/json' -d '{
    "set-placement-plugin" : null
  }' http://localhost:8983/api/cluster
 * </pre>
 */
public class SamplePluginAffinityReplicaPlacement implements PlacementPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * This factory is instantiated by config from its class name. Using it is the only way to create instances of
   * {@link SamplePluginAffinityReplicaPlacement}.
   */
  static public class Factory implements PlacementPluginFactory {

    /**
     * Empty public constructor is used to instantiate this factory. Using a factory pattern to allow the factory to do one
     * time costly operations if needed, and to only have to instantiate a default constructor class by name, rather than
     * having to call a constructor with more parameters (if we were to instantiate the plugin class directly without going
     * through a factory).
     */
    public Factory() {
    }

    @Override
    public PlacementPlugin createPluginInstance(PlacementPluginConfig config) {
      final long minimalFreeDiskGB = config.getLongConfig("minimalFreeDiskGB", 20L);
      final long deprioritizedFreeDiskGB = config.getLongConfig("deprioritizedFreeDiskGB", 100L);
      return new SamplePluginAffinityReplicaPlacement(minimalFreeDiskGB, deprioritizedFreeDiskGB);
    }
  }


  /**
   * <p>Name of the system property on a node indicating which (public cloud) Availability Zone that node is in. The value
   * is any string, different strings denote different availability zones.
   *
   * <p>Nodes on which this system property is not defined are considered being in the same Availability Zone
   * {@link #UNDEFINED_AVAILABILITY_ZONE} (hopefully the value of this constant is not the name of a real Availability Zone :).
   */
  public static final String AVAILABILITY_ZONE_SYSPROP = "availability_zone";
  /** This is the "AZ" name for nodes that do not define an AZ. Should not match a real AZ name (I think we're safe) */
  public static final String UNDEFINED_AVAILABILITY_ZONE = "uNd3f1NeD";

  /**
   * <p>Name of the system property on a node indicating the type of replicas allowed on that node.
   * The value of that system property is a comma separated list or a single string of value names of
   * {@link org.apache.solr.cluster.Replica.ReplicaType} (case insensitive). If that property is not defined, that node is
   * considered accepting all replica types (i.e. undefined is equivalent to {@code "NRT,Pull,tlog"}).
   *
   * <p>See {@link #getNodesPerReplicaType}.
   */
  public static final String REPLICA_TYPE_SYSPROP = "replica_type";

  /**
   * If a node has strictly less GB of free disk than this value, the node is excluded from assignment decisions.
   * Set to 0 or less to disable.
   */
  private final long minimalFreeDiskGB;

  /**
   * Replica allocation will assign replicas to nodes with at least this number of GB of free disk space regardless
   * of the number of cores on these nodes rather than assigning replicas to nodes with less than this amount of free
   * disk space if that's an option (if that's not an option, replicas can still be assigned to nodes with less than this
   * amount of free space).
   */
  private final long deprioritizedFreeDiskGB;

  /**
   * The factory has decoded the configuration for the plugin instance and passes it the parameters it needs.
   */
  private SamplePluginAffinityReplicaPlacement(long minimalFreeDiskGB, long deprioritizedFreeDiskGB) {
    this.minimalFreeDiskGB = minimalFreeDiskGB;
    this.deprioritizedFreeDiskGB = deprioritizedFreeDiskGB;
  }

  @SuppressForbidden(reason = "Ordering.arbitrary() has no equivalent in Comparator class. Rather reuse than copy.")
  public PlacementPlan computePlacement(Cluster cluster, PlacementRequest request, AttributeFetcher attributeFetcher,
                                        PlacementPlanFactory placementPlanFactory) throws PlacementException {
    Set<Node> nodes = request.getTargetNodes();
    SolrCollection solrCollection = request.getCollection();

    // Request all needed attributes
    attributeFetcher.requestNodeSystemProperty(AVAILABILITY_ZONE_SYSPROP).requestNodeSystemProperty(REPLICA_TYPE_SYSPROP);
    attributeFetcher.requestNodeCoreCount().requestNodeFreeDisk();
    attributeFetcher.fetchFrom(nodes);
    final AttributeValues attrValues = attributeFetcher.fetchAttributes();

    // Split the set of nodes into 3 sets of nodes accepting each replica type (sets can overlap if nodes accept multiple replica types)
    // These subsets sets are actually maps, because we capture the number of cores (of any replica type) present on each node.
    // Also get the number of currently existing cores per node, so we can keep update as we place new cores to not end up
    // always selecting the same node(s).
    Pair<EnumMap<Replica.ReplicaType, Set<Node>>, Map<Node, Integer>> p = getNodesPerReplicaType(nodes, attrValues);

    EnumMap<Replica.ReplicaType, Set<Node>> replicaTypeToNodes = p.first();
    Map<Node, Integer> coresOnNodes = p.second();

    // All available zones of live nodes. Due to some nodes not being candidates for placement, and some existing replicas
    // being one availability zones that might be offline (i.e. their nodes are not live), this set might contain zones
    // on which it is impossible to place replicas. That's ok.
    ImmutableSet<String> availabilityZones = getZonesFromNodes(nodes, attrValues);

    // Build the replica placement decisions here
    Set<ReplicaPlacement> replicaPlacements = new HashSet<>();

    // Let's now iterate on all shards to create replicas for and start finding home sweet homes for the replicas
    for (String shardName : request.getShardNames()) {
      // Iterate on the replica types in the enum order. We place more strategic replicas first
      // (NRT is more strategic than TLOG more strategic than PULL). This is in case we eventually decide that less
      // strategic replica placement impossibility is not a problem that should lead to replica placement computation
      // failure. Current code does fail if placement is impossible (constraint is at most one replica of a shard on any node).
      for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
        makePlacementDecisions(solrCollection, shardName, availabilityZones, replicaType, request.getCountReplicasToCreate(replicaType),
                attrValues, replicaTypeToNodes, coresOnNodes, placementPlanFactory, replicaPlacements);
      }
    }

    return placementPlanFactory.createPlacementPlan(request, replicaPlacements);
  }

  private ImmutableSet<String> getZonesFromNodes(Set<Node> nodes, final AttributeValues attrValues) {
    Set<String> azs = new HashSet<>();

    for (Node n : nodes) {
      azs.add(getNodeAZ(n, attrValues));
    }

    return ImmutableSet.copyOf(azs);
  }

  /**
   * Resolves the AZ of a node and takes care of nodes that have no defined AZ in system property {@link #AVAILABILITY_ZONE_SYSPROP}
   * to then return {@link #UNDEFINED_AVAILABILITY_ZONE} as the AZ name.
   */
  private String getNodeAZ(Node n, final AttributeValues attrValues) {
    Optional<String> nodeAz = attrValues.getSystemProperty(n, AVAILABILITY_ZONE_SYSPROP);
    // All nodes with undefined AZ will be considered part of the same AZ. This also works for deployments that do not care about AZ's
    return nodeAz.orElse(UNDEFINED_AVAILABILITY_ZONE);
  }

  /**
   * This class captures an availability zone and the nodes that are legitimate targets for replica placement in that
   * Availability Zone. Instances are used as values in a {@link TreeMap} in which the total number of already
   * existing replicas in the AZ is the key. This allows easily picking the set of nodes from which to select a node for
   * placement in order to balance the number of replicas per AZ. Picking one of the nodes from the set is done using
   * different criteria unrelated to the Availability Zone (picking the node is based on the {@link CoresAndDiskComparator}
   * ordering).
   */
  private static class AzWithNodes {
    final String azName;
    List<Node> availableNodesForPlacement;
    boolean hasBeenSorted;

    AzWithNodes(String azName, List<Node> availableNodesForPlacement) {
      this.azName = azName;
      this.availableNodesForPlacement = availableNodesForPlacement;
      // Once the list is sorted to an order we're happy with, this flag is set to true to avoid sorting multiple times
      // unnecessarily.
      this.hasBeenSorted = false;
    }
  }

  /**
   * Given the set of all nodes on which to do placement and fetched attributes, builds the sets representing
   * candidate nodes for placement of replicas of each replica type.
   * These sets are packaged and returned in an EnumMap keyed by replica type (1st member of the Pair).
   * Also builds the number of existing cores on each node present in the returned EnumMap (2nd member of the returned Pair).
   * Nodes for which the number of cores is not available for whatever reason are excluded from acceptable candidate nodes
   * as it would not be possible to make any meaningful placement decisions.
   * @param nodes all nodes on which this plugin should compute placement
   * @param attrValues attributes fetched for the nodes. This method uses system property {@link #REPLICA_TYPE_SYSPROP} as
   *                   well as the number of cores on each node.
   */
  private Pair<EnumMap<Replica.ReplicaType, Set<Node>>, Map<Node, Integer>> getNodesPerReplicaType(Set<Node> nodes, final AttributeValues attrValues) {
    EnumMap<Replica.ReplicaType, Set<Node>> replicaTypeToNodes = new EnumMap<>(Replica.ReplicaType.class);
    Map<Node, Integer> coresOnNodes = Maps.newHashMap();

    for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
      replicaTypeToNodes.put(replicaType, new HashSet<>());
    }

    for (Node node : nodes) {
      // Exclude nodes with unknown or too small disk free space
      if (attrValues.getFreeDisk(node).isEmpty()) {
        if (log.isWarnEnabled()) {
          log.warn("Unknown free disk on node {}, excluding it from placement decisions.", node.getName());
        }
        // We rely later on the fact that the free disk optional is present (see CoresAndDiskComparator), be careful it you change anything here.
        continue;
      } if (attrValues.getFreeDisk(node).get() < minimalFreeDiskGB) {
        if (log.isWarnEnabled()) {
          log.warn("Node {} free disk ({}GB) lower than configured minimum {}GB, excluding it from placement decisions.", node.getName(), attrValues.getFreeDisk(node).get(), minimalFreeDiskGB);
        }
        continue;
      }

      if (attrValues.getCoresCount(node).isEmpty()) {
        if (log.isWarnEnabled()) {
          log.warn("Unknown number of cores on node {}, excluding it from placement decisions.", node.getName());
        }
        // We rely later on the fact that the number of cores optional is present (see CoresAndDiskComparator), be careful it you change anything here.
        continue;
      }

      Integer coresCount = attrValues.getCoresCount(node).get();
      coresOnNodes.put(node, coresCount);

      String supportedReplicaTypes = attrValues.getSystemProperty(node, REPLICA_TYPE_SYSPROP).isPresent() ? attrValues.getSystemProperty(node, REPLICA_TYPE_SYSPROP).get() : null;
      // If property not defined or is only whitespace on a node, assuming node can take any replica type
      if (supportedReplicaTypes == null || supportedReplicaTypes.isBlank()) {
        for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
          replicaTypeToNodes.get(rt).add(node);
        }
      } else {
        Set<String> acceptedTypes = Arrays.stream(supportedReplicaTypes.split(",")).map(String::trim).map(s -> s.toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
        for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
          if (acceptedTypes.contains(rt.name().toLowerCase(Locale.ROOT))) {
            replicaTypeToNodes.get(rt).add(node);
          }
        }
      }
    }
    return new Pair<>(replicaTypeToNodes, coresOnNodes);
  }

  /**
   * <p>Picks nodes from {@code targetNodes} for placing {@code numReplicas} replicas.
   *
   * <p>The criteria used in this method are, in this order:
   * <ol>
   *     <li>No more than one replica of a given shard on a given node (strictly enforced)</li>
   *     <li>Balance as much as possible the number of replicas of the given {@link org.apache.solr.cluster.Replica.ReplicaType} over available AZ's.
   *     This balancing takes into account existing replicas <b>of the corresponding replica type</b>, if any.</li>
   *     <li>Place replicas is possible on nodes having more than a certain amount of free disk space (note that nodes with a too small
   *     amount of free disk space were eliminated as placement targets earlier, in {@link #getNodesPerReplicaType}). There's
   *     a threshold here rather than sorting on the amount of free disk space, because sorting on that value would in
   *     practice lead to never considering the number of cores on a node.</li>
   *     <li>Place replicas on nodes having a smaller number of cores (the number of cores considered
   *     for this decision includes decisions made during the processing of the placement request)</li>
   * </ol>
   */
  @SuppressForbidden(reason = "Ordering.arbitrary() has no equivalent in Comparator class. Rather reuse than copy.")
  private void makePlacementDecisions(SolrCollection solrCollection, String shardName, ImmutableSet<String> availabilityZones,
                                      Replica.ReplicaType replicaType, int numReplicas, final AttributeValues attrValues,
                                      EnumMap<Replica.ReplicaType, Set<Node>> replicaTypeToNodes, Map<Node, Integer> coresOnNodes,
                                      PlacementPlanFactory placementPlanFactory, Set<ReplicaPlacement> replicaPlacements) throws PlacementException {
    // Build the set of candidate nodes, i.e. nodes not having (yet) a replica of the given shard
    Set<Node> candidateNodes = new HashSet<>(replicaTypeToNodes.get(replicaType));

    // Count existing replicas per AZ. We count only instances the type of replica for which we need to do placement. This
    // can be changed in the loop below if we want to count all replicas for the shard.
    Map<String, Integer> azToNumReplicas = Maps.newHashMap();
    // Add all "interesting" AZ's, i.e. AZ's for which there's a chance we can do placement.
    for (String az : availabilityZones) {
      azToNumReplicas.put(az, 0);
    }

    Shard shard = solrCollection.getShard(shardName);
    if (shard != null) {
      // shard is non null if we're adding replicas to an already existing collection.
      // If we're creating the collection, the shards do not exist yet.
      for (Replica replica : shard.replicas()) {
        // Nodes already having any type of replica for the shard can't get another replica.
        candidateNodes.remove(replica.getNode());
        // The node's AZ has to be counted as having a replica if it has a replica of the same type as the one we need
        // to place here (remove the "if" below to balance the number of replicas per AZ across all replica types rather
        // than within each replica type, but then there's a risk that all NRT replicas for example end up on the same AZ).
        // Note that if in the cluster nodes are configured to accept a single replica type and not multiple ones, the
        // two options are equivalent (governed by system property AVAILABILITY_ZONE_SYSPROP on each node)
        if (replica.getType() == replicaType) {
          final String az = getNodeAZ(replica.getNode(), attrValues);
          if (azToNumReplicas.containsKey(az)) {
            // We do not count replicas on AZ's for which we don't have any node to place on because it would not help
            // the placement decision. If we did want to do that, note the dereferencing below can't be assumed as the
            // entry will not exist in the map.
            azToNumReplicas.put(az, azToNumReplicas.get(az) + 1);
          }
        }
      }
    }

    // We now have the set of real candidate nodes, we've enforced "No more than one replica of a given shard on a given node".
    // We also counted for the shard and replica type under consideration how many replicas were per AZ, so we can place
    // (or try to place) replicas on AZ's that have fewer replicas

    // Get the candidate nodes per AZ in order to build (further down) a mapping of AZ to placement candidates.
    Map<String, List<Node>> nodesPerAz = Maps.newHashMap();
    for (Node node : candidateNodes) {
      String nodeAz = getNodeAZ(node, attrValues);
      List<Node> nodesForAz = nodesPerAz.computeIfAbsent(nodeAz, k -> new ArrayList<>());
      nodesForAz.add(node);
    }

    // Build a treeMap sorted by the number of replicas per AZ and including candidates nodes suitable for placement on the
    // AZ, so we can easily select the next AZ to get a replica assignment and quickly (constant time) decide if placement
    // on this AZ is possible or not.
    TreeMultimap<Integer, AzWithNodes> azByExistingReplicas = TreeMultimap.create(Comparator.naturalOrder(), Ordering.arbitrary());
    for (Map.Entry<String, List<Node>> e : nodesPerAz.entrySet()) {
      azByExistingReplicas.put(azToNumReplicas.get(e.getKey()), new AzWithNodes(e.getKey(), e.getValue()));
    }

    CoresAndDiskComparator coresAndDiskComparator = new CoresAndDiskComparator(attrValues, coresOnNodes, deprioritizedFreeDiskGB);

    // Now we have for each AZ on which we might have a chance of placing a replica, the list of candidate nodes for replicas
    // (candidate: does not already have a replica of this shard and is in the corresponding AZ).
    // We must now select those of the nodes on which we actually place the replicas, and will do that based on the total
    // number of cores already present on these nodes as well as the free disk space.
    // We sort once by the order related to number of cores and disk space each list of nodes on an AZ. We do not sort all
    // of them ahead of time because we might be placing a small number of replicas and it might be wasted work.
    for (int i = 0; i < numReplicas; i++) {
      // Pick the AZ having the lowest number of replicas for this shard, and if that AZ has available nodes, pick the
      // most appropriate one (based on number of cores and disk space constraints). In the process, remove entries (AZ's)
      // that do not have nodes to place replicas on because these are useless to us.
      Map.Entry<Integer, AzWithNodes> azWithNodesEntry = null;
      for (Iterator<Map.Entry<Integer, AzWithNodes>> it = azByExistingReplicas.entries().iterator(); it.hasNext(); ) {
        Map.Entry<Integer, AzWithNodes> entry = it.next();
        if (!entry.getValue().availableNodesForPlacement.isEmpty()) {
          azWithNodesEntry = entry;
          // Remove this entry. Will add it back after a node has been removed from the list of available nodes and the number
          // of replicas on the AZ has been increased by one (search for "azByExistingReplicas.put" below).
          it.remove();
          break;
        } else {
          it.remove();
        }
      }

      if (azWithNodesEntry == null) {
        // This can happen because not enough nodes for the placement request or already too many nodes with replicas of
        // the shard that can't accept new replicas or not enough nodes with enough free disk space.
        throw new PlacementException("Not enough nodes to place " + numReplicas + " replica(s) of type " + replicaType +
                " for shard " + shardName + " of collection " + solrCollection.getName());
      }

      AzWithNodes azWithNodes = azWithNodesEntry.getValue();
      List<Node> nodes = azWithNodes.availableNodesForPlacement;

      if (!azWithNodes.hasBeenSorted) {
        // Make sure we do not tend to use always the same nodes (within an AZ) if all conditions are identical (well, this
        // likely is not the case since after having added a replica to a node its number of cores increases for the next
        // placement decision, but let's be defensive here, given that multiple concurrent placement decisions might see
        // the same initial cluster state, and we want placement to be reasonable even in that case without creating an
        // unnecessary imbalance).
        // For example, if all nodes have 0 cores and same amount of free disk space, ideally we want to pick a random node
        // for placement, not always the same one due to some internal ordering.
        Collections.shuffle(nodes, new Random());

        // Sort by increasing number of cores but pushing nodes with low free disk space to the end of the list
        nodes.sort(coresAndDiskComparator);

        azWithNodes.hasBeenSorted = true;
      }

      Node assignTarget = nodes.remove(0);

      // Insert back a corrected entry for the AZ: one more replica living there and one less node that can accept new replicas
      // (the remaining candidate node list might be empty, in which case it will be cleaned up on the next iteration).
      azByExistingReplicas.put(azWithNodesEntry.getKey() + 1, azWithNodes);

      // Track that the node has one more core. These values are only used during the current run of the plugin.
      coresOnNodes.merge(assignTarget, 1, Integer::sum);

      // Register the replica assignment just decided
      replicaPlacements.add(placementPlanFactory.createReplicaPlacement(solrCollection, shardName, assignTarget, replicaType));
    }
  }

  /**
   * Comparator implementing the placement strategy based on free space and number of cores: we want to place new replicas
   * on nodes with the less number of cores, but only if they do have enough disk space (expressed as a threshold value).
   */
  static class CoresAndDiskComparator implements Comparator<Node> {
    private final AttributeValues attrValues;
    private final Map<Node, Integer> coresOnNodes;
    private final long deprioritizedFreeDiskGB;


    /**
     * The data we sort on is not part of the {@link Node} instances but has to be retrieved from the attributes and configuration.
     * The number of cores per node is passed in a map whereas the free disk is fetched from the attributes due to the
     * fact that we update the number of cores per node as we do allocations, but we do not update the free disk. The
     * attrValues correpsonding to the number of cores per node are the initial values, but we want to comapre the actual
     * value taking into account placement decisions already made during the current execution of the placement plugin.
     */
    CoresAndDiskComparator(AttributeValues attrValues, Map<Node, Integer> coresOnNodes, long deprioritizedFreeDiskGB) {
      this.attrValues = attrValues;
      this.coresOnNodes = coresOnNodes;
      this.deprioritizedFreeDiskGB = deprioritizedFreeDiskGB;
    }

    @Override
    public int compare(Node a, Node b) {
      // Note all nodes do have free disk defined. This has been verified earlier.
      boolean aHasLowFreeSpace = attrValues.getFreeDisk(a).get() < deprioritizedFreeDiskGB;
      boolean bHasLowFreeSpace = attrValues.getFreeDisk(b).get() < deprioritizedFreeDiskGB;
      if (aHasLowFreeSpace != bHasLowFreeSpace) {
        // A node with low free space should be considered > node with high free space since it needs to come later in sort order
        return Boolean.compare(aHasLowFreeSpace, bHasLowFreeSpace);
      }
      // The ordering on the number of cores is the natural order.
      return Integer.compare(coresOnNodes.get(a), coresOnNodes.get(b));
    }
  }
}
