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

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
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
 * <p>This factory is instantiated by config from its class name. Using it is the only way to create instances of
 * {@link AffinityPlacementPlugin}.</p>
 *
 * <p>In order to configure this plugin to be used for placement decisions, the following {@code curl} command (or something
 * equivalent) has to be executed once the cluster is already running in order to set
 * the appropriate Zookeeper stored configuration. Replace {@code localhost:8983} by one of your servers' IP address and port.</p>
 *
 * <pre>
 *
 * curl -X POST -H 'Content-type:application/json' -d '{
 * "add": {
 *   "name": ".placement-plugin",
 *   "class": "org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory",
 *   "config": {
 *     "minimalFreeDiskGB": 10,
 *     "prioritizedFreeDiskGB": 50
 *   }
 * }
 * }' http://localhost:8983/api/cluster/plugin
 * </pre>
 *
 * <p>In order to delete the placement-plugin section (and to fallback to either Legacy
 * or rule based placement if configured for a collection), execute:</p>
 *
 * <pre>
 *
 * curl -X POST -H 'Content-type:application/json' -d '{
 * "remove" : ".placement-plugin"
 * }' http://localhost:8983/api/cluster/plugin
 * </pre>
 *
 *
 * <p>{@link AffinityPlacementPlugin} implements placing replicas in a way that replicate past Autoscaling config defined
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
 *     For each shard on which placing replicas is required and then for each replica type to place (starting with NRT,
 *     then TLOG then PULL): <ul>
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
 */
public class AffinityPlacementFactory implements PlacementPluginFactory<AffinityPlacementConfig> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * <p>Name of the system property on a node indicating which (public cloud) Availability Zone that node is in. The value
   * is any string, different strings denote different availability zones.
   *
   * <p>Nodes on which this system property is not defined are considered being in the same Availability Zone
   * {@link #UNDEFINED_AVAILABILITY_ZONE} (hopefully the value of this constant is not the name of a real Availability Zone :).
   */
  public static final String AVAILABILITY_ZONE_SYSPROP = "availability_zone";

  /**
   * <p>Name of the system property on a node indicating the type of replicas allowed on that node.
   * The value of that system property is a comma separated list or a single string of value names of
   * {@link org.apache.solr.cluster.Replica.ReplicaType} (case insensitive). If that property is not defined, that node is
   * considered accepting all replica types (i.e. undefined is equivalent to {@code "NRT,Pull,tlog"}).
   */
  public static final String REPLICA_TYPE_SYSPROP = "replica_type";

  /**
   * This is the "AZ" name for nodes that do not define an AZ. Should not match a real AZ name (I think we're safe)
   */
  public static final String UNDEFINED_AVAILABILITY_ZONE = "uNd3f1NeD";

  private AffinityPlacementConfig config = AffinityPlacementConfig.DEFAULT;

  /**
   * Empty public constructor is used to instantiate this factory. Using a factory pattern to allow the factory to do one
   * time costly operations if needed, and to only have to instantiate a default constructor class by name, rather than
   * having to call a constructor with more parameters (if we were to instantiate the plugin class directly without going
   * through a factory).
   */
  public AffinityPlacementFactory() {
  }

  @Override
  public PlacementPlugin createPluginInstance() {
    return new AffinityPlacementPlugin(config.minimalFreeDiskGB, config.prioritizedFreeDiskGB);
  }

  @Override
  public void configure(AffinityPlacementConfig cfg) {
    Objects.requireNonNull(cfg, "configuration must never be null");
    this.config = cfg;
  }

  @Override
  public AffinityPlacementConfig getConfig() {
    return config;
  }

  /**
   * See {@link AffinityPlacementFactory} for instructions on how to configure a cluster to use this plugin and details
   * on what the plugin does.
   */
  static class AffinityPlacementPlugin implements PlacementPlugin {

    private final long minimalFreeDiskGB;

    private final long prioritizedFreeDiskGB;

    private final Random replicaPlacementRandom = new Random(); // ok even if random sequence is predictable.

    /**
     * The factory has decoded the configuration for the plugin instance and passes it the parameters it needs.
     */
    private AffinityPlacementPlugin(long minimalFreeDiskGB, long prioritizedFreeDiskGB) {
      this.minimalFreeDiskGB = minimalFreeDiskGB;
      this.prioritizedFreeDiskGB = prioritizedFreeDiskGB;

      // We make things reproducible in tests by using test seed if any
      String seed = System.getProperty("tests.seed");
      if (seed != null) {
        replicaPlacementRandom.setSeed(seed.hashCode());
      }
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
      Set<String> availabilityZones = getZonesFromNodes(nodes, attrValues);

      // Build the replica placement decisions here
      Set<ReplicaPlacement> replicaPlacements = new HashSet<>();

      // Let's now iterate on all shards to create replicas for and start finding home sweet homes for the replicas
      for (String shardName : request.getShardNames()) {
        // Inventory nodes (if any) that already have a replica of any type for the shard, because we can't be placing
        // additional replicas on these. This data structure is updated after each replica to node assign and is used to
        // make sure different replica types are not allocated to the same nodes (protecting same node assignments within
        // a given replica type is done "by construction" in makePlacementDecisions()).
        Set<Node> nodesWithReplicas = new HashSet<>();
        Shard shard = solrCollection.getShard(shardName);
        if (shard != null) {
          for (Replica r : shard.replicas()) {
            nodesWithReplicas.add(r.getNode());
          }
        }

        // Iterate on the replica types in the enum order. We place more strategic replicas first
        // (NRT is more strategic than TLOG more strategic than PULL). This is in case we eventually decide that less
        // strategic replica placement impossibility is not a problem that should lead to replica placement computation
        // failure. Current code does fail if placement is impossible (constraint is at most one replica of a shard on any node).
        for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
          makePlacementDecisions(solrCollection, shardName, availabilityZones, replicaType, request.getCountReplicasToCreate(replicaType),
              attrValues, replicaTypeToNodes, nodesWithReplicas, coresOnNodes, placementPlanFactory, replicaPlacements);
        }
      }

      return placementPlanFactory.createPlacementPlan(request, replicaPlacements);
    }

    private Set<String> getZonesFromNodes(Set<Node> nodes, final AttributeValues attrValues) {
      Set<String> azs = new HashSet<>();

      for (Node n : nodes) {
        azs.add(getNodeAZ(n, attrValues));
      }

      return Collections.unmodifiableSet(azs);
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
     * Availability Zone. Instances are used as values in a {@link java.util.TreeMap} in which the total number of already
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
     *
     * @param nodes      all nodes on which this plugin should compute placement
     * @param attrValues attributes fetched for the nodes. This method uses system property {@link #REPLICA_TYPE_SYSPROP} as
     *                   well as the number of cores on each node.
     */
    private Pair<EnumMap<Replica.ReplicaType, Set<Node>>, Map<Node, Integer>> getNodesPerReplicaType(Set<Node> nodes, final AttributeValues attrValues) {
      EnumMap<Replica.ReplicaType, Set<Node>> replicaTypeToNodes = new EnumMap<>(Replica.ReplicaType.class);
      Map<Node, Integer> coresOnNodes = new HashMap<>();

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
        }
        if (attrValues.getFreeDisk(node).get() < minimalFreeDiskGB) {
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
     *     <li>Balance as much as possible replicas of a given {@link org.apache.solr.cluster.Replica.ReplicaType} over available AZ's.
     *     This balancing takes into account existing replicas <b>of the corresponding replica type</b>, if any.</li>
     *     <li>Place replicas if possible on nodes having more than a certain amount of free disk space (note that nodes with a too small
     *     amount of free disk space were eliminated as placement targets earlier, in {@link #getNodesPerReplicaType}). There's
     *     a threshold here rather than sorting on the amount of free disk space, because sorting on that value would in
     *     practice lead to never considering the number of cores on a node.</li>
     *     <li>Place replicas on nodes having a smaller number of cores (the number of cores considered
     *     for this decision includes previous placement decisions made during the processing of the placement request)</li>
     * </ol>
     */
    @SuppressForbidden(reason = "Ordering.arbitrary() has no equivalent in Comparator class. Rather reuse than copy.")
    private void makePlacementDecisions(SolrCollection solrCollection, String shardName, Set<String> availabilityZones,
                                        Replica.ReplicaType replicaType, int numReplicas, final AttributeValues attrValues,
                                        EnumMap<Replica.ReplicaType, Set<Node>> replicaTypeToNodes, Set<Node> nodesWithReplicas,
                                        Map<Node, Integer> coresOnNodes, PlacementPlanFactory placementPlanFactory,
                                        Set<ReplicaPlacement> replicaPlacements) throws PlacementException {
      // Count existing replicas per AZ. We count only instances of the type of replica for which we need to do placement.
      // If we ever want to balance replicas of any type across AZ's (and not each replica type balanced independently),
      // we'd have to move this data structure to the caller of this method so it can be reused across different replica
      // type placements for a given shard. Note then that this change would be risky. For example all NRT's and PULL
      // replicas for a shard my be correctly balanced over three AZ's, but then all NRT can end up in the same AZ...
      Map<String, Integer> azToNumReplicas = new HashMap<>();
      for (String az : availabilityZones) {
        azToNumReplicas.put(az, 0);
      }

      // Build the set of candidate nodes for the placement, i.e. nodes that can accept the replica type
      Set<Node> candidateNodes = new HashSet<>(replicaTypeToNodes.get(replicaType));
      // Remove nodes that already have a replica for the shard (no two replicas of same shard can be put on same node)
      candidateNodes.removeAll(nodesWithReplicas);

      Shard shard = solrCollection.getShard(shardName);
      if (shard != null) {
        // shard is non null if we're adding replicas to an already existing collection.
        // If we're creating the collection, the shards do not exist yet.
        for (Replica replica : shard.replicas()) {
          // The node's AZ is counted as having a replica if it has a replica of the same type as the one we need
          // to place here.
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
      Map<String, List<Node>> nodesPerAz = new HashMap<>();
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

      CoresAndDiskComparator coresAndDiskComparator = new CoresAndDiskComparator(attrValues, coresOnNodes, prioritizedFreeDiskGB);

      for (int i = 0; i < numReplicas; i++) {
        // We have for each AZ on which we might have a chance of placing a replica, the list of candidate nodes for replicas
        // (candidate: does not already have a replica of this shard and is in the corresponding AZ).
        // Among the AZ's with the minimal number of replicas of the given replica type for the shard, we must pick the AZ that
        // offers the best placement (based on number of cores and free disk space). In order to do so, for these "minimal" AZ's
        // we sort the nodes from best to worst placement candidate (based on the number of cores and free disk space) then pick
        // the AZ that has the best best node. We don't sort all AZ's because that will not necessarily be needed.
        int minNumberOfReplicasPerAz = 0; // This value never observed but compiler can't tell
        Set<Map.Entry<Integer, AzWithNodes>> candidateAzEntries = null;
        // Iterate over AZ's (in the order of increasing number of replicas on that AZ) and do two things: 1. remove those AZ's that
        // have no nodes, no use iterating over these again and again (as we compute placement for more replicas), and 2. collect
        // all those AZ with a minimal number of replicas.
        for (Iterator<Map.Entry<Integer, AzWithNodes>> it = azByExistingReplicas.entries().iterator(); it.hasNext(); ) {
          Map.Entry<Integer, AzWithNodes> entry = it.next();
          int numberOfNodes = entry.getValue().availableNodesForPlacement.size();
          if (numberOfNodes == 0) {
            it.remove();
          } else { // AZ does have node(s) for placement
            if (candidateAzEntries == null) {
              // First AZ with nodes that can take the replica. Initialize tracking structures
              minNumberOfReplicasPerAz = numberOfNodes;
              candidateAzEntries = new HashSet<>();
            }
            if (minNumberOfReplicasPerAz != numberOfNodes) {
              // AZ's with more replicas than the minimum number seen are not placement candidates
              break;
            }
            candidateAzEntries.add(entry);
            // We remove all entries that are candidates: the "winner" will be modified, all entries might also be sorted,
            // so we'll insert back the updated versions later.
            it.remove();
          }
        }

        if (candidateAzEntries == null) {
          // This can happen because not enough nodes for the placement request or already too many nodes with replicas of
          // the shard that can't accept new replicas or not enough nodes with enough free disk space.
          throw new PlacementException("Not enough nodes to place " + numReplicas + " replica(s) of type " + replicaType +
              " for shard " + shardName + " of collection " + solrCollection.getName());
        }

        // Iterate over all candidate AZ's, sort them if needed and find the best one to use for this placement
        Map.Entry<Integer, AzWithNodes> selectedAz = null;
        Node selectedAzBestNode = null;
        for (Map.Entry<Integer, AzWithNodes> candidateAzEntry : candidateAzEntries) {
          AzWithNodes azWithNodes = candidateAzEntry.getValue();
          List<Node> nodes = azWithNodes.availableNodesForPlacement;

          if (!azWithNodes.hasBeenSorted) {
            // Make sure we do not tend to use always the same nodes (within an AZ) if all conditions are identical (well, this
            // likely is not the case since after having added a replica to a node its number of cores increases for the next
            // placement decision, but let's be defensive here, given that multiple concurrent placement decisions might see
            // the same initial cluster state, and we want placement to be reasonable even in that case without creating an
            // unnecessary imbalance).
            // For example, if all nodes have 0 cores and same amount of free disk space, ideally we want to pick a random node
            // for placement, not always the same one due to some internal ordering.
            Collections.shuffle(nodes, replicaPlacementRandom);

            // Sort by increasing number of cores but pushing nodes with low free disk space to the end of the list
            nodes.sort(coresAndDiskComparator);

            azWithNodes.hasBeenSorted = true;
          }

          // Which one is better, the new one or the previous best?
          if (selectedAz == null || coresAndDiskComparator.compare(nodes.get(0), selectedAzBestNode) < 0) {
            selectedAz = candidateAzEntry;
            selectedAzBestNode = nodes.get(0);
          }
        }

        // Now actually remove the selected node from the winning AZ
        AzWithNodes azWithNodes = selectedAz.getValue();
        List<Node> nodes = selectedAz.getValue().availableNodesForPlacement;
        Node assignTarget = nodes.remove(0);

        // Insert back all the qualifying but non winning AZ's removed while searching for the one
        for (Map.Entry<Integer, AzWithNodes> removedAzs : candidateAzEntries) {
          if (removedAzs != selectedAz) {
            azByExistingReplicas.put(removedAzs.getKey(), removedAzs.getValue());
          }
        }

        // Insert back a corrected entry for the winning AZ: one more replica living there and one less node that can accept new replicas
        // (the remaining candidate node list might be empty, in which case it will be cleaned up on the next iteration).
        azByExistingReplicas.put(selectedAz.getKey() + 1, azWithNodes);

        // Do not assign that node again for replicas of other replica type for this shard
        // (this update of the set is not useful in the current execution of this method but for following ones only)
        nodesWithReplicas.add(assignTarget);

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
      private final long prioritizedFreeDiskGB;


      /**
       * The data we sort on is not part of the {@link Node} instances but has to be retrieved from the attributes and configuration.
       * The number of cores per node is passed in a map whereas the free disk is fetched from the attributes due to the
       * fact that we update the number of cores per node as we do allocations, but we do not update the free disk. The
       * attrValues corresponding to the number of cores per node are the initial values, but we want to compare the actual
       * value taking into account placement decisions already made during the current execution of the placement plugin.
       */
      CoresAndDiskComparator(AttributeValues attrValues, Map<Node, Integer> coresOnNodes, long prioritizedFreeDiskGB) {
        this.attrValues = attrValues;
        this.coresOnNodes = coresOnNodes;
        this.prioritizedFreeDiskGB = prioritizedFreeDiskGB;
      }

      @Override
      public int compare(Node a, Node b) {
        // Note all nodes do have free disk defined. This has been verified earlier.
        boolean aHasLowFreeSpace = attrValues.getFreeDisk(a).get() < prioritizedFreeDiskGB;
        boolean bHasLowFreeSpace = attrValues.getFreeDisk(b).get() < prioritizedFreeDiskGB;
        if (aHasLowFreeSpace != bHasLowFreeSpace) {
          // A node with low free space should be considered > node with high free space since it needs to come later in sort order
          return Boolean.compare(aHasLowFreeSpace, bHasLowFreeSpace);
        }
        // The ordering on the number of cores is the natural order.
        return Integer.compare(coresOnNodes.get(a), coresOnNodes.get(b));
      }
    }
  }
}

