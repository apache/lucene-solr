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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.*;
import org.apache.solr.cluster.placement.Builders;
import org.apache.solr.cluster.placement.impl.PlacementPlanFactoryImpl;
import org.apache.solr.cluster.placement.impl.PlacementPluginConfigImpl;
import org.apache.solr.cluster.placement.impl.PlacementRequestImpl;
import org.apache.solr.common.util.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Unit test for {@link AffinityPlacementFactory}
 */
public class AffinityPlacementFactoryTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static PlacementPlugin plugin;

  private final static long MINIMAL_FREE_DISK_GB = 10L;
  private final static long PRIORITIZED_FREE_DISK_GB = 50L;

  @BeforeClass
  public static void setupPlugin() {
    PlacementPluginConfig config = PlacementPluginConfigImpl.createConfigFromProperties(
        Map.of("minimalFreeDiskGB", MINIMAL_FREE_DISK_GB, "prioritizedFreeDiskGB", PRIORITIZED_FREE_DISK_GB));
    plugin = new AffinityPlacementFactory().createPluginInstance(config);
    ((AffinityPlacementFactory.AffinityPlacementPlugin) plugin).setRandom(random());
  }

  @Test
  public void testBasicPlacementNewCollection() throws Exception {
    testBasicPlacementInternal(false);
  }

  @Test
  public void testBasicPlacementExistingCollection() throws Exception {
    testBasicPlacementInternal(true);
  }

  /**
   * When this test places a replica for a new collection, it should pick the node with less cores.<p>
   * <p>
   * When it places a replica for an existing collection, it should pick the node with more cores that doesn't already have a replica for the shard.
   */
  private void testBasicPlacementInternal(boolean hasExistingCollection) throws Exception {
    String collectionName = "basicCollection";

    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeNodes(2);
    LinkedList<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getNodeBuilders();
    nodeBuilders.get(0).setCoreCount(1).setFreeDiskGB(PRIORITIZED_FREE_DISK_GB + 1);
    nodeBuilders.get(1).setCoreCount(10).setFreeDiskGB(PRIORITIZED_FREE_DISK_GB + 1);

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);

    if (hasExistingCollection) {
      // Existing collection has replicas for its shards and is visible in the cluster state
      collectionBuilder.initializeShardsReplicas(1, 1, 0, 0, nodeBuilders);
      clusterBuilder.addCollection(collectionBuilder);
    } else {
      // New collection to create has the shards defined but no replicas and is not present in cluster state
      collectionBuilder.initializeShardsReplicas(1, 0, 0, 0, List.of());
    }

    Cluster cluster = clusterBuilder.build();
    AttributeFetcher attributeFetcher = clusterBuilder.buildAttributeFetcher();

    SolrCollection solrCollection = collectionBuilder.build();
    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    // Place a new replica for the (only) existing shard of the collection
    PlacementRequestImpl placementRequest = new PlacementRequestImpl(solrCollection,
        Set.of(solrCollection.shards().iterator().next().getShardName()), new HashSet<>(liveNodes),
        1, 0, 0);

    PlacementPlan pp = plugin.computePlacement(cluster, placementRequest, attributeFetcher, new PlacementPlanFactoryImpl());

    assertEquals(1, pp.getReplicaPlacements().size());
    ReplicaPlacement rp = pp.getReplicaPlacements().iterator().next();
    assertEquals(hasExistingCollection ? liveNodes.get(1) : liveNodes.get(0), rp.getNode());
  }

  /**
   * Test not placing replicas on nodes low free disk unless no other option
   */
  @Test
  public void testLowSpaceNode() throws Exception {
    String collectionName = "lowSpaceCollection";

    final int LOW_SPACE_NODE_INDEX = 0;
    final int NO_SPACE_NODE_INDEX = 1;


    // Cluster nodes and their attributes
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeNodes(4);
    LinkedList<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getNodeBuilders();
    nodeBuilders.get(LOW_SPACE_NODE_INDEX).setCoreCount(1).setFreeDiskGB(MINIMAL_FREE_DISK_GB + 1); // Low space
    nodeBuilders.get(NO_SPACE_NODE_INDEX).setCoreCount(10).setFreeDiskGB(1L); // Really not enough space
    nodeBuilders.get(2).setCoreCount(10).setFreeDiskGB(PRIORITIZED_FREE_DISK_GB + 1);
    nodeBuilders.get(3).setCoreCount(10).setFreeDiskGB(PRIORITIZED_FREE_DISK_GB + 1);
    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    // The collection to create (shards are defined but no replicas)
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(3, 0, 0, 0, List.of());
    SolrCollection solrCollection = collectionBuilder.build();

    // Place two replicas of each type for each shard
    PlacementRequestImpl placementRequest = new PlacementRequestImpl(solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes),
        2, 2, 2);

    PlacementPlan pp = plugin.computePlacement(clusterBuilder.build(), placementRequest, clusterBuilder.buildAttributeFetcher(), new PlacementPlanFactoryImpl());

    assertEquals(18, pp.getReplicaPlacements().size()); // 3 shards, 6 replicas total each
    // Verify no two replicas of same type of same shard placed on same node
    Set<Pair<Pair<Replica.ReplicaType, String>, Node>> placements = new HashSet<>();
    for (ReplicaPlacement rp : pp.getReplicaPlacements()) {
      assertTrue("two replicas of same type for same shard placed on same node",
          placements.add(new Pair<>(new Pair<>(rp.getReplicaType(), rp.getShardName()), rp.getNode())));
      assertNotEquals("Replica unnecessarily placed on node with low free space", rp.getNode(), liveNodes.get(LOW_SPACE_NODE_INDEX));
      assertNotEquals("Replica placed on node with not enough free space", rp.getNode(), liveNodes.get(NO_SPACE_NODE_INDEX));
    }

    // Verify that if we ask for 3 replicas, the placement will use the low free space node
    // Place two replicas of each type for each shard
    placementRequest = new PlacementRequestImpl(solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes),
        3, 0, 0);
    pp = plugin.computePlacement(clusterBuilder.build(), placementRequest, clusterBuilder.buildAttributeFetcher(), new PlacementPlanFactoryImpl());
    assertEquals(9, pp.getReplicaPlacements().size()); // 3 shards, 3 replicas each
    // Verify no two replicas of same shard placed on same node
    placements = new HashSet<>();
    for (ReplicaPlacement rp : pp.getReplicaPlacements()) {
      assertEquals("Only NRT replicas should be created", Replica.ReplicaType.NRT, rp.getReplicaType());
      assertTrue("two replicas for same shard placed on same node",
          placements.add(new Pair<>(new Pair<>(rp.getReplicaType(), rp.getShardName()), rp.getNode())));
      assertNotEquals("Replica placed on node with not enough free space", rp.getNode(), liveNodes.get(NO_SPACE_NODE_INDEX));
    }

    // Verify that if we ask for 4 replicas, the placement will fail
    try {
      placementRequest = new PlacementRequestImpl(solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes),
          4, 0, 0);
      plugin.computePlacement(clusterBuilder.build(), placementRequest, clusterBuilder.buildAttributeFetcher(), new PlacementPlanFactoryImpl());
      fail("Placing 4 replicas should not be possible given only three nodes have enough space");
    } catch (PlacementException e) {
      // expected
    }
  }

  /**
   * Test that existing collection replicas are taken into account to not place more than one replica of a given type
   * for a given shard per node
   */
  @Test
  public void testPlacementWithExistingReplicas() throws Exception {
    String collectionName = "existingCollection";

    final int LOW_SPACE_NODE_INDEX = 0;
    final int NO_SPACE_NODE_INDEX = 1;


    // Cluster nodes and their attributes
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeNodes(4);
    LinkedList<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getNodeBuilders();
    int coresOnNode = 10;
    for (Builders.NodeBuilder nodeBuilder : nodeBuilders) {
      nodeBuilder.setCoreCount(coresOnNode).setFreeDiskGB(PRIORITIZED_FREE_DISK_GB + 1);
      coresOnNode += 10;
    }

    // The collection already exists with shards and replicas
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    // Build the collection letting the code pick up nodes...
    collectionBuilder.initializeShardsReplicas(2, 2, 1, 0, nodeBuilders);
    // Now explicitly change the nodes to create the collection distribution we want to test:
    // (note this collection is an illegal placement: shard 1 has two replicas on node 0. The placement plugin should still
    // be able to place new replicas as long as they don't break the rules).
    //  +--------------+----+----+----+----+
    //  |         Node |  0 |  1 |  2 |  3 |
    //  |Cores on node | 10 | 20 | 30 | 40 |
    //  +----------------------------------+
    //  |   Shard 1:   |    |    |    |    |
    //  |         NRT  |  X |    |    |  X |
    //  |         TLOG |  X |    |    |    |
    //  +----------------------------------+
    //  |   Shard 2:   |    |    |    |    |
    //  |         NRT  |    |  X |    |  X |
    //  |         TLOG |    |    |  X |    |
    //  +--------------+----+----+----+----+

    // The code below is not ideal... We only modify the parts of the collection that we need to change (replica nodes).
    // If this only happens in this test then it is likely the simplest approach.
    // If we need to do this elsewhere, we need to allow the collection builder to accept a sharding/replication
    // description and place the replicas accordingly on nodes.
    List<Builders.ShardBuilder> shardBuilders = collectionBuilder.getShardBuilders();
    List<Builders.ReplicaBuilder> replicas;
    Builders.ReplicaBuilder replica;

    // Replicas of shard 1
    replicas = shardBuilders.get(1 - 1).getReplicaBuilders();

    // Shard 1 first NRT goes to node 0
    replica = replicas.get(0);
    assertEquals(Replica.ReplicaType.NRT, replica.getReplicaType());
    replica.setReplicaNode(nodeBuilders.get(0));

    // Shard 1 second NRT goes to node 3
    replica = replicas.get(1);
    assertEquals(Replica.ReplicaType.NRT, replica.getReplicaType());
    replica.setReplicaNode(nodeBuilders.get(3));

    // Shard 1 TLOG goes to node 0
    replica = replicas.get(2);
    assertEquals(Replica.ReplicaType.TLOG, replica.getReplicaType());
    replica.setReplicaNode(nodeBuilders.get(0));

    // Replicas of shard 2
    replicas = shardBuilders.get(2 - 1).getReplicaBuilders();

    // Shard 2 first NRT goes to node 1
    replica = replicas.get(0);
    assertEquals(Replica.ReplicaType.NRT, replica.getReplicaType());
    replica.setReplicaNode(nodeBuilders.get(1));

    // Shard 2 second NRT goes to node 3
    replica = replicas.get(1);
    assertEquals(Replica.ReplicaType.NRT, replica.getReplicaType());
    replica.setReplicaNode(nodeBuilders.get(3));

    // Shard 2 TLOG goes to node 2
    replica = replicas.get(2);
    assertEquals(Replica.ReplicaType.TLOG, replica.getReplicaType());
    replica.setReplicaNode(nodeBuilders.get(2));

    SolrCollection solrCollection = collectionBuilder.build();



    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    // We now request placement of 1 NRT and 1 TLOG for each shard. They must be placed on the most appropriate nodes,
    // i.e. those that do not already have a replica for the shard and then on the node with the lowest
    // number of cores. NRT are placed first.
    // We therefore expect the placement of the new replicas to look like:
    //  +--------------+----+----+----+----+
    //  |         Node |  0 |  1 |  2 |  3 |
    //  |Cores on node | 10 | 20 | 30 | 40 |
    //  +----------------------------------+
    //  |   Shard 1:   |    |    |    |    |
    //  |         NRT  |  X |  N |    |  X |
    //  |         TLOG |  X |    |  N |    |
    //  +----------------------------------+
    //  |   Shard 2:   |    |    |    |    |
    //  |         NRT  |  N |  X |    |  X |
    //  |         TLOG |    |  N |  X |    | <-- We don't really expect this. It should be impossible to place this TLOG with 4 nodes
    //  +--------------+----+----+----+----+


    // Place two replicas of each type for each shard
    PlacementRequestImpl placementRequest = new PlacementRequestImpl(solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes),
        1, 1, 0);

    PlacementPlan pp = plugin.computePlacement(clusterBuilder.build(), placementRequest, clusterBuilder.buildAttributeFetcher(), new PlacementPlanFactoryImpl());

    // Let's check that the new replicas are placed where expected.
    Set<ReplicaPlacement> replicaPlacements = pp.getReplicaPlacements();

    // Each expected placement is represented as a string "shard replica-type node"
    Set<String> expectedPlacements = Set.of("1 NRT 1", "1 TLOG 2", "2 NRT 0", "2 TLOG 1");
    verifyPlacements(expectedPlacements, replicaPlacements, shardBuilders, liveNodes);
  }


  /**
   * Verifies that a computed set of placements does match the expected placement on nodes.
   * @param expectedPlacements a set of strings of the form {@code "1 NRT 3"} where 1 would be the shard index, NRT the
   *                           replica type and 3 the node on which the replica is placed. Shards are 1 based. Nodes 0 based.
   */
  private static void verifyPlacements(Set<String> expectedPlacements, Set<ReplicaPlacement> computedPlacements,
                                       List<Builders.ShardBuilder> shardBuilders, List<Node> liveNodes) {
    assertEquals("Wrong number of computed placements", expectedPlacements.size(), computedPlacements.size());

    // Prepare structures for looking up shard name index and node index
    Map<String, Integer> shardNumbering = new HashMap<>();
    int index = 1; // first shard is 1 not 0
    for (Builders.ShardBuilder sb : shardBuilders) {
      shardNumbering.put(sb.getShardName(), index++);
    }
    Map<Node, Integer> nodeNumbering = new HashMap<>();
    index = 0;
    for (Node n : liveNodes) {
      nodeNumbering.put(n, index++);
    }

    Set<String> expected = new HashSet<>(expectedPlacements);
    for (ReplicaPlacement p : computedPlacements) {
      String lookUpPlacementResult = shardNumbering.get(p.getShardName()) + " " + p.getReplicaType().name() + " " +  nodeNumbering.get(p.getNode());
      assertTrue(expected.remove(lookUpPlacementResult));
    }
  }

  @Test
  public void testAvailabilityZones() throws Exception {
    String collectionName = "azCollection";
    int NUM_NODES = 6;
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeNodes(NUM_NODES);
    for (int i = 0; i < NUM_NODES; i++) {
      Builders.NodeBuilder nodeBuilder = clusterBuilder.getNodeBuilders().get(i);
      nodeBuilder.setCoreCount(0);
      nodeBuilder.setFreeDiskGB(100L);
      if (i < NUM_NODES / 2) {
        nodeBuilder.setSysprop(AffinityPlacementFactory.AVAILABILITY_ZONE_SYSPROP, "az1");
      } else {
        nodeBuilder.setSysprop(AffinityPlacementFactory.AVAILABILITY_ZONE_SYSPROP, "az2");
      }
    }

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(2, 0, 0, 0, clusterBuilder.getNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    Cluster cluster = clusterBuilder.build();

    SolrCollection solrCollection = cluster.getCollection(collectionName);

    PlacementRequestImpl placementRequest = new PlacementRequestImpl(solrCollection,
        StreamSupport.stream(solrCollection.shards().spliterator(), false)
            .map(Shard::getShardName).collect(Collectors.toSet()),
        cluster.getLiveNodes(), 2, 2, 2);

    PlacementPlanFactory placementPlanFactory = new PlacementPlanFactoryImpl();
    AttributeFetcher attributeFetcher = clusterBuilder.buildAttributeFetcher();
    PlacementPlan pp = plugin.computePlacement(cluster, placementRequest, attributeFetcher, placementPlanFactory);
    // 2 shards, 6 replicas
    assertEquals(12, pp.getReplicaPlacements().size());
    // shard -> AZ -> replica count
    Map<Replica.ReplicaType, Map<String, Map<String, AtomicInteger>>> replicas = new HashMap<>();
    AttributeValues attributeValues = attributeFetcher.fetchAttributes();
    for (ReplicaPlacement rp : pp.getReplicaPlacements()) {
      Optional<String> azOptional = attributeValues.getSystemProperty(rp.getNode(), AffinityPlacementFactory.AVAILABILITY_ZONE_SYSPROP);
      if (!azOptional.isPresent()) {
        fail("missing AZ sysprop for node " + rp.getNode());
      }
      String az = azOptional.get();
      replicas.computeIfAbsent(rp.getReplicaType(), type -> new HashMap<>())
          .computeIfAbsent(rp.getShardName(), shard -> new HashMap<>())
          .computeIfAbsent(az, zone -> new AtomicInteger()).incrementAndGet();
    }
    replicas.forEach((type, perTypeReplicas) -> {
      perTypeReplicas.forEach((shard, azCounts) -> {
        assertEquals("number of AZs", 2, azCounts.size());
        azCounts.forEach((az, count) -> {
          assertTrue("too few replicas shard=" + shard + ", type=" + type + ", az=" + az,
              count.get() >= 1);
        });
      });
    });
  }

  @Test
  public void testReplicaType() throws Exception {
    String collectionName = "replicaTypeCollection";
    int NUM_NODES = 6;
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeNodes(NUM_NODES);
    for (int i = 0; i < NUM_NODES; i++) {
      Builders.NodeBuilder nodeBuilder = clusterBuilder.getNodeBuilders().get(i);
      nodeBuilder.setCoreCount(0);
      nodeBuilder.setFreeDiskGB(100L);
      if (i < NUM_NODES / 2) {
        nodeBuilder.setSysprop(AffinityPlacementFactory.REPLICA_TYPE_SYSPROP, "Nrt,Tlog");
        nodeBuilder.setSysprop("group", "one");
      } else {
        nodeBuilder.setSysprop(AffinityPlacementFactory.REPLICA_TYPE_SYSPROP, "Pull, foobar");
        nodeBuilder.setSysprop("group", "two");
      }
    }

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(2, 0, 0, 0, clusterBuilder.getNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    Cluster cluster = clusterBuilder.build();

    SolrCollection solrCollection = cluster.getCollection(collectionName);

    PlacementRequestImpl placementRequest = new PlacementRequestImpl(solrCollection,
        StreamSupport.stream(solrCollection.shards().spliterator(), false)
            .map(Shard::getShardName).collect(Collectors.toSet()),
        cluster.getLiveNodes(), 2, 2, 2);

    PlacementPlanFactory placementPlanFactory = new PlacementPlanFactoryImpl();
    AttributeFetcher attributeFetcher = clusterBuilder.buildAttributeFetcher();
    PlacementPlan pp = plugin.computePlacement(cluster, placementRequest, attributeFetcher, placementPlanFactory);
    // 2 shards, 6 replicas
    assertEquals(12, pp.getReplicaPlacements().size());
    // shard -> group -> replica count
    Map<Replica.ReplicaType, Map<String, Map<String, AtomicInteger>>> replicas = new HashMap<>();
    AttributeValues attributeValues = attributeFetcher.fetchAttributes();
    for (ReplicaPlacement rp : pp.getReplicaPlacements()) {
      Optional<String> groupOptional = attributeValues.getSystemProperty(rp.getNode(), "group");
      if (!groupOptional.isPresent()) {
        fail("missing group sysprop for node " + rp.getNode());
      }
      String group = groupOptional.get();
      if (group.equals("one")) {
        assertTrue("wrong replica type in group one",
            (rp.getReplicaType() == Replica.ReplicaType.NRT) || rp.getReplicaType() == Replica.ReplicaType.TLOG);
      } else {
        assertEquals("wrong replica type in group two", Replica.ReplicaType.PULL, rp.getReplicaType());
      }
      replicas.computeIfAbsent(rp.getReplicaType(), type -> new HashMap<>())
          .computeIfAbsent(rp.getShardName(), shard -> new HashMap<>())
          .computeIfAbsent(group, g -> new AtomicInteger()).incrementAndGet();
    }
    replicas.forEach((type, perTypeReplicas) -> {
      perTypeReplicas.forEach((shard, groupCounts) -> {
        assertEquals("number of groups", 1, groupCounts.size());
        groupCounts.forEach((group, count) -> {
          assertTrue("too few replicas shard=" + shard + ", type=" + type + ", group=" + group,
              count.get() >= 1);
        });
      });
    });

  }

  @Test
  public void testFreeDiskConstraints() throws Exception {
    String collectionName = "freeDiskCollection";
    int NUM_NODES = 3;
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeNodes(NUM_NODES);
    Node smallNode = null;
    for (int i = 0; i < NUM_NODES; i++) {
      Builders.NodeBuilder nodeBuilder = clusterBuilder.getNodeBuilders().get(i);
      nodeBuilder.setCoreCount(0);
      if (i == 0) {
        // default minimalFreeDiskGB == 20
        nodeBuilder.setFreeDiskGB(1L);
        smallNode = nodeBuilder.build();
      } else {
        nodeBuilder.setFreeDiskGB(100L);
      }
    }

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(2, 0, 0, 0, clusterBuilder.getNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    Cluster cluster = clusterBuilder.build();

    SolrCollection solrCollection = cluster.getCollection(collectionName);

    PlacementRequestImpl placementRequest = new PlacementRequestImpl(solrCollection,
        StreamSupport.stream(solrCollection.shards().spliterator(), false)
            .map(Shard::getShardName).collect(Collectors.toSet()),
        cluster.getLiveNodes(), 2, 0, 2);

    PlacementPlanFactory placementPlanFactory = new PlacementPlanFactoryImpl();
    AttributeFetcher attributeFetcher = clusterBuilder.buildAttributeFetcher();
    PlacementPlan pp = plugin.computePlacement(cluster, placementRequest, attributeFetcher, placementPlanFactory);
    assertEquals(8, pp.getReplicaPlacements().size());
    for (ReplicaPlacement rp : pp.getReplicaPlacements()) {
      assertFalse("should not put any replicas on " + smallNode, rp.getNode().equals(smallNode));
    }
  }

  @Test @Slow
  public void testScalability() throws Exception {
    log.info("==== numNodes ====");
    runTestScalability(1000, 100, 40, 40, 20);
    runTestScalability(2000, 100, 40, 40, 20);
    runTestScalability(5000, 100, 40, 40, 20);
    runTestScalability(10000, 100, 40, 40, 20);
    runTestScalability(20000, 100, 40, 40, 20);
    log.info("==== numShards ====");
    runTestScalability(5000, 100, 40, 40, 20);
    runTestScalability(5000, 200, 40, 40, 20);
    runTestScalability(5000, 500, 40, 40, 20);
    runTestScalability(5000, 1000, 40, 40, 20);
    runTestScalability(5000, 2000, 40, 40, 20);
    log.info("==== numReplicas ====");
    runTestScalability(5000, 100, 100, 0, 0);
    runTestScalability(5000, 100, 200, 0, 0);
    runTestScalability(5000, 100, 500, 0, 0);
    runTestScalability(5000, 100, 1000, 0, 0);
    runTestScalability(5000, 100, 2000, 0, 0);
  }

  private void runTestScalability(int numNodes, int numShards, int nrtReplicas, int tlogReplicas, int pullReplicas) throws Exception {
    String collectionName = "scaleCollection";

    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeNodes(numNodes);
    LinkedList<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getNodeBuilders();
    for (int i = 0; i < numNodes; i++) {
      nodeBuilders.get(i).setCoreCount(0).setFreeDiskGB(Long.valueOf(numNodes));
    }

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(numShards, 0, 0, 0, List.of());

    Cluster cluster = clusterBuilder.build();
    AttributeFetcher attributeFetcher = clusterBuilder.buildAttributeFetcher();

    SolrCollection solrCollection = collectionBuilder.build();
    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    // Place replicas for all the shards of the (newly created since it has no replicas yet) collection
    PlacementRequestImpl placementRequest = new PlacementRequestImpl(solrCollection, solrCollection.getShardNames(),
        new HashSet<>(liveNodes), nrtReplicas, tlogReplicas, pullReplicas);

    long start = System.nanoTime();
    PlacementPlan pp = plugin.computePlacement(cluster, placementRequest, attributeFetcher, new PlacementPlanFactoryImpl());
    long end = System.nanoTime();

    final int REPLICAS_PER_SHARD = nrtReplicas + tlogReplicas + pullReplicas;
    final int TOTAL_REPLICAS = numShards * REPLICAS_PER_SHARD;

    log.info("ComputePlacement: {} nodes, {} shards, {} total replicas, elapsed time {} ms.", numNodes, numShards, TOTAL_REPLICAS, TimeUnit.NANOSECONDS.toMillis(end - start)); //nowarn
    assertEquals("incorrect number of calculated placements", TOTAL_REPLICAS,
        pp.getReplicaPlacements().size());
    // check that replicas are correctly placed
    Map<Node, AtomicInteger> replicasPerNode = new HashMap<>();
    Map<Node, Set<String>> shardsPerNode = new HashMap<>();
    Map<String, AtomicInteger> replicasPerShard = new HashMap<>();
    Map<Replica.ReplicaType, AtomicInteger> replicasByType = new HashMap<>();
    for (ReplicaPlacement placement : pp.getReplicaPlacements()) {
      replicasPerNode.computeIfAbsent(placement.getNode(), n -> new AtomicInteger()).incrementAndGet();
      shardsPerNode.computeIfAbsent(placement.getNode(), n -> new HashSet<>()).add(placement.getShardName());
      replicasByType.computeIfAbsent(placement.getReplicaType(), t -> new AtomicInteger()).incrementAndGet();
      replicasPerShard.computeIfAbsent(placement.getShardName(), s -> new AtomicInteger()).incrementAndGet();
    }
    int perNode = TOTAL_REPLICAS > numNodes ? TOTAL_REPLICAS / numNodes : 1;
    replicasPerNode.forEach((node, count) -> {
      assertEquals(count.get(), perNode);
    });
    shardsPerNode.forEach((node, names) -> {
      assertEquals(names.size(), perNode);
    });

    replicasPerShard.forEach((shard, count) -> {
      assertEquals(count.get(), REPLICAS_PER_SHARD);
    });
  }
}
