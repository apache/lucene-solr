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
import org.apache.solr.cluster.placement.impl.PlacementRequestImpl;
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

  @BeforeClass
  public static void setupPlugin() {
    AffinityPlacementConfig config = new AffinityPlacementConfig(10L, 50L);
    AffinityPlacementFactory factory = new AffinityPlacementFactory();
    factory.configure(config);
    plugin = factory.createPluginInstance();
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
    String collectionName = "testCollection";

    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeNodes(2);
    LinkedList<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getNodeBuilders();
    nodeBuilders.get(0).setCoreCount(1).setFreeDiskGB(100L);
    nodeBuilders.get(1).setCoreCount(10).setFreeDiskGB(100L);

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

  @Test
  public void testAvailabilityZones() throws Exception {
    String collectionName = "testCollection";
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
    String collectionName = "testCollection";
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
    String collectionName = "testCollection";
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

  @Test
  //@Ignore
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

  private void runTestScalability(int numNodes, int numShards,
                                  int nrtReplicas, int tlogReplicas,
                                  int pullReplicas) throws Exception {

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
