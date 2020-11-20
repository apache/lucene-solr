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

package org.apache.solr.cluster.placement.impl;

import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.*;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit test for {@link AffinityPlacementFactory}
 */
public class AffinityPlacementFactoryTest extends Assert {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static PlacementPlugin plugin;

    @BeforeClass
    public static void setupPlugin() {
        PlacementPluginConfig config = PlacementPluginConfigImpl.createConfigFromProperties(
                Map.of("minimalFreeDiskGB", 10L, "deprioritizedFreeDiskGB", 50L));
        plugin = new AffinityPlacementFactory().createPluginInstance(config);
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
     *
     * When it places a replica for an existing collection, it should pick the node with more cores that doesn't already have a replica for the shard.
     */
    private void testBasicPlacementInternal(boolean hasExistingCollection) throws Exception {
        String collectionName = "testCollection";

        Node node1 = new ClusterAbstractionsForTest.NodeImpl("node1");
        Node node2 = new ClusterAbstractionsForTest.NodeImpl("node2");
        Set<Node> liveNodes = Set.of(node1, node2);

        ClusterAbstractionsForTest.SolrCollectionImpl solrCollection = new ClusterAbstractionsForTest.SolrCollectionImpl(collectionName, Map.of());
        // Make sure new collections are not visible in the cluster state and existing ones are
        final Map<String, SolrCollection> clusterCollections;
        final Map<String, Shard> shards;
        if (hasExistingCollection) {
            // An existing collection with a single replica on node 1. Note that new collections already exist by the time the plugin is called, but are empty
            shards = PluginTestHelper.createShardsAndReplicas(solrCollection, 1, 1, Set.of(node1));
            solrCollection.setShards(shards);
            clusterCollections = Map.of(solrCollection.getName(), solrCollection);
        } else {
            // A new collection has the shards defined ok but no replicas
            shards = PluginTestHelper.createShardsAndReplicas(solrCollection, 1, 0, Set.of());
            solrCollection.setShards(shards);
            clusterCollections = Map.of();
        }

        Cluster cluster = new ClusterAbstractionsForTest.ClusterImpl(liveNodes, clusterCollections);
        // Place a new replica for the (only) existing shard of the collection
        PlacementRequestImpl placementRequest = new PlacementRequestImpl(solrCollection, Set.of(shards.keySet().iterator().next()), liveNodes, 1, 0, 0);
        // More cores on node2
        Map<Node, Integer> nodeToCoreCount = Map.of(node1, 1, node2, 10);
        // A lot of free disk on the two nodes
        final Map<Node, Long> nodeToFreeDisk = Map.of(node1, 100L, node2, 100L);
        AttributeValues attributeValues = new AttributeValuesImpl(nodeToCoreCount, Map.of(), nodeToFreeDisk, Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
        AttributeFetcher attributeFetcher = new AttributeFetcherForTest(attributeValues);
        PlacementPlanFactory placementPlanFactory = new PlacementPlanFactoryImpl();

        PlacementPlan pp = plugin.computePlacement(cluster, placementRequest, attributeFetcher, placementPlanFactory);


        assertEquals(1, pp.getReplicaPlacements().size());
        ReplicaPlacement rp = pp.getReplicaPlacements().iterator().next();
        assertEquals(hasExistingCollection ? node2 : node1, rp.getNode());
    }

    @Test
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

        int REPLICAS_PER_SHARD = nrtReplicas + tlogReplicas + pullReplicas;
        int TOTAL_REPLICAS = numShards * REPLICAS_PER_SHARD;

        String collectionName = "testCollection";

        final Set<Node> liveNodes = new HashSet<>();
        final Map<Node, Long> nodeToFreeDisk = new HashMap<>();
        final Map<Node, Integer> nodeToCoreCount = new HashMap<>();
        for (int i = 0; i < numNodes; i++) {
            Node node = new ClusterAbstractionsForTest.NodeImpl("node_" + i);
            liveNodes.add(node);
            nodeToFreeDisk.put(node, Long.valueOf(numNodes));
            nodeToCoreCount.put(node, 0);
        }
        ClusterAbstractionsForTest.SolrCollectionImpl solrCollection = new ClusterAbstractionsForTest.SolrCollectionImpl(collectionName, Map.of());
        Map<String, Shard> shards = PluginTestHelper.createShardsAndReplicas(solrCollection, numShards, 0, Set.of());
        solrCollection.setShards(shards);

        Cluster cluster = new ClusterAbstractionsForTest.ClusterImpl(liveNodes, Map.of());
        PlacementRequestImpl placementRequest = new PlacementRequestImpl(solrCollection, shards.keySet(), liveNodes,
            nrtReplicas, tlogReplicas, pullReplicas);

        AttributeValues attributeValues = new AttributeValuesImpl(nodeToCoreCount, Map.of(), nodeToFreeDisk, Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
        AttributeFetcher attributeFetcher = new AttributeFetcherForTest(attributeValues);
        PlacementPlanFactory placementPlanFactory = new PlacementPlanFactoryImpl();

        long start = System.nanoTime();
        PlacementPlan pp = plugin.computePlacement(cluster, placementRequest, attributeFetcher, placementPlanFactory);
        long end = System.nanoTime();
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
