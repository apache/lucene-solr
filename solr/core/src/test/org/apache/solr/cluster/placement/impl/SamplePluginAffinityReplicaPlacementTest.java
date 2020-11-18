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
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.placement.*;
import org.apache.solr.cluster.placement.plugins.SamplePluginAffinityReplicaPlacement;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

/**
 * Unit test for {@link SamplePluginAffinityReplicaPlacement}
 */
public class SamplePluginAffinityReplicaPlacementTest extends Assert {

    private static PlacementPlugin plugin;

    @BeforeClass
    public static void setupCluster() {
        PlacementPluginConfig config = PlacementPluginConfigImpl.createConfigFromProperties(
                Map.of("minimalFreeDiskGB", 10L, "deprioritizedFreeDiskGB", 50L));
        plugin = new SamplePluginAffinityReplicaPlacement.Factory().createPluginInstance(config);
    }

    // FIXME IG: this test fails (expecting placement on node1 but placement happens on node2)
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
//        Set<Node> liveNodes = ClusterAbstractionsForTest.NodeImpl.getNodes(Set.of("node1", "node2", "node3"));
        Set<Node> liveNodes = Set.of(node1, node2);
        ClusterAbstractionsForTest.SolrCollectionImpl solrCollection = new ClusterAbstractionsForTest.SolrCollectionImpl(collectionName, Map.of());
        // An existing collection with a single replica on node 1
        Map<String, Shard> shards = PluginTestHelper.createShardsAndReplicas(solrCollection, 1, 1, Set.of(node1));
        solrCollection.setShards(shards);

        Cluster cluster = new ClusterAbstractionsForTest.ClusterImpl(liveNodes, hasExistingCollection ? Map.of(solrCollection.getName(), solrCollection) : Map.of());
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
}
