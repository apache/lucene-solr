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

package org.apache.solr.store.shared;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.util.BlobStoreUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests around core discovery from Zookeeper.
 */
public class SharedCoreDiscoveryTest extends SolrCloudSharedStoreTestCase {

  /**
   * Will be shut down in {@link SolrCloudTestCase#shutdownCluster()}
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupCluster(2);
  }

  @After
  public void teardownTest() throws Exception {
    cluster.deleteAllCollections();
  }

  /**
   * Tests that shared replica is discovered from zookeeper.
   * Also makes sure that replicas are discovered only on their specific nodes.
   */
  @Test
  public void testMissingSharedCoreIsDiscoveredFromZk() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String collectionName = "sharedCollection";
    int maxShardsPerNode = 1;
    int numReplicas = 1;
    String shardName = "shard1";
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = shardName;
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);

    // add second replica
    assertTrue(CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Replica.Type.SHARED)
        .process(cloudClient).isSuccess());

    // Verify that second replica is created
    waitForState("Timed-out waiting for second replica to be created", collectionName, clusterShape(1, 2));

    testCoreDiscovery(cloudClient, collectionName, true);
  }

  /**
   * Tests that non-shared replica is not discovered from zookeeper
   */
  @Test
  public void testMissingNonSharedCoreIsNotDiscoveredFromZk() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String collectionName = "nonSharedCollection";
    String shardName = "shard1";
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = shardName;
    CollectionAdminRequest.Create create = CollectionAdminRequest
        .createCollectionWithImplicitRouter(collectionName, "conf", shardNames, 0)
        .setSharedIndex(false)
        .setNrtReplicas(1);
    create.process(cloudClient);

    // Verify that collection was created
    waitForState("Timed-out waiting for collection to be created", collectionName, clusterShape(1, 1));

    // add second replica
    assertTrue(CollectionAdminRequest.addReplicaToShard(collectionName, "shard1", Type.NRT)
        .process(cloudClient).isSuccess());

    // Verify that second replica is created
    waitForState("Timed-out waiting for second replica to be created", collectionName, clusterShape(1, 2));

    testCoreDiscovery(cloudClient, collectionName, false);

  }

  /**
   * This creates a single-replica collection and make sures that the node without any replica
   * restarts successfully and without any core descriptor.
   */
  @Test
  public void testNodeWithNoReplicaStartsSuccessfully() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String collectionName = "sharedCollection";
    int maxShardsPerNode = 1;
    int numReplicas = 1;
    String shardName = "shard1";
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = shardName;
    setupSharedCollectionWithShardNames(collectionName, maxShardsPerNode, numReplicas, shardNames);

    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica shardLeaderReplica = collection.getLeader("shard1");

    JettySolrRunner nodeWithNoReplica = cluster.getReplicaJetty(shardLeaderReplica) == cluster.getJettySolrRunner(0) ?
        cluster.getJettySolrRunner(1) : cluster.getJettySolrRunner(0);

    cluster.stopJettySolrRunner(nodeWithNoReplica);

    cluster.waitForJettyToStop(nodeWithNoReplica);

    nodeWithNoReplica = cluster.startJettySolrRunner(nodeWithNoReplica, true);

    cluster.waitForNode(nodeWithNoReplica, /* seconds */ 30);

    assertTrue("Core container is not empty", nodeWithNoReplica.getCoreContainer().getCoreDescriptors().isEmpty());
  }

  /**
   * Starts with two replicas on two separate nodes.
   * 1. Make sure cores exist for both replicas and only on their specific nodes
   * 2. Stop nodes, delete the cores locally and restart nodes
   * 3. Assert cores exist according to {@code shouldCoreBeDiscovered} and only on their specific nodes
   * 4. Stop nodes and restart nodes
   * 5. Assert nothing is changed around core existence.
   */
  private void testCoreDiscovery(CloudSolrClient cloudClient, String sharedCollectionName, boolean shouldCoreBeDiscovered) throws Exception {
    assertEquals("Cluster is not setup with 2 nodes.", 2, cluster.getJettySolrRunners().size());

    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(sharedCollectionName);
    assertEquals("Incorrect number of replicas.", 2, collection.getReplicas().size());
    Replica firstReplica = collection.getReplicas().get(0);
    Replica secondReplica = collection.getReplicas().get(1);
    assertNotEquals("Two replicas are not on separate nodes.", firstReplica.getNodeName(), secondReplica.getNodeName());


    Map<String, Properties> expectedProperties = new HashMap<>(2);
    CoreContainer cc1 = getCoreContainer(firstReplica.getNodeName());
    Path corePropertiesPath1 = cc1.getCoreRootDirectory().resolve(firstReplica.getCoreName()).resolve(CORE_PROPERTIES_FILENAME);
    Properties expectedCoreProperties1 = new Properties();
    try (InputStreamReader is = new InputStreamReader(new FileInputStream(corePropertiesPath1.toFile()), StandardCharsets.UTF_8)) {
      expectedCoreProperties1.load(is);
    }
    removeNumShardsProperty(expectedCoreProperties1);
    expectedProperties.put(firstReplica.getName(), expectedCoreProperties1);

    CoreContainer cc2 = getCoreContainer(secondReplica.getNodeName());
    Path corePropertiesPath2 = cc2.getCoreRootDirectory().resolve(secondReplica.getCoreName()).resolve(CORE_PROPERTIES_FILENAME);
    Properties expectedCoreProperties2 = new Properties();
    try (InputStreamReader is = new InputStreamReader(new FileInputStream(corePropertiesPath2.toFile()), StandardCharsets.UTF_8)) {
      expectedCoreProperties2.load(is);
    }
    removeNumShardsProperty(expectedCoreProperties2);
    expectedProperties.put(secondReplica.getName(), expectedCoreProperties2);

    // 1. sanity, in the beginning core exist and only on their specific nodes
    assertCoreState(firstReplica, expectedProperties.get(firstReplica.getName()), secondReplica, true);
    assertCoreState(secondReplica, expectedProperties.get(secondReplica.getName()), firstReplica, true);

    // get the core directory of first replica
    File coreIndexDir1 = new File(cc1.getCoreRootDirectory() + "/" + firstReplica.getCoreName());


    // get the core directory of second replica
    File coreIndexDir2 = new File(cc2.getCoreRootDirectory() + "/" + secondReplica.getCoreName());

    // 2. stop the cluster's nodes, remove the cores locally and start up the nodes again
    List<JettySolrRunner> runners = stopNodes();
    FileUtils.deleteDirectory(coreIndexDir1);
    FileUtils.deleteDirectory(coreIndexDir2);
    restartNodes(runners);

    // 3. Assert cores exist according to {@code shouldCoreBeDiscovered} and only on their specific nodes
    collection = cloudClient.getZkStateReader().getClusterState().getCollection(sharedCollectionName);
    firstReplica = collection.getReplicas().get(0);
    secondReplica = collection.getReplicas().get(1);
    assertNotEquals("Two replicas are not on separate nodes.", firstReplica.getNodeName(), secondReplica.getNodeName());
    assertCoreState(firstReplica, expectedProperties.get(firstReplica.getName()), secondReplica, shouldCoreBeDiscovered);
    assertCoreState(secondReplica, expectedProperties.get(secondReplica.getName()), firstReplica, shouldCoreBeDiscovered);

    // 4. stop and restart the cluster's nodes
    runners = stopNodes();
    restartNodes(runners);

    // 5. Assert nothing is changed around core existence.
    collection = cloudClient.getZkStateReader().getClusterState().getCollection(sharedCollectionName);
    firstReplica = collection.getReplicas().get(0);
    secondReplica = collection.getReplicas().get(1);
    assertNotEquals("Two replicas are not on separate nodes.", firstReplica.getNodeName(), secondReplica.getNodeName());
    assertCoreState(firstReplica, expectedProperties.get(firstReplica.getName()), secondReplica, shouldCoreBeDiscovered);
    assertCoreState(secondReplica, expectedProperties.get(secondReplica.getName()), firstReplica, shouldCoreBeDiscovered);
  }

  private void restartNodes(List<JettySolrRunner> stoppedRunners) throws Exception {
    for (JettySolrRunner runner : stoppedRunners) {
      runner = cluster.startJettySolrRunner(runner, true);
      cluster.waitForNode(runner, /* seconds */ 30);
    }
  }

  private List<JettySolrRunner> stopNodes() throws Exception {
    List<JettySolrRunner> stoppedRunners = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      // stop will remove the runner from active runners list, therefore using 0 each time
      JettySolrRunner runner = cluster.stopJettySolrRunner(0);
      cluster.waitForJettyToStop(runner);
      stoppedRunners.add(runner);
    }
    assertEquals("not all nodes stopped", 0, cluster.getJettySolrRunners().size());
    return stoppedRunners;
  }

  private void assertCoreState(Replica replica, Properties expectedCoreProperties, Replica otherNodesReplica, boolean shouldCoreBeDiscovered) throws Exception {
    CoreContainer cc = getCoreContainer(replica.getNodeName());
    SolrCore core = cc.getCore(replica.getCoreName());
    Path corePropertiesPath = cc.getCoreRootDirectory().resolve(replica.getCoreName()).resolve(CORE_PROPERTIES_FILENAME);
    SolrCore otherNodesCore = cc.getCore(otherNodesReplica.getCoreName());
    Path otherNodesCorePropertiesPath = cc.getCoreRootDirectory().resolve(otherNodesReplica.getCoreName()).resolve(CORE_PROPERTIES_FILENAME);

    try {
      if (shouldCoreBeDiscovered) {
        assertNotNull("Core not found.", core);
        assertTrue("core.properties not found", Files.exists(corePropertiesPath));
        Properties coreProperties = new Properties();
        try (InputStreamReader is = new InputStreamReader(new FileInputStream(corePropertiesPath.toFile()), StandardCharsets.UTF_8)) {
          coreProperties.load(is);
        }
        removeNumShardsProperty(coreProperties);
        assertEquals("wrong number of core properties", expectedCoreProperties.size(), coreProperties.size());
        for (Object key : expectedCoreProperties.keySet()) {
          assertTrue(key + " is missing", coreProperties.containsKey(key));
          assertEquals(key + "'s value is wrong", expectedCoreProperties.get(key), coreProperties.get(key));
        }

      } else {
        assertNull("Core found when not expected.", core);
        assertFalse("core.properties found when not expected", Files.exists(corePropertiesPath));
      }
      // core from other node's replica should not be discovered
      assertNull("Other node's replica core found when not expected.", otherNodesCore);
      assertFalse("Other node's replica core.properties found when not expected", Files.exists(otherNodesCorePropertiesPath));
    } finally {
      if (core != null) {
        core.close();
      }
      if (otherNodesCore != null) {
        otherNodesCore.close();
      }
    }
  }

  /**
   * see comment inside {@link BlobStoreUtils#getSharedCoreProperties(ZkStateReader, DocCollection, Replica)}
   */
  private void removeNumShardsProperty(Properties coreProperties) {
    coreProperties.remove("numShards");
  }
}
