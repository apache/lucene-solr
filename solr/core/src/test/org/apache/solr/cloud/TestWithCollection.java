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

package org.apache.solr.cloud;

import static org.apache.solr.common.params.CollectionAdminParams.WITH_COLLECTION;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.ComputePlanAction;
import org.apache.solr.cloud.autoscaling.ExecutePlanAction;
import org.apache.solr.cloud.autoscaling.TriggerActionBase;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for co-locating a collection with another collection such that any Collection API
 * always ensures that the co-location is never broken.
 *
 * See SOLR-11990 for more details.
 */
@LogLevel("org.apache.solr.cloud.autoscaling=TRACE;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG;org.apache.solr.cloud.overseer=DEBUG")
public class TestWithCollection extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static SolrCloudManager cloudManager;

  private static final int NUM_JETTIES = 2;

  @Before
  public void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(NUM_JETTIES)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    if (zkClient().exists(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, true))  {
      zkClient().setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, "{}".getBytes(StandardCharsets.UTF_8), true);
    }

    cluster.getSolrClient().setDefaultCollection(null);

    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);
    LATCH = new CountDownLatch(1);
  }
  
  @After
  public void teardownCluster() throws Exception {
    shutdownCluster();
  }

  @AfterClass
  public static void cleanUpAfterClass() throws Exception {
    cloudManager = null;
  }

  private void deleteChildrenRecursively(String path) throws Exception {
    cloudManager.getDistribStateManager().removeRecursively(path, true, false);
  }

  @Test
  public void testCreateCollectionNoWithCollection() throws IOException, SolrServerException {
    String prefix = "testCreateCollectionNoWithCollection";
    String xyz = prefix + "_xyz";
    String abc = prefix + "_abc";

    CloudSolrClient solrClient = cluster.getSolrClient();
    try {

      CollectionAdminRequest.createCollection(xyz, 1, 1)
          .setWithCollection(abc).process(solrClient);
    } catch (HttpSolrClient.RemoteSolrException e) {
      assertTrue(e.getMessage().contains("The 'withCollection' does not exist"));
    }

    CollectionAdminRequest.createCollection(abc, 2, 1)
        .process(solrClient);
    try {
      CollectionAdminRequest.createCollection(xyz, 1, 1)
          .setWithCollection(abc).process(solrClient);
    } catch (HttpSolrClient.RemoteSolrException e) {
      assertTrue(e.getMessage().contains("The `withCollection` must have only one shard, found: 2"));
    }
  }

  public void testCreateCollection() throws Exception {
    String prefix = "testCreateCollection";
    String xyz = prefix + "_xyz";
    String abc = prefix + "_abc";

    CloudSolrClient solrClient = cluster.getSolrClient();

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "    ]" +
        "}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    solrClient.request(req);

    String chosenNode = cluster.getRandomJetty(random()).getNodeName();
    CollectionAdminRequest.createCollection(abc, 1, 1)
        .setCreateNodeSet(chosenNode) // randomize to avoid choosing the first node always
        .process(solrClient);
    CollectionAdminRequest.createCollection(xyz, 1, 1)
        .setWithCollection(abc)
        .process(solrClient);

    DocCollection c1 = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(xyz);
    assertNotNull(c1);
    assertEquals(abc, c1.getStr(WITH_COLLECTION));
    Replica replica = c1.getReplicas().get(0);
    String nodeName = replica.getNodeName();

    assertEquals(chosenNode, nodeName);
  }

  @Test
  //Commented 14-Oct-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 23-Aug-2018
  public void testDeleteWithCollection() throws IOException, SolrServerException, InterruptedException {
    String prefix = "testDeleteWithCollection";
    String xyz = prefix + "_xyz";
    String abc = prefix + "_abc";

    CloudSolrClient solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(abc, 1, 1)
        .process(solrClient);
    CollectionAdminRequest.createCollection(xyz, 1, 1)
        .setWithCollection(abc)
        .process(solrClient);
    try {
      CollectionAdminResponse response = CollectionAdminRequest.deleteCollection(abc).process(solrClient);
      fail("Deleting collection: " + abc + " should have failed with an exception. Instead response was: " + response.getResponse());
    } catch (HttpSolrClient.RemoteSolrException e) {
      assertTrue(e.getMessage().contains("is co-located with collection"));
    }

    // delete the co-located collection first
    CollectionAdminRequest.deleteCollection(xyz).process(solrClient);
    // deleting the with collection should succeed now
    CollectionAdminRequest.deleteCollection(abc).process(solrClient);

    xyz = xyz + "_2";
    abc = abc + "_2";
    CollectionAdminRequest.createCollection(abc, 1, 1)
        .process(solrClient);
    CollectionAdminRequest.createCollection(xyz, 1, 1)
        .setWithCollection(abc)
        .process(solrClient);
    // sanity check
    try {
      CollectionAdminResponse response = CollectionAdminRequest.deleteCollection(abc).process(solrClient);
      fail("Deleting collection: " + abc + " should have failed with an exception. Instead response was: " + response.getResponse());
    } catch (HttpSolrClient.RemoteSolrException e) {
      assertTrue(e.getMessage().contains("is co-located with collection"));
    }

    CollectionAdminRequest.modifyCollection(xyz, null)
        .unsetAttribute("withCollection")
        .process(solrClient);
    TimeOut timeOut = new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      DocCollection c1 = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(xyz);
      if (c1.getStr("withCollection") == null) break;
      Thread.sleep(200);
    }
    DocCollection c1 = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(xyz);
    assertNull(c1.getStr("withCollection"));
    CollectionAdminRequest.deleteCollection(abc).process(solrClient);
  }

  @Test
  public void testAddReplicaSimple() throws Exception {
    String prefix = "testAddReplica";
    String xyz = prefix + "_xyz";
    String abc = prefix + "_abc";

    CloudSolrClient solrClient = cluster.getSolrClient();
    String chosenNode = cluster.getRandomJetty(random()).getNodeName();
    log.info("Chosen node {} for collection {}", chosenNode, abc);
    CollectionAdminRequest.createCollection(abc, 1, 1)
        .setCreateNodeSet(chosenNode) // randomize to avoid choosing the first node always
        .process(solrClient);
    CollectionAdminRequest.createCollection(xyz, 1, 1)
        .setWithCollection(abc)
        .process(solrClient);

    String otherNode = null;
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      if (!chosenNode.equals(jettySolrRunner.getNodeName())) {
        otherNode = jettySolrRunner.getNodeName();
      }
    }
    CollectionAdminRequest.addReplicaToShard(xyz, "shard1")
        .setNode(otherNode)
        .process(solrClient);
    DocCollection collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    DocCollection withCollection = solrClient.getZkStateReader().getClusterState().getCollection(abc);

    assertTrue(collection.getReplicas().stream().noneMatch(replica -> withCollection.getReplicas(replica.getNodeName()).isEmpty()));
  }

  @Test
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testAddReplicaWithPolicy() throws Exception {
    String prefix = "testAddReplicaWithPolicy";
    String xyz = prefix + "_xyz";
    String abc = prefix + "_abc";

    CloudSolrClient solrClient = cluster.getSolrClient();
    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'node':'#ANY'}," +
        "    ]" +
        "}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    solrClient.request(req);

    String chosenNode = cluster.getRandomJetty(random()).getNodeName();
    log.info("Chosen node {} for collection {}", chosenNode, abc);
    CollectionAdminRequest.createCollection(abc, 1, 1)
        .setCreateNodeSet(chosenNode) // randomize to avoid choosing the first node always
        .process(solrClient);
    CollectionAdminRequest.createCollection(xyz, 1, 1)
        .setWithCollection(abc)
        .process(solrClient);

    DocCollection collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    assertEquals(chosenNode, collection.getReplicas().iterator().next().getNodeName());

//    zkClient().printLayoutToStdOut();

    CollectionAdminRequest.addReplicaToShard(xyz, "shard1")
        .process(solrClient);
    collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    DocCollection withCollection = solrClient.getZkStateReader().getClusterState().getCollection(abc);

    assertTrue(collection.getReplicas().stream().noneMatch(
        replica -> withCollection.getReplicas(replica.getNodeName()) == null
            || withCollection.getReplicas(replica.getNodeName()).isEmpty()));
  }

  @Test
  public void testMoveReplicaMainCollection() throws Exception {
    String prefix = "testMoveReplicaMainCollection";
    String xyz = prefix + "_xyz";
    String abc = prefix + "_abc";

    CloudSolrClient solrClient = cluster.getSolrClient();

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'node':'#ANY'}," +
        "    ]" +
        "}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    solrClient.request(req);

    String chosenNode = cluster.getRandomJetty(random()).getNodeName();
    log.info("Chosen node {} for collection {}", chosenNode, abc);
    CollectionAdminRequest.createCollection(abc, 1, 1)
        .setCreateNodeSet(chosenNode) // randomize to avoid choosing the first node always
        .process(solrClient);
    CollectionAdminRequest.createCollection(xyz, 1, 1)
        .setWithCollection(abc)
        .process(solrClient);

    String otherNode = null;
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      if (!chosenNode.equals(jettySolrRunner.getNodeName())) {
        otherNode = jettySolrRunner.getNodeName();
      }
    }

    DocCollection collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    DocCollection withCollection = solrClient.getZkStateReader().getClusterState().getCollection(abc);
    assertNull(collection.getReplicas(otherNode)); // sanity check
    assertNull(withCollection.getReplicas(otherNode)); // sanity check

    CollectionAdminRequest.MoveReplica moveReplica = new CollectionAdminRequest.MoveReplica(xyz, collection.getReplicas().iterator().next().getName(), otherNode);
    moveReplica.setWaitForFinalState(true);
    moveReplica.process(solrClient);
//    zkClient().printLayoutToStdOut();
    collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz); // refresh
    DocCollection withCollectionRefreshed = solrClient.getZkStateReader().getClusterState().getCollection(abc); // refresh
    assertTrue(collection.getReplicas().stream().noneMatch(
        replica -> withCollectionRefreshed.getReplicas(replica.getNodeName()) == null
            || withCollectionRefreshed.getReplicas(replica.getNodeName()).isEmpty()));
  }

  @Test
  //Commented 14-Oct-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 23-Aug-2018
  public void testMoveReplicaWithCollection() throws Exception {
    String prefix = "testMoveReplicaWithCollection";
    String xyz = prefix + "_xyz";
    String abc = prefix + "_abc";

    CloudSolrClient solrClient = cluster.getSolrClient();

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'node':'#ANY'}," +
        "    ]" +
        "}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    solrClient.request(req);

    String chosenNode = cluster.getRandomJetty(random()).getNodeName();
    log.info("Chosen node {} for collection {}", chosenNode, abc);
    CollectionAdminRequest.createCollection(abc, 1, 1)
        .setCreateNodeSet(chosenNode) // randomize to avoid choosing the first node always
        .process(solrClient);
    CollectionAdminRequest.createCollection(xyz, 1, 1)
        .setWithCollection(abc)
        .process(solrClient);

    DocCollection collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    assertEquals(chosenNode, collection.getReplicas().iterator().next().getNodeName());

    String otherNode = null;
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      if (!chosenNode.equals(jettySolrRunner.getNodeName())) {
        otherNode = jettySolrRunner.getNodeName();
      }
    }

    collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    DocCollection withCollection = solrClient.getZkStateReader().getClusterState().getCollection(abc);
    assertNull(collection.getReplicas(otherNode)); // sanity check
    assertNull(withCollection.getReplicas(otherNode)); // sanity check

    try {
      new CollectionAdminRequest.MoveReplica(abc, collection.getReplicas().iterator().next().getName(), otherNode)
          .process(solrClient);
      fail("Expected moving a replica of 'withCollection': " + abc + " to fail");
    } catch (HttpSolrClient.RemoteSolrException e) {
      assertTrue(e.getMessage().contains("Collection: testMoveReplicaWithCollection_abc is co-located with collection: testMoveReplicaWithCollection_xyz"));
    }
//    zkClient().printLayoutToStdOut();
    collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz); // refresh
    DocCollection withCollectionRefreshed = solrClient.getZkStateReader().getClusterState().getCollection(abc); // refresh

    // sanity check that the failed move operation didn't actually change our co-location guarantees
    assertTrue(collection.getReplicas().stream().noneMatch(
        replica -> withCollectionRefreshed.getReplicas(replica.getNodeName()) == null
            || withCollectionRefreshed.getReplicas(replica.getNodeName()).isEmpty()));
  }

  /**
   * Tests that when a new node is added to the cluster and autoscaling framework
   * moves replicas to the new node, we maintain all co-locating guarantees
   */
  // commented out on: 01-Apr-2019  // added 15-Sep-2018
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // fixed on 9.0, broken? in 8.x
  public void testNodeAdded() throws Exception  {
    String prefix = "testNodeAdded";
    String xyz = prefix + "_xyz";
    String abc = prefix + "_abc";

    CloudSolrClient solrClient = cluster.getSolrClient();

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'node':'#ANY'}," +
        "    ]" +
        "}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    solrClient.request(req);

    String chosenNode = cluster.getRandomJetty(random()).getNodeName();
    log.info("Chosen node {} for collection {}", chosenNode, abc);
    CollectionAdminRequest.createCollection(abc, 1, 1)
        .setCreateNodeSet(chosenNode) // randomize to avoid choosing the first node always
        .process(solrClient);
    CollectionAdminRequest.createCollection(xyz, 1, 1)
        .setWithCollection(abc)
        .process(solrClient);

    DocCollection collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    assertEquals(chosenNode, collection.getReplicas().iterator().next().getNodeName());

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger1'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '0s'," +
        "'actions' : [" +
        "{'name' : 'compute', 'class' : '" + ComputePlanAction.class.getName() + "'}" +
        "{'name' : 'execute', 'class' : '" + ExecutePlanAction.class.getName() + "'}" +
        "{'name' : 'compute', 'class' : '" + CapturingAction.class.getName() + "'}" +
        "]" +
        "}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    solrClient.request(req);

    Optional<JettySolrRunner> other = cluster.getJettySolrRunners()
        .stream().filter(j -> !chosenNode.equals(j.getNodeName())).findAny();
    String otherNode = other.orElseThrow(AssertionError::new).getNodeName();

    // add an extra replica of abc collection on a different node
    CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest.addReplicaToShard(abc, "shard1")
        .setNode(otherNode);
    addReplica.setWaitForFinalState(true);
    addReplica.process(solrClient);

    // refresh
    collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    DocCollection withCollection = solrClient.getZkStateReader().getClusterState().getCollection(abc);

    // sanity check
    assertColocated(collection, otherNode, withCollection);

    assertEquals(1, collection.getReplicas().size());
    Replica xyzReplica = collection.getReplicas().get(0);

    // start a new node
    JettySolrRunner newNode = cluster.startJettySolrRunner();
    assertTrue("Action was not fired till 30 seconds", LATCH.await(30, TimeUnit.SECONDS));
    // refresh
    collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    withCollection = solrClient.getZkStateReader().getClusterState().getCollection(abc);

    // sanity check
    assertColocated(collection, otherNode, withCollection);

    // assert that the replica of xyz collection was not moved
    assertNotNull(collection.getReplica(xyzReplica.getName()));
    assertEquals(chosenNode, collection.getReplicas().get(0).getNodeName());

    // add an extra replica of xyz collection -- this should be placed on the 'otherNode'
    addReplica = CollectionAdminRequest.addReplicaToShard(xyz, "shard1");
    addReplica.setWaitForFinalState(true);
    addReplica.process(solrClient);

    // refresh
    collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    withCollection = solrClient.getZkStateReader().getClusterState().getCollection(abc);

    List<Replica> replicas = collection.getReplicas(otherNode);
    assertNotNull(replicas);
    assertEquals(1, replicas.size());
    replicas = withCollection.getReplicas(otherNode);
    assertNotNull(replicas);
    assertEquals(1, replicas.size());

    // add an extra replica of xyz collection -- this should be placed on the 'newNode'
    addReplica = CollectionAdminRequest.addReplicaToShard(xyz, "shard1");
    addReplica.setWaitForFinalState(true);
    addReplica.process(solrClient);

    // refresh
    collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    withCollection = solrClient.getZkStateReader().getClusterState().getCollection(abc);

    assertNotNull(collection.getReplicas(newNode.getNodeName()));
    replicas = collection.getReplicas(newNode.getNodeName());
    assertNotNull(replicas);
    assertEquals(1, replicas.size());
    replicas = withCollection.getReplicas(newNode.getNodeName());
    assertNotNull(replicas);
    assertEquals(1, replicas.size());
  }

  public void testMultipleWithCollections() throws Exception {
    String prefix = "testMultipleWithCollections";
    String xyz = prefix + "_xyz";
    String xyz2 = prefix + "_xyz2";
    String abc = prefix + "_abc";
    String abc2 = prefix + "_abc2";

    // start 2 more nodes so we have 4 in total
    cluster.startJettySolrRunner();
    cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);

    CloudSolrClient solrClient = cluster.getSolrClient();

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'node':'#ANY'}," +
        "    ]" +
        "}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    solrClient.request(req);

    String chosenNode = cluster.getJettySolrRunner(0).getNodeName();
    log.info("Chosen node {} for collection {}", chosenNode, abc);

    CollectionAdminRequest.createCollection(abc, 1, 1)
        .setCreateNodeSet(chosenNode)
        .process(solrClient);
    CollectionAdminRequest.createCollection(xyz, 1, 1)
        .setWithCollection(abc)
        .process(solrClient);

    String chosenNode2 = cluster.getJettySolrRunner(1).getNodeName();
    log.info("Chosen node {} for collection {}", chosenNode2, abc2);
    CollectionAdminRequest.createCollection(abc2, 1, 1)
        .setCreateNodeSet(chosenNode2)
        .process(solrClient);
    CollectionAdminRequest.createCollection(xyz2, 1, 1)
        .setWithCollection(abc2)
        .process(solrClient);

    // refresh
    DocCollection collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    DocCollection collection2 = solrClient.getZkStateReader().getClusterState().getCollection(xyz2);
    DocCollection withCollection = solrClient.getZkStateReader().getClusterState().getCollection(abc);
    DocCollection withCollection2 = solrClient.getZkStateReader().getClusterState().getCollection(abc2);

    // sanity check
    assertColocated(collection, chosenNode2, withCollection); // no replica should be on chosenNode2
    assertColocated(collection2, chosenNode, withCollection2); // no replica should be on chosenNode

    String chosenNode3 = cluster.getJettySolrRunner(2).getNodeName();
    CollectionAdminRequest.addReplicaToShard(xyz, "shard1")
        .setNode(chosenNode3)
        .process(solrClient);
    String chosenNode4 = cluster.getJettySolrRunner(2).getNodeName();
    CollectionAdminRequest.addReplicaToShard(xyz2, "shard1")
        .setNode(chosenNode4)
        .process(solrClient);

    collection = solrClient.getZkStateReader().getClusterState().getCollection(xyz);
    collection2 = solrClient.getZkStateReader().getClusterState().getCollection(xyz2);
    withCollection = solrClient.getZkStateReader().getClusterState().getCollection(abc);
    withCollection2 = solrClient.getZkStateReader().getClusterState().getCollection(abc2);

    // sanity check
    assertColocated(collection, null, withCollection);
    assertColocated(collection2, null, withCollection2);
  }

  /**
   * Asserts that all replicas of collection are colocated with at least one
   * replica of the withCollection and none of them should be on the given 'noneOnNode'.
   */
  private void assertColocated(DocCollection collection, String noneOnNode, DocCollection withCollection) {
    // sanity check
    assertTrue(collection.getReplicas().stream().noneMatch(
        replica -> withCollection.getReplicas(replica.getNodeName()) == null
            || withCollection.getReplicas(replica.getNodeName()).isEmpty()));

    if (noneOnNode != null) {
      assertTrue(collection.getReplicas().stream().noneMatch(
          replica -> noneOnNode.equals(replica.getNodeName())));
    }
  }

  private static CountDownLatch LATCH = new CountDownLatch(1);
  public static class CapturingAction extends TriggerActionBase {
    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      LATCH.countDown();
    }
  }
}
