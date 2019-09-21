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

package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

/**
 * Test for {@link ComputePlanAction}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.cloud.Overseer=DEBUG;org.apache.solr.cloud.overseer=DEBUG;org.apache.solr.client.solrj.impl.SolrClientDataProvider=DEBUG;")
public class ComputePlanActionTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final AtomicBoolean fired = new AtomicBoolean(false);
  private static final int NODE_COUNT = 1;
  private static CountDownLatch triggerFiredLatch = new CountDownLatch(1);
  private static final AtomicReference<Map> actionContextPropsRef = new AtomicReference<>();
  private static final AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();
  private static SolrCloudManager cloudManager;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    // remove everything from autoscaling.json in ZK
    zkClient().setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, "{}".getBytes(UTF_8), true);

    if (cluster.getJettySolrRunners().size() > NODE_COUNT) {
      // stop some to get to original state
      int numJetties = cluster.getJettySolrRunners().size();
      for (int i = 0; i < numJetties - NODE_COUNT; i++) {
        JettySolrRunner randomJetty = cluster.getRandomJetty(random());
        List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
        for (int i1 = 0; i1 < jettySolrRunners.size(); i1++) {
          JettySolrRunner jettySolrRunner = jettySolrRunners.get(i1);
          if (jettySolrRunner == randomJetty) {
            JettySolrRunner j = cluster.stopJettySolrRunner(i1);
            cluster.waitForJettyToStop(j);
            break;
          }
        }
      }
    }

    cluster.deleteAllCollections();

    CloudSolrClient solrClient = cluster.getSolrClient();

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setClusterPreferencesCommand = "{" +
        "'set-cluster-preferences': [" +
        "{'minimize': 'cores'}," +
        "{'maximize': 'freedisk','precision': 100}]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPreferencesCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);

    reset();
  }

  private void reset() {
    fired.set(false);
    triggerFiredLatch = new CountDownLatch(1);
    actionContextPropsRef.set(null);
    eventRef.set(null);
    AssertingTriggerAction.expectedNode = null;
  }

  private void deleteChildrenRecursively(String path) throws Exception {
    cloudManager.getDistribStateManager().removeRecursively(path, true, false);
  }

  @After
  public void printState() throws Exception {
    log.debug("-------------_ FINAL STATE --------------");
    SolrCloudManager cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    for (String node: cloudManager.getClusterStateProvider().getLiveNodes()) {
      Map<String, Object> values = cloudManager.getNodeStateProvider().getNodeValues(node, ImplicitSnitch.tags);
      log.debug("* Node values: " + node + "\n" + Utils.toJSONString(values));
    }
    log.debug("* Live nodes: " + cloudManager.getClusterStateProvider().getLiveNodes());
    ClusterState state = cloudManager.getClusterStateProvider().getClusterState();
    state.forEachCollection(coll -> log.debug("* Collection " + coll.getName() + " state: " + coll));
  }

  @Test
  @LuceneTestCase.AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") 
  public void testNodeLost() throws Exception  {
    // let's start a node so that we have at least two
    JettySolrRunner runner = cluster.startJettySolrRunner();
    String node = runner.getNodeName();
    AssertingTriggerAction.expectedNode = node;

    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '7s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'test','class':'" + ComputePlanActionTest.AssertingTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("testNodeLost",
        "conf",1, 2);
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        "testNodeLost", clusterShape(1, 2));

    ClusterState clusterState = cluster.getSolrClient().getZkStateReader().getClusterState();
    DocCollection collection = clusterState.getCollection("testNodeLost");
    List<Replica> replicas = collection.getReplicas(node);
    assertNotNull(replicas);
    assertFalse(replicas.isEmpty());

    // start another node because because when the other node goes away, the cluster policy requires only
    // 1 replica per node and none on the overseer
    JettySolrRunner node2 = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    assertTrue(node2.getNodeName() + "is not live yet", cluster.getSolrClient().getZkStateReader().getClusterState().liveNodesContain(node2.getNodeName()) );

    // stop the original node
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jettySolrRunner = cluster.getJettySolrRunners().get(i);
      if (jettySolrRunner == runner)  {
        cluster.stopJettySolrRunner(i);
        break;
      }
    }
    log.info("Stopped_node : {}", node);
    cluster.waitForAllNodes(30);

    assertTrue("Trigger was not fired even after 10 seconds", triggerFiredLatch.await(10, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map context = actionContextPropsRef.get();
    assertNotNull(context);
    List<SolrRequest> operations = (List<SolrRequest>) context.get("operations");
    assertNotNull("The operations computed by ComputePlanAction should not be null , "+ getNodeStateProviderState() + eventRef.get(), operations);
    assertEquals("ComputePlanAction should have computed exactly 1 operation", 1, operations.size());
    SolrRequest solrRequest = operations.get(0);
    SolrParams params = solrRequest.getParams();
    assertEquals("Expected MOVEREPLICA action after adding node", MOVEREPLICA, CollectionParams.CollectionAction.get(params.get("action")));
    String replicaToBeMoved = params.get("replica");
    assertEquals("Unexpected node in computed operation", replicas.get(0).getName(), replicaToBeMoved);

    // shutdown the extra node that we had started
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jettySolrRunner = cluster.getJettySolrRunners().get(i);
      if (jettySolrRunner == node2)  {
        JettySolrRunner j = cluster.stopJettySolrRunner(i);
        cluster.waitForJettyToStop(j);
        break;
      }
    }
  }
  static String getNodeStateProviderState() {
    String result = "SolrClientNodeStateProvider.DEBUG";
    if(SolrClientNodeStateProvider.INST != null) {
      result+= Utils.toJSONString(SolrClientNodeStateProvider.INST);
    }
    return result;

  }

  // commented out on: 24-Dec-2018   @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  public void testNodeWithMultipleReplicasLost() throws Exception {
    // start 3 more nodes
    cluster.startJettySolrRunner();
    cluster.startJettySolrRunner();
    cluster.startJettySolrRunner();

    cluster.waitForAllNodes(30);

    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'test','class':'" + ComputePlanActionTest.AssertingTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("testNodeWithMultipleReplicasLost",
        "conf", 2, 3);
    create.setMaxShardsPerNode(2);
    create.process(solrClient);
    
    cluster.waitForActiveCollection("testNodeWithMultipleReplicasLost", 2, 6);

    waitForState("Timed out waiting for replicas of new collection to be active",
        "testNodeWithMultipleReplicasLost", clusterShape(2, 6));

    ClusterState clusterState = cluster.getSolrClient().getZkStateReader().getClusterState();
    DocCollection docCollection = clusterState.getCollection("testNodeWithMultipleReplicasLost");

    // lets find a node with at least 2 replicas
    String stoppedNodeName = null;
    List<Replica> replicasToBeMoved = null;
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jettySolrRunner = cluster.getJettySolrRunners().get(i);
      List<Replica> replicas = docCollection.getReplicas(jettySolrRunner.getNodeName());
      if (replicas != null && replicas.size() == 2) {
        stoppedNodeName = jettySolrRunner.getNodeName();
        replicasToBeMoved = replicas;
        JettySolrRunner j = cluster.stopJettySolrRunner(i);
        cluster.waitForJettyToStop(j);
        break;
      }
    }
    assertNotNull(stoppedNodeName);

    assertTrue("Trigger was not fired even after 5 seconds", triggerFiredLatch.await(15, TimeUnit.SECONDS));
    assertTrue(fired.get());

    TriggerEvent triggerEvent = eventRef.get();
    assertNotNull(triggerEvent);
    assertEquals(TriggerEventType.NODELOST, triggerEvent.getEventType());
    // TODO assertEquals(stoppedNodeName, triggerEvent.getProperty(TriggerEvent.NODE_NAME));

    Map context = actionContextPropsRef.get();
    assertNotNull(context);
    List<SolrRequest> operations = (List<SolrRequest>) context.get("operations");
    assertNotNull("The operations computed by ComputePlanAction should not be null "+ getNodeStateProviderState() + actionContextPropsRef.get(), operations);
    operations.forEach(solrRequest -> log.info(solrRequest.getParams().toString()));
    assertEquals("ComputePlanAction should have computed exactly 2 operation", 2, operations.size());

    for (SolrRequest solrRequest : operations) {
      SolrParams params = solrRequest.getParams();
      assertEquals("Expected MOVEREPLICA action after adding node", MOVEREPLICA, CollectionParams.CollectionAction.get(params.get("action")));
      String moved = params.get("replica");
      assertTrue(replicasToBeMoved.stream().anyMatch(replica -> replica.getName().equals(moved)));
    }
  }

  @Test
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testNodeAdded() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'test','class':'" + ComputePlanActionTest.AssertingTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // the default policy limits 1 replica per node, we need more right now
    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<3', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("testNodeAdded",
        "conf",1, 2).setMaxShardsPerNode(2);
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        "testNodeAdded", (liveNodes, collectionState) -> collectionState.getReplicas().stream().allMatch(replica -> replica.isActive(liveNodes)));

    // reset to the original policy which has only 1 replica per shard per node
    setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // start a node so that the 'violation' created by the previous policy update is fixed
    JettySolrRunner runner = cluster.startJettySolrRunner();
    assertTrue("Trigger was not fired even after 5 seconds", triggerFiredLatch.await(5, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map context = actionContextPropsRef.get();
    assertNotNull(context);
    List<SolrRequest> operations = (List<SolrRequest>) context.get("operations");
    assertNotNull("The operations computed by ComputePlanAction should not be null" + getNodeStateProviderState() + context, operations);
    assertEquals("ComputePlanAction should have computed exactly 1 operation", 1, operations.size());
    SolrRequest request = operations.get(0);
    SolrParams params = request.getParams();
    assertEquals("Expected MOVEREPLICA action after adding node", MOVEREPLICA, CollectionParams.CollectionAction.get(params.get("action")));
    String nodeAdded = params.get("targetNode");
    assertEquals("Unexpected node in computed operation", runner.getNodeName(), nodeAdded);
  }

  public static class AssertingTriggerAction implements TriggerAction {
    static volatile String expectedNode;

    @Override
    public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {

    }

    @Override
    public void init() {

    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public void process(TriggerEvent event, ActionContext context) {
      if (expectedNode != null) {
        Collection nodes = (Collection) event.getProperty(TriggerEvent.NODE_NAMES);
        if (nodes == null || !nodes.contains(expectedNode)) return;//this is not the event we are looking for
      }
      if (fired.compareAndSet(false, true)) {
        eventRef.set(event);
        actionContextPropsRef.set(context.getProperties());
        triggerFiredLatch.countDown();
      }
    }

    @Override
    public void close() throws IOException {

    }
  }

  @Test
  //2018-06-18 (commented) @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 09-Apr-2018
  public void testSelectedCollections() throws Exception {
    log.info("Found number of jetties: {}", cluster.getJettySolrRunners().size());
    // start 3 more nodes
    cluster.startJettySolrRunner();
    cluster.startJettySolrRunner();
    cluster.startJettySolrRunner();

    cluster.waitForAllNodes(30);

    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction', 'collections' : 'testSelected1,testSelected2'}," +
        "{'name':'test','class':'" + ComputePlanActionTest.AssertingTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("testSelected1",
        "conf", 2, 2);
    create.process(solrClient);

    create = CollectionAdminRequest.createCollection("testSelected2",
        "conf", 2, 2);
    create.process(solrClient);

    create = CollectionAdminRequest.createCollection("testSelected3",
        "conf", 2, 2);
    create.process(solrClient);
    
    cluster.waitForActiveCollection("testSelected1", 2, 4);
    cluster.waitForActiveCollection("testSelected2", 2, 4);
    cluster.waitForActiveCollection("testSelected3", 2, 4);
    
    waitForState("Timed out waiting for replicas of new collection to be active",
        "testSelected1", clusterShape(2, 4));

    waitForState("Timed out waiting for replicas of new collection to be active",
        "testSelected2", clusterShape(2, 4));

    waitForState("Timed out waiting for replicas of new collection to be active",
        "testSelected3", clusterShape(2, 4));

    // find a node that has replicas from all collections
    SolrCloudManager cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    NodeStateProvider stateProvider = cloudManager.getNodeStateProvider();
    List<String> nodes = new ArrayList<>();
    cloudManager.getClusterStateProvider().getLiveNodes().forEach(n -> {
      Map<String, Map<String, List<ReplicaInfo>>> map = stateProvider.getReplicaInfo(n, ImplicitSnitch.tags);
      if (map.containsKey("testSelected3") && map.containsKey("testSelected2") && map.containsKey("testSelected1")) {
        nodes.add(n);
      }
    });
    assertTrue(nodes.size() > 0);
    // kill first such node
    String node = nodes.get(0);
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      if (cluster.getJettySolrRunner(i).getNodeName().equals(node)) {
        JettySolrRunner j = cluster.stopJettySolrRunner(i);
        cluster.waitForJettyToStop(j);
        break;
      }
    }
    assertTrue("Trigger was not fired even after 5 seconds", triggerFiredLatch.await(5, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map context = actionContextPropsRef.get();
    assertNotNull(context);
    List<SolrRequest> operations = (List<SolrRequest>) context.get("operations");
    assertNotNull("The operations computed by ComputePlanAction should not be null. " + getNodeStateProviderState() + context, operations);
    assertEquals("ComputePlanAction should have computed exactly 2 operations", 2, operations.size());
    SolrRequest request = operations.get(0);
    SolrParams params = request.getParams();
    assertEquals("Expected MOVEREPLICA action after adding node", MOVEREPLICA, CollectionParams.CollectionAction.get(params.get("action")));
    assertFalse("not expected testSelected3", "testSelected3".equals(params.get("collection")));
    request = operations.get(1);
    params = request.getParams();
    assertEquals("Expected MOVEREPLICA action after adding node", MOVEREPLICA, CollectionParams.CollectionAction.get(params.get("action")));
    assertFalse("not expected testSelected3", "testSelected3".equals(params.get("collection")));
  }

  @Test
  public void testNodeAddedTriggerWithAddReplicaPreferredOp_1Shard() throws Exception {
    String collectionNamePrefix = "testNodeAddedTriggerWithAddReplicaPreferredOp_1Shard";
    int numShards = 1;
    int numCollections = 5;

    nodeAddedTriggerWithAddReplicaPreferredOp(collectionNamePrefix, numShards, numCollections);
  }

  @Test
  public void testNodeAddedTriggerWithAddReplicaPreferredOpReplicaType_1Shard() throws Exception {
    String collectionNamePrefix = "testNodeAddedTriggerWithAddReplicaPreferredOpReplicaType_1Shard";
    int numShards = 1;
    int numCollections = 5;

    nodeAddedTriggerWithAddReplicaPreferredOpReplicaType(collectionNamePrefix, numShards, numCollections);
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testNodeAddedTriggerWithAddReplicaPreferredOp_2Shard() throws Exception {
    String collectionNamePrefix = "testNodeAddedTriggerWithAddReplicaPreferredOp_2Shard";
    int numShards = 2;
    int numCollections = 5;

    nodeAddedTriggerWithAddReplicaPreferredOp(collectionNamePrefix, numShards, numCollections);
  }
  private void nodeAddedTriggerWithAddReplicaPreferredOp(String collectionNamePrefix, int numShards, int numCollections) throws Exception {
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'" + AutoScalingParams.PREFERRED_OP + "':'addreplica'," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'test','class':'" + AssertingTriggerAction.class.getName() + "'}]" +
        "}}";

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<" + (1 + numCollections * numShards) + "', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";

    nodeAddedTriggerWithAddReplicaPreferredOp(collectionNamePrefix, numShards, numCollections, setTriggerCommand, setClusterPolicyCommand);
  }

  private void nodeAddedTriggerWithAddReplicaPreferredOpReplicaType(String collectionNamePrefix, int numShards, int numCollections) throws Exception {
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'" + AutoScalingParams.PREFERRED_OP + "':'addreplica'," +
        "'" + AutoScalingParams.REPLICA_TYPE + "':'" + Replica.Type.PULL + "'," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'test','class':'" + AssertingTriggerAction.class.getName() + "'}]" +
        "}}";

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<" + (1 + numCollections * numShards) + "', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";

    nodeAddedTriggerWithAddReplicaPreferredOp(collectionNamePrefix, numShards, numCollections, setTriggerCommand, setClusterPolicyCommand, 0, 1, 0);
  }

  private void nodeAddedTriggerWithAddReplicaPreferredOp(String collectionNamePrefix, int numShards, int numCollections, String setTriggerCommand, String setClusterPolicyCommand) throws Exception {
    nodeAddedTriggerWithAddReplicaPreferredOp(collectionNamePrefix, numShards, numCollections, setTriggerCommand, setClusterPolicyCommand, 1, null, null);
  }
  private void nodeAddedTriggerWithAddReplicaPreferredOp(String collectionNamePrefix, int numShards, int numCollections, String setTriggerCommand, String setClusterPolicyCommand, Integer nNrtReplicas, Integer nTlogReplicas, Integer nPullReplicas) throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");


    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionNamePrefix + "_0",
        "conf", numShards, nNrtReplicas, nTlogReplicas, nPullReplicas).setMaxShardsPerNode(2);
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionNamePrefix + "_0", (liveNodes, collectionState) ->
            collectionState.getReplicas().stream().allMatch(replica -> replica.isActive(liveNodes)));

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    assertTrue(triggerFiredLatch.await(30, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map actionContext = actionContextPropsRef.get();
    List operations = (List) actionContext.get("operations");
    assertNotNull(operations);
    assertEquals(numShards, operations.size());
    Set<String> affectedShards = new HashSet<>(2);
    for (Object operation : operations) {
      assertTrue(operation instanceof CollectionAdminRequest.AddReplica);
      CollectionAdminRequest.AddReplica addReplica = (CollectionAdminRequest.AddReplica) operation;
      assertEquals(newNode.getNodeName(), addReplica.getNode());
      assertEquals(collectionNamePrefix + "_0", addReplica.getCollection());
      affectedShards.add(addReplica.getShard());
    }
    assertEquals(numShards, affectedShards.size());

    for (int i = 1; i < numCollections; i++) {
      create = CollectionAdminRequest.createCollection(collectionNamePrefix + "_" + i,
          "conf", numShards, 2).setMaxShardsPerNode(numShards * 2);
      create.process(solrClient);

      waitForState("Timed out waiting for replicas of new collection to be active",
          collectionNamePrefix + "_" + i, (liveNodes, collectionState) ->
              collectionState.getReplicas().stream().allMatch(replica -> replica.isActive(liveNodes)));
    }

    reset();

    newNode = cluster.startJettySolrRunner();
    assertTrue(triggerFiredLatch.await(30, TimeUnit.SECONDS));
    assertTrue(fired.get());
    actionContext = actionContextPropsRef.get();
    operations = (List) actionContext.get("operations");
    assertNotNull(operations);
    assertEquals(numCollections * numShards, operations.size());
    Set<String> affectedCollections = new HashSet<>(numCollections);
    affectedShards = new HashSet<>(numShards);
    Set<Pair<String, String>> affectedCollShards = new HashSet<>(numCollections * numShards);
    for (Object operation : operations) {
      assertTrue(operation instanceof CollectionAdminRequest.AddReplica);
      CollectionAdminRequest.AddReplica addReplica = (CollectionAdminRequest.AddReplica) operation;
      assertEquals(newNode.getNodeName(), addReplica.getNode());
      affectedCollections.add(addReplica.getCollection());
      affectedShards.add(addReplica.getShard());
      affectedCollShards.add(new Pair<>(addReplica.getCollection(), addReplica.getShard()));
    }
    assertEquals(numCollections, affectedCollections.size());
    assertEquals(numShards, affectedShards.size());
    assertEquals(numCollections * numShards, affectedCollShards.size());
  }

  @Test
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testNodeLostTriggerWithDeleteNodePreferredOp() throws Exception {
    String collectionNamePrefix = "testNodeLostTriggerWithDeleteNodePreferredOp";
    int numCollections = 1 + random().nextInt(3), numShards = 1 + random().nextInt(3);

    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'" + AutoScalingParams.PREFERRED_OP + "':'deletenode'," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'execute_plan','class':'solr.ExecutePlanAction'}" +
        "{'name':'test','class':'" + AssertingTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<" + (1 + numCollections * numShards) + "', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    // cache the node name because it won't be available once the node is shutdown
    String newNodeName = newNode.getNodeName();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionNamePrefix + "_0",
        "conf", numShards, 2).setMaxShardsPerNode(numShards * 2);
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionNamePrefix + "_0", (liveNodes, collectionState) ->
            collectionState.getReplicas().stream().allMatch(replica -> replica.isActive(liveNodes)));

    cluster.stopJettySolrRunner(newNode);
    assertTrue(triggerFiredLatch.await(30, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map actionContext = actionContextPropsRef.get();
    List operations = (List) actionContext.get("operations");
    assertNotNull(operations);
    assertEquals(1, operations.size());
    for (Object operation : operations) {
      assertTrue(operation instanceof CollectionAdminRequest.DeleteNode);
      CollectionAdminRequest.DeleteNode deleteNode = (CollectionAdminRequest.DeleteNode) operation;
      SolrParams deleteNodeParams = deleteNode.getParams();
      assertEquals(newNodeName, deleteNodeParams.get("node"));
    }

    waitForState("Timed out waiting for all shards to have only 1 replica",
        collectionNamePrefix + "_0", clusterShape(numShards, numShards));
  }
}
