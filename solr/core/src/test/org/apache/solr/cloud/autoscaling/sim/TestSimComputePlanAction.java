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

package org.apache.solr.cloud.autoscaling.sim;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.ComputePlanAction;
import org.apache.solr.cloud.autoscaling.ScheduledTriggers;
import org.apache.solr.cloud.autoscaling.TriggerAction;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerValidationException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

/**
 * Test for {@link ComputePlanAction}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.cloud.Overseer=DEBUG;org.apache.solr.cloud.overseer=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG;")
public class TestSimComputePlanAction extends SimSolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final AtomicBoolean fired = new AtomicBoolean(false);
  private static final int NODE_COUNT = 1;
  private static CountDownLatch triggerFiredLatch = new CountDownLatch(1);
  private static final AtomicReference<Map> actionContextPropsRef = new AtomicReference<>();
  private static final AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();

  @Before
  public void init() throws Exception {
    configureCluster(1, TimeSource.get("simTime:50"));
    fired.set(false);
    triggerFiredLatch = new CountDownLatch(1);
    actionContextPropsRef.set(null);

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    SolrResponse rsp = cluster.request(req);
    NamedList<Object> response = rsp.getResponse();
    assertEquals(response.get("result").toString(), "success");

    String setClusterPreferencesCommand = "{" +
        "'set-cluster-preferences': [" +
        "{'minimize': 'cores'}," +
        "{'maximize': 'freedisk','precision': 100}]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPreferencesCommand);
    rsp = cluster.request(req);
    response = rsp.getResponse();
    assertEquals(response.get("result").toString(), "success");
    assertAutoscalingUpdateComplete();
    cluster.getTimeSource().sleep(TimeUnit.SECONDS.toMillis(ScheduledTriggers.DEFAULT_COOLDOWN_PERIOD_SECONDS));
  }

  @After
  public void printState() throws Exception {
    if (null == cluster) {
      // test didn't init, nothing to do
      return;
    }
                          
    log.info("-------------_ FINAL STATE --------------");
    log.info("* Node values: " + Utils.toJSONString(cluster.getSimNodeStateProvider().simGetAllNodeValues()));
    log.info("* Live nodes: " + cluster.getClusterStateProvider().getLiveNodes());
    ClusterState state = cluster.getClusterStateProvider().getClusterState();
    for (String coll: cluster.getSimClusterStateProvider().simListCollections()) {
      log.info("* Collection " + coll + " state: " + state.getCollection(coll));
    }
    shutdownCluster();
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-12028") // if you beast this, eventually you will see
                                                                          // creation of 'testNodeLost' collection fail
                                                                          // because shard1 elects no leader
  public void testNodeLost() throws Exception {
    // let's start a node so that we have at least two
    String node = cluster.simAddNode();
    AssertingTriggerAction.expectedNode = node;

    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '7s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'test','class':'" + TestSimComputePlanAction.AssertingTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertAutoscalingUpdateComplete();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("testNodeLost",
        "conf",1, 2);
    create.process(solrClient);

    CloudUtil.waitForState(cluster, "Timed out waiting for replicas of new collection to be active",
        "testNodeLost", CloudUtil.clusterShape(1, 2, false, true));

    ClusterState clusterState = cluster.getClusterStateProvider().getClusterState();
    log.debug("-- cluster state: {}", clusterState);
    DocCollection collection = clusterState.getCollection("testNodeLost");
    List<Replica> replicas = collection.getReplicas(node);
    assertNotNull(replicas);
    assertFalse(replicas.isEmpty());

    // start another node because because when the other node goes away, the cluster policy requires only
    // 1 replica per node and none on the overseer
    String node2 = cluster.simAddNode();
    assertTrue(node2 + "is not live yet", cluster.getClusterStateProvider().getClusterState().liveNodesContain(node2) );

    // stop the original node
    cluster.simRemoveNode(node, false);
    log.info("Stopped_node : {}", node);

    assertTrue("Trigger was not fired even after 10 seconds", triggerFiredLatch.await(10, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map context = actionContextPropsRef.get();
    assertNotNull(context);
    List<SolrRequest> operations = (List<SolrRequest>) context.get("operations");
    assertNotNull("The operations computed by ComputePlanAction should not be null , " + eventRef.get(), operations);
    assertEquals("ComputePlanAction should have computed exactly 1 operation", 1, operations.size());
    SolrRequest solrRequest = operations.get(0);
    SolrParams params = solrRequest.getParams();
    assertEquals("Expected MOVEREPLICA action after adding node", MOVEREPLICA, CollectionParams.CollectionAction.get(params.get("action")));
    String replicaToBeMoved = params.get("replica");
    assertEquals("Unexpected node in computed operation", replicas.get(0).getName(), replicaToBeMoved);

    // shutdown the extra node that we had started
    cluster.simRemoveNode(node2, false);
  }

  // TODO: AwaitsFix - some checks had to be ignore in this test
  public void testNodeWithMultipleReplicasLost() throws Exception {
    AssertingTriggerAction.expectedNode = null;

    // start 3 more nodes
    cluster.simAddNode();
    cluster.simAddNode();
    cluster.simAddNode();

    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'test','class':'" + AssertingTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertAutoscalingUpdateComplete();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("testNodeWithMultipleReplicasLost",
        "conf",2, 3);
    create.setMaxShardsPerNode(2);
    create.process(solrClient);

    CloudUtil.waitForState(cluster, "Timed out waiting for replicas of new collection to be active",
        "testNodeWithMultipleReplicasLost", CloudUtil.clusterShape(2, 3, false, true));

    ClusterState clusterState = cluster.getClusterStateProvider().getClusterState();
    log.debug("-- cluster state: {}", clusterState);
    DocCollection docCollection = clusterState.getCollection("testNodeWithMultipleReplicasLost");

    // lets find a node with at least 2 replicas
    String stoppedNodeName = null;
    List<Replica> replicasToBeMoved = null;
    for (String node : cluster.getClusterStateProvider().getLiveNodes()) {
      List<Replica> replicas = docCollection.getReplicas(node);
      if (replicas != null && replicas.size() == 2) {
        stoppedNodeName = node;
        replicasToBeMoved = replicas;
        cluster.simRemoveNode(node, false);
        break;
      }
    }
    assertNotNull(stoppedNodeName);

    assertTrue("Trigger was not fired even after 5 seconds", triggerFiredLatch.await(5, TimeUnit.SECONDS));
    assertTrue(fired.get());

    TriggerEvent triggerEvent = eventRef.get();
    assertNotNull(triggerEvent);
    assertEquals(TriggerEventType.NODELOST, triggerEvent.getEventType());
    // TODO assertEquals(stoppedNodeName, triggerEvent.getProperty(TriggerEvent.NODE_NAME));

    Map context = actionContextPropsRef.get();
    assertNotNull(context);
    List<SolrRequest> operations = (List<SolrRequest>) context.get("operations");
    assertNotNull("The operations computed by ComputePlanAction should not be null " + actionContextPropsRef.get() + "\nevent: " + eventRef.get(), operations);
    operations.forEach(solrRequest -> log.info(solrRequest.getParams().toString()));
    
    // TODO: this can be 3!
    // assertEquals("ComputePlanAction should have computed exactly 2 operation", 2, operations.size());

    for (SolrRequest solrRequest : operations) {
      SolrParams params = solrRequest.getParams();
      assertEquals("Expected MOVEREPLICA action after adding node", MOVEREPLICA, CollectionParams.CollectionAction.get(params.get("action")));
      String moved = params.get("replica");
      
      // TODO: this can fail!
      // assertTrue(replicasToBeMoved.stream().anyMatch(replica -> replica.getName().equals(moved)));
    }
  }

  @Test
  //17-Aug-2018 commented @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 28-June-2018
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testNodeAdded() throws Exception {
    AssertingTriggerAction.expectedNode = null;
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'test','class':'" + TestSimComputePlanAction.AssertingTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // the default policy limits 1 replica per node, we need more right now
    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<5', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertAutoscalingUpdateComplete();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("testNodeAdded",
        "conf",1, 4).setMaxShardsPerNode(-1);
    create.process(solrClient);

    CloudUtil.waitForState(cluster, "Timed out waiting for replicas of new collection to be active",
        "testNodeAdded", (liveNodes, collectionState) -> collectionState.getReplicas().stream().allMatch(replica -> replica.isActive(liveNodes)));

    // reset to the original policy which has only 1 replica per shard per node
    setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<3', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertAutoscalingUpdateComplete();

    // start a node so that the 'violation' created by the previous policy update is fixed
    String newNode = cluster.simAddNode();
    assertTrue("Trigger was not fired even after 5 seconds", triggerFiredLatch.await(5, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map context = actionContextPropsRef.get();
    assertNotNull(context);
    log.info("Node values: " + Utils.toJSONString(cluster.getSimNodeStateProvider().simGetAllNodeValues()));
    log.info("Live nodes: " + cluster.getClusterStateProvider().getLiveNodes() + ", collection state: " + cluster.getClusterStateProvider().getClusterState().getCollection("testNodeAdded"));
    List<SolrRequest> operations = (List<SolrRequest>) context.get("operations");
    assertNotNull("The operations computed by ComputePlanAction should not be null" + context, operations);

    // TODO: can be 2!
    // assertEquals("ComputePlanAction should have computed exactly 1 operation, but was: " + operations, 1, operations.size());
    
    SolrRequest request = operations.get(0);
    SolrParams params = request.getParams();
    assertEquals("Expected MOVEREPLICA action after adding node", MOVEREPLICA, CollectionParams.CollectionAction.get(params.get("action")));
    String nodeAdded = params.get("targetNode");
    assertEquals("Unexpected node in computed operation", newNode, nodeAdded);
  }

  public static class AssertingTriggerAction implements TriggerAction {
    static String expectedNode;

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
}
