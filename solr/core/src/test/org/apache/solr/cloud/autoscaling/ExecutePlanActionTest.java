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

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TestInjection;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 * Test for {@link ExecutePlanAction}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class ExecutePlanActionTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int NODE_COUNT = 2;

  private SolrResourceLoader loader;
  private SolrCloudManager cloudManager;

  public static class StartAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      startedProcessing.countDown();
    }
  }

  private static CountDownLatch startedProcessing = new CountDownLatch(1);

  public static class FinishAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      finishedProcessing.countDown();
    }
  }

  private static CountDownLatch finishedProcessing = new CountDownLatch(1);

  @BeforeClass
  public static void setupCluster() throws Exception {

  }

  @Before
  public void setUp() throws Exception  {
    super.setUp();
    System.setProperty("metricsEnabled", "true");
    configureCluster(NODE_COUNT)
    .addConfig("conf", configset("cloud-minimal"))
    .configure();
    
    // clear any persisted auto scaling configuration
    Stat stat = zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);


    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();

    finishedProcessing = new CountDownLatch(1);
    startedProcessing = new CountDownLatch(1);
  }
  

  @After
  public void tearDown() throws Exception  {
    shutdownCluster();
    super.tearDown();
    TestInjection.reset();
  }

  @Test
  public void testExecute() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "testExecute";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 1, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);
    
    cluster.waitForActiveCollection(collectionName, 1, 2);

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionName, clusterShape(1, 2));

    JettySolrRunner sourceNode = cluster.getRandomJetty(random());
    String sourceNodeName = sourceNode.getNodeName();
    ClusterState clusterState = solrClient.getZkStateReader().getClusterState();
    DocCollection docCollection = clusterState.getCollection(collectionName);
    List<Replica> replicas = docCollection.getReplicas(sourceNodeName);
    assertNotNull(replicas);
    assertFalse(replicas.isEmpty());

    List<JettySolrRunner> otherJetties = cluster.getJettySolrRunners().stream()
        .filter(jettySolrRunner -> jettySolrRunner != sourceNode).collect(Collectors.toList());
    assertFalse(otherJetties.isEmpty());
    JettySolrRunner survivor = otherJetties.get(0);

    try (ExecutePlanAction action = new ExecutePlanAction()) {
      action.configure(loader, cloudManager, Collections.singletonMap("name", "execute_plan"));

      // used to signal if we found that ExecutePlanAction did in fact create the right znode before executing the operation
      AtomicBoolean znodeCreated = new AtomicBoolean(false);

      CollectionAdminRequest.AsyncCollectionAdminRequest moveReplica = new CollectionAdminRequest.MoveReplica(collectionName, replicas.get(0).getName(), survivor.getNodeName());
      CollectionAdminRequest.AsyncCollectionAdminRequest mockRequest = new CollectionAdminRequest.AsyncCollectionAdminRequest(CollectionParams.CollectionAction.OVERSEERSTATUS) {
        @Override
        public void setAsyncId(String asyncId) {
          super.setAsyncId(asyncId);
          String parentPath = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/xyz/execute_plan";
          try {
            if (zkClient().exists(parentPath, true)) {
              java.util.List<String> children = zkClient().getChildren(parentPath, null, true);
              if (!children.isEmpty()) {
                String child = children.get(0);
                byte[] data = zkClient().getData(parentPath + "/" + child, null, null, true);
                @SuppressWarnings({"rawtypes"})
                Map m = (Map) Utils.fromJSON(data);
                if (m.containsKey("requestid")) {
                  znodeCreated.set(m.get("requestid").equals(asyncId));
                }
              }
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

        }
      };
      List<CollectionAdminRequest.AsyncCollectionAdminRequest> operations = Lists.asList(moveReplica, new CollectionAdminRequest.AsyncCollectionAdminRequest[]{mockRequest});
      NodeLostTrigger.NodeLostEvent nodeLostEvent = new NodeLostTrigger.NodeLostEvent
        (TriggerEventType.NODELOST, "mock_trigger_name",
         Collections.singletonList(cloudManager.getTimeSource().getTimeNs()),
         Collections.singletonList(sourceNodeName),
         CollectionParams.CollectionAction.MOVEREPLICA.toLower());
      ActionContext actionContext = new ActionContext(survivor.getCoreContainer().getZkController().getSolrCloudManager(), null,
          new HashMap<>(Collections.singletonMap("operations", operations)));
      action.process(nodeLostEvent, actionContext);

//      assertTrue("ExecutePlanAction should have stored the requestid in ZK before executing the request", znodeCreated.get());
      @SuppressWarnings({"unchecked"})
      List<NamedList<Object>> responses = (List<NamedList<Object>>) actionContext.getProperty("responses");
      assertNotNull(responses);
      assertEquals(2, responses.size());
      NamedList<Object> response = responses.get(0);
      assertNull(response.get("failure"));
      assertNotNull(response.get("success"));
    }

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionName, clusterShape(1, 2));
  }

  @Test
  public void testIntegration() throws Exception  {
    CloudSolrClient solrClient = cluster.getSolrClient();

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'execute_plan','class':'solr.ExecutePlanAction'}]" +
        "}}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String collectionName = "testIntegration";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 1, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);
    
    cluster.waitForActiveCollection(collectionName, 1, 2);

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionName, clusterShape(1, 2));

    JettySolrRunner sourceNode = cluster.getRandomJetty(random());
    String sourceNodeName = sourceNode.getNodeName();
    ClusterState clusterState = solrClient.getZkStateReader().getClusterState();
    DocCollection docCollection = clusterState.getCollection(collectionName);
    List<Replica> replicas = docCollection.getReplicas(sourceNodeName);
    assertNotNull(replicas);
    assertFalse(replicas.isEmpty());

    List<JettySolrRunner> otherJetties = cluster.getJettySolrRunners().stream()
        .filter(jettySolrRunner -> jettySolrRunner != sourceNode).collect(Collectors.toList());
    assertFalse(otherJetties.isEmpty());
    JettySolrRunner survivor = otherJetties.get(0);

    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner runner = cluster.getJettySolrRunner(i);
      if (runner == sourceNode) {
        JettySolrRunner j = cluster.stopJettySolrRunner(i);
        cluster.waitForJettyToStop(j);
      }
    }
    
    Thread.sleep(1000);

    waitForState("Timed out waiting for replicas of collection to be 2 again",
        collectionName, clusterShape(1, 2));

    clusterState = solrClient.getZkStateReader().getClusterState();
    docCollection = clusterState.getCollection(collectionName);
    List<Replica> replicasOnSurvivor = docCollection.getReplicas(survivor.getNodeName());
    assertNotNull(replicasOnSurvivor);
    assertEquals(docCollection.toString(), 2, replicasOnSurvivor.size());
  }

  @Test
  public void testTaskTimeout() throws Exception  {
    int DELAY = 2000;
    boolean taskTimeoutFail = random().nextBoolean();
    TestInjection.delayInExecutePlanAction = DELAY;
    CloudSolrClient solrClient = cluster.getSolrClient();
    String triggerName = "node_lost_trigger2";

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : '" + triggerName + "'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'execute_plan','class':'solr.ExecutePlanAction', 'taskTimeoutSeconds' : '1','taskTimeoutFail':'" + taskTimeoutFail + "'}," +
        "{'name':'finish','class':'" + FinishAction.class.getName() + "'}]" +
        "}}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String collectionName = "testTaskTimeout";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 1, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);

    cluster.waitForActiveCollection(collectionName, 1, 2);

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionName, clusterShape(1, 2));

    JettySolrRunner sourceNode = cluster.getRandomJetty(random());

    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner runner = cluster.getJettySolrRunner(i);
      if (runner == sourceNode) {
        JettySolrRunner j = cluster.stopJettySolrRunner(i);
        cluster.waitForJettyToStop(j);
      }
    }

    boolean await = finishedProcessing.await(DELAY * 5, TimeUnit.MILLISECONDS);
    if (taskTimeoutFail) {
      assertFalse("finished processing event but should fail", await);
    } else {
      assertTrue("did not finish processing event in time", await);
    }
    String path = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + triggerName + "/execute_plan";
    assertTrue(path + " does not exist", zkClient().exists(path, true));
    List<String> requests = zkClient().getChildren(path, null, true);
    assertFalse("some requests should be still present", requests.isEmpty());

    // in either case the task will complete and move the replica as needed
    waitForState("Timed out waiting for replicas of collection to be 2 again",
        collectionName, clusterShape(1, 2));
  }

  @Test
  public void testTaskFail() throws Exception  {
    TestInjection.failInExecutePlanAction = true;
    CloudSolrClient solrClient = cluster.getSolrClient();
    String triggerName = "node_lost_trigger3";

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : '" + triggerName + "'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'start', 'class' : '" + StartAction.class.getName() + "'}," +
        "{'name':'compute_plan','class':'solr.ComputePlanAction'}," +
        "{'name':'execute_plan','class':'solr.ExecutePlanAction'}," +
        "{'name':'finish','class':'" + FinishAction.class.getName() + "'}]" +
        "}}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String collectionName = "testTaskFail";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 1, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);

    cluster.waitForActiveCollection(collectionName, 1, 2);

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionName, clusterShape(1, 2));

    // don't stop the jetty that runs our SolrCloudManager
    JettySolrRunner runner = cluster.stopJettySolrRunner(1);
    cluster.waitForJettyToStop(runner);

    boolean await = startedProcessing.await(10, TimeUnit.SECONDS);
    assertTrue("did not start processing event in time", await);
    await = finishedProcessing.await(2, TimeUnit.SECONDS);
    assertFalse("finished processing event but should fail", await);

    String path = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/" + triggerName + "/execute_plan";
    assertTrue(path + " does not exist", zkClient().exists(path, true));
    List<String> requests = zkClient().getChildren(path, null, true);
    assertTrue("there should be no requests pending but got " + requests, requests.isEmpty());

    // the task never completed - we actually lost a replica
    try {
      CloudUtil.waitForState(cloudManager, collectionName, 5, TimeUnit.SECONDS,
          CloudUtil.clusterShape(1, 2));
      fail("completed a task that should have failed");
    } catch (TimeoutException te) {
      // expected
    }
  }
}
