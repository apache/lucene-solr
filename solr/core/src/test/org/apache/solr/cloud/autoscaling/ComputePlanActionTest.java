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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Charsets;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

/**
 * Test for {@link ComputePlanAction}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class ComputePlanActionTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final AtomicBoolean fired = new AtomicBoolean(false);
  private static CountDownLatch triggerFiredLatch = new CountDownLatch(1);
  private static final AtomicReference<Object> eventContextRef = new AtomicReference<>();

  private String path = null;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    fired.set(false);
    triggerFiredLatch = new CountDownLatch(1);
    eventContextRef.set(null);
    this.path = "/admin/autoscaling";

    // remove everything from autoscaling.json in ZK
    zkClient().setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, "{}".getBytes(Charsets.UTF_8), true);

    CloudSolrClient solrClient = cluster.getSolrClient();
    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    SolrRequest req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setClusterPolicyCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setClusterPreferencesCommand = "{" +
        "'set-cluster-preferences': [" +
        "{'minimize': 'cores','precision': 3}," +
        "{'maximize': 'freedisk','precision': 100}]" +
        "}";
    req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setClusterPreferencesCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
  }

  @Test
  public void testNodeLost() throws Exception  {
    // let's start a node so that we have at least two
    JettySolrRunner runner = cluster.startJettySolrRunner();
    String node = runner.getNodeName();

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
    SolrRequest req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("testNodeLost",
        1, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        "testNodeLost", (liveNodes, collectionState) -> collectionState.getReplicas().stream().allMatch(replica -> replica.isActive(liveNodes)));

    ClusterState clusterState = cluster.getSolrClient().getZkStateReader().getClusterState();
    DocCollection collection = clusterState.getCollection("testNodeLost");
    List<Replica> replicas = collection.getReplicas(node);
    assertNotNull(replicas);
    assertFalse(replicas.isEmpty());

    // start another node because because when the other node goes away, the cluster policy requires only
    // 1 replica per node and none on the overseer
    cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);

    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jettySolrRunner = cluster.getJettySolrRunners().get(i);
      if (jettySolrRunner == runner)  {
        cluster.stopJettySolrRunner(i);
        break;
      }
    }
    cluster.waitForAllNodes(30);

    assertTrue("Trigger was not fired even after 5 seconds", triggerFiredLatch.await(5, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map context = (Map) eventContextRef.get();
    assertNotNull(context);
    List<SolrRequest> operations = (List<SolrRequest>) context.get("operations");
    assertNotNull("The operations computed by ComputePlanAction should not be null", operations);
    assertEquals("ComputePlanAction should have computed exactly 1 operation", 1, operations.size());
    SolrRequest solrRequest = operations.get(0);
    SolrParams params = solrRequest.getParams();
    assertEquals("Expected MOVEREPLICA action after adding node", MOVEREPLICA, CollectionParams.CollectionAction.get(params.get("action")));
    String replicaToBeMoved = params.get("replica");
    assertEquals("Unexpected node in computed operation", replicas.get(0).getName(), replicaToBeMoved);
  }

  @Test
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
    SolrRequest req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("testNodeAdded",
        1, 2);
    create.setMaxShardsPerNode(2);
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        "testNodeAdded", (liveNodes, collectionState) -> collectionState.getReplicas().stream().allMatch(replica -> replica.isActive(liveNodes)));

    JettySolrRunner runner = cluster.startJettySolrRunner();
    assertTrue("Trigger was not fired even after 5 seconds", triggerFiredLatch.await(5, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map context = (Map) eventContextRef.get();
    assertNotNull(context);
    List<SolrRequest> operations = (List<SolrRequest>) context.get("operations");
    assertNotNull("The operations computed by ComputePlanAction should not be null", operations);
    assertEquals("ComputePlanAction should have computed exactly 1 operation", 1, operations.size());
    SolrRequest request = operations.get(0);
    SolrParams params = request.getParams();
    assertEquals("Expected MOVEREPLICA action after adding node", MOVEREPLICA, CollectionParams.CollectionAction.get(params.get("action")));
    String nodeAdded = params.get("targetNode");
    assertEquals("Unexpected node in computed operation", runner.getNodeName(), nodeAdded);
  }

  public static class AssertingTriggerAction implements TriggerAction {

    @Override
    public String getName() {
      return null;
    }

    @Override
    public void process(TriggerEvent event, ActionContext context) {
      if (fired.compareAndSet(false, true)) {
        eventContextRef.set(context.getProperties());
        triggerFiredLatch.countDown();
      }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void init(Map<String, String> args) {

    }
  }
}
