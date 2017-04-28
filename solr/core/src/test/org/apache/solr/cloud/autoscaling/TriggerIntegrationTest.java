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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An end-to-end integration test for triggers
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class TriggerIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CountDownLatch actionCreated;
  private static CountDownLatch triggerFiredLatch;
  private static int waitForSeconds = 1;
  private static AtomicBoolean triggerFired;
  private static AtomicReference<AutoScaling.TriggerEvent> eventRef;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    waitForSeconds = 1 + random().nextInt(3);
  }

  @Before
  public void setupTest() {
    actionCreated = new CountDownLatch(1);
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired = new AtomicBoolean(false);
    eventRef = new AtomicReference<>();
  }

  @Test
  public void testNodeAddedTrigger() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    // todo nocommit -- add testing for the v2 path
    // String path = random().nextBoolean() ? "/admin/autoscaling" : "/v2/cluster/autoscaling";
    String path = "/admin/autoscaling";
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionCreated.await(3, TimeUnit.SECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) eventRef.get();
    assertNotNull(nodeAddedEvent);
    assertEquals("The node added trigger was fired but for a different node",
        newNode.getNodeName(), nodeAddedEvent.getNodeName());
  }

  @Test
  public void testNodeLostTrigger() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    // todo nocommit -- add testing for the v2 path
    // String path = random().nextBoolean() ? "/admin/autoscaling" : "/v2/cluster/autoscaling";
    String path = "/admin/autoscaling";
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    NamedList<Object> overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    String overseerLeader = (String) overSeerStatus.get("leader");
    int nonOverseerLeaderIndex = 0;
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jetty = cluster.getJettySolrRunner(i);
      if (!jetty.getNodeName().equals(overseerLeader)) {
        nonOverseerLeaderIndex = i;
      }
    }
    SolrRequest req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionCreated.await(3, TimeUnit.SECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    String lostNodeName = cluster.getJettySolrRunner(nonOverseerLeaderIndex).getNodeName();
    cluster.stopJettySolrRunner(nonOverseerLeaderIndex);
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeLostTrigger.NodeLostEvent nodeLostEvent = (NodeLostTrigger.NodeLostEvent) eventRef.get();
    assertNotNull(nodeLostEvent);
    assertEquals("The node lost trigger was fired but for a different node",
        lostNodeName, nodeLostEvent.getNodeName());
  }

  public static class TestTriggerAction implements TriggerAction {

    public TestTriggerAction() {
      log.info("TestTriggerAction instantiated");
      actionCreated.countDown();
    }

    @Override
    public String getName() {
      return "TestTriggerAction";
    }

    @Override
    public String getClassName() {
      return this.getClass().getName();
    }

    @Override
    public void process(AutoScaling.TriggerEvent event) {
      if (triggerFired.compareAndSet(false, true))  {
        eventRef.set(event);
        if (System.nanoTime() - event.getEventNanoTime() <= TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS)) {
          fail("NodeAddedListener was fired before the configured waitFor period");
        }
        triggerFiredLatch.countDown();
      } else  {
        fail("NodeAddedTrigger was fired more than once!");
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
