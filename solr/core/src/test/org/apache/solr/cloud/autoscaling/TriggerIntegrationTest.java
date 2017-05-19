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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.ScheduledTriggers.DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS;
import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

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

  private String path;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void setupTest() throws Exception {
    waitForSeconds = 1 + random().nextInt(3);
    actionCreated = new CountDownLatch(1);
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired = new AtomicBoolean(false);
    eventRef = new AtomicReference<>();
    // clear any persisted auto scaling configuration
    Stat stat = zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);
    log.info(SOLR_AUTOSCALING_CONF_PATH + " reset, new znode version {}", stat.getVersion());
    // todo nocommit -- add testing for the v2 path
    // String path = random().nextBoolean() ? "/admin/autoscaling" : "/v2/cluster/autoscaling";
    this.path = "/admin/autoscaling";
    while (cluster.getJettySolrRunners().size() < 2) {
      // perhaps a test stopped a node but didn't start it back
      // lets start a node
      cluster.startJettySolrRunner();
    }
  }

  @Test
  public void testTriggerThrottling() throws Exception  {
    // for this test we want to create two triggers so we must assert that the actions were created twice
    TriggerIntegrationTest.actionCreated = new CountDownLatch(2);
    // similarly we want both triggers to fire
    triggerFiredLatch = new CountDownLatch(2);

    CloudSolrClient solrClient = cluster.getSolrClient();

    // first trigger
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger1'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + ThrottingTesterAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // second trigger
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger2'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + ThrottingTesterAction.class.getName() + "'}]" +
        "}}";
    req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // wait until the two instances of action are created
    if (!actionCreated.await(3, TimeUnit.SECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    JettySolrRunner newNode = cluster.startJettySolrRunner();

    if (!triggerFiredLatch.await(10, TimeUnit.SECONDS)) {
      fail("Both triggers should have fired by now");
    }

    // reset shared state
    lastActionExecutedAt.set(0);
    TriggerIntegrationTest.actionCreated = new CountDownLatch(2);
    triggerFiredLatch = new CountDownLatch(2);

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger1'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + ThrottingTesterAction.class.getName() + "'}]" +
        "}}";
    req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger2'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + ThrottingTesterAction.class.getName() + "'}]" +
        "}}";
    req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // wait until the two instances of action are created
    if (!actionCreated.await(3, TimeUnit.SECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    // stop the node we had started earlier
    List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
    for (int i = 0; i < jettySolrRunners.size(); i++) {
      JettySolrRunner jettySolrRunner = jettySolrRunners.get(i);
      if (jettySolrRunner == newNode) {
        cluster.stopJettySolrRunner(i);
        break;
      }
    }

    if (!triggerFiredLatch.await(10, TimeUnit.SECONDS)) {
      fail("Both triggers should have fired by now");
    }
  }

  static AtomicLong lastActionExecutedAt = new AtomicLong(0);
  static ReentrantLock lock = new ReentrantLock();
  public static class ThrottingTesterAction extends TestTriggerAction {
    // nanos are very precise so we need a delta for comparison with ms
    private static final long DELTA_MS = 2;

    // sanity check that an action instance is only invoked once
    private final AtomicBoolean onlyOnce = new AtomicBoolean(false);

    @Override
    public void process(AutoScaling.TriggerEvent event) {
      boolean locked = lock.tryLock();
      if (!locked)  {
        log.info("We should never have a tryLock fail because actions are never supposed to be executed concurrently");
        return;
      }
      try {
        if (lastActionExecutedAt.get() != 0)  {
          log.info("last action at " + lastActionExecutedAt.get() + " nano time = " + System.nanoTime());
          if (System.nanoTime() - lastActionExecutedAt.get() < TimeUnit.NANOSECONDS.convert(ScheduledTriggers.DEFAULT_MIN_MS_BETWEEN_ACTIONS - DELTA_MS, TimeUnit.MILLISECONDS)) {
            log.info("action executed again before minimum wait time from {}", event.getSource().getName());
            fail("TriggerListener was fired before the throttling period");
          }
        }
        if (onlyOnce.compareAndSet(false, true)) {
          log.info("action executed from {}", event.getSource().getName());
          lastActionExecutedAt.set(System.nanoTime());
          triggerFiredLatch.countDown();
        } else  {
          log.info("action executed more than once from {}", event.getSource().getName());
          fail("Trigger should not have fired more than once!");
        }
      } finally {
        if (locked) {
          lock.unlock();
        }
      }
    }
  }

  @Test
  public void testNodeLostTriggerRestoreState() throws Exception {
    // for this test we want to update the trigger so we must assert that the actions were created twice
    TriggerIntegrationTest.actionCreated = new CountDownLatch(2);

    // start a new node
    JettySolrRunner newNode = cluster.startJettySolrRunner();
    String nodeName = newNode.getNodeName();

    CloudSolrClient solrClient = cluster.getSolrClient();
    waitForSeconds = 5;
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_restore_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '5s'," + // should be enough for us to update the trigger
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    TimeOut timeOut = new TimeOut(2, TimeUnit.SECONDS);
    while (actionCreated.getCount() == 0 && !timeOut.hasTimedOut()) {
      Thread.sleep(200);
    }
    assertTrue("The action specified in node_lost_restore_trigger was not instantiated even after 2 seconds",actionCreated.getCount() > 0);

    List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
    int index = -1;
    for (int i = 0; i < jettySolrRunners.size(); i++) {
      JettySolrRunner runner = jettySolrRunners.get(i);
      if (runner == newNode) index = i;
    }
    assertFalse(index == -1);
    cluster.stopJettySolrRunner(index);

    // ensure that the old trigger sees the stopped node, todo find a better way to do this
    Thread.sleep(500 + TimeUnit.MILLISECONDS.convert(DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS, TimeUnit.SECONDS));

    waitForSeconds = 0;
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_restore_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '0s'," + // update a property so that it replaces the old trigger, also we want it to fire immediately
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // wait until the second instance of action is created
    if (!actionCreated.await(3, TimeUnit.SECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    boolean await = triggerFiredLatch.await(5, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeLostTrigger.NodeLostEvent nodeLostEvent = (NodeLostTrigger.NodeLostEvent) eventRef.get();
    assertNotNull(nodeLostEvent);
    assertEquals("The node added trigger was fired but for a different node",
        nodeName, nodeLostEvent.getNodeName());
  }

  @Test
  public void testNodeAddedTriggerRestoreState() throws Exception {
    // for this test we want to update the trigger so we must assert that the actions were created twice
    TriggerIntegrationTest.actionCreated = new CountDownLatch(2);

    CloudSolrClient solrClient = cluster.getSolrClient();
    waitForSeconds = 5;
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_restore_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '5s'," + // should be enough for us to update the trigger
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    TimeOut timeOut = new TimeOut(2, TimeUnit.SECONDS);
    while (actionCreated.getCount() == 0 && !timeOut.hasTimedOut()) {
      Thread.sleep(200);
    }
    assertTrue("The action specified in node_added_restore_trigger was not instantiated even after 2 seconds",actionCreated.getCount() > 0);

    // start a new node
    JettySolrRunner newNode = cluster.startJettySolrRunner();

    // ensure that the old trigger sees the new node, todo find a better way to do this
    Thread.sleep(500 + TimeUnit.MILLISECONDS.convert(DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS, TimeUnit.SECONDS));

    waitForSeconds = 0;
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_restore_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '0s'," + // update a property so that it replaces the old trigger, also we want it to fire immediately
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // wait until the second instance of action is created
    if (!actionCreated.await(3, TimeUnit.SECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    boolean await = triggerFiredLatch.await(5, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) eventRef.get();
    assertNotNull(nodeAddedEvent);
    assertEquals("The node added trigger was fired but for a different node",
        newNode.getNodeName(), nodeAddedEvent.getNodeName());
  }

  @Test
  public void testNodeAddedTrigger() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
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

  @Test
  public void testContinueTriggersOnOverseerRestart() throws Exception  {
    CollectionAdminRequest.OverseerStatus status = new CollectionAdminRequest.OverseerStatus();
    CloudSolrClient solrClient = cluster.getSolrClient();
    CollectionAdminResponse adminResponse = status.process(solrClient);
    NamedList<Object> response = adminResponse.getResponse();
    String leader = (String) response.get("leader");
    JettySolrRunner overseerNode = null;
    int index = -1;
    List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
    for (int i = 0; i < jettySolrRunners.size(); i++) {
      JettySolrRunner runner = jettySolrRunners.get(i);
      if (runner.getNodeName().equals(leader)) {
        overseerNode = runner;
        index = i;
        break;
      }
    }
    assertNotNull(overseerNode);

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = new AutoScalingHandlerTest.AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    if (!actionCreated.await(3, TimeUnit.SECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    // stop the overseer, somebody else will take over as the overseer
    cluster.stopJettySolrRunner(index);

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) eventRef.get();
    assertNotNull(nodeAddedEvent);
    assertEquals("The node added trigger was fired but for a different node",
        newNode.getNodeName(), nodeAddedEvent.getNodeName());
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
