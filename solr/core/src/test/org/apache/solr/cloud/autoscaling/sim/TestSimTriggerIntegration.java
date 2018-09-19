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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.ComputePlanAction;
import org.apache.solr.cloud.autoscaling.ExecutePlanAction;
import org.apache.solr.cloud.autoscaling.NodeLostTrigger;
import org.apache.solr.cloud.autoscaling.ScheduledTriggers;
import org.apache.solr.cloud.autoscaling.SearchRateTrigger;
import org.apache.solr.cloud.autoscaling.TriggerActionBase;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerEventQueue;
import org.apache.solr.cloud.autoscaling.TriggerListenerBase;
import org.apache.solr.cloud.autoscaling.CapturedEvent;
import org.apache.solr.cloud.autoscaling.TriggerValidationException;
import org.apache.solr.common.cloud.LiveNodesListener;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;
import static org.apache.solr.cloud.autoscaling.ScheduledTriggers.DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS;

/**
 * An end-to-end integration test for triggers
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class TestSimTriggerIntegration extends SimSolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int SPEED = 50;

  private static CountDownLatch actionConstructorCalled;
  private static CountDownLatch actionInitCalled;
  private static CountDownLatch triggerFiredLatch;
  private static int waitForSeconds = 1;
  private static CountDownLatch actionStarted;
  private static CountDownLatch actionInterrupted;
  private static CountDownLatch actionCompleted;
  private static CountDownLatch triggerStartedLatch;
  private static CountDownLatch triggerFinishedLatch;
  private static AtomicInteger triggerStartedCount;
  private static AtomicInteger triggerFinishedCount;
  private static AtomicBoolean triggerFired;
  private static Set<TriggerEvent> events = ConcurrentHashMap.newKeySet();

  private static final long WAIT_FOR_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(5);

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2, TimeSource.get("simTime:" + SPEED));
  }

  private static CountDownLatch getTriggerFiredLatch() {
    return triggerFiredLatch;
  }

  private static CountDownLatch getActionStarted() {
    return actionStarted;
  }

  private static CountDownLatch getActionInterrupted() {
    return actionInterrupted;
  }

  private static CountDownLatch getActionCompleted() {
    return actionCompleted;
  }

  @Before
  public void setupTest() throws Exception {
    // disable .scheduled_maintenance
    String suspendTriggerCommand = "{" +
        "'suspend-trigger' : {'name' : '.scheduled_maintenance'}" +
        "}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
    SolrClient solrClient = cluster.simGetSolrClient();
    NamedList<Object> response = solrClient.request(req);
    String result = response.get("result").toString();
    if (!"success".equals(result) && !result.contains("No trigger exists")) {
      fail("Unexpected response: " + result);
    }

    waitForSeconds = 1 + random().nextInt(3);
    actionConstructorCalled = new CountDownLatch(1);
    actionInitCalled = new CountDownLatch(1);
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired = new AtomicBoolean(false);
    actionStarted = new CountDownLatch(1);
    actionInterrupted = new CountDownLatch(1);
    actionCompleted = new CountDownLatch(1);
    triggerStartedLatch = new CountDownLatch(1);
    triggerFinishedLatch = new CountDownLatch(1);
    triggerStartedCount = new AtomicInteger();
    triggerFinishedCount = new AtomicInteger();
    events.clear();
    listenerEvents.clear();
    while (cluster.getClusterStateProvider().getLiveNodes().size() < 2) {
      // perhaps a test stopped a node but didn't start it back
      // lets start a node
      cluster.simAddNode();
    }
    // do this in advance if missing
    if (!cluster.getSimClusterStateProvider().simListCollections().contains(CollectionAdminParams.SYSTEM_COLL)) {
      cluster.getSimClusterStateProvider().createSystemCollection();
      CloudTestUtils.waitForState(cluster, CollectionAdminParams.SYSTEM_COLL, 120, TimeUnit.SECONDS,
          CloudTestUtils.clusterShape(1, 1, false, true));
    }
  }

  @Test
  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2018-06-18
  public void testTriggerThrottling() throws Exception  {
    // for this test we want to create two triggers so we must assert that the actions were created twice
    actionInitCalled = new CountDownLatch(2);
    // similarly we want both triggers to fire
    triggerFiredLatch = new CountDownLatch(2);

    SolrClient solrClient = cluster.simGetSolrClient();

    // first trigger
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger1'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + ThrottlingTesterAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // second trigger
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger2'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + ThrottlingTesterAction.class.getName() + "'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // wait until the two instances of action are created
    if (!actionInitCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    String newNode = cluster.simAddNode();

    if (!triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS)) {
      fail("Both triggers should have fired by now");
    }

    // reset shared state
    lastActionExecutedAt.set(0);
    TestSimTriggerIntegration.actionInitCalled = new CountDownLatch(2);
    triggerFiredLatch = new CountDownLatch(2);

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger1'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + ThrottlingTesterAction.class.getName() + "'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger2'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + ThrottlingTesterAction.class.getName() + "'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // wait until the two instances of action are created
    if (!actionInitCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    // stop the node we had started earlier
    cluster.simRemoveNode(newNode, false);

    if (!triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS)) {
      fail("Both triggers should have fired by now");
    }
  }

  static AtomicLong lastActionExecutedAt = new AtomicLong(0);
  static ReentrantLock lock = new ReentrantLock();
  public static class ThrottlingTesterAction extends TestTriggerAction {
    // nanos are very precise so we need a delta for comparison with ms
    private static final long DELTA_MS = 2;

    // sanity check that an action instance is only invoked once
    private final AtomicBoolean onlyOnce = new AtomicBoolean(false);

    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {
      boolean locked = lock.tryLock();
      if (!locked)  {
        log.info("We should never have a tryLock fail because actions are never supposed to be executed concurrently");
        return;
      }
      try {
        if (lastActionExecutedAt.get() != 0)  {
          log.info("last action at " + lastActionExecutedAt.get() + " time = " + cluster.getTimeSource().getTimeNs());
          if (TimeUnit.NANOSECONDS.toMillis(cluster.getTimeSource().getTimeNs() - lastActionExecutedAt.get()) <
              TimeUnit.SECONDS.toMillis(ScheduledTriggers.DEFAULT_ACTION_THROTTLE_PERIOD_SECONDS) - DELTA_MS) {
            log.info("action executed again before minimum wait time from {}", event.getSource());
            fail("TriggerListener was fired before the throttling period");
          }
        }
        if (onlyOnce.compareAndSet(false, true)) {
          log.info("action executed from {}", event.getSource());
          lastActionExecutedAt.set(cluster.getTimeSource().getTimeNs());
          getTriggerFiredLatch().countDown();
        } else  {
          log.info("action executed more than once from {}", event.getSource());
          fail("Trigger should not have fired more than once!");
        }
      } finally {
        lock.unlock();
      }
    }
  }

  @Test
  // commented 20-July-2018  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  // commented 4-Sep-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 09-Aug-2018
  public void testNodeLostTriggerRestoreState() throws Exception {
    // for this test we want to update the trigger so we must assert that the actions were created twice
    TestSimTriggerIntegration.actionInitCalled = new CountDownLatch(2);

    // start a new node
    String nodeName = cluster.simAddNode();

    SolrClient solrClient = cluster.simGetSolrClient();
    waitForSeconds = 5;
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_restore_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '5s'," + // should be enough for us to update the trigger
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    TimeOut timeOut = new TimeOut(2, TimeUnit.SECONDS, cluster.getTimeSource());
    while (actionInitCalled.getCount() == 0 && !timeOut.hasTimedOut()) {
      timeOut.sleep(200);
    }
    assertTrue("The action specified in node_lost_restore_trigger was not instantiated even after 2 seconds", actionInitCalled.getCount() > 0);

    cluster.simRemoveNode(nodeName, false);

    // ensure that the old trigger sees the stopped node, todo find a better way to do this
    timeOut.sleep(500 + TimeUnit.SECONDS.toMillis(DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS));

    waitForSeconds = 0;
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_restore_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '0s'," + // update a property so that it replaces the old trigger, also we want it to fire immediately
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // wait until the second instance of action is created
    if (!actionInitCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    boolean await = triggerFiredLatch.await(5000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeLostTrigger.NodeLostEvent nodeLostEvent = (NodeLostTrigger.NodeLostEvent) events.iterator().next();
    assertNotNull(nodeLostEvent);
    List<String> nodeNames = (List<String>)nodeLostEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(nodeName));
  }

  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 09-Apr-2018
  public void testNodeAddedTriggerRestoreState() throws Exception {
    // for this test we want to update the trigger so we must assert that the actions were created twice
    TestSimTriggerIntegration.actionInitCalled = new CountDownLatch(2);

    SolrClient solrClient = cluster.simGetSolrClient();
    waitForSeconds = 5;
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_restore_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '5s'," + // should be enough for us to update the trigger
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    TimeOut timeOut = new TimeOut(2, TimeUnit.SECONDS, cluster.getTimeSource());
    while (actionInitCalled.getCount() == 0 && !timeOut.hasTimedOut()) {
      timeOut.sleep(200);
    }
    assertTrue("The action specified in node_added_restore_trigger was not instantiated even after 2 seconds", actionInitCalled.getCount() > 0);

    // start a new node
    String newNode = cluster.simAddNode();

    // ensure that the old trigger sees the new node, todo find a better way to do this
    cluster.getTimeSource().sleep(500 + TimeUnit.SECONDS.toMillis(DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS));

    waitForSeconds = 0;
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_restore_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '0s'," + // update a property so that it replaces the old trigger, also we want it to fire immediately
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // wait until the second instance of action is created
    if (!actionInitCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    boolean await = triggerFiredLatch.await(5000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    TriggerEvent nodeAddedEvent = events.iterator().next();
    assertNotNull(nodeAddedEvent);
    List<String> nodeNames = (List<String>)nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.toString(), nodeNames.contains(newNode));
  }

  @Test
  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2018-06-18
  public void testNodeAddedTrigger() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionInitCalled.await(5000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    String newNode = cluster.simAddNode();
    boolean await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    TriggerEvent nodeAddedEvent = events.iterator().next();
    assertNotNull(nodeAddedEvent);
    List<String> nodeNames = (List<String>)nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeAddedEvent.toString(), nodeNames.contains(newNode));

    // reset
    actionConstructorCalled = new CountDownLatch(1);
    actionInitCalled = new CountDownLatch(1);

    // update the trigger with exactly the same data
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // this should be a no-op so the action should have been created but init should not be called
    if (!actionConstructorCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    assertFalse(actionInitCalled.await(2000 / SPEED, TimeUnit.MILLISECONDS));
  }

  @Test
  // commented 4-Sep-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 26-Mar-2018
  public void testNodeLostTrigger() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionInitCalled.await(5000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    String lostNodeName = cluster.getSimClusterStateProvider().simGetRandomNode(random());
    cluster.simRemoveNode(lostNodeName, false);
    boolean await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    TriggerEvent nodeLostEvent = events.iterator().next();
    assertNotNull(nodeLostEvent);
    List<String> nodeNames = (List<String>)nodeLostEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(lostNodeName));

    // reset
    actionConstructorCalled = new CountDownLatch(1);
    actionInitCalled = new CountDownLatch(1);

    // update the trigger with exactly the same data
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // this should be a no-op so the action should have been created but init should not be called
    if (!actionConstructorCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    assertFalse(actionInitCalled.await(2000 / SPEED, TimeUnit.MILLISECONDS));
  }

  // simulator doesn't support overseer functionality yet
  /*
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
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    if (!actionInitCalled.await(3, TimeUnit.SECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    // stop the overseer, somebody else will take over as the overseer
    cluster.stopJettySolrRunner(index);
    Thread.sleep(10000);
    JettySolrRunner newNode = cluster.startJettySolrRunner();
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) events.iterator().next();
    assertNotNull(nodeAddedEvent);
    List<String> nodeNames = (List<String>)nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(newNode.getNodeName()));
  }

*/

  public static class TestTriggerAction extends TriggerActionBase {

    public TestTriggerAction() {
      actionConstructorCalled.countDown();
    }

    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {
      try {
        if (triggerFired.compareAndSet(false, true))  {
          events.add(event);
          long currentTimeNanos = cluster.getTimeSource().getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail(event.getSource() + " was fired before the configured waitFor period");
          }
          getTriggerFiredLatch().countDown();
        } else  {
          fail(event.getSource() + " was fired more than once!");
        }
      } catch (Throwable t) {
        log.debug("--throwable", t);
        throw t;
      }
    }

    @Override
    public void init() throws Exception {
      log.info("TestTriggerAction init");
      super.init();
      actionInitCalled.countDown();
    }

  }

  public static class TestEventQueueAction extends TriggerActionBase {

    public TestEventQueueAction() {
      log.info("TestEventQueueAction instantiated");
    }

    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {
      log.info("-- event: " + event);
      events.add(event);
      getActionStarted().countDown();
      try {
        Thread.sleep(eventQueueActionWait);
        triggerFired.compareAndSet(false, true);
        getActionCompleted().countDown();
      } catch (InterruptedException e) {
        getActionInterrupted().countDown();
        return;
      }
    }

    @Override
    public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> args) throws TriggerValidationException {
      log.debug("TestTriggerAction init");
      actionInitCalled.countDown();
      super.configure(loader, cloudManager, args);
    }
  }

  public static long eventQueueActionWait = 5000;

  @Test
  // commented 4-Sep-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 16-Apr-2018
  public void testEventQueue() throws Exception {
    waitForSeconds = 1;
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger1'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestEventQueueAction.class.getName() + "'}]" +
        "}}";

    String overseerLeader = cluster.getSimClusterStateProvider().simGetRandomNode(random());

    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionInitCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    // add node to generate the event
    String newNode = cluster.simAddNode();
    boolean await = actionStarted.await(60000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("action did not start", await);
    // event should be there
    TriggerEvent nodeAddedEvent = events.iterator().next();
    assertNotNull(nodeAddedEvent);
    // but action did not complete yet so the event is still enqueued
    assertFalse(triggerFired.get());
    events.clear();
    actionStarted = new CountDownLatch(1);
    eventQueueActionWait = 1;
    // kill overseer
    cluster.simRestartOverseer(overseerLeader);
    cluster.getTimeSource().sleep(5000);
    // new overseer leader should be elected and run triggers
    await = actionInterrupted.await(3000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("action wasn't interrupted", await);
    // it should fire again from enqueued event
    await = actionStarted.await(60000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("action wasn't started", await);
    TriggerEvent replayedEvent = events.iterator().next();
    assertTrue(replayedEvent.getProperty(TriggerEventQueue.ENQUEUE_TIME) != null);
    assertTrue(events + "\n" + replayedEvent.toString(), replayedEvent.getProperty(TriggerEventQueue.DEQUEUE_TIME) != null);
    await = actionCompleted.await(10000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("action wasn't completed", await);
    assertTrue(triggerFired.get());
  }

  @Test
  // commented 4-Sep-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") //2018-03-10
  public void testEventFromRestoredState() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '10s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionInitCalled.await(10000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    events.clear();

    String newNode = cluster.simAddNode();
    boolean await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    // reset
    triggerFired.set(false);
    triggerFiredLatch = new CountDownLatch(1);
    TriggerEvent nodeAddedEvent = events.iterator().next();
    assertNotNull(nodeAddedEvent);
    List<String> nodeNames = (List<String>)nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(newNode));
    // add a second node - state of the trigger will change but it won't fire for waitFor sec.
    String newNode2 = cluster.simAddNode();
    cluster.getTimeSource().sleep(10000);
    // kill overseer
    cluster.simRestartOverseer(null);
    await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
  }

  private static class TestLiveNodesListener implements LiveNodesListener {
    Set<String> lostNodes = new HashSet<>();
    Set<String> addedNodes = new HashSet<>();
    CountDownLatch onChangeLatch = new CountDownLatch(1);

    public void reset() {
      lostNodes.clear();
      addedNodes.clear();
      onChangeLatch = new CountDownLatch(1);
    }

    @Override
    public void onChange(SortedSet<String> oldLiveNodes, SortedSet<String> newLiveNodes) {
      onChangeLatch.countDown();
      Set<String> old = new HashSet<>(oldLiveNodes);
      old.removeAll(newLiveNodes);
      if (!old.isEmpty()) {
        lostNodes.addAll(old);
      }
      newLiveNodes.removeAll(oldLiveNodes);
      if (!newLiveNodes.isEmpty()) {
        addedNodes.addAll(newLiveNodes);
      }
    }
  }

  private TestLiveNodesListener registerLiveNodesListener() {
    TestLiveNodesListener listener = new TestLiveNodesListener();
    cluster.getLiveNodesSet().registerLiveNodesListener(listener);
    return listener;
  }

  public static class TestEventMarkerAction extends TriggerActionBase {

    public TestEventMarkerAction() {
      actionConstructorCalled.countDown();
    }

    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {
      boolean locked = lock.tryLock();
      if (!locked)  {
        log.info("We should never have a tryLock fail because actions are never supposed to be executed concurrently");
        return;
      }
      try {
        events.add(event);
        getTriggerFiredLatch().countDown();
      } catch (Throwable t) {
        log.debug("--throwable", t);
        throw t;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> args) throws TriggerValidationException {
      log.info("TestEventMarkerAction init");
      actionInitCalled.countDown();
      super.configure(loader, cloudManager, args);
    }
  }

  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void testNodeMarkersRegistration() throws Exception {
    // for this test we want to create two triggers so we must assert that the actions were created twice
    actionInitCalled = new CountDownLatch(2);
    // similarly we want both triggers to fire
    triggerFiredLatch = new CountDownLatch(2);
    TestLiveNodesListener listener = registerLiveNodesListener();

    SolrClient solrClient = cluster.simGetSolrClient();

    // pick overseer node
    String overseerLeader = cluster.getSimClusterStateProvider().simGetRandomNode(random());

    // add a node
    String node = cluster.simAddNode();
    if (!listener.onChangeLatch.await(10000 / SPEED, TimeUnit.MILLISECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }
    assertEquals(1, listener.addedNodes.size());
    assertEquals(node, listener.addedNodes.iterator().next());
    // verify that a znode doesn't exist (no trigger)
    String pathAdded = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + node;
    assertFalse("Path " + pathAdded + " was created but there are no nodeAdded triggers",
        cluster.getDistribStateManager().hasData(pathAdded));
    listener.reset();
    // stop overseer
    log.info("====== KILL OVERSEER 1");
    cluster.simRestartOverseer(overseerLeader);
    if (!listener.onChangeLatch.await(10000 / SPEED, TimeUnit.MILLISECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }
    assertEquals(1, listener.lostNodes.size());
    assertEquals(overseerLeader, listener.lostNodes.iterator().next());
    assertEquals(0, listener.addedNodes.size());
    // wait until the new overseer is up
    cluster.getTimeSource().sleep(5000);
    // verify that a znode does NOT exist - there's no nodeLost trigger,
    // so the new overseer cleaned up existing nodeLost markers
    String pathLost = ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH + "/" + overseerLeader;
    assertFalse("Path " + pathLost + " exists", cluster.getDistribStateManager().hasData(pathLost));

    listener.reset();

    // set up triggers

    log.info("====== ADD TRIGGERS");
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestEventMarkerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestEventMarkerAction.class.getName() + "'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    overseerLeader = cluster.getSimClusterStateProvider().simGetRandomNode(random());

    // create another node
    log.info("====== ADD NODE 1");
    String node1 = cluster.simAddNode();
    if (!listener.onChangeLatch.await(10000 / SPEED, TimeUnit.MILLISECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }
    assertEquals(1, listener.addedNodes.size());
    assertEquals(node1, listener.addedNodes.iterator().next());
    // verify that a znode exists
    pathAdded = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + node1;
    assertTrue("Path " + pathAdded + " wasn't created", cluster.getDistribStateManager().hasData(pathAdded));

    cluster.getTimeSource().sleep(5000);
    // nodeAdded marker should be consumed now by nodeAdded trigger
    assertFalse("Path " + pathAdded + " should have been deleted",
        cluster.getDistribStateManager().hasData(pathAdded));

    listener.reset();
    events.clear();
    triggerFiredLatch = new CountDownLatch(1);
    // kill overseer again
    log.info("====== KILL OVERSEER 2");
    cluster.simRestartOverseer(overseerLeader);
    if (!listener.onChangeLatch.await(10000 / SPEED, TimeUnit.MILLISECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }


    if (!triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS)) {
      fail("Trigger should have fired by now");
    }
    assertEquals(1, events.size());
    TriggerEvent ev = events.iterator().next();
    List<String> nodeNames = (List<String>)ev.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(overseerLeader));
    assertEquals(TriggerEventType.NODELOST, ev.getEventType());
  }

  static Map<String, List<CapturedEvent>> listenerEvents = new ConcurrentHashMap<>();
  static List<CapturedEvent> allListenerEvents = new ArrayList<>();
  static CountDownLatch listenerCreated = new CountDownLatch(1);
  static boolean failDummyAction = false;

  public static class TestTriggerListener extends TriggerListenerBase {
    @Override
    public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) throws TriggerValidationException {
      super.configure(loader, cloudManager, config);
      listenerCreated.countDown();
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
      CapturedEvent ev = new CapturedEvent(cluster.getTimeSource().getTimeNs(), context, config, stage, actionName, event, message);
      lst.add(ev);
      allListenerEvents.add(ev);
    }
  }

  public static class TestDummyAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) {
      if (failDummyAction) {
        throw new RuntimeException("failure");
      }

    }
  }

  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void testListeners() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}," +
        "{'name':'test1','class':'" + TestDummyAction.class.getName() + "'}," +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionInitCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'foo'," +
        "'trigger' : 'node_added_trigger'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED', 'FAILED']," +
        "'beforeAction' : 'test'," +
        "'afterAction' : ['test', 'test1']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand1 = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'bar'," +
        "'trigger' : 'node_added_trigger'," +
        "'stage' : ['FAILED','SUCCEEDED']," +
        "'beforeAction' : ['test', 'test1']," +
        "'afterAction' : 'test'," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand1);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    listenerEvents.clear();
    failDummyAction = false;

    String newNode = cluster.simAddNode();
    boolean await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());

    assertEquals("both listeners should have fired", 2, listenerEvents.size());

    cluster.getTimeSource().sleep(2000);

    // check foo events
    List<CapturedEvent> testEvents = listenerEvents.get("foo");
    assertNotNull("foo events: " + testEvents, testEvents);
    assertEquals("foo events: " + testEvents, 5, testEvents.size());

    assertEquals(TriggerEventProcessorStage.STARTED, testEvents.get(0).stage);

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, testEvents.get(1).stage);
    assertEquals("test", testEvents.get(1).actionName);

    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, testEvents.get(2).stage);
    assertEquals("test", testEvents.get(2).actionName);

    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, testEvents.get(3).stage);
    assertEquals("test1", testEvents.get(3).actionName);

    assertEquals(TriggerEventProcessorStage.SUCCEEDED, testEvents.get(4).stage);

    // check bar events
    testEvents = listenerEvents.get("bar");
    assertNotNull("bar events", testEvents);
    assertEquals("bar events", 4, testEvents.size());

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, testEvents.get(0).stage);
    assertEquals("test", testEvents.get(0).actionName);

    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, testEvents.get(1).stage);
    assertEquals("test", testEvents.get(1).actionName);

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, testEvents.get(2).stage);
    assertEquals("test1", testEvents.get(2).actionName);

    assertEquals(TriggerEventProcessorStage.SUCCEEDED, testEvents.get(3).stage);

    // check global ordering of events (SOLR-12668)
    int fooIdx = -1;
    int barIdx = -1;
    for (int i = 0; i < allListenerEvents.size(); i++) {
      CapturedEvent ev = allListenerEvents.get(i);
      if (ev.stage == TriggerEventProcessorStage.BEFORE_ACTION && ev.actionName.equals("test")) {
        if (ev.config.name.equals("foo")) {
          fooIdx = i;
        } else if (ev.config.name.equals("bar")) {
          barIdx = i;
        }
      }
    }
    assertTrue("fooIdx not found", fooIdx != -1);
    assertTrue("barIdx not found", barIdx != -1);
    assertTrue("foo fired later than bar: fooIdx=" + fooIdx + ", barIdx=" + barIdx, fooIdx < barIdx);

    // reset
    triggerFired.set(false);
    triggerFiredLatch = new CountDownLatch(1);
    listenerEvents.clear();
    failDummyAction = true;

    newNode = cluster.simAddNode();
    await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);

    cluster.getTimeSource().sleep(2000);

    // check foo events
    testEvents = listenerEvents.get("foo");
    assertNotNull("foo events: " + testEvents, testEvents);
    assertEquals("foo events: " + testEvents, 4, testEvents.size());

    assertEquals(TriggerEventProcessorStage.STARTED, testEvents.get(0).stage);

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, testEvents.get(1).stage);
    assertEquals("test", testEvents.get(1).actionName);

    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, testEvents.get(2).stage);
    assertEquals("test", testEvents.get(2).actionName);

    assertEquals(TriggerEventProcessorStage.FAILED, testEvents.get(3).stage);
    assertEquals("test1", testEvents.get(3).actionName);

    // check bar events
    testEvents = listenerEvents.get("bar");
    assertNotNull("bar events", testEvents);
    assertEquals("bar events", 4, testEvents.size());

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, testEvents.get(0).stage);
    assertEquals("test", testEvents.get(0).actionName);

    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, testEvents.get(1).stage);
    assertEquals("test", testEvents.get(1).actionName);

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, testEvents.get(2).stage);
    assertEquals("test1", testEvents.get(2).actionName);

    assertEquals(TriggerEventProcessorStage.FAILED, testEvents.get(3).stage);
    assertEquals("test1", testEvents.get(3).actionName);
  }

  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void testCooldown() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    failDummyAction = false;
    waitForSeconds = 1;
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_cooldown_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand1 = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'bar'," +
        "'trigger' : 'node_added_cooldown_trigger'," +
        "'stage' : ['FAILED','SUCCEEDED', 'IGNORED']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand1);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    listenerCreated = new CountDownLatch(1);
    listenerEvents.clear();

    String newNode = cluster.simAddNode();
    boolean await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    // wait for listener to capture the SUCCEEDED stage
    cluster.getTimeSource().sleep(5000);

    List<CapturedEvent> capturedEvents = listenerEvents.get("bar");
    assertNotNull("no events for 'bar'!", capturedEvents);
    // we may get a few IGNORED events if other tests caused events within cooldown period
    assertTrue(capturedEvents.toString(), capturedEvents.size() > 0);
    long prevTimestamp = capturedEvents.get(capturedEvents.size() - 1).timestamp;

    // reset the trigger and captured events
    listenerEvents.clear();
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired.compareAndSet(true, false);

    String newNode2 = cluster.simAddNode();
    await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    cluster.getTimeSource().sleep(2000);

    // there must be exactly one SUCCEEDED event
    capturedEvents = listenerEvents.get("bar");
    assertTrue(capturedEvents.toString(), capturedEvents.size() >= 1);
    CapturedEvent ev = capturedEvents.get(capturedEvents.size() - 1);
    assertEquals(ev.toString(), TriggerEventProcessorStage.SUCCEEDED, ev.stage);
    // the difference between timestamps of the first SUCCEEDED and the last SUCCEEDED
    // must be larger than cooldown period
    assertTrue("timestamp delta is less than default cooldown period", ev.timestamp - prevTimestamp > TimeUnit.SECONDS.toNanos(ScheduledTriggers.DEFAULT_COOLDOWN_PERIOD_SECONDS));
  }

  public static class TestSearchRateAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      try {
        events.add(event);
        long currentTimeNanos = cluster.getTimeSource().getTimeNs();
        long eventTimeNanos = event.getEventTime();
        long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
        if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
          fail(event.getSource() + " was fired before the configured waitFor period");
        }
        getTriggerFiredLatch().countDown();
      } catch (Throwable t) {
        log.debug("--throwable", t);
        throw t;
      }
    }
  }

  public static class FinishTriggerAction extends TriggerActionBase {
    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      triggerFinishedCount.incrementAndGet();
      triggerFinishedLatch.countDown();
    }
  }

  public static class StartTriggerAction extends TriggerActionBase {
    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      triggerStartedLatch.countDown();
      triggerStartedCount.incrementAndGet();
    }
  }



  @Test
  //@BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void testSearchRate() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String COLL1 = "collection1";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 1, 2);
    create.process(solrClient);
    CloudTestUtils.waitForState(cluster, COLL1, 10, TimeUnit.SECONDS, CloudTestUtils.clusterShape(1, 2, false, true));

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'search_rate_trigger'," +
        "'event' : 'searchRate'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'aboveRate' : 1.0," +
        "'aboveNodeRate' : 1.0," +
        "'actions' : [" +
        "{'name':'start','class':'" + StartTriggerAction.class.getName() + "'}," +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + TestSearchRateAction.class.getName() + "'}" +
        "{'name':'finish','class':'" + FinishTriggerAction.class.getName() + "'}," +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand1 = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'srt'," +
        "'trigger' : 'search_rate_trigger'," +
        "'stage' : ['FAILED','SUCCEEDED']," +
        "'afterAction': ['compute', 'execute', 'test']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand1);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

//    SolrParams query = params(CommonParams.Q, "*:*");
//    for (int i = 0; i < 500; i++) {
//      solrClient.query(COLL1, query);
//    }

    cluster.getSimClusterStateProvider().simSetCollectionValue(COLL1, "QUERY./select.requestTimes:1minRate", 500, false, true);

    boolean await = triggerStartedLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not start in time", await);
    await = triggerFinishedLatch.await(60000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not finish in time", await);
    // wait for listener to capture the SUCCEEDED stage
    cluster.getTimeSource().sleep(5000);
    List<CapturedEvent> events = listenerEvents.get("srt");

    assertEquals(listenerEvents.toString(), 4, events.size());
    assertEquals("AFTER_ACTION", events.get(0).stage.toString());
    assertEquals("compute", events.get(0).actionName);
    assertEquals("AFTER_ACTION", events.get(1).stage.toString());
    assertEquals("execute", events.get(1).actionName);
    assertEquals("AFTER_ACTION", events.get(2).stage.toString());
    assertEquals("test", events.get(2).actionName);
    assertEquals("SUCCEEDED", events.get(3).stage.toString());
    assertNull(events.get(3).actionName);

    CapturedEvent ev = events.get(0);
    long now = cluster.getTimeSource().getTimeNs();
    // verify waitFor
    assertTrue(TimeUnit.SECONDS.convert(waitForSeconds, TimeUnit.NANOSECONDS) - WAIT_FOR_DELTA_NANOS <= now - ev.event.getEventTime());
    Map<String, Double> nodeRates = (Map<String, Double>)ev.event.getProperties().get(SearchRateTrigger.HOT_NODES);
    assertNotNull("nodeRates", nodeRates);
    assertTrue(nodeRates.toString(), nodeRates.size() > 0);
    AtomicDouble totalNodeRate = new AtomicDouble();
    nodeRates.forEach((n, r) -> totalNodeRate.addAndGet(r));
    List<ReplicaInfo> replicaRates = (List<ReplicaInfo>)ev.event.getProperties().get(SearchRateTrigger.HOT_REPLICAS);
    assertNotNull("replicaRates", replicaRates);
    assertTrue(replicaRates.toString(), replicaRates.size() > 0);
    AtomicDouble totalReplicaRate = new AtomicDouble();
    replicaRates.forEach(r -> {
      assertTrue(r.toString(), r.getVariable("rate") != null);
      totalReplicaRate.addAndGet((Double)r.getVariable("rate"));
    });
    Map<String, Object> shardRates = (Map<String, Object>)ev.event.getProperties().get(SearchRateTrigger.HOT_SHARDS);
    assertNotNull("shardRates", shardRates);
    assertEquals(shardRates.toString(), 1, shardRates.size());
    shardRates = (Map<String, Object>)shardRates.get(COLL1);
    assertNotNull("shardRates", shardRates);
    assertEquals(shardRates.toString(), 1, shardRates.size());
    AtomicDouble totalShardRate = new AtomicDouble();
    shardRates.forEach((s, r) -> totalShardRate.addAndGet((Double)r));
    Map<String, Double> collectionRates = (Map<String, Double>)ev.event.getProperties().get(SearchRateTrigger.HOT_COLLECTIONS);
    assertNotNull("collectionRates", collectionRates);
    assertEquals(collectionRates.toString(), 1, collectionRates.size());
    Double collectionRate = collectionRates.get(COLL1);
    assertNotNull(collectionRate);
    assertTrue(collectionRate > 100.0);
    assertTrue(totalNodeRate.get() > 100.0);
    assertTrue(totalShardRate.get() > 100.0);
    assertTrue(totalReplicaRate.get() > 100.0);

    // check operations
    List<Map<String, Object>> ops = (List<Map<String, Object>>)ev.context.get("properties.operations");
    assertNotNull(ops);
    assertTrue(ops.size() > 1);
    for (Map<String, Object> m : ops) {
      assertEquals("ADDREPLICA", m.get("params.action"));
    }
  }
}
