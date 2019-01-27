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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.AutoScaling;
import org.apache.solr.cloud.autoscaling.CapturedEvent;
import org.apache.solr.cloud.autoscaling.ComputePlanAction;
import org.apache.solr.cloud.autoscaling.ExecutePlanAction;
import org.apache.solr.cloud.autoscaling.NodeAddedTrigger;
import org.apache.solr.cloud.autoscaling.NodeLostTrigger;
import org.apache.solr.cloud.autoscaling.ScheduledTriggers;
import org.apache.solr.cloud.autoscaling.SearchRateTrigger;
import org.apache.solr.cloud.autoscaling.TriggerActionBase;
import org.apache.solr.cloud.autoscaling.TriggerBase;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerEventQueue;
import org.apache.solr.cloud.autoscaling.TriggerListenerBase;
import org.apache.solr.cloud.autoscaling.TriggerValidationException;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.LiveNodesListener;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * An end-to-end integration test for triggers
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class TestSimTriggerIntegration extends SimSolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int SPEED = 50;

  private static volatile CountDownLatch actionConstructorCalled;
  private static volatile CountDownLatch actionInitCalled;
  private static volatile CountDownLatch triggerFiredLatch;
  private static volatile int waitForSeconds = 1;
  private static volatile CountDownLatch actionStarted;
  private static volatile CountDownLatch actionInterrupted;
  private static volatile CountDownLatch actionCompleted;
  private static volatile CountDownLatch triggerStartedLatch;
  private static volatile CountDownLatch triggerFinishedLatch;
  private static volatile AtomicInteger triggerStartedCount;
  private static volatile AtomicInteger triggerFinishedCount;
  private static volatile AtomicBoolean triggerFired;
  private static Set<TriggerEvent> events = ConcurrentHashMap.newKeySet();

  private static final long WAIT_FOR_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(5);


  @After
  public void afterTest() throws Exception {
    shutdownCluster();
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
    configureCluster(2, TimeSource.get("simTime:" + SPEED));

    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cluster, ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cluster, ".scheduled_maintenance");
    
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
    allListenerEvents.clear();
    failDummyAction = false;
    listenerCreated = new CountDownLatch(1);
    listenerEventLatch = new CountDownLatch(0);
  }

  @Test
  public void testTriggerThrottling() throws Exception  {
    // for this test we want to create two triggers so we must assert that the actions were created twice
    actionInitCalled = new CountDownLatch(2);
    // similarly we want both triggers to fire
    triggerFiredLatch = new CountDownLatch(2);

    SolrClient solrClient = cluster.simGetSolrClient();

    // first trigger
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_trigger1'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '0s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + ThrottlingTesterAction.class.getName() + "'}]" +
       "}}");

    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_trigger2'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '0s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + ThrottlingTesterAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();

    // wait until the two instances of action are created
    if (!actionInitCalled.await(10000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    String newNode = cluster.simAddNode();

    if (!triggerFiredLatch.await(420000 / SPEED, TimeUnit.MILLISECONDS)) {
      fail("Both triggers should have fired by now");
    }

    // reset shared state
    lastActionExecutedAt.set(0);
    actionInitCalled = new CountDownLatch(2);
    triggerFiredLatch = new CountDownLatch(2);

    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_lost_trigger1'," +
       "'event' : 'nodeLost'," +
       "'waitFor' : '0s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + ThrottlingTesterAction.class.getName() + "'}]" +
       "}}");

    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_lost_trigger2'," +
       "'event' : 'nodeLost'," +
       "'waitFor' : '0s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + ThrottlingTesterAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();

    // wait until the two instances of action are created
    if (!actionInitCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    // stop the node we had started earlier
    cluster.simRemoveNode(newNode, false);

    // AwaitsFix - maybe related to leaders not always getting elected in sim
//    if (!triggerFiredLatch.await(34000 / SPEED, TimeUnit.MILLISECONDS)) {
//      fail("Both triggers should have fired by now");
//    }
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
  public void testNodeLostTriggerRestoreState() throws Exception {
    
    final String triggerName = "node_lost_restore_trigger";
      
    // should be enough to ensure trigger doesn't fire any actions until we replace the trigger
    waitForSeconds = 500000;
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : '"+triggerName+"'," +
       "'event' : 'nodeLost'," +
       "'waitFor' : '"+waitForSeconds+"s'," + 
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");

    assertTrue("Trigger was not init()ed even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));

    assertAutoscalingUpdateComplete();
    
    // start a new node that we can kill later
    final String nodeName = cluster.simAddNode();
    
    // poll the internal state of the trigger until it run()s at least once and updates
    // it's internal state to know the node we added is live
    //
    // (this should run roughly once a second of simulated time)
    (new TimeOut(30, TimeUnit.SECONDS, cluster.getTimeSource()))
    .waitFor("initial trigger never ran to detect new live node", () ->
             (((Collection<String>) getTriggerState(triggerName).get("lastLiveNodes"))
              .contains(nodeName)));
    
    // kill our node
    cluster.simRemoveNode(nodeName, false);
    
    // poll the internal state of the trigger until it run()s at least once (more) and updates
    // it's internal state to know the node we killed is no longer alive
    //
    // (this should run roughly once a second of simulated time)
    (new TimeOut(30, TimeUnit.SECONDS, cluster.getTimeSource()))
    .waitFor("initial trigger never ran to detect lost node", () ->
             ! (((Collection<String>) getTriggerState(triggerName).get("lastLiveNodes"))
                .contains(nodeName)));
    (new TimeOut(30, TimeUnit.SECONDS, cluster.getTimeSource()))
        .waitFor("initial trigger never ran to detect lost node", () ->
            (((Map<String, Long>) getTriggerState(triggerName).get("nodeNameVsTimeRemoved"))
                .containsKey(nodeName)));

    Map<String, Long> nodeNameVsTimeRemoved = (Map<String, Long>) getTriggerState(triggerName).get("nodeNameVsTimeRemoved");
    
    // since we know the nodeLost event has been detected, we can recored the current timestamp
    // (relative to the cluster's time source) and later assert that (restored state) correctly
    // tracked that the event happened prior to "now"
    final long maxEventTimeNs = cluster.getTimeSource().getTimeNs();
    
    // even though our trigger has detected a lost node, the *action* we registered should not have
    // been run yet, due to the large waitFor configuration...
    assertEquals("initial trigger action should not have fired", false, triggerFired.get());
    assertEquals("initial trigger action latch should not have counted down",
                 1, triggerFiredLatch.getCount());
    assertEquals("initial trigger action should not have recorded any events: " + events.toString(),
                 0, events.size());

    //
    // now replace the trigger with a new instance to test that the state gets copied over correctly
    //
    
    // reset the actionInitCalled counter so we can confirm the second instances is inited
    actionInitCalled = new CountDownLatch(1);
    // use a low waitTime to ensure it processes the event quickly.
    // (this updated property also ensures the set-trigger won't be treated as a No-Op)
    waitForSeconds = 0 + random().nextInt(3);
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : '"+triggerName+"'," +
       "'event' : 'nodeLost'," +
       "'waitFor' : '"+waitForSeconds+"s'," + 
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");
    
    assertTrue("Trigger was not init()ed even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));
               
    // the trigger actions should now (eventually) record that the node is lost
    assertTrue("Second instance of our trigger never fired the action to process the event",
               triggerFiredLatch.await(30, TimeUnit.SECONDS));
    
    final TriggerEvent event = assertSingleEvent(nodeName, maxEventTimeNs);
    assertTrue("Event should have been a nodeLost event: " + event.getClass(),
               event instanceof NodeLostTrigger.NodeLostEvent);

    // assert that the time nodes were removed was actually restored
    NodeLostTrigger.NodeLostEvent nodeLostEvent = (NodeLostTrigger.NodeLostEvent) event;
    List<String> removedNodeNames = (List<String>) nodeLostEvent.getProperty(TriggerEvent.NODE_NAMES);
    List<Long> removedNodeTimes = (List<Long>) nodeLostEvent.getProperty(TriggerEvent.EVENT_TIMES);

    assertFalse("Empty removedNodeNames", removedNodeNames.isEmpty());
    assertEquals("Size of removedNodeNames and removedNodeTimes does not match",
        removedNodeNames.size(), removedNodeTimes.size());
    for (int i = 0; i < removedNodeNames.size(); i++) {
      String nn = removedNodeNames.get(i);
      Long nt = removedNodeTimes.get(i);
      assertEquals(nodeNameVsTimeRemoved.get(nn), nt);
    }
  }

  @Test
  public void testNodeAddedTriggerRestoreState() throws Exception {
    
    final String triggerName = "node_added_restore_trigger";
      
    // should be enough to ensure trigger doesn't fire any actions until we replace the trigger
    waitForSeconds = 500000;
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : '"+triggerName+"'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '"+waitForSeconds+"s'," + 
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();

    assertTrue("Trigger was not init()ed even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));

    // start a new node
    final String nodeName = cluster.simAddNode();

    // poll the internal state of the trigger until it run()s at least once and updates
    // it's internal state to know the node we added is live
    //
    // (this should run roughly once a second of simulated time)
    (new TimeOut(30, TimeUnit.SECONDS, cluster.getTimeSource()))
    .waitFor("initial trigger never ran to detect new live node", () ->
             (((Collection<String>) getTriggerState(triggerName).get("lastLiveNodes"))
              .contains(nodeName)));
    (new TimeOut(30, TimeUnit.SECONDS, cluster.getTimeSource()))
        .waitFor("initial trigger never ran to detect new live node", () ->
            (((Map<String, Long>) getTriggerState(triggerName).get("nodeNameVsTimeAdded"))
                .containsKey(nodeName)));

    Map<String, Long> nodeNameVsTimeAdded = (Map<String, Long>) getTriggerState(triggerName).get("nodeNameVsTimeAdded");


    // since we know the nodeAddded event has been detected, we can recored the current timestamp
    // (relative to the cluster's time source) and later assert that (restored state) correctly
    // tracked that the event happened prior to "now"
    final long maxEventTimeNs = cluster.getTimeSource().getTimeNs();
    
    // even though our trigger has detected an added node, the *action* we registered should not have
    // been run yet, due to the large waitFor configuration...
    assertEquals("initial trigger action should not have fired", false, triggerFired.get());
    assertEquals("initial trigger action latch should not have counted down",
                 1, triggerFiredLatch.getCount());
    assertEquals("initial trigger action should not have recorded any events: " + events.toString(),
                 0, events.size());

    //
    // now replace the trigger with a new instance to test that the state gets copied over correctly
    //
    
    // reset the actionInitCalled counter so we can confirm the second instances is inited
    actionInitCalled = new CountDownLatch(1);
    // use a low waitTime to ensure it processes the event quickly.
    // (this updated property also ensures the set-trigger won't be treated as a No-Op)
    waitForSeconds = 0 + random().nextInt(3);
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : '"+triggerName+"'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '"+waitForSeconds+"s'," + 
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();

    assertTrue("Trigger was not init()ed even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));
    
    // the trigger actions should now (eventually) record that the new node is added
    assertTrue("Second instance of our trigger never fired the action to process the event",
               triggerFiredLatch.await(30, TimeUnit.SECONDS));
    
    final TriggerEvent event = assertSingleEvent(nodeName, maxEventTimeNs);
    assertTrue("Event should have been a nodeAdded event: " + event.getClass(),
               event instanceof NodeAddedTrigger.NodeAddedEvent);

    // assert that the time nodes were added was actually restored
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) event;
    List<String> addedNodeNames = (List<String>) nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
    List<Long> addedNodeTimes = (List<Long>) nodeAddedEvent.getProperty(TriggerEvent.EVENT_TIMES);

    assertFalse("Empty addedNodeNames", addedNodeNames.isEmpty());
    assertEquals("Size of addedNodeNames and addedNodeTimes does not match",
        addedNodeNames.size(), addedNodeTimes.size());
    for (int i = 0; i < addedNodeNames.size(); i++) {
      String nn = addedNodeNames.get(i);
      Long nt = addedNodeTimes.get(i);
      assertEquals(nodeNameVsTimeAdded.get(nn), nt);
    }
  }

  @Test
  public void testNodeAddedTrigger() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_trigger'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();

    if (!actionInitCalled.await(5000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    String newNode = cluster.simAddNode();
    boolean await = triggerFiredLatch.await(240000 / SPEED, TimeUnit.MILLISECONDS);
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
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_trigger'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();

    // this should be a no-op so the action should have been created but init should not be called
    if (!actionConstructorCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    assertFalse(actionInitCalled.await(2000 / SPEED, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testNodeLostTrigger() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_lost_trigger'," +
       "'event' : 'nodeLost'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();

    if (!actionInitCalled.await(5000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    String lostNodeName = cluster.getSimClusterStateProvider().simGetRandomNode();
    cluster.simRemoveNode(lostNodeName, false);
    boolean await = triggerFiredLatch.await(45000 / SPEED, TimeUnit.MILLISECONDS);
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
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_lost_trigger'," +
       "'event' : 'nodeLost'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();

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
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
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
  //@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13072") // this test fails easily
  public void testEventQueue() throws Exception {
    waitForSeconds = 1;
    SolrClient solrClient = cluster.simGetSolrClient();
    String overseerLeader = cluster.getSimClusterStateProvider().simGetRandomNode();

    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_trigger1'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestEventQueueAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();

    if (!actionInitCalled.await(3000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    // wait for the trigger to run at least once
    cluster.getTimeSource().sleep(2 * waitForSeconds * 1000);

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

    assertAutoscalingUpdateComplete();

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
  public void testEventFromRestoredState() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_trigger'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '10s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();

    if (!actionInitCalled.await(10000 / SPEED, TimeUnit.MILLISECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    events.clear();

    String newNode = cluster.simAddNode();
    boolean await = triggerFiredLatch.await(90000 / SPEED, TimeUnit.MILLISECONDS);
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
    await = triggerFiredLatch.await(60000 / SPEED, TimeUnit.MILLISECONDS);
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
    public boolean onChange(SortedSet<String> oldLiveNodes, SortedSet<String> newLiveNodes) {
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
      return false;
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
  public void testNodeMarkersRegistration() throws Exception {
    // for this test we want to create two triggers so we must assert that the actions were created twice
    actionInitCalled = new CountDownLatch(2);
    // similarly we want both triggers to fire
    triggerFiredLatch = new CountDownLatch(2);
    TestLiveNodesListener listener = registerLiveNodesListener();

    SolrClient solrClient = cluster.simGetSolrClient();

    // get overseer node
    String overseerLeader = cluster.getSimClusterStateProvider().simGetOverseerLeader();

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
    assertAutoscalingUpdateComplete();

    if (!listener.onChangeLatch.await(10000, TimeUnit.MILLISECONDS)) {
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
    
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeout.waitFor("Path " + pathLost + " exists", () -> {
      try {
        return !cluster.getDistribStateManager().hasData(pathLost);
      } catch (IOException | KeeperException | InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });

    assertFalse("Path " + pathLost + " exists", cluster.getDistribStateManager().hasData(pathLost));

    listener.reset();

    // set up triggers

    log.info("====== ADD TRIGGERS");
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_trigger'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '1s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestEventMarkerAction.class.getName() + "'}]" +
       "}}");

    assertAutoScalingRequest
      ("{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestEventMarkerAction.class.getName() + "'}]" +
       "}}");

    assertAutoscalingUpdateComplete();
    overseerLeader = cluster.getSimClusterStateProvider().simGetOverseerLeader();

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

    listener.reset();
    events.clear();
    // one nodeAdded (not cleared yet) and one nodeLost
    triggerFiredLatch = new CountDownLatch(2);
    // kill overseer again
    log.info("====== KILL OVERSEER 2");
    cluster.simRestartOverseer(overseerLeader);
    if (!listener.onChangeLatch.await(10000 / SPEED, TimeUnit.MILLISECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }

    assertAutoscalingUpdateComplete();

    if (!triggerFiredLatch.await(120000 / SPEED, TimeUnit.MILLISECONDS)) {
      fail("Trigger should have fired by now");
    }
    assertEquals(2, events.size());
    TriggerEvent nodeAdded = null;
    TriggerEvent nodeLost = null;
    for (TriggerEvent ev : events) {
      switch (ev.getEventType()) {
        case NODEADDED:
          nodeAdded = ev;
          break;
        case NODELOST:
          nodeLost = ev;
          break;
        default:
          fail("unexpected event type: " + ev);
      }
    }
    assertNotNull("expected nodeAdded event", nodeAdded);
    assertNotNull("expected nodeLost event", nodeLost);
    List<String> nodeNames = (List<String>)nodeLost.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(overseerLeader));
    nodeNames = (List<String>)nodeAdded.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(node1));
  }

  static final Map<String, List<CapturedEvent>> listenerEvents = new ConcurrentHashMap<>();
  static final List<CapturedEvent> allListenerEvents = Collections.synchronizedList(new ArrayList<>());
  static volatile CountDownLatch listenerCreated = new CountDownLatch(1);
  static volatile CountDownLatch listenerEventLatch = new CountDownLatch(0);
  static volatile boolean failDummyAction = false;

  public static class TestTriggerListener extends TriggerListenerBase {
    @Override
    public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) throws TriggerValidationException {
      super.configure(loader, cloudManager, config);
      listenerCreated.countDown();
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      CapturedEvent ev = new CapturedEvent(cluster.getTimeSource().getTimeNs(), context, config, stage, actionName, event, message);
      final CountDownLatch latch = listenerEventLatch;
      synchronized (latch) {
        if (0 == latch.getCount()) {
          log.warn("Ignoring captured event since latch is 'full': {}", ev);
        } else {
          List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
          lst.add(ev);
          allListenerEvents.add(ev);
          latch.countDown();
        }
      }
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
  public void testListeners() throws Exception {
    listenerEventLatch = new CountDownLatch(4 + 5);
    
    SolrClient solrClient = cluster.simGetSolrClient();
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_trigger'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : true," +
       "'actions' : [" +
       "{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}," +
       "{'name':'test1','class':'" + TestDummyAction.class.getName() + "'}," +
       "]" +
       "}}");

    assertAutoScalingRequest
      ("{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'foo'," +
       "'trigger' : 'node_added_trigger'," +
       "'stage' : ['STARTED','ABORTED','SUCCEEDED', 'FAILED']," +
       "'beforeAction' : 'test'," +
       "'afterAction' : ['test', 'test1']," +
       "'class' : '" + TestTriggerListener.class.getName() + "'" +
       "}" +
       "}");

    assertAutoScalingRequest
      ("{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'bar'," +
       "'trigger' : 'node_added_trigger'," +
       "'stage' : ['FAILED','SUCCEEDED']," +
       "'beforeAction' : ['test', 'test1']," +
       "'afterAction' : 'test'," +
       "'class' : '" + TestTriggerListener.class.getName() + "'" +
       "}" +
       "}");

    assertAutoscalingUpdateComplete();
    assertTrue("The TriggerAction should have been init'ed w/in a reasonable amount of time",
               actionInitCalled.await(10, TimeUnit.SECONDS));

    listenerEvents.clear();
    failDummyAction = false;

    String newNode = cluster.simAddNode();
    boolean await = triggerFiredLatch.await(45000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());

    assertTrue("the listeners shou;d have recorded all events w/in a reasonable amount of time",
               listenerEventLatch.await(10, TimeUnit.SECONDS));
    assertEquals("at least 2 event types should have been recorded", 2, listenerEvents.size());

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
    listenerEventLatch = new CountDownLatch(4 + 4); // fewer total due to failDummyAction

    newNode = cluster.simAddNode();
    await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);

    assertTrue("the listeners shoud have recorded all events w/in a reasonable amount of time",
               listenerEventLatch.await(10, TimeUnit.SECONDS));
    assertEquals("at least 2 event types should have been recorded", 2, listenerEvents.size());

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
  public void testCooldown() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    failDummyAction = false;
    listenerEventLatch = new CountDownLatch(1);
    waitForSeconds = 1;
    assertAutoScalingRequest
      ("{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_cooldown_trigger'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : true," +
       "'actions' : [" +
       "{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}" +
       "]" +
       "}}");

    assertAutoScalingRequest
      ("{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'bar'," +
       "'trigger' : 'node_added_cooldown_trigger'," +
       "'stage' : ['FAILED','SUCCEEDED', 'IGNORED']," +
       "'class' : '" + TestTriggerListener.class.getName() + "'" +
       "}" +
       "}");

    assertAutoscalingUpdateComplete();
    assertTrue("The TriggerAction should have been init'ed w/in a reasonable amount of time",
               actionInitCalled.await(10, TimeUnit.SECONDS));

    listenerCreated = new CountDownLatch(1);
    listenerEvents.clear();

    String newNode = cluster.simAddNode();
    boolean await = triggerFiredLatch.await(45000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    assertTrue("the listener should have recorded all events w/in a reasonable amount of time",
               listenerEventLatch.await(10, TimeUnit.SECONDS));

    List<CapturedEvent> capturedEvents = listenerEvents.get("bar");
    assertNotNull("no events for 'bar'!", capturedEvents);

    assertEquals(capturedEvents.toString(), 1, capturedEvents.size());
    long prevTimestamp = capturedEvents.get(0).timestamp;

    // reset the trigger and captured events
    listenerEventLatch = new CountDownLatch(1);
    listenerEvents.clear();
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired.compareAndSet(true, false);

    String newNode2 = cluster.simAddNode();
    await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue("the listener should have recorded all events w/in a reasonable amount of time",
               listenerEventLatch.await(10, TimeUnit.SECONDS));

    // there must be exactly one SUCCEEDED event
    capturedEvents = listenerEvents.get("bar");
    assertNotNull(capturedEvents);
    assertEquals(capturedEvents.toString(), 1, capturedEvents.size());
    CapturedEvent ev = capturedEvents.get(0);
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
  public void testSearchRate() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String COLL1 = "collection1";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 1, 2);
    create.process(solrClient);
    CloudTestUtils.waitForState(cluster, COLL1, 10, TimeUnit.SECONDS, CloudTestUtils.clusterShape(1, 2, false, true));

    listenerEventLatch = new CountDownLatch(4);
    
    assertAutoScalingRequest
      ("{" +
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
       "}}");

    assertAutoScalingRequest
      ("{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'srt'," +
       "'trigger' : 'search_rate_trigger'," +
       "'stage' : ['FAILED','SUCCEEDED']," +
       "'afterAction': ['compute', 'execute', 'test']," +
       "'class' : '" + TestTriggerListener.class.getName() + "'" +
       "}" +
       "}");

    assertAutoscalingUpdateComplete();


    cluster.getSimClusterStateProvider().simSetCollectionValue(COLL1, "QUERY./select.requestTimes:1minRate", 500, false, true);

    boolean await = triggerStartedLatch.await(30000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not start in time", await);
    await = triggerFinishedLatch.await(60000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not finish in time", await);

    assertTrue("the listener should have recorded all events w/in a reasonable amount of time",
               listenerEventLatch.await(10, TimeUnit.SECONDS));

    List<CapturedEvent> events = new ArrayList<>(listenerEvents.get("srt"));
    assertNotNull("Could not find events for srt", events);
    assertEquals(events.toString(), 4, events.size());
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
    List<MapWriter> ops = (List<MapWriter>)ev.context.get("properties.operations");
    assertNotNull(ops);
    assertTrue(ops.size() > 1);
    for (MapWriter m : ops) {
      assertEquals("ADDREPLICA", m._get("params.action", null));
    }
  }

  /** 
   * Helper method for getting a copy of the current (internal) trigger state of a scheduled trigger. 
   */
  private Map<String, Object> getTriggerState(final String name) {
    final AutoScaling.Trigger t = cluster.getOverseerTriggerThread().getScheduledTriggers().getTrigger(name);
    assertNotNull(name + " is not a currently scheduled trigger", t);
    assertTrue(name + " is not a TriggerBase w/state: " + t.getClass(),
               t instanceof TriggerBase);
    return ((TriggerBase)t).deepCopyState();
  }

  /**
   * Helper method for making some common assertions about {@link #events}:
   * <ul>
   *  <li>Exactly one event that is not null</li>
   *  <li>Event refers to exactly one expected {@link TriggerEvent#NODE_NAMES}</li>
   *  <li>Event has exactly one {@link TriggerEvent#EVENT_TIMES} (which matches {@link TriggerEvent#getEventTime}) which is less then the  <code>maxExpectedEventTimeNs</code></li>
   * </ul>
   * @return the event found so that other assertions can be made
   */
  private static TriggerEvent assertSingleEvent(final String expectedNodeName,
                                                final long maxExpectedEventTimeNs) {
    
    assertEquals("Wrong number of events recorded: " + events.toString(),
                 1, events.size());
    
    final TriggerEvent event = events.iterator().next();
    assertNotNull("null event???", event);
    assertNotNull("event is missing NODE_NAMES: " + event, event.getProperty(TriggerEvent.NODE_NAMES));
    assertEquals("event has incorrect NODE_NAMES: " + event,
                 Collections.singletonList(expectedNodeName),
                 event.getProperty(TriggerEvent.NODE_NAMES));
    
    assertTrue("event TS is too late, should be before (max) expected TS @ "
               + maxExpectedEventTimeNs + ": " + event,
               event.getEventTime() < maxExpectedEventTimeNs);
    
    assertNotNull("event is missing EVENT_TIMES: " + event, event.getProperty(TriggerEvent.EVENT_TIMES));
    assertEquals("event has unexpeted number of EVENT_TIMES: " + event,
                 1, ((Collection)event.getProperty(TriggerEvent.EVENT_TIMES)).size());
    assertEquals("event's TS doesn't match EVENT_TIMES: " + event,
                 event.getEventTime(),
                 ((Collection)event.getProperty(TriggerEvent.EVENT_TIMES)).iterator().next());
    return event;
  }

}
