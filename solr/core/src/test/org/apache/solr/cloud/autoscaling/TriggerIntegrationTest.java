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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.LiveNodesListener;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;
import static org.apache.solr.cloud.autoscaling.ScheduledTriggers.DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS;
import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 * An end-to-end integration test for triggers
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class TriggerIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CountDownLatch actionConstructorCalled;
  private static CountDownLatch actionInitCalled;
  private static CountDownLatch triggerFiredLatch;
  private static int waitForSeconds = 1;
  private static CountDownLatch actionStarted;
  private static CountDownLatch actionInterrupted;
  private static CountDownLatch actionCompleted;
  private static AtomicBoolean triggerFired;
  private static Set<TriggerEvent> events = ConcurrentHashMap.newKeySet();
  private static ZkStateReader zkStateReader;
  private static SolrCloudManager cloudManager;

  // use the same time source as triggers use
  private static final TimeSource timeSource = TimeSource.CURRENT_TIME;

  private static final long WAIT_FOR_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(5);

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    zkStateReader = cluster.getSolrClient().getZkStateReader();
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
    // clear any persisted auto scaling configuration
    Stat stat = zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);
    log.info(SOLR_AUTOSCALING_CONF_PATH + " reset, new znode version {}", stat.getVersion());

    throttlingDelayMs.set(TimeUnit.SECONDS.toMillis(ScheduledTriggers.DEFAULT_ACTION_THROTTLE_PERIOD_SECONDS));
    waitForSeconds = 1 + random().nextInt(3);
    actionConstructorCalled = new CountDownLatch(1);
    actionInitCalled = new CountDownLatch(1);
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired = new AtomicBoolean(false);
    actionStarted = new CountDownLatch(1);
    actionInterrupted = new CountDownLatch(1);
    actionCompleted = new CountDownLatch(1);
    events.clear();
    listenerEvents.clear();
    lastActionExecutedAt.set(0);
    // clear any events or markers
    // todo: consider the impact of such cleanup on regular cluster restarts
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);
    while (cluster.getJettySolrRunners().size() < 2) {
      // perhaps a test stopped a node but didn't start it back
      // lets start a node
      cluster.startJettySolrRunner();
    }
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
  }

  private void deleteChildrenRecursively(String path) throws Exception {
    List<String> paths = zkClient().getChildren(path, null, true);
    paths.forEach(n -> {
      try {
        ZKUtil.deleteRecursive(zkClient().getSolrZooKeeper(), path + "/" + n);
      } catch (KeeperException.NoNodeException e) {
        // ignore
      } catch (KeeperException | InterruptedException e) {
        log.warn("Error deleting old data", e);
      }
    });
  }

  @Test
  public void testTriggerThrottling() throws Exception  {
    // for this test we want to create two triggers so we must assert that the actions were created twice
    actionInitCalled = new CountDownLatch(2);
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
    if (!actionInitCalled.await(3, TimeUnit.SECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    JettySolrRunner newNode = cluster.startJettySolrRunner();

    if (!triggerFiredLatch.await(30, TimeUnit.SECONDS)) {
      fail("Both triggers should have fired by now");
    }

    // reset shared state
    lastActionExecutedAt.set(0);
    TriggerIntegrationTest.actionInitCalled = new CountDownLatch(2);
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
    if (!actionInitCalled.await(3, TimeUnit.SECONDS))  {
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

    if (!triggerFiredLatch.await(30, TimeUnit.SECONDS)) {
      fail("Both triggers should have fired by now");
    }
  }

  static AtomicLong lastActionExecutedAt = new AtomicLong(0);
  static AtomicLong throttlingDelayMs = new AtomicLong(TimeUnit.SECONDS.toMillis(ScheduledTriggers.DEFAULT_ACTION_THROTTLE_PERIOD_SECONDS));
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
        long currentTime = timeSource.getTime();
        if (lastActionExecutedAt.get() != 0)  {
          long minDiff = TimeUnit.MILLISECONDS.toNanos(throttlingDelayMs.get() - DELTA_MS);
          log.info("last action at " + lastActionExecutedAt.get() + " current time = " + currentTime +
              "\nreal diff: " + (currentTime - lastActionExecutedAt.get()) +
              "\n min diff: " + minDiff);
          if (currentTime - lastActionExecutedAt.get() < minDiff) {
            log.info("action executed again before minimum wait time from {}", event.getSource());
            fail("TriggerListener was fired before the throttling period");
          }
        }
        if (onlyOnce.compareAndSet(false, true)) {
          log.info("action executed from {}", event.getSource());
          lastActionExecutedAt.set(currentTime);
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
    // for this test we want to update the trigger so we must assert that the actions were created twice
    TriggerIntegrationTest.actionInitCalled = new CountDownLatch(2);

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
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    TimeOut timeOut = new TimeOut(2, TimeUnit.SECONDS, cloudManager.getTimeSource());
    while (actionInitCalled.getCount() == 0 && !timeOut.hasTimedOut()) {
      Thread.sleep(200);
    }
    assertTrue("The action specified in node_lost_restore_trigger was not instantiated even after 2 seconds", actionInitCalled.getCount() > 0);

    List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
    int index = -1;
    for (int i = 0; i < jettySolrRunners.size(); i++) {
      JettySolrRunner runner = jettySolrRunners.get(i);
      if (runner == newNode) index = i;
    }
    assertFalse(index == -1);
    cluster.stopJettySolrRunner(index);

    // ensure that the old trigger sees the stopped node, todo find a better way to do this
    Thread.sleep(500 + TimeUnit.SECONDS.toMillis(DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS));

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
    if (!actionInitCalled.await(3, TimeUnit.SECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    boolean await = triggerFiredLatch.await(5, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeLostTrigger.NodeLostEvent nodeLostEvent = (NodeLostTrigger.NodeLostEvent) events.iterator().next();
    assertNotNull(nodeLostEvent);
    List<String> nodeNames = (List<String>)nodeLostEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(nodeName));
  }

  @Test
  public void testNodeAddedTriggerRestoreState() throws Exception {
    // for this test we want to update the trigger so we must assert that the actions were created twice
    TriggerIntegrationTest.actionInitCalled = new CountDownLatch(2);

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
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    TimeOut timeOut = new TimeOut(2, TimeUnit.SECONDS, cloudManager.getTimeSource());
    while (actionInitCalled.getCount() == 0 && !timeOut.hasTimedOut()) {
      Thread.sleep(200);
    }
    assertTrue("The action specified in node_added_restore_trigger was not instantiated even after 2 seconds", actionInitCalled.getCount() > 0);

    // start a new node
    JettySolrRunner newNode = cluster.startJettySolrRunner();

    // ensure that the old trigger sees the new node, todo find a better way to do this
    Thread.sleep(500 + TimeUnit.SECONDS.toMillis(DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS));

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
    if (!actionInitCalled.await(3, TimeUnit.SECONDS))  {
      fail("Two TriggerAction instances should have been created by now");
    }

    boolean await = triggerFiredLatch.await(5, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) events.iterator().next();
    assertNotNull(nodeAddedEvent);
    List<String> nodeNames = (List<String>)nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(newNode.getNodeName()));
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
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionInitCalled.await(3, TimeUnit.SECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) events.iterator().next();
    assertNotNull(nodeAddedEvent);
    List<String> nodeNames = (List<String>)nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(newNode.getNodeName()));

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
    if (!actionConstructorCalled.await(3, TimeUnit.SECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    assertFalse(actionInitCalled.await(2, TimeUnit.SECONDS));
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
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionInitCalled.await(3, TimeUnit.SECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    String lostNodeName = cluster.getJettySolrRunner(nonOverseerLeaderIndex).getNodeName();
    cluster.stopJettySolrRunner(nonOverseerLeaderIndex);
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeLostTrigger.NodeLostEvent nodeLostEvent = (NodeLostTrigger.NodeLostEvent) events.iterator().next();
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
    if (!actionConstructorCalled.await(3, TimeUnit.SECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    assertFalse(actionInitCalled.await(2, TimeUnit.SECONDS));
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

  public static class TestTriggerAction extends TriggerActionBase {

    public TestTriggerAction() {
      actionConstructorCalled.countDown();
    }

    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {
      try {
        if (triggerFired.compareAndSet(false, true))  {
          events.add(event);
          long currentTimeNanos = timeSource.getTime();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail(event.source + " was fired before the configured waitFor period");
          }
          getTriggerFiredLatch().countDown();
        } else  {
          fail(event.source + " was fired more than once!");
        }
      } catch (Throwable t) {
        log.debug("--throwable", t);
        throw t;
      }
    }

    @Override
    public void init(Map<String, String> args) {
      log.info("TestTriggerAction init");
      actionInitCalled.countDown();
      super.init(args);
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
    public void init(Map<String, String> args) {
      log.debug("TestTriggerAction init");
      actionInitCalled.countDown();
      super.init(args);
    }
  }

  public static long eventQueueActionWait = 5000;

  @Test
  public void testEventQueue() throws Exception {
    waitForSeconds = 1;
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger1'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestEventQueueAction.class.getName() + "'}]" +
        "}}";
    NamedList<Object> overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    String overseerLeader = (String) overSeerStatus.get("leader");
    int overseerLeaderIndex = 0;
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jetty = cluster.getJettySolrRunner(i);
      if (jetty.getNodeName().equals(overseerLeader)) {
        overseerLeaderIndex = i;
        break;
      }
    }
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionInitCalled.await(3, TimeUnit.SECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    // add node to generate the event
    JettySolrRunner newNode = cluster.startJettySolrRunner();
    boolean await = actionStarted.await(60, TimeUnit.SECONDS);
    assertTrue("action did not start", await);
    // event should be there
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) events.iterator().next();
    assertNotNull(nodeAddedEvent);
    // but action did not complete yet so the event is still enqueued
    assertFalse(triggerFired.get());
    events.clear();
    actionStarted = new CountDownLatch(1);
    eventQueueActionWait = 1;
    // kill overseer leader
    cluster.stopJettySolrRunner(overseerLeaderIndex);
    Thread.sleep(5000);
    // new overseer leader should be elected and run triggers
    await = actionInterrupted.await(3, TimeUnit.SECONDS);
    assertTrue("action wasn't interrupted", await);
    // it should fire again from enqueued event
    await = actionStarted.await(60, TimeUnit.SECONDS);
    assertTrue("action wasn't started", await);
    TriggerEvent replayedEvent = events.iterator().next();
    assertTrue(replayedEvent.getProperty(TriggerEventQueue.ENQUEUE_TIME) != null);
    assertTrue(events + "\n" + replayedEvent.toString(), replayedEvent.getProperty(TriggerEventQueue.DEQUEUE_TIME) != null);
    await = actionCompleted.await(10, TimeUnit.SECONDS);
    assertTrue("action wasn't completed", await);
    assertTrue(triggerFired.get());
  }

  @Test
  public void testEventFromRestoredState() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
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

    if (!actionInitCalled.await(10, TimeUnit.SECONDS))  {
      fail("The TriggerAction should have been created by now");
    }

    NamedList<Object> overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    String overseerLeader = (String) overSeerStatus.get("leader");
    int overseerLeaderIndex = 0;
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jetty = cluster.getJettySolrRunner(i);
      if (jetty.getNodeName().equals(overseerLeader)) {
        overseerLeaderIndex = i;
        break;
      }
    }

    events.clear();

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    // reset
    triggerFired.set(false);
    triggerFiredLatch = new CountDownLatch(1);
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) events.iterator().next();
    assertNotNull(nodeAddedEvent);
    List<String> nodeNames = (List<String>)nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(newNode.getNodeName()));
    // add a second node - state of the trigger will change but it won't fire for waitFor sec.
    JettySolrRunner newNode2 = cluster.startJettySolrRunner();
    Thread.sleep(10000);
    // kill overseer leader
    cluster.stopJettySolrRunner(overseerLeaderIndex);
    await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
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
    zkStateReader.registerLiveNodesListener(listener);
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
    public void init(Map<String, String> args) {
      log.info("TestEventMarkerAction init");
      actionInitCalled.countDown();
      super.init(args);
    }
  }

  @Test
  public void testNodeMarkersRegistration() throws Exception {
    // for this test we want to create two triggers so we must assert that the actions were created twice
    actionInitCalled = new CountDownLatch(2);
    // similarly we want both triggers to fire
    triggerFiredLatch = new CountDownLatch(2);
    TestLiveNodesListener listener = registerLiveNodesListener();

    NamedList<Object> overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    String overseerLeader = (String) overSeerStatus.get("leader");
    int overseerLeaderIndex = 0;
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jetty = cluster.getJettySolrRunner(i);
      if (jetty.getNodeName().equals(overseerLeader)) {
        overseerLeaderIndex = i;
        break;
      }
    }
    // add a node
    JettySolrRunner node = cluster.startJettySolrRunner();
    if (!listener.onChangeLatch.await(10, TimeUnit.SECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }
    assertEquals(1, listener.addedNodes.size());
    assertEquals(node.getNodeName(), listener.addedNodes.iterator().next());
    // verify that a znode doesn't exist (no trigger)
    String pathAdded = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + node.getNodeName();
    assertFalse("Path " + pathAdded + " was created but there are no nodeAdded triggers", zkClient().exists(pathAdded, true));
    listener.reset();
    // stop overseer
    log.info("====== KILL OVERSEER 1");
    cluster.stopJettySolrRunner(overseerLeaderIndex);
    if (!listener.onChangeLatch.await(10, TimeUnit.SECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }
    assertEquals(1, listener.lostNodes.size());
    assertEquals(overseerLeader, listener.lostNodes.iterator().next());
    assertEquals(0, listener.addedNodes.size());
    // wait until the new overseer is up
    Thread.sleep(5000);
    // verify that a znode does NOT exist - there's no nodeLost trigger,
    // so the new overseer cleaned up existing nodeLost markers
    String pathLost = ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH + "/" + overseerLeader;
    assertFalse("Path " + pathLost + " exists", zkClient().exists(pathLost, true));

    listener.reset();

    // set up triggers
    CloudSolrClient solrClient = cluster.getSolrClient();

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

    overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    overseerLeader = (String) overSeerStatus.get("leader");
    overseerLeaderIndex = 0;
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jetty = cluster.getJettySolrRunner(i);
      if (jetty.getNodeName().equals(overseerLeader)) {
        overseerLeaderIndex = i;
        break;
      }
    }

    // create another node
    log.info("====== ADD NODE 1");
    JettySolrRunner node1 = cluster.startJettySolrRunner();
    if (!listener.onChangeLatch.await(10, TimeUnit.SECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }
    assertEquals(1, listener.addedNodes.size());
    assertEquals(node1.getNodeName(), listener.addedNodes.iterator().next());
    // verify that a znode exists
    pathAdded = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + node1.getNodeName();
    assertTrue("Path " + pathAdded + " wasn't created", zkClient().exists(pathAdded, true));

    Thread.sleep(5000);
    // nodeAdded marker should be consumed now by nodeAdded trigger
    assertFalse("Path " + pathAdded + " should have been deleted", zkClient().exists(pathAdded, true));

    listener.reset();
    events.clear();
    triggerFiredLatch = new CountDownLatch(1);
    // kill overseer again
    log.info("====== KILL OVERSEER 2");
    cluster.stopJettySolrRunner(overseerLeaderIndex);
    if (!listener.onChangeLatch.await(10, TimeUnit.SECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }


    if (!triggerFiredLatch.await(20, TimeUnit.SECONDS)) {
      fail("Trigger should have fired by now");
    }
    assertEquals(1, events.size());
    TriggerEvent ev = events.iterator().next();
    List<String> nodeNames = (List<String>)ev.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(overseerLeader));
    assertEquals(TriggerEventType.NODELOST, ev.getEventType());
  }

  static Map<String, List<CapturedEvent>> listenerEvents = new HashMap<>();
  static CountDownLatch listenerCreated = new CountDownLatch(1);
  static boolean failDummyAction = false;

  public static class TestTriggerListener extends TriggerListenerBase {
    @Override
    public void init(SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) {
      super.init(cloudManager, config);
      listenerCreated.countDown();
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
      lst.add(new CapturedEvent(timeSource.getTime(), context, config, stage, actionName, event, message));
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
    CloudSolrClient solrClient = cluster.getSolrClient();
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

    if (!actionInitCalled.await(3, TimeUnit.SECONDS))  {
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

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());

    assertEquals("both listeners should have fired", 2, listenerEvents.size());

    Thread.sleep(2000);

    // check foo events
    List<CapturedEvent> capturedEvents = listenerEvents.get("foo");
    assertNotNull("foo events: " + capturedEvents, capturedEvents);
    assertEquals("foo events: " + capturedEvents, 5, capturedEvents.size());

    assertEquals(TriggerEventProcessorStage.STARTED, capturedEvents.get(0).stage);

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, capturedEvents.get(1).stage);
    assertEquals("test", capturedEvents.get(1).actionName);

    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, capturedEvents.get(2).stage);
    assertEquals("test", capturedEvents.get(2).actionName);

    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, capturedEvents.get(3).stage);
    assertEquals("test1", capturedEvents.get(3).actionName);

    assertEquals(TriggerEventProcessorStage.SUCCEEDED, capturedEvents.get(4).stage);

    // check bar events
    capturedEvents = listenerEvents.get("bar");
    assertNotNull("bar events", capturedEvents);
    assertEquals("bar events", 4, capturedEvents.size());

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, capturedEvents.get(0).stage);
    assertEquals("test", capturedEvents.get(0).actionName);

    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, capturedEvents.get(1).stage);
    assertEquals("test", capturedEvents.get(1).actionName);

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, capturedEvents.get(2).stage);
    assertEquals("test1", capturedEvents.get(2).actionName);

    assertEquals(TriggerEventProcessorStage.SUCCEEDED, capturedEvents.get(3).stage);

    // reset
    triggerFired.set(false);
    triggerFiredLatch = new CountDownLatch(1);
    listenerEvents.clear();
    failDummyAction = true;

    newNode = cluster.startJettySolrRunner();
    await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);

    Thread.sleep(2000);

    // check foo events
    capturedEvents = listenerEvents.get("foo");
    assertNotNull("foo events: " + capturedEvents, capturedEvents);
    assertEquals("foo events: " + capturedEvents, 4, capturedEvents.size());

    assertEquals(TriggerEventProcessorStage.STARTED, capturedEvents.get(0).stage);

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, capturedEvents.get(1).stage);
    assertEquals("test", capturedEvents.get(1).actionName);

    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, capturedEvents.get(2).stage);
    assertEquals("test", capturedEvents.get(2).actionName);

    assertEquals(TriggerEventProcessorStage.FAILED, capturedEvents.get(3).stage);
    assertEquals("test1", capturedEvents.get(3).actionName);

    // check bar events
    capturedEvents = listenerEvents.get("bar");
    assertNotNull("bar events", capturedEvents);
    assertEquals("bar events", 4, capturedEvents.size());

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, capturedEvents.get(0).stage);
    assertEquals("test", capturedEvents.get(0).actionName);

    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, capturedEvents.get(1).stage);
    assertEquals("test", capturedEvents.get(1).actionName);

    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, capturedEvents.get(2).stage);
    assertEquals("test1", capturedEvents.get(2).actionName);

    assertEquals(TriggerEventProcessorStage.FAILED, capturedEvents.get(3).stage);
    assertEquals("test1", capturedEvents.get(3).actionName);
  }

  @Test
  public void testCooldown() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
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

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    // wait for listener to capture the SUCCEEDED stage
    Thread.sleep(1000);

    List<CapturedEvent> capturedEvents = listenerEvents.get("bar");
    // we may get a few IGNORED events if other tests caused events within cooldown period
    assertTrue(capturedEvents.toString(), capturedEvents.size() > 0);
    long prevTimestamp = capturedEvents.get(capturedEvents.size() - 1).timestamp;

    // reset the trigger and captured events
    listenerEvents.clear();
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired.compareAndSet(true, false);

    JettySolrRunner newNode2 = cluster.startJettySolrRunner();
    await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    Thread.sleep(2000);

    // there must be at least one IGNORED event due to cooldown, and one SUCCEEDED event
    capturedEvents = listenerEvents.get("bar");
    assertTrue(capturedEvents.toString(), capturedEvents.size() > 1);
    for (int i = 0; i < capturedEvents.size() - 1; i++) {
      CapturedEvent ev = capturedEvents.get(i);
      assertEquals(ev.toString(), TriggerEventProcessorStage.IGNORED, ev.stage);
      assertTrue(ev.toString(), ev.message.contains("cooldown"));
    }
    CapturedEvent ev = capturedEvents.get(capturedEvents.size() - 1);
    assertEquals(ev.toString(), TriggerEventProcessorStage.SUCCEEDED, ev.stage);
    // the difference between timestamps of the first SUCCEEDED and the last SUCCEEDED
    // must be larger than cooldown period
    assertTrue("timestamp delta is less than default cooldown period", ev.timestamp - prevTimestamp > TimeUnit.SECONDS.toNanos(ScheduledTriggers.DEFAULT_COOLDOWN_PERIOD_SECONDS));
    prevTimestamp = ev.timestamp;

    // this also resets the cooldown period
    long modifiedCooldownPeriodSeconds = 7;
    String setPropertiesCommand = "{\n" +
        "\t\"set-properties\" : {\n" +
        "\t\t\"" + AutoScalingParams.TRIGGER_COOLDOWN_PERIOD_SECONDS + "\" : " + modifiedCooldownPeriodSeconds + "\n" +
        "\t}\n" +
        "}";
    solrClient.request(createAutoScalingRequest(SolrRequest.METHOD.POST, setPropertiesCommand));
    req = createAutoScalingRequest(SolrRequest.METHOD.GET, null);
    response = solrClient.request(req);

    // reset the trigger and captured events
    listenerEvents.clear();
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired.compareAndSet(true, false);

    JettySolrRunner newNode3 = cluster.startJettySolrRunner();
    await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired.compareAndSet(true, false);
    // add another node
    JettySolrRunner newNode4 = cluster.startJettySolrRunner();
    await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    Thread.sleep(2000);

    // there must be at least one SUCCEEDED (due to newNode3) then for newNode4 one IGNORED
    // event due to cooldown, and one SUCCEEDED
    capturedEvents = listenerEvents.get("bar");
    assertTrue(capturedEvents.toString(), capturedEvents.size() > 2);
    // first event should be SUCCEEDED
    ev = capturedEvents.get(0);
    assertEquals(ev.toString(), TriggerEventProcessorStage.SUCCEEDED, ev.stage);

    for (int i = 1; i < capturedEvents.size() - 1; i++) {
      ev = capturedEvents.get(i);
      assertEquals(ev.toString(), TriggerEventProcessorStage.IGNORED, ev.stage);
      assertTrue(ev.toString(), ev.message.contains("cooldown"));
    }
    ev = capturedEvents.get(capturedEvents.size() - 1);
    assertEquals(ev.toString(), TriggerEventProcessorStage.SUCCEEDED, ev.stage);
    // the difference between timestamps of the first SUCCEEDED and the last SUCCEEDED
    // must be larger than the modified cooldown period
    assertTrue("timestamp delta is less than default cooldown period", ev.timestamp - prevTimestamp > TimeUnit.SECONDS.toNanos(modifiedCooldownPeriodSeconds));
  }

  public void testSetProperties() throws Exception  {
    JettySolrRunner runner = cluster.getJettySolrRunner(0);
    SolrResourceLoader resourceLoader = runner.getCoreContainer().getResourceLoader();
    SolrCloudManager solrCloudManager = runner.getCoreContainer().getZkController().getSolrCloudManager();
    AtomicLong diff = new AtomicLong(0);
    triggerFiredLatch = new CountDownLatch(2); // have the trigger run twice to capture time difference
    try (ScheduledTriggers scheduledTriggers = new ScheduledTriggers(resourceLoader, solrCloudManager)) {
      AutoScalingConfig config = new AutoScalingConfig(Collections.emptyMap());
      scheduledTriggers.setAutoScalingConfig(config);
      scheduledTriggers.add(new TriggerBase(TriggerEventType.NODELOST, "x", Collections.emptyMap(), resourceLoader, solrCloudManager) {
        @Override
        protected Map<String, Object> getState() {
          return Collections.singletonMap("x","y");
        }

        @Override
        protected void setState(Map<String, Object> state) {

        }

        @Override
        public void restoreState(AutoScaling.Trigger old) {

        }

        @Override
        public void run() {
          if (getTriggerFiredLatch().getCount() == 0)  return;
          long l = diff.get();
          diff.set(timeSource.getTime() - l);
          getTriggerFiredLatch().countDown();
        }
      });
      assertTrue(getTriggerFiredLatch().await(4, TimeUnit.SECONDS));
      assertTrue(diff.get() - TimeUnit.SECONDS.toNanos(ScheduledTriggers.DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS) >= 0);

      // change schedule delay
      config = config.withProperties(Collections.singletonMap(AutoScalingParams.TRIGGER_SCHEDULE_DELAY_SECONDS, 4));
      scheduledTriggers.setAutoScalingConfig(config);
      triggerFiredLatch = new CountDownLatch(2);
      assertTrue("Timed out waiting for latch to fire", getTriggerFiredLatch().await(10, TimeUnit.SECONDS));
      assertTrue(diff.get() - TimeUnit.SECONDS.toNanos(4) >= 0);

      // reset with default properties
      scheduledTriggers.remove("x"); // remove the old trigger
      config = config.withProperties(ScheduledTriggers.DEFAULT_PROPERTIES);
      scheduledTriggers.setAutoScalingConfig(config);

      // test core thread count
      List<AutoScaling.Trigger> triggerList = new ArrayList<>();
      final Set<String> threadNames = Collections.synchronizedSet(new HashSet<>());
      final Set<String> triggerNames = Collections.synchronizedSet(new HashSet<>());
      triggerFiredLatch = new CountDownLatch(8);
      for (int i = 0; i < 8; i++) {
        triggerList.add(new MockTrigger(TriggerEventType.NODELOST, "x" + i, Collections.emptyMap(), resourceLoader, solrCloudManager)  {
          @Override
          public void run() {
            try {
              // If core pool size is increased then new threads won't be started if existing threads
              // aren't busy with tasks. So we make this thread wait longer than necessary
              // so that the pool is forced to start threads for other triggers
              Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
            if (triggerNames.add(getName()))  {
              getTriggerFiredLatch().countDown();
              threadNames.add(Thread.currentThread().getName());
            }
          }
        });
        scheduledTriggers.add(triggerList.get(i));
      }
      assertTrue("Timed out waiting for latch to fire", getTriggerFiredLatch().await(20, TimeUnit.SECONDS));
      assertEquals("Expected 8 triggers but found: " + triggerNames,8, triggerNames.size());
      assertEquals("Expected " + ScheduledTriggers.DEFAULT_TRIGGER_CORE_POOL_SIZE
          + " threads but found: " + threadNames,
          ScheduledTriggers.DEFAULT_TRIGGER_CORE_POOL_SIZE, threadNames.size());

      // change core pool size
      config = config.withProperties(Collections.singletonMap(AutoScalingParams.TRIGGER_CORE_POOL_SIZE, 6));
      scheduledTriggers.setAutoScalingConfig(config);
      triggerFiredLatch = new CountDownLatch(8);
      threadNames.clear();
      triggerNames.clear();
      assertTrue(getTriggerFiredLatch().await(20, TimeUnit.SECONDS));
      assertEquals("Expected 8 triggers but found: " + triggerNames,8, triggerNames.size());
      assertEquals("Expected 6 threads but found: " + threadNames,6, threadNames.size());

      // reset
      for (int i = 0; i < 8; i++) {
        scheduledTriggers.remove(triggerList.get(i).getName());
      }

      config = config.withProperties(Collections.singletonMap(AutoScalingParams.ACTION_THROTTLE_PERIOD_SECONDS, 6));
      scheduledTriggers.setAutoScalingConfig(config);
      lastActionExecutedAt.set(0);
      throttlingDelayMs.set(TimeUnit.SECONDS.toMillis(6));
      triggerFiredLatch = new CountDownLatch(2);
      Map<String, Object> props = map("waitFor", 0L, "actions", Collections.singletonList(map("name","throttler", "class", ThrottlingTesterAction.class.getName())));
      scheduledTriggers.add(new NodeAddedTrigger("y1", props, resourceLoader, solrCloudManager));
      scheduledTriggers.add(new NodeAddedTrigger("y2", props, resourceLoader, solrCloudManager));
      scheduledTriggers.resetActionThrottle();
      JettySolrRunner newNode = cluster.startJettySolrRunner();
      assertTrue(getTriggerFiredLatch().await(20, TimeUnit.SECONDS));
      for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
        if (cluster.getJettySolrRunner(i) == newNode) {
          cluster.stopJettySolrRunner(i);
          break;
        }
      }
    }
  }

  public static class MockTrigger extends TriggerBase {

    public MockTrigger(TriggerEventType eventType, String name, Map<String, Object> properties, SolrResourceLoader loader, SolrCloudManager cloudManager) {
      super(eventType, name, properties, loader, cloudManager);
    }

    @Override
    protected Map<String, Object> getState() {
      return Collections.emptyMap();
    }

    @Override
    protected void setState(Map<String, Object> state) {

    }

    @Override
    public void restoreState(AutoScaling.Trigger old) {

    }

    @Override
    public void run() {

    }
  }

  public static class TestSearchRateAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      try {
        events.add(event);
        long currentTimeNanos = timeSource.getTime();
        long eventTimeNanos = event.getEventTime();
        long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
        if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
          fail(event.source + " was fired before the configured waitFor period");
        }
        getTriggerFiredLatch().countDown();
      } catch (Throwable t) {
        log.debug("--throwable", t);
        throw t;
      }
    }
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-11714")
  public void testSearchRate() throws Exception {
    // start a few more jetty-s
    for (int i = 0; i < 3; i++) {
      cluster.startJettySolrRunner();
    }
    CloudSolrClient solrClient = cluster.getSolrClient();
    String COLL1 = "collection1";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 1, 2);
    create.process(solrClient);
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'search_rate_trigger'," +
        "'event' : 'searchRate'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'rate' : 1.0," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + TestSearchRateAction.class.getName() + "'}" +
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
    SolrParams query = params(CommonParams.Q, "*:*");
    for (int i = 0; i < 500; i++) {
      solrClient.query(COLL1, query);
    }
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    Thread.sleep(2000);
    assertEquals(listenerEvents.toString(), 1, listenerEvents.get("srt").size());
    CapturedEvent ev = listenerEvents.get("srt").get(0);
    long now = timeSource.getTime();
    // verify waitFor
    assertTrue(TimeUnit.SECONDS.convert(waitForSeconds, TimeUnit.NANOSECONDS) - WAIT_FOR_DELTA_NANOS <= now - ev.event.getEventTime());
    Map<String, Double> nodeRates = (Map<String, Double>)ev.event.getProperties().get("node");
    assertNotNull("nodeRates", nodeRates);
    assertTrue(nodeRates.toString(), nodeRates.size() > 0);
    AtomicDouble totalNodeRate = new AtomicDouble();
    nodeRates.forEach((n, r) -> totalNodeRate.addAndGet(r));
    List<ReplicaInfo> replicaRates = (List<ReplicaInfo>)ev.event.getProperties().get("replica");
    assertNotNull("replicaRates", replicaRates);
    assertTrue(replicaRates.toString(), replicaRates.size() > 0);
    AtomicDouble totalReplicaRate = new AtomicDouble();
    replicaRates.forEach(r -> {
      assertTrue(r.toString(), r.getVariable("rate") != null);
      totalReplicaRate.addAndGet((Double)r.getVariable("rate"));
    });
    Map<String, Object> shardRates = (Map<String, Object>)ev.event.getProperties().get("shard");
    assertNotNull("shardRates", shardRates);
    assertEquals(shardRates.toString(), 1, shardRates.size());
    shardRates = (Map<String, Object>)shardRates.get(COLL1);
    assertNotNull("shardRates", shardRates);
    assertEquals(shardRates.toString(), 1, shardRates.size());
    AtomicDouble totalShardRate = new AtomicDouble();
    shardRates.forEach((s, r) -> totalShardRate.addAndGet((Double)r));
    Map<String, Double> collectionRates = (Map<String, Double>)ev.event.getProperties().get("collection");
    assertNotNull("collectionRates", collectionRates);
    assertEquals(collectionRates.toString(), 1, collectionRates.size());
    Double collectionRate = collectionRates.get(COLL1);
    assertNotNull(collectionRate);
    assertTrue(collectionRate > 5.0);
    assertEquals(collectionRate, totalNodeRate.get(), 5.0);
    assertEquals(collectionRate, totalShardRate.get(), 5.0);
    assertEquals(collectionRate, totalReplicaRate.get(), 5.0);
  }

  @Test
  public void testMetricTrigger() throws Exception {
    // at least 3 nodes
    for (int i = cluster.getJettySolrRunners().size(); i < 3; i++) {
      cluster.startJettySolrRunner();
    }
    cluster.waitForAllNodes(5);

    String collectionName = "testMetricTrigger";
    CloudSolrClient solrClient = cluster.getSolrClient();
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 1);
    create.process(solrClient);
    solrClient.setDefaultCollection(collectionName);

    waitForState("Timed out waiting for collection:" + collectionName + " to become active", collectionName, clusterShape(2, 1));

    DocCollection docCollection = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    String shardId = "shard1";
    Replica replica = docCollection.getSlice(shardId).getReplicas().iterator().next();
    String coreName = replica.getCoreName();
    String replicaName = Utils.parseMetricsReplicaName(collectionName, coreName);
    long waitForSeconds = 2 + random().nextInt(5);
    String registry = SolrCoreMetricManager.createRegistryName(true, collectionName, shardId, replicaName, null);
    String tag = "metrics:" + registry + ":INDEX.sizeInBytes";

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'metric_trigger'," +
        "'event' : 'metric'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'metric': '" + tag + "'" +
        "'above' : 100.0," +
        "'collection': '" + collectionName + "'" +
        "'shard':'"  + shardId + "'" +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + TestSearchRateAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand1 = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'srt'," +
        "'trigger' : 'metric_trigger'," +
        "'stage' : ['FAILED','SUCCEEDED']," +
        "'afterAction': ['compute', 'execute', 'test']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand1);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    for (int i = 0; i < 500; i++) {
      solrClient.add(new SolrInputDocument("id", String.valueOf(i), "x_s", "x" + i));
    }
    solrClient.commit();

    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    Thread.sleep(2000);
    assertEquals(listenerEvents.toString(), 4, listenerEvents.get("srt").size());
    CapturedEvent ev = listenerEvents.get("srt").get(0);
    long now = timeSource.getTime();
    // verify waitFor
    assertTrue(TimeUnit.SECONDS.convert(waitForSeconds, TimeUnit.NANOSECONDS) - WAIT_FOR_DELTA_NANOS <= now - ev.event.getEventTime());
    assertEquals(collectionName, ev.event.getProperties().get("collection"));

    String oldReplicaName = replica.getName();
    docCollection = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertEquals(2, docCollection.getReplicas().size());
    assertNull(docCollection.getReplica(oldReplicaName));

    // todo uncomment the following code once SOLR-11714 is fixed
    // find a new replica and create its metric name
//    replica = docCollection.getSlice(shardId).getReplicas().iterator().next();
//    coreName = replica.getCoreName();
//    replicaName = Utils.parseMetricsReplicaName(collectionName, coreName);
//    registry = SolrCoreMetricManager.createRegistryName(true, collectionName, shardId, replicaName, null);
//    tag = "metrics:" + registry + ":INDEX.sizeInBytes";
//
//    setTriggerCommand = "{" +
//        "'set-trigger' : {" +
//        "'name' : 'metric_trigger'," +
//        "'event' : 'metric'," +
//        "'waitFor' : '" + waitForSeconds + "s'," +
//        "'enabled' : true," +
//        "'metric': '" + tag + "'" +
//        "'above' : 100.0," +
//        "'collection': '" + collectionName + "'" +
//        "'shard':'"  + shardId + "'" +
//        "'preferredOperation':'addreplica'" +
//        "'actions' : [" +
//        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
//        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
//        "{'name':'test','class':'" + TestSearchRateAction.class.getName() + "'}" +
//        "]" +
//        "}}";
//    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
//    response = solrClient.request(req);
//    assertEquals(response.get("result").toString(), "success");
//
//    triggerFiredLatch = new CountDownLatch(1);
//    listenerEvents.clear();
//    await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
//    assertTrue("The trigger did not fire at all", await);
//    // wait for listener to capture the SUCCEEDED stage
//    Thread.sleep(2000);
//    assertEquals(listenerEvents.toString(), 4, listenerEvents.get("srt").size());
//    ev = listenerEvents.get("srt").get(0);
//    now = timeSource.getTime();
//    // verify waitFor
//    assertTrue(TimeUnit.SECONDS.convert(waitForSeconds, TimeUnit.NANOSECONDS) - WAIT_FOR_DELTA_NANOS <= now - ev.event.getEventTime());
//    assertEquals(collectionName, ev.event.getProperties().get("collection"));
//    docCollection = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
//    assertEquals(3, docCollection.getReplicas().size());
  }
}
