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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 * An end-to-end integration test for triggers
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
public class TriggerIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int NODE_COUNT = 2;

  private static volatile CountDownLatch actionConstructorCalled;
  private static volatile CountDownLatch actionInitCalled;
  private static volatile CountDownLatch triggerFiredLatch;
  private static volatile int waitForSeconds = 1;
  private static volatile CountDownLatch actionStarted;
  private static volatile CountDownLatch actionInterrupted;
  private static volatile CountDownLatch actionCompleted;
  private static AtomicBoolean triggerFired;
  private static Set<TriggerEvent> events = ConcurrentHashMap.newKeySet();
  private static SolrCloudManager cloudManager;

  static final long WAIT_FOR_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(5);

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
  }

  @AfterClass
  public static void cleanUpAfterClass() throws Exception {
    cloudManager = null;
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
    SolrCloudTestCase.ensureRunningJettys(NODE_COUNT, 5);

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
    Overseer overseer = cluster.getJettySolrRunner(overseerLeaderIndex).getCoreContainer().getZkController().getOverseer();
    ScheduledTriggers scheduledTriggers = ((OverseerTriggerThread)overseer.getTriggerThread().getThread()).getScheduledTriggers();
    // aggressively remove all active scheduled triggers
    scheduledTriggers.removeAll();

    // clear any persisted auto scaling configuration
    Stat stat = zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);
    if (log.isInfoEnabled()) {
      log.info("{} reset, new znode version {}", SOLR_AUTOSCALING_CONF_PATH, stat.getVersion());
    }

    cluster.deleteAllCollections();
    cluster.getSolrClient().setDefaultCollection(null);

    // restart Overseer. Even though we reset the autoscaling config some already running
    // trigger threads may still continue to execute and produce spurious events
    JettySolrRunner j = cluster.stopJettySolrRunner(overseerLeaderIndex);
    cluster.waitForJettyToStop(j);
    Thread.sleep(5000);

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
    while (cluster.getJettySolrRunners().size() < 2) {
      // perhaps a test stopped a node but didn't start it back
      // lets start a node
      cluster.startJettySolrRunner();
    }
    cluster.waitForAllNodes(30);
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    // clear any events or markers
    // todo: consider the impact of such cleanup on regular cluster restarts
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);
  }

  private void deleteChildrenRecursively(String path) throws Exception {
    cloudManager.getDistribStateManager().removeRecursively(path, true, false);
  }

  @Test
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
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
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // wait until the two instances of action are created
    assertTrue("Two TriggerAction instances were not created "+
               "even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    assertTrue("Both triggers did not fire event after await()ing an excessive amount of time",
               triggerFiredLatch.await(60, TimeUnit.SECONDS));

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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // wait until the two instances of action are created
    assertTrue("Two TriggerAction instances were not created "+
               "even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));

    // stop the node we had started earlier
    List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
    for (int i = 0; i < jettySolrRunners.size(); i++) {
      JettySolrRunner jettySolrRunner = jettySolrRunners.get(i);
      if (jettySolrRunner == newNode) {
        JettySolrRunner j = cluster.stopJettySolrRunner(i);
        cluster.waitForJettyToStop(j);
        break;
      }
    }

    assertTrue("Both triggers did not fire event after await()ing an excessive amount of time",
               triggerFiredLatch.await(60, TimeUnit.SECONDS));
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
        long currentTime = actionContext.getCloudManager().getTimeSource().getTimeNs();
        if (lastActionExecutedAt.get() != 0)  {
          long minDiff = TimeUnit.MILLISECONDS.toNanos(throttlingDelayMs.get() - DELTA_MS);
          if (log.isInfoEnabled()) {
            log.info("last action at {} current time = {}\nreal diff: {}\n min diff: {}"
                , lastActionExecutedAt.get(), currentTime
                , (currentTime - lastActionExecutedAt.get())
                , minDiff);
          }
          if (currentTime - lastActionExecutedAt.get() < minDiff) {
            if (log.isInfoEnabled()) {
              log.info("action executed again before minimum wait time from {}", event.getSource());
            }
            fail("TriggerListener was fired before the throttling period");
          }
        }
        if (onlyOnce.compareAndSet(false, true)) {
          if (log.isInfoEnabled()) {
            log.info("action executed from {}", event.getSource());
          }
          lastActionExecutedAt.set(currentTime);
          getTriggerFiredLatch().countDown();
        } else  {
          if (log.isInfoEnabled()) {
            log.info("action executed more than once from {}", event.getSource());
          }
          fail("Trigger should not have fired more than once!");
        }
      } finally {
        lock.unlock();
      }
    }
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
        "'name' : 'node_added_triggerCTOOR'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    assertTrue("Trigger was not init()ed even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));

    // stop the overseer, somebody else will take over as the overseer
    JettySolrRunner j = cluster.stopJettySolrRunner(index);
    cluster.waitForJettyToStop(j);
    Thread.sleep(10000);
    JettySolrRunner newNode = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    assertTrue("trigger did not fire even after await()ing an excessive amount of time",
               triggerFiredLatch.await(60, TimeUnit.SECONDS));
    assertTrue(triggerFired.get());
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) events.iterator().next();
    assertNotNull(nodeAddedEvent);
    @SuppressWarnings({"unchecked"})
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
          long currentTimeNanos = actionContext.getCloudManager().getTimeSource().getTimeNs();
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
    public void init() throws Exception {
      log.info("TestTriggerAction init");
      actionInitCalled.countDown();
      super.init();
    }
  }

  public static class TestEventQueueAction extends TriggerActionBase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static volatile CountDownLatch stall = new CountDownLatch(0);
    public TestEventQueueAction() {
      log.info("TestEventQueueAction instantiated");
    }

    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {
      // make a local copy of the latch so we're using it consistently even as test thread changes tings
      final CountDownLatch stallLatch = stall;
      log.info("processing: stall={} event={} ", stallLatch, event);
      events.add(event);
      getActionStarted().countDown();
      try {
        if (stallLatch.await(60, TimeUnit.SECONDS)) {
          log.info("Firing trigger event after await()ing 'stall' countdown");
          triggerFired.set(true);
        } else {
          log.error("Timed out await()ing 'stall' countdown");
        }
        getActionCompleted().countDown();
      } catch (InterruptedException e) {
        log.info("Interrupted");
        getActionInterrupted().countDown();
      }
    }

    @Override
    public void init() throws Exception {
      log.info("TestEventQueueAction init");
      actionInitCalled.countDown();
      super.init();
    }
  }

  @Test
  public void testEventQueue() throws Exception {
    waitForSeconds = 1;
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_triggerEQ'," +
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
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertTrue("Trigger was not init()ed even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));

    // setup the trigger action to stall so we can test interupting it w/overseer change
    // NOTE: we will never release this latch, instead we expect the interupt on overseer shutdown
    TestEventQueueAction.stall = new CountDownLatch(1);
    
    // add node to generate the event
    JettySolrRunner newNode = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    assertTrue("Action did not start even after await()ing an excessive amount of time",
               actionStarted.await(60, TimeUnit.SECONDS));
    
    // event should be there
    final TriggerEvent nodeAddedEvent = events.iterator().next();
    assertNotNull(nodeAddedEvent);
    assertNotNull(nodeAddedEvent.getId());
    assertNotNull(nodeAddedEvent.getEventType());
    assertNotNull(nodeAddedEvent.getProperty(TriggerEventQueue.ENQUEUE_TIME));

    // but action did not complete yet so the event is still enqueued
    assertFalse(triggerFired.get());

    // we know the event action has started, so we can re-set state for the next instance
    // that will run after the overseer change
    events.clear();
    actionStarted = new CountDownLatch(1);
    TestEventQueueAction.stall = new CountDownLatch(0); // so replay won't wait
    
    // kill overseer leader
    JettySolrRunner j = cluster.stopJettySolrRunner(overseerLeaderIndex);
    cluster.waitForJettyToStop(j);
    Thread.sleep(5000);
    // new overseer leader should be elected and run triggers
    assertTrue("Action was not interupted even after await()ing an excessive amount of time",
               actionInterrupted.await(60, TimeUnit.SECONDS));
    // it should fire again from enqueued event
    assertTrue("Action did not (re-)start even after await()ing an excessive amount of time",
               actionStarted.await(60, TimeUnit.SECONDS));
    
    final TriggerEvent replayedEvent = events.iterator().next();
    assertNotNull(replayedEvent);

    assertTrue("Action did not complete even after await()ing an excessive amount of time",
               actionCompleted.await(60, TimeUnit.SECONDS));
    assertTrue(triggerFired.get());
    
    assertEquals(nodeAddedEvent.getId(), replayedEvent.getId());
    assertEquals(nodeAddedEvent.getEventTime(), replayedEvent.getEventTime());
    assertEquals(nodeAddedEvent.getEventType(), replayedEvent.getEventType());
    assertEquals(nodeAddedEvent.getProperty(TriggerEventQueue.ENQUEUE_TIME),
                 replayedEvent.getProperty(TriggerEventQueue.ENQUEUE_TIME));
    assertEquals(Boolean.TRUE, replayedEvent.getProperty(TriggerEvent.REPLAYING));
  }

  static Map<String, List<CapturedEvent>> listenerEvents = new HashMap<>();
  static List<CapturedEvent> allListenerEvents = new ArrayList<>();
  static CountDownLatch listenerCreated = new CountDownLatch(1);
  static boolean failDummyAction = false;

  public static class TestTriggerListener extends TriggerListenerBase {
    private TimeSource timeSource;
    @Override
    public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) throws TriggerValidationException {
      super.configure(loader, cloudManager, config);
      listenerCreated.countDown();
      timeSource = cloudManager.getTimeSource();
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
      CapturedEvent ev = new CapturedEvent(timeSource.getTimeNs(), context, config, stage, actionName, event, message);
                                           
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
  public void testListeners() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_triggerL'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}," +
        "{'name':'test1','class':'" + TestDummyAction.class.getName() + "'}," +
        "]" +
        "}}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertTrue("Trigger was not init()ed even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'foo'," +
        "'trigger' : 'node_added_triggerL'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED', 'FAILED']," +
        "'beforeAction' : 'test'," +
        "'afterAction' : ['test', 'test1']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand1 = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'bar'," +
        "'trigger' : 'node_added_triggerL'," +
        "'stage' : ['FAILED','SUCCEEDED']," +
        "'beforeAction' : ['test', 'test1']," +
        "'afterAction' : 'test'," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand1);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    listenerEvents.clear();
    failDummyAction = false;

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    assertTrue("trigger did not fire even after await()ing an excessive amount of time",
               triggerFiredLatch.await(60, TimeUnit.SECONDS));
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
    allListenerEvents.clear();
    failDummyAction = true;

    newNode = cluster.startJettySolrRunner();
    assertTrue("trigger did not fire event after await()ing an excessive amount of time",
               triggerFiredLatch.await(60, TimeUnit.SECONDS));

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
}
