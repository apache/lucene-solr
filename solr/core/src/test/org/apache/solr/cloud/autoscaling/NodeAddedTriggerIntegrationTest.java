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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.WAIT_FOR_DELTA_NANOS;
import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

// TODO: this class shares duplicated code with NodeLostTriggerIntegrationTest ... merge?

@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
public class NodeAddedTriggerIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static volatile CountDownLatch actionConstructorCalled;
  private static volatile CountDownLatch actionInitCalled;
  private static volatile CountDownLatch triggerFiredLatch;
  private static volatile int waitForSeconds = 1;
  private static volatile AtomicBoolean triggerFired;
  private static volatile Set<TriggerEvent> events = ConcurrentHashMap.newKeySet();
  private static volatile SolrCloudManager cloudManager;

  @After 
  public void after() throws Exception {
    shutdownCluster();
  }

  @AfterClass
  public static void cleanUpAfterClass() throws Exception {
    cloudManager = null;
  }

  private static CountDownLatch getTriggerFiredLatch() {
    return triggerFiredLatch;
  }

  @Before
  public void setupTest() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    
    final Overseer overseer = cluster.getOpenOverseer();
    assertNotNull(overseer);
    cloudManager = overseer.getSolrCloudManager();
    assertNotNull(cloudManager);
      
    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cloudManager, ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cloudManager, ".scheduled_maintenance");

    // aggressively remove all active scheduled triggers
    final ScheduledTriggers scheduledTriggers = ((OverseerTriggerThread) overseer.getTriggerThread().getThread()).getScheduledTriggers();
    // TODO: is this really safe? is it possible overseer is still in process of adding some to schedule?
    scheduledTriggers.removeAll();

    // clear any persisted auto scaling configuration
    Stat stat = zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);
    if (log.isInfoEnabled()) {
      log.info("{} reset, new znode version {}", SOLR_AUTOSCALING_CONF_PATH, stat.getVersion());
    }

    cluster.getSolrClient().setDefaultCollection(null);

    waitForSeconds = 1 + random().nextInt(3);
    actionConstructorCalled = new CountDownLatch(1);
    actionInitCalled = new CountDownLatch(1);
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired = new AtomicBoolean(false);
    events.clear();

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
  @SuppressWarnings({"unchecked"})
  public void testNodeAddedTriggerRestoreState() throws Exception {
    
    final String triggerName = "node_added_restore_trigger";

    // should be enough to ensure trigger doesn't fire any actions until we replace the trigger
    waitForSeconds = 500000;
    CloudTestUtils.assertAutoScalingRequest
      (cloudManager,
       "{" +
       "'set-trigger' : {" +
       "'name' : '"+triggerName+"'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '"+waitForSeconds+"s'," + 
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");
    
    assertTrue("Trigger was not init()ed even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));

    // start a new node
    final JettySolrRunner newNode = cluster.startJettySolrRunner();
    final String nodeName = newNode.getNodeName();

    // poll the internal state of the trigger until it run()s at least once and updates
    // it's internal state to know the node we added is live
    //
    // (this should run roughly once a second)
    (new TimeOut(30, TimeUnit.SECONDS, cloudManager.getTimeSource()))
    .waitFor("initial trigger never ran to detect new live node", () ->
             (((Collection<String>) getTriggerState(triggerName).get("lastLiveNodes"))
              .contains(nodeName)));
    
    // since we know the nodeAdded event has been detected, we can recored the current timestamp
    // (relative to the cluster's time source) and later assert that (restored state) correctly
    // tracked that the event happened prior to "now"
    final long maxEventTimeNs = cloudManager.getTimeSource().getTimeNs();
    
    //
    // now replace the trigger with a new instance to test that the state gets copied over correctly
    //
    
    // reset the actionInitCalled counter so we can confirm the second instances is inited
    actionInitCalled = new CountDownLatch(1);
    // use a low waitTime to ensure it processes the event quickly.
    // (this updated property also ensures the set-trigger won't be treated as a No-Op)
    waitForSeconds = 0 + random().nextInt(3);
    CloudTestUtils.assertAutoScalingRequest
      (cloudManager,
       "{" +
       "'set-trigger' : {" +
       "'name' : '"+triggerName+"'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '"+waitForSeconds+"s'," + 
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");
    
    assertTrue("Trigger was not init()ed even after await()ing an excessive amount of time",
               actionInitCalled.await(60, TimeUnit.SECONDS));

    // the trigger actions should now (eventually) record that the node was added
    assertTrue("Second instance of our trigger never fired the action to process the event",
               triggerFiredLatch.await(30, TimeUnit.SECONDS));
    
    assertEquals("Wrong number of events recorded: " + events.toString(),
                 1, events.size());
    
    final TriggerEvent event = events.iterator().next();
    assertNotNull("null event???", event);
    assertTrue("Event should have been a nodeAdded event: " + event.getClass(),
               event instanceof NodeAddedTrigger.NodeAddedEvent);

    assertNotNull("event is missing NODE_NAMES: " + event, event.getProperty(TriggerEvent.NODE_NAMES));
    assertEquals("event has incorrect NODE_NAMES: " + event,
                 Collections.singletonList(nodeName),
                 event.getProperty(TriggerEvent.NODE_NAMES));
    
    assertTrue("event TS is too late, should be before (max) expected TS @ "
               + maxEventTimeNs + ": " + event,
               event.getEventTime() < maxEventTimeNs);
    
    assertNotNull("event is missing EVENT_TIMES: " + event, event.getProperty(TriggerEvent.EVENT_TIMES));
    assertEquals("event has unexpeted number of EVENT_TIMES: " + event,
                 1, ((Collection)event.getProperty(TriggerEvent.EVENT_TIMES)).size());
    assertEquals("event's TS doesn't match EVENT_TIMES: " + event,
                 event.getEventTime(),
                 ((Collection)event.getProperty(TriggerEvent.EVENT_TIMES)).iterator().next());
  }

  @Test
  public void testNodeAddedTrigger() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    CloudTestUtils.assertAutoScalingRequest
      (cloudManager,
       "{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_trigger'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");

    if (!actionInitCalled.await(3, TimeUnit.SECONDS)) {
      fail("The TriggerAction should have been created by now");
    }

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(15);
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) events.iterator().next();
    assertNotNull(nodeAddedEvent);
    @SuppressWarnings({"unchecked"})
    List<String> nodeNames = (List<String>) nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(newNode.getNodeName()));

    // reset
    actionConstructorCalled = new CountDownLatch(1);
    actionInitCalled = new CountDownLatch(1);

    // update the trigger with exactly the same data
    CloudTestUtils.assertAutoScalingRequest
      (cloudManager,
       "{" +
       "'set-trigger' : {" +
       "'name' : 'node_added_trigger'," +
       "'event' : 'nodeAdded'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : true," +
       "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
       "}}");

    // this should be a no-op so the action should have been created but init should not be called
    if (!actionConstructorCalled.await(3, TimeUnit.SECONDS)) {
      fail("The TriggerAction should have been created by now");
    }

    assertFalse(actionInitCalled.await(2, TimeUnit.SECONDS));
  }

  public static class TestTriggerAction extends TriggerActionBase {

    public TestTriggerAction() {
      actionConstructorCalled.countDown();
    }

    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {
      try {
        if (triggerFired.compareAndSet(false, true)) {
          events.add(event);
          long currentTimeNanos = actionContext.getCloudManager().getTimeSource().getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail(event.source + " was fired before the configured waitFor period");
          }
          getTriggerFiredLatch().countDown();
        } else {
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
  
  /** 
   * Helper method for getting a copy of the current (internal) trigger state of a scheduled trigger. 
   */
  private Map<String, Object> getTriggerState(final String name) {
    final Overseer overseer = cluster.getOpenOverseer();
    final ScheduledTriggers scheduledTriggers = ((OverseerTriggerThread) overseer.getTriggerThread().getThread()).getScheduledTriggers();
    final AutoScaling.Trigger t = scheduledTriggers.getTrigger(name);
    assertNotNull(name + " is not a currently scheduled trigger", t);
    assertTrue(name + " is not a TriggerBase w/state: " + t.getClass(),
               t instanceof TriggerBase);
    return ((TriggerBase)t).deepCopyState();
  }
}
