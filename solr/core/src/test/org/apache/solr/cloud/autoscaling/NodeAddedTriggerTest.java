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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link NodeAddedTrigger}
 */
public class NodeAddedTriggerTest extends SolrCloudTestCase {
  private static AtomicBoolean actionConstructorCalled = new AtomicBoolean(false);
  private static AtomicBoolean actionInitCalled = new AtomicBoolean(false);
  private static AtomicBoolean actionCloseCalled = new AtomicBoolean(false);

  private AutoScaling.TriggerEventProcessor noFirstRunProcessor = event -> {
    fail("Did not expect the processor to fire on first run! event=" + event);
    return true;
  };

  private static final long WAIT_FOR_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(2);

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");

  }

  @Before
  public void beforeTest() throws Exception {
    actionConstructorCalled = new AtomicBoolean(false);
    actionInitCalled = new AtomicBoolean(false);
    actionCloseCalled = new AtomicBoolean(false);
    configureCluster(1)
    .addConfig("conf", configset("cloud-minimal"))
    .configure();
  }
  
  @After
  public void afterTest() throws Exception {
    shutdownCluster();
  }

  @Test
  public void testTrigger() throws Exception {
    CoreContainer container = cluster.getJettySolrRunners().get(0).getCoreContainer();
    long waitForSeconds = 1 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);

    try (NodeAddedTrigger trigger = new NodeAddedTrigger("node_added_trigger1")) {
      final SolrCloudManager cloudManager = container.getZkController().getSolrCloudManager();
      trigger.configure(container.getResourceLoader(), cloudManager, props);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();

      JettySolrRunner newNode1 = cluster.startJettySolrRunner();
      JettySolrRunner newNode2 = cluster.startJettySolrRunner();
      
      cluster.waitForAllNodes(30);
      
      AtomicBoolean fired = new AtomicBoolean(false);
      AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();
      trigger.setProcessor(event -> {
        if (fired.compareAndSet(false, true)) {
          eventRef.set(event);
          long currentTimeNanos = cloudManager.getTimeSource().getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail("processor was fired before the configured waitFor period: currentTimeNanos=" + currentTimeNanos + ", eventTimeNanos=" +  eventTimeNanos + ",waitForNanos=" + waitForNanos);
          }
        } else {
          fail("NodeAddedTrigger was fired more than once!");
        }
        return true;
      });
      int counter = 0;
      do {
        trigger.run();
        Thread.sleep(1000);
        if (counter++ > 10) {
          fail("Newly added node was not discovered by trigger even after 10 seconds");
        }
      } while (!fired.get());

      TriggerEvent nodeAddedEvent = eventRef.get();
      assertNotNull(nodeAddedEvent);
      @SuppressWarnings({"unchecked"})
      List<String> nodeNames = (List<String>)nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
      assertTrue(nodeNames.contains(newNode1.getNodeName()));
      assertTrue(nodeNames.contains(newNode2.getNodeName()));
    }

    // clean nodeAdded markers - normally done by OverseerTriggerThread
    container.getZkController().getSolrCloudManager().getDistribStateManager()
        .removeRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH, true, false);

    // add a new node but remove it before the waitFor period expires
    // and assert that the trigger doesn't fire at all
    try (NodeAddedTrigger trigger = new NodeAddedTrigger("node_added_trigger2")) {
      final SolrCloudManager cloudManager = container.getZkController().getSolrCloudManager();
      trigger.configure(container.getResourceLoader(), cloudManager, props);
      trigger.init();
      final long waitTime = 2;
      props.put("waitFor", waitTime);
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();

      JettySolrRunner newNode = cluster.startJettySolrRunner();
      AtomicBoolean fired = new AtomicBoolean(false);
      trigger.setProcessor(event -> {
        if (fired.compareAndSet(false, true)) {
          long currentTimeNanos = cloudManager.getTimeSource().getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail("NodeAddedListener was fired before the configured waitFor period: currentTimeNanos=" + currentTimeNanos + ", eventTimeNanos=" +  eventTimeNanos + ",waitForNanos=" + waitForNanos);
          }
        } else {
          fail("NodeAddedTrigger was fired more than once!");
        }
        return true;
      });
      trigger.run(); // first run should detect the new node
      newNode.stop(); // stop the new jetty
      int counter = 0;
      do {
        trigger.run();
        Thread.sleep(1000);
        if (counter++ > waitTime + 1) { // run it a little more than the wait time
          break;
        }
      } while (true);

      // ensure the event was not fired
      assertFalse(fired.get());
    }
  }

  public void testActionLifecycle() throws Exception {
    CoreContainer container = cluster.getJettySolrRunners().get(0).getCoreContainer();
    Map<String, Object> props = createTriggerProps(0);
    @SuppressWarnings({"unchecked"})
    List<Map<String, String>> actions = (List<Map<String, String>>) props.get("actions");
    Map<String, String> action = new HashMap<>(2);
    action.put("name", "testActionInit");
    action.put("class", NodeAddedTriggerTest.AssertInitTriggerAction.class.getName());
    actions.add(action);
    try (NodeAddedTrigger trigger = new NodeAddedTrigger("node_added_trigger")) {
      trigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), props);
      assertEquals(true, actionConstructorCalled.get());
      assertEquals(false, actionInitCalled.get());
      assertEquals(false, actionCloseCalled.get());
      trigger.init();
      assertEquals(true, actionInitCalled.get());
      assertEquals(false, actionCloseCalled.get());
    }
    assertEquals(true, actionCloseCalled.get());
  }

  public static class AssertInitTriggerAction implements TriggerAction  {
    public AssertInitTriggerAction() {
      actionConstructorCalled.set(true);
    }

    @Override
    public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, Map<String, Object> properties) throws TriggerValidationException {

    }

    @Override
    public void init() {
      actionInitCalled.compareAndSet(false, true);
    }

    @Override
    public String getName() {
      return "";
    }

    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {

    }

    @Override
    public void close() throws IOException {
      actionCloseCalled.compareAndSet(false, true);
    }
  }

  @Test
  public void testListenerAcceptance() throws Exception {
    CoreContainer container = cluster.getJettySolrRunners().get(0).getCoreContainer();
    Map<String, Object> props = createTriggerProps(0);
    try (NodeAddedTrigger trigger = new NodeAddedTrigger("node_added_trigger")) {
      trigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), props);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run(); // starts tracking live nodes

      JettySolrRunner newNode = cluster.startJettySolrRunner();
      AtomicInteger callCount = new AtomicInteger(0);
      AtomicBoolean fired = new AtomicBoolean(false);

      trigger.setProcessor(event -> {
        if (callCount.incrementAndGet() < 2) {
          return false;
        } else  {
          fired.compareAndSet(false, true);
          return true;
        }
      });

      trigger.run(); // first run should detect the new node and fire immediately but listener isn't ready
      assertEquals(1, callCount.get());
      assertFalse(fired.get());
      trigger.run(); // second run should again fire
      assertEquals(2, callCount.get());
      assertTrue(fired.get());
      trigger.run(); // should not fire
      assertEquals(2, callCount.get());
    }
  }

  @Test
  public void testRestoreState() throws Exception {
    CoreContainer container = cluster.getJettySolrRunners().get(0).getCoreContainer();
    long waitForSeconds = 1 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);

    // add a new node but update the trigger before the waitFor period expires
    // and assert that the new trigger still fires
    NodeAddedTrigger trigger = new NodeAddedTrigger("node_added_trigger");
    trigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), props);
    trigger.setProcessor(noFirstRunProcessor);
    trigger.run();

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    trigger.setProcessor(null); // the processor may get called for old nodes
    trigger.run(); // this run should detect the new node
    trigger.close(); // close the old trigger

    try (NodeAddedTrigger newTrigger = new NodeAddedTrigger("some_different_name"))  {
      newTrigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), props);
      newTrigger.init();
      try {
        newTrigger.restoreState(trigger);
        fail("Trigger should only be able to restore state from an old trigger of the same name");
      } catch (AssertionError e) {
        // expected
      }
    }

    try (NodeAddedTrigger newTrigger = new NodeAddedTrigger("node_added_trigger"))  {
      final SolrCloudManager cloudManager = container.getZkController().getSolrCloudManager();
      newTrigger.configure(container.getResourceLoader(), cloudManager, props);
      newTrigger.init();
      AtomicBoolean stop = new AtomicBoolean(false);
      AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();
      newTrigger.setProcessor(event -> {
        //the processor may get called 2 times, for newly added node and initial nodes
        long currentTimeNanos = cloudManager.getTimeSource().getTimeNs();
        long eventTimeNanos = event.getEventTime();
        long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
        if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
          fail("NodeAddedListener was fired before the configured waitFor period: currentTimeNanos=" + currentTimeNanos + ", eventTimeNanos=" +  eventTimeNanos + ",waitForNanos=" + waitForNanos);
        }
        @SuppressWarnings({"unchecked"})
        List<String> nodeNames = (List<String>) event.getProperty(NodeAddedTrigger.NodeAddedEvent.NODE_NAMES);
        if (nodeNames.contains(newNode.getNodeName())) {
          stop.set(true);
          eventRef.set(event);
        }
        return true;
      });
      newTrigger.restoreState(trigger); // restore state from the old trigger
      int counter = 0;
      do {
        newTrigger.run();
        Thread.sleep(1000);
        if (counter++ > 10) {
          fail("Newly added node was not discovered by trigger even after 10 seconds");
        }
      } while (!stop.get());

      // ensure the event was fired
      assertTrue(stop.get());
      TriggerEvent nodeAddedEvent = eventRef.get();
      assertNotNull(nodeAddedEvent);
    }
  }

  private Map<String, Object> createTriggerProps(long waitForSeconds) {
    Map<String, Object> props = new HashMap<>();
    props.put("event", "nodeLost");
    props.put("waitFor", waitForSeconds);
    props.put("enabled", true);
    List<Map<String, String>> actions = new ArrayList<>(3);
    Map<String, String> map = new HashMap<>(2);
    map.put("name", "compute_plan");
    map.put("class", "solr.ComputePlanAction");
    actions.add(map);
    map = new HashMap<>(2);
    map.put("name", "execute_plan");
    map.put("class", "solr.ExecutePlanAction");
    actions.add(map);
    props.put("actions", actions);
    return props;
  }
}
