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
import org.apache.solr.core.CoreContainer;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link NodeLostTrigger}
 */
public class NodeLostTriggerTest extends SolrCloudTestCase {
  private static AtomicBoolean actionConstructorCalled = new AtomicBoolean(false);
  private static AtomicBoolean actionInitCalled = new AtomicBoolean(false);
  private static AtomicBoolean actionCloseCalled = new AtomicBoolean(false);

  private AutoScaling.TriggerEventProcessor noFirstRunProcessor = event -> {
    fail("Did not expect the listener to fire on first run!");
    return true;
  };

  // use the same time source as the trigger
  private final TimeSource timeSource = TimeSource.CURRENT_TIME;
  // currentTimeMillis is not as precise so to avoid false positives while comparing time of fire, we add some delta
  private static final long WAIT_FOR_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(5);

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(5)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void beforeTest() throws Exception {
    actionConstructorCalled = new AtomicBoolean(false);
    actionInitCalled = new AtomicBoolean(false);
    actionCloseCalled = new AtomicBoolean(false);
  }

  @Test
  public void testTrigger() throws Exception {
    CoreContainer container = cluster.getJettySolrRunners().get(0).getCoreContainer();
    long waitForSeconds = 1 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);

    try (NodeLostTrigger trigger = new NodeLostTrigger("node_lost_trigger")) {
      trigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), props);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();
      String lostNodeName1 = cluster.getJettySolrRunner(1).getNodeName();
      cluster.stopJettySolrRunner(1);
      String lostNodeName2 = cluster.getJettySolrRunner(1).getNodeName();
      cluster.stopJettySolrRunner(1);
      Thread.sleep(1000);

      AtomicBoolean fired = new AtomicBoolean(false);
      AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();
      trigger.setProcessor(event -> {
        if (fired.compareAndSet(false, true)) {
          eventRef.set(event);
          long currentTimeNanos = timeSource.getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail("NodeLostListener was fired before the configured waitFor period: currentTimeNanos=" + currentTimeNanos + ", eventTimeNanos=" +  eventTimeNanos + ",waitForNanos=" + waitForNanos);
          }
        } else {
          fail("NodeLostListener was fired more than once!");
        }
        return true;
      });
      int counter = 0;
      do {
        trigger.run();
        Thread.sleep(1000);
        if (counter++ > 10) {
          fail("Lost node was not discovered by trigger even after 10 seconds");
        }
      } while (!fired.get());

      TriggerEvent nodeLostEvent = eventRef.get();
      assertNotNull(nodeLostEvent);
      List<String> nodeNames = (List<String>)nodeLostEvent.getProperty(TriggerEvent.NODE_NAMES);
      assertTrue(nodeNames + " doesn't contain " + lostNodeName1, nodeNames.contains(lostNodeName1));
      assertTrue(nodeNames + " doesn't contain " + lostNodeName2, nodeNames.contains(lostNodeName2));

    }

    // remove a node but add it back before the waitFor period expires
    // and assert that the trigger doesn't fire at all
    try (NodeLostTrigger trigger = new NodeLostTrigger("node_lost_trigger")) {
      trigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), props);
      final long waitTime = 2;
      props.put("waitFor", waitTime);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();

      JettySolrRunner lostNode = cluster.getJettySolrRunner(1);
      lostNode.stop();
      AtomicBoolean fired = new AtomicBoolean(false);
      trigger.setProcessor(event -> {
        if (fired.compareAndSet(false, true)) {
          long currentTimeNanos = timeSource.getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitTime, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail("NodeLostListener was fired before the configured waitFor period: currentTimeNanos=" + currentTimeNanos + ", eventTimeNanos=" +  eventTimeNanos + ",waitForNanos=" + waitForNanos);
          }
        } else {
          fail("NodeLostListener was fired more than once!");
        }
        return true;
      });
      trigger.run(); // first run should detect the lost node
      int counter = 0;
      do {
        if (container.getZkController().getZkStateReader().getClusterState().getLiveNodes().size() == 2) {
          break;
        }
        Thread.sleep(100);
        if (counter++ > 20) {
          fail("Live nodes not updated!");
        }
      } while (true);
      counter = 0;
      lostNode.start();
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
    List<Map<String, String>> actions = (List<Map<String, String>>) props.get("actions");
    Map<String, String> action = new HashMap<>(2);
    action.put("name", "testActionInit");
    action.put("class", AssertInitTriggerAction.class.getName());
    actions.add(action);
    try (NodeLostTrigger trigger = new NodeLostTrigger("node_added_trigger")) {
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
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 16-Apr-2018
  public void testListenerAcceptance() throws Exception {
    CoreContainer container = cluster.getJettySolrRunners().get(0).getCoreContainer();
    Map<String, Object> props = createTriggerProps(0);
    try (NodeLostTrigger trigger = new NodeLostTrigger("node_added_trigger")) {
      trigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), props);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);

      JettySolrRunner newNode = cluster.startJettySolrRunner();
      cluster.waitForAllNodes(5);

      trigger.run(); // starts tracking live nodes

      // stop the newly created node
      List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
      for (int i = 0; i < jettySolrRunners.size(); i++) {
        JettySolrRunner jettySolrRunner = jettySolrRunners.get(i);
        if (newNode == jettySolrRunner) {
          cluster.stopJettySolrRunner(i);
          break;
        }
      }
      cluster.waitForAllNodes(5);

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

      trigger.run(); // first run should detect the lost node and fire immediately but listener isn't ready
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

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    String lostNodeName = newNode.getNodeName();

    // remove a node but update the trigger before the waitFor period expires
    // and assert that the new trigger still fires

    NodeLostTrigger trigger = new NodeLostTrigger("node_lost_trigger");
    trigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), props);
    trigger.init();
    trigger.setProcessor(noFirstRunProcessor);
    trigger.run();

    // stop the newly created node
    List<JettySolrRunner> jettySolrRunners = cluster.getJettySolrRunners();
    for (int i = 0; i < jettySolrRunners.size(); i++) {
      JettySolrRunner jettySolrRunner = jettySolrRunners.get(i);
      if (newNode == jettySolrRunner) {
        cluster.stopJettySolrRunner(i);
        break;
      }
    }

    trigger.run(); // this run should detect the lost node
    trigger.close(); // close the old trigger

    try (NodeLostTrigger newTrigger = new NodeLostTrigger("some_different_name"))  {
      newTrigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), props);
      newTrigger.init();
      try {
        newTrigger.restoreState(trigger);
        fail("Trigger should only be able to restore state from an old trigger of the same name");
      } catch (AssertionError e) {
        // expected
      }
    }

    try (NodeLostTrigger newTrigger = new NodeLostTrigger("node_lost_trigger")) {
      newTrigger.configure(container.getResourceLoader(), container.getZkController().getSolrCloudManager(), props);
      newTrigger.init();
      AtomicBoolean fired = new AtomicBoolean(false);
      AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();
      newTrigger.setProcessor(event -> {
        if (fired.compareAndSet(false, true)) {
          eventRef.set(event);
          long currentTimeNanos = timeSource.getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail("NodeLostListener was fired before the configured waitFor period: currentTimeNanos=" + currentTimeNanos + ", eventTimeNanos=" + eventTimeNanos + ",waitForNanos=" + waitForNanos);
          }
        } else {
          fail("NodeLostListener was fired more than once!");
        }
        return true;
      });
      newTrigger.restoreState(trigger); // restore state from the old trigger
      int counter = 0;
      do {
        newTrigger.run();
        Thread.sleep(1000);
        if (counter++ > 10) {
          fail("Lost node was not discovered by trigger even after 10 seconds");
        }
      } while (!fired.get());

      TriggerEvent nodeLostEvent = eventRef.get();
      assertNotNull(nodeLostEvent);
      List<String> nodeNames = (List<String>)nodeLostEvent.getProperty(TriggerEvent.NODE_NAMES);
      assertTrue(nodeNames.contains(lostNodeName));
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
