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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.AutoScaling;
import org.apache.solr.cloud.autoscaling.NodeLostTrigger;
import org.apache.solr.cloud.autoscaling.TriggerAction;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerValidationException;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for {@link NodeLostTrigger}
 */
public class TestSimNodeLostTrigger extends SimSolrCloudTestCase {
  private static AtomicBoolean actionConstructorCalled = new AtomicBoolean(false);
  private static AtomicBoolean actionInitCalled = new AtomicBoolean(false);
  private static AtomicBoolean actionCloseCalled = new AtomicBoolean(false);

  private AutoScaling.TriggerEventProcessor noFirstRunProcessor = event -> {
    fail("Did not expect the listener to fire on first run!");
    return true;
  };

  private static final int SPEED = 50;
  // use the same time source as the trigger
  private static TimeSource timeSource;
  // currentTimeMillis is not as precise so to avoid false positives while comparing time of fire, we add some delta
  private static final long WAIT_FOR_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(5);

  @Before
  public void beforeTest() throws Exception {
    configureCluster(5, TimeSource.get("simTime:" + SPEED));
    timeSource = cluster.getTimeSource();
    actionConstructorCalled = new AtomicBoolean(false);
    actionInitCalled = new AtomicBoolean(false);
    actionCloseCalled = new AtomicBoolean(false);
  }
  
  @After
  public void afterTest() throws Exception {
    shutdownCluster();
  }

  @Test
  public void testTrigger() throws Exception {
    long waitForSeconds = 1 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);

    try (NodeLostTrigger trigger = new NodeLostTrigger("node_lost_trigger")) {
      trigger.configure(cluster.getLoader(), cluster, props);
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();
      Iterator<String> it = cluster.getLiveNodesSet().get().iterator();
      String lostNodeName1 = it.next();
      String lostNodeName2 = it.next();
      cluster.simRemoveNode(lostNodeName1, false);
      cluster.simRemoveNode(lostNodeName2, false);
      timeSource.sleep(1000);

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
        timeSource.sleep(1000);
        if (counter++ > 20) {
          fail("Lost node was not discovered by trigger even after 10 seconds");
        }
      } while (!fired.get());

      TriggerEvent nodeLostEvent = eventRef.get();
      assertNotNull(nodeLostEvent);
      @SuppressWarnings({"unchecked"})
      List<String> nodeNames = (List<String>)nodeLostEvent.getProperty(TriggerEvent.NODE_NAMES);
      assertTrue(nodeNames + " doesn't contain " + lostNodeName1, nodeNames.contains(lostNodeName1));
      assertTrue(nodeNames + " doesn't contain " + lostNodeName2, nodeNames.contains(lostNodeName2));

    }

    // remove a node but add it back before the waitFor period expires
    // and assert that the trigger doesn't fire at all
    try (NodeLostTrigger trigger = new NodeLostTrigger("node_lost_trigger")) {
      trigger.configure(cluster.getLoader(), cluster, props);
      final long waitTime = 2;
      props.put("waitFor", waitTime);
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();

      String lostNode = cluster.getSimClusterStateProvider().simGetRandomNode();
      cluster.simRemoveNode(lostNode, false);
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
        if (cluster.getLiveNodesSet().get().size() == 2) {
          break;
        }
        timeSource.sleep(100);
        if (counter++ > 20) {
          fail("Live nodes not updated!");
        }
      } while (true);
      counter = 0;
      cluster.getSimClusterStateProvider().simRestoreNode(lostNode);
      do {
        trigger.run();
        timeSource.sleep(1000);
        if (counter++ > waitTime + 1) { // run it a little more than the wait time
          break;
        }
      } while (true);

      // ensure the event was not fired
      assertFalse(fired.get());
    }
  }

  public void testActionLifecycle() throws Exception {
    Map<String, Object> props = createTriggerProps(0);
    @SuppressWarnings({"unchecked"})
    List<Map<String, String>> actions = (List<Map<String, String>>) props.get("actions");
    Map<String, String> action = new HashMap<>(2);
    action.put("name", "testActionInit");
    action.put("class", AssertInitTriggerAction.class.getName());
    actions.add(action);
    try (NodeLostTrigger trigger = new NodeLostTrigger("node_added_trigger")) {
      trigger.configure(cluster.getLoader(), cluster, props);
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
    Map<String, Object> props = createTriggerProps(0);
    try (NodeLostTrigger trigger = new NodeLostTrigger("node_added_trigger")) {
      trigger.configure(cluster.getLoader(), cluster, props);
      trigger.setProcessor(noFirstRunProcessor);

      String newNode = cluster.simAddNode();

      trigger.run(); // starts tracking live nodes

      // stop the newly created node
      cluster.simRemoveNode(newNode, false);

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
    long waitForSeconds = 1 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);

    String newNode = cluster.simAddNode();

    // remove a node but update the trigger before the waitFor period expires
    // and assert that the new trigger still fires

    NodeLostTrigger trigger = new NodeLostTrigger("node_lost_trigger");
    trigger.configure(cluster.getLoader(), cluster, props);
    trigger.setProcessor(noFirstRunProcessor);
    trigger.run();

    // stop the newly created node
    cluster.simRemoveNode(newNode, false);

    trigger.run(); // this run should detect the lost node
    trigger.close(); // close the old trigger

    try (NodeLostTrigger newTrigger = new NodeLostTrigger("some_different_name"))  {
      newTrigger.configure(cluster.getLoader(), cluster, props);
      try {
        newTrigger.restoreState(trigger);
        fail("Trigger should only be able to restore state from an old trigger of the same name");
      } catch (AssertionError e) {
        // expected
      }
    }

    try (NodeLostTrigger newTrigger = new NodeLostTrigger("node_lost_trigger")) {
      newTrigger.configure(cluster.getLoader(), cluster, props);
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
        timeSource.sleep(1000);
        if (counter++ > 10) {
          fail("Lost node was not discovered by trigger even after 10 seconds");
        }
      } while (!fired.get());

      TriggerEvent nodeLostEvent = eventRef.get();
      assertNotNull(nodeLostEvent);
      @SuppressWarnings({"unchecked"})
      List<String> nodeNames = (List<String>)nodeLostEvent.getProperty(TriggerEvent.NODE_NAMES);
      assertTrue(nodeNames.contains(newNode));
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
