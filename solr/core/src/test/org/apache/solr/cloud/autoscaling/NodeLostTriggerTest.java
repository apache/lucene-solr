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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.CoreContainer;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link NodeLostTrigger}
 */
public class NodeLostTriggerTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(5)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void testTrigger() throws Exception {
    CoreContainer container = cluster.getJettySolrRunners().get(0).getCoreContainer();
    long waitForSeconds = 1 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);

    try (NodeLostTrigger trigger = new NodeLostTrigger("node_lost_trigger", props, container)) {
      trigger.setListener(event -> fail("Did not expect the listener to fire on first run!"));
      trigger.run();
      String lostNodeName = cluster.getJettySolrRunner(1).getNodeName();
      cluster.stopJettySolrRunner(1);

      AtomicBoolean fired = new AtomicBoolean(false);
      AtomicReference<NodeLostTrigger.NodeLostEvent> eventRef = new AtomicReference<>();
      trigger.setListener(event -> {
        if (fired.compareAndSet(false, true)) {
          eventRef.set(event);
          if (System.nanoTime() - event.getEventNanoTime() <= TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS)) {
            fail("NodeLostListener was fired before the configured waitFor period");
          }
        } else {
          fail("NodeLostListener was fired more than once!");
        }
      });
      int counter = 0;
      do {
        trigger.run();
        Thread.sleep(1000);
        if (counter++ > 10) {
          fail("Lost node was not discovered by trigger even after 10 seconds");
        }
      } while (!fired.get());

      NodeLostTrigger.NodeLostEvent nodeLostEvent = eventRef.get();
      assertNotNull(nodeLostEvent);
      assertEquals("", lostNodeName, nodeLostEvent.getNodeName());

    }

    // remove a node but add it back before the waitFor period expires
    // and assert that the trigger doesn't fire at all
    try (NodeLostTrigger trigger = new NodeLostTrigger("node_lost_trigger", props, container)) {
      final long waitTime = 2;
      props.put("waitFor", waitTime);
      trigger.setListener(event -> fail("Did not expect the listener to fire on first run!"));
      trigger.run();

      JettySolrRunner lostNode = cluster.getJettySolrRunner(1);
      lostNode.stop();
      AtomicBoolean fired = new AtomicBoolean(false);
      trigger.setListener(event -> {
        if (fired.compareAndSet(false, true)) {
          if (System.nanoTime() - event.getEventNanoTime() <= TimeUnit.NANOSECONDS.convert(waitTime, TimeUnit.SECONDS)) {
            fail("NodeLostListener was fired before the configured waitFor period");
          }
        } else {
          fail("NodeLostListener was fired more than once!");
        }
      });
      trigger.run(); // first run should detect the lost node
      int counter = 0;
      do {
        if (container.getZkController().getZkStateReader().getClusterState().getLiveNodes().size() == 3) {
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

  @Test
  public void testRestoreState() throws Exception {
    CoreContainer container = cluster.getJettySolrRunners().get(0).getCoreContainer();
    long waitForSeconds = 1 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    String lostNodeName = newNode.getNodeName();

    // remove a node but update the trigger before the waitFor period expires
    // and assert that the new trigger still fires

    NodeLostTrigger trigger = new NodeLostTrigger("node_lost_trigger", props, container);
    trigger.setListener(event -> fail("Did not expect the listener to fire on first run!"));
    trigger.run();
    newNode.stop();
    trigger.run(); // this run should detect the lost node
    trigger.close(); // close the old trigger

    try (NodeLostTrigger newTrigger = new NodeLostTrigger("some_different_name", props, container))  {
      try {
        newTrigger.restoreState(trigger);
        fail("Trigger should only be able to restore state from an old trigger of the same name");
      } catch (AssertionError e) {
        // expected
      }
    }

    try (NodeLostTrigger newTrigger = new NodeLostTrigger("node_lost_trigger", props, container)) {
      AtomicBoolean fired = new AtomicBoolean(false);
      AtomicReference<NodeLostTrigger.NodeLostEvent> eventRef = new AtomicReference<>();
      newTrigger.setListener(event -> {
        if (fired.compareAndSet(false, true)) {
          eventRef.set(event);
          if (System.nanoTime() - event.getEventNanoTime() <= TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS)) {
            fail("NodeLostListener was fired before the configured waitFor period");
          }
        } else {
          fail("NodeLostListener was fired more than once!");
        }
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

      NodeLostTrigger.NodeLostEvent nodeLostEvent = eventRef.get();
      assertNotNull(nodeLostEvent);
      assertEquals("", lostNodeName, nodeLostEvent.getNodeName());
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
    map = new HashMap<>(2);
    map.put("name", "log_plan");
    map.put("class", "solr.LogPlanAction");
    actions.add(map);
    props.put("actions", actions);
    return props;
  }
}
