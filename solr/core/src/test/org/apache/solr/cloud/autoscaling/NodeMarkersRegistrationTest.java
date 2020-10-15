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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.LiveNodesListener;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.OverseerTriggerThread.MARKER_ACTIVE;
import static org.apache.solr.cloud.autoscaling.OverseerTriggerThread.MARKER_INACTIVE;
import static org.apache.solr.cloud.autoscaling.OverseerTriggerThread.MARKER_STATE;

@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
public class NodeMarkersRegistrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static volatile CountDownLatch triggerFiredLatch;
  private static volatile CountDownLatch listenerEventLatch;
  private static Set<TriggerEvent> events = ConcurrentHashMap.newKeySet();
  private volatile ZkStateReader zkStateReader;
  private static final ReentrantLock lock = new ReentrantLock();

  @Before
  public void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    zkStateReader = cluster.getSolrClient().getZkStateReader();

    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
  }
  
  @After
  public void teardownCluster() throws Exception {
    shutdownCluster();
  }

  private static CountDownLatch getTriggerFiredLatch() {
    return triggerFiredLatch;
  }

  //@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13376")
  @Test
  public void testNodeMarkersRegistration() throws Exception {
    triggerFiredLatch = new CountDownLatch(1);
    listenerEventLatch = new CountDownLatch(1);
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
    // add a nodes
    JettySolrRunner node = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    if (!listener.onChangeLatch.await(10, TimeUnit.SECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }
    assertEquals(1, listener.addedNodes.size());
    assertTrue(listener.addedNodes.toString(), listener.addedNodes.contains(node.getNodeName()));
    // verify that a znode doesn't exist (no trigger)
    String pathAdded = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + node.getNodeName();
    assertFalse("Path " + pathAdded + " was created but there are no nodeAdded triggers", zkClient().exists(pathAdded, true));
    listener.reset();

    // stop overseer
    log.info("====== KILL OVERSEER 1");
    JettySolrRunner j = cluster.stopJettySolrRunner(overseerLeaderIndex);
    cluster.waitForJettyToStop(j);
    if (!listener.onChangeLatch.await(10, TimeUnit.SECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }

    assertEquals(0, listener.addedNodes.size());
    // wait until the new overseer is up
    Thread.sleep(5000);
    String newOverseerLeader;
    do {
      overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
      newOverseerLeader = (String) overSeerStatus.get("leader");
    } while (newOverseerLeader == null || newOverseerLeader.equals(overseerLeader));
    
    assertEquals(1, listener.lostNodes.size());
    assertEquals(overseerLeader, listener.lostNodes.iterator().next());
    
    
    String pathLost = ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH + "/" + overseerLeader;
    
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    AtomicBoolean markerInactive = new AtomicBoolean();
    try {
      timeout.waitFor("nodeLost marker to get inactive", () -> {
        try {
          if (!zkClient().exists(pathLost, true)) {
            throw new RuntimeException("marker " + pathLost + " should exist!");
          }
          Map<String, Object> markerData = Utils.getJson(zkClient(), pathLost, true);
          markerInactive.set(markerData.getOrDefault(MARKER_STATE, MARKER_ACTIVE).equals(MARKER_INACTIVE));
          return markerInactive.get();
        } catch (KeeperException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          return false;
        }
      });
    } catch (TimeoutException e) {
      // okay
    }

    // verify that the marker is inactive - the new overseer should deactivate markers once they are processed
    assertTrue("Marker " + pathLost + " still active!", markerInactive.get());

    listener.reset();

    // set up triggers
    CloudSolrClient solrClient = cluster.getSolrClient();

    log.info("====== ADD TRIGGERS");
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_triggerMR'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestEventMarkerAction.class.getName() + "'}]" +
        "}}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_triggerMR'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestEventMarkerAction.class.getName() + "'}]" +
        "}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListener = "{\n" +
        "  \"set-listener\" : {\n" +
        "    \"name\" : \"listener_node_added_triggerMR\",\n" +
        "    \"trigger\" : \"node_added_triggerMR\",\n" +
        "    \"stage\" : \"STARTED\",\n" +
        "    \"class\" : \"" + AssertingListener.class.getName()  + "\"\n" +
        "  }\n" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListener);
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
    cluster.waitForAllNodes(30);
    if (!listener.onChangeLatch.await(10, TimeUnit.SECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }
    assertEquals(1, listener.addedNodes.size());
    assertEquals(node1.getNodeName(), listener.addedNodes.iterator().next());
    // verify that a znode exists
    pathAdded = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + node1.getNodeName();
    assertTrue("Path " + pathAdded + " wasn't created", zkClient().exists(pathAdded, true));

    listenerEventLatch.countDown(); // let the trigger thread continue

    assertTrue(triggerFiredLatch.await(10, TimeUnit.SECONDS));

    // kill this node
    listener.reset();
    events.clear();
    triggerFiredLatch = new CountDownLatch(1);

    String node1Name = node1.getNodeName();
    cluster.stopJettySolrRunner(node1);
    if (!listener.onChangeLatch.await(10, TimeUnit.SECONDS)) {
      fail("onChange listener didn't execute on cluster change");
    }
    assertEquals(1, listener.lostNodes.size());
    assertEquals(node1Name, listener.lostNodes.iterator().next());
    // verify that a znode exists
    String pathLost2 = ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH + "/" + node1Name;
    assertTrue("Path " + pathLost2 + " wasn't created", zkClient().exists(pathLost2, true));

    listenerEventLatch.countDown(); // let the trigger thread continue

    assertTrue(triggerFiredLatch.await(10, TimeUnit.SECONDS));

    // triggers don't remove markers
    assertTrue("Path " + pathLost2 + " should still exist", zkClient().exists(pathLost2, true));

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
    @SuppressWarnings({"unchecked"})
    List<String> nodeNames = (List<String>) ev.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(overseerLeader));
    assertEquals(TriggerEventType.NODELOST, ev.getEventType());
  }

  private TestLiveNodesListener registerLiveNodesListener() {
    TestLiveNodesListener listener = new TestLiveNodesListener();
    zkStateReader.registerLiveNodesListener(listener);
    return listener;
  }

  private static class TestLiveNodesListener implements LiveNodesListener {
    Set<String> lostNodes = ConcurrentHashMap.newKeySet();
    Set<String> addedNodes = ConcurrentHashMap.newKeySet();
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

  public static class TestEventMarkerAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {
      boolean locked = lock.tryLock();
      if (!locked) {
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
    public void init() throws Exception {
      log.info("TestEventMarkerAction init");
      super.init();
    }
  }

  public static class AssertingListener extends TriggerListenerBase {
    @Override
    public void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName, ActionContext context, Throwable error, String message) throws Exception {
      if (!Thread.currentThread().getName().startsWith("ScheduledTrigger")) {
        // for future safety
        throw new IllegalThreadStateException("AssertingListener should have been invoked by a thread from the scheduled trigger thread pool");
      }
      log.debug(" --- listener fired for event: {}, stage: {}", event, stage);
      listenerEventLatch.await();
      log.debug(" --- listener wait complete for event: {}, stage: {}", event, stage);
    }
  }
}
