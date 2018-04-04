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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;
import static org.apache.solr.cloud.autoscaling.ScheduledTriggers.DEFAULT_SCHEDULED_TRIGGER_DELAY_SECONDS;
import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.WAIT_FOR_DELTA_NANOS;
import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
public class NodeLostTriggerIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CountDownLatch actionConstructorCalled;
  private static CountDownLatch actionInitCalled;
  private static CountDownLatch triggerFiredLatch;
  private static int waitForSeconds = 1;
  private static AtomicBoolean triggerFired;
  private static Set<TriggerEvent> events = ConcurrentHashMap.newKeySet();
  private static SolrCloudManager cloudManager;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    // disable .scheduled_maintenance
    String suspendTriggerCommand = "{" +
        "'suspend-trigger' : {'name' : '.scheduled_maintenance'}" +
        "}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
    SolrClient solrClient = cluster.getSolrClient();
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
  }

  private static CountDownLatch getTriggerFiredLatch() {
    return triggerFiredLatch;
  }

  @Before
  public void setupTest() throws Exception {
    // ensure that exactly 2 jetty nodes are running
    int numJetties = cluster.getJettySolrRunners().size();
    log.info("Found {} jetty instances running", numJetties);
    for (int i = 2; i < numJetties; i++) {
      int r = random().nextInt(cluster.getJettySolrRunners().size());
      log.info("Shutdown extra jetty instance at port {}", cluster.getJettySolrRunner(r).getLocalPort());
      cluster.stopJettySolrRunner(r);
    }
    for (int i = cluster.getJettySolrRunners().size(); i < 2; i++) {
      // start jetty instances
      cluster.startJettySolrRunner();
    }
    cluster.waitForAllNodes(5);

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
    ScheduledTriggers scheduledTriggers = ((OverseerTriggerThread) overseer.getTriggerThread().getThread()).getScheduledTriggers();
    // aggressively remove all active scheduled triggers
    scheduledTriggers.removeAll();

    // clear any persisted auto scaling configuration
    Stat stat = zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);
    log.info(SOLR_AUTOSCALING_CONF_PATH + " reset, new znode version {}", stat.getVersion());

    cluster.deleteAllCollections();
    cluster.getSolrClient().setDefaultCollection(null);

    // restart Overseer. Even though we reset the autoscaling config some already running
    // trigger threads may still continue to execute and produce spurious events
    cluster.stopJettySolrRunner(overseerLeaderIndex);
    Thread.sleep(5000);

    waitForSeconds = 1 + random().nextInt(3);
    actionConstructorCalled = new CountDownLatch(1);
    actionInitCalled = new CountDownLatch(1);
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired = new AtomicBoolean(false);
    events.clear();

    while (cluster.getJettySolrRunners().size() < 2) {
      // perhaps a test stopped a node but didn't start it back
      // lets start a node
      cluster.startJettySolrRunner();
    }

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
  public void testNodeLostTriggerRestoreState() throws Exception {
    // for this test we want to update the trigger so we must assert that the actions were created twice
    actionInitCalled = new CountDownLatch(2);

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
    if (!actionInitCalled.await(3, TimeUnit.SECONDS)) {
      fail("Two TriggerAction instances should have been created by now");
    }

    boolean await = triggerFiredLatch.await(5, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeLostTrigger.NodeLostEvent nodeLostEvent = (NodeLostTrigger.NodeLostEvent) events.iterator().next();
    assertNotNull(nodeLostEvent);
    List<String> nodeNames = (List<String>) nodeLostEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(nodeName));
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

    if (!actionInitCalled.await(3, TimeUnit.SECONDS)) {
      fail("The TriggerAction should have been created by now");
    }

    triggerFired.set(false);
    triggerFiredLatch = new CountDownLatch(1);
    String lostNodeName = cluster.getJettySolrRunner(nonOverseerLeaderIndex).getNodeName();
    cluster.stopJettySolrRunner(nonOverseerLeaderIndex);
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    NodeLostTrigger.NodeLostEvent nodeLostEvent = (NodeLostTrigger.NodeLostEvent) events.iterator().next();
    assertNotNull(nodeLostEvent);
    List<String> nodeNames = (List<String>) nodeLostEvent.getProperty(TriggerEvent.NODE_NAMES);
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
          long currentTimeNanos = TriggerIntegrationTest.timeSource.getTimeNs();
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
}
