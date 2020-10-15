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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.WAIT_FOR_DELTA_NANOS;

/**
 * Integration test to ensure that triggers can restore state from ZooKeeper after overseer restart
 * so that events detected before restart are not lost.
 *
 * Added in SOLR-10515
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
public class RestoreTriggerStateTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CountDownLatch actionInitCalled;
  private static CountDownLatch triggerFiredLatch;
  private static AtomicBoolean triggerFired;
  private static CountDownLatch actionConstructorCalled;
  private static Set<TriggerEvent> events = ConcurrentHashMap.newKeySet();
  private static int waitForSeconds = 1;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
    
    actionInitCalled = new CountDownLatch(1);
    triggerFiredLatch = new CountDownLatch(1);
    actionConstructorCalled = new CountDownLatch(1);
    triggerFired = new AtomicBoolean();
  }

  @Test
  public void testEventFromRestoredState() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_triggerEFRS'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '10s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    if (!actionInitCalled.await(10, TimeUnit.SECONDS)) {
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
    cluster.waitForAllNodes(30);
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    // reset
    triggerFired.set(false);
    triggerFiredLatch = new CountDownLatch(1);
    NodeAddedTrigger.NodeAddedEvent nodeAddedEvent = (NodeAddedTrigger.NodeAddedEvent) events.iterator().next();
    assertNotNull(nodeAddedEvent);
    @SuppressWarnings({"unchecked"})
    List<String> nodeNames = (List<String>) nodeAddedEvent.getProperty(TriggerEvent.NODE_NAMES);
    assertTrue(nodeNames.contains(newNode.getNodeName()));
    // add a second node - state of the trigger will change but it won't fire for waitFor sec.
    JettySolrRunner newNode2 = cluster.startJettySolrRunner();
    Thread.sleep(10000);
    // kill overseer leader
    JettySolrRunner j = cluster.stopJettySolrRunner(overseerLeaderIndex);
    cluster.waitForJettyToStop(j);
    await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
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
          triggerFiredLatch.countDown();
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
}
