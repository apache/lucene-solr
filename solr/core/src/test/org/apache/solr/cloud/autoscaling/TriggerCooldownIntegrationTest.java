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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.WAIT_FOR_DELTA_NANOS;

@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
public class TriggerCooldownIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int waitForSeconds = 1;
  
  private static final Map<String, List<CapturedEvent>> listenerEvents = new HashMap<>();
  private static CountDownLatch triggerFiredLatch = new CountDownLatch(1);
  private static final AtomicBoolean triggerFired = new AtomicBoolean();

  private static final void resetTriggerAndListenerState() {
    // reset the trigger and captured events
    listenerEvents.clear();
    triggerFiredLatch = new CountDownLatch(1);
    triggerFired.compareAndSet(true, false);
  }
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    resetTriggerAndListenerState();
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    
    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
  }

  @Test
  public void testCooldown() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
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
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand1);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    JettySolrRunner newNode = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    assertTrue(triggerFired.get());
    // wait for listener to capture the SUCCEEDED stage
    Thread.sleep(1000);

    List<CapturedEvent> capturedEvents = listenerEvents.get("bar");
    // we may get a few IGNORED events if other tests caused events within cooldown period
    assertTrue(capturedEvents.toString(), capturedEvents.size() > 0);
    long prevTimestamp = capturedEvents.get(capturedEvents.size() - 1).timestamp;

    resetTriggerAndListenerState();

    JettySolrRunner newNode2 = cluster.startJettySolrRunner();
    await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    Thread.sleep(2000);

    // there must be at least one IGNORED event due to cooldown, and one SUCCEEDED event
    capturedEvents = listenerEvents.get("bar");
    assertEquals(capturedEvents.toString(), 1, capturedEvents.size());
    CapturedEvent ev = capturedEvents.get(0);
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
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, setPropertiesCommand));
    req = AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    response = solrClient.request(req);

    resetTriggerAndListenerState();

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

    // there must be two SUCCEEDED (due to newNode3 and newNode4) and maybe some ignored events
    capturedEvents = listenerEvents.get("bar");
    assertTrue(capturedEvents.toString(), capturedEvents.size() >= 2);
    // first event should be SUCCEEDED
    ev = capturedEvents.get(0);
    assertEquals(ev.toString(), TriggerEventProcessorStage.SUCCEEDED, ev.stage);

    ev = capturedEvents.get(capturedEvents.size() - 1);
    assertEquals(ev.toString(), TriggerEventProcessorStage.SUCCEEDED, ev.stage);
    // the difference between timestamps of the first SUCCEEDED and the last SUCCEEDED
    // must be larger than the modified cooldown period
    assertTrue("timestamp delta is less than default cooldown period", ev.timestamp - prevTimestamp > TimeUnit.SECONDS.toNanos(modifiedCooldownPeriodSeconds));
  }

  public static class TestTriggerAction extends TriggerActionBase {

    public TestTriggerAction() {
      // No-Op
    }

    @Override
    public void process(TriggerEvent event, ActionContext actionContext) {
      try {
        if (triggerFired.compareAndSet(false, true)) {
          long currentTimeNanos = actionContext.getCloudManager().getTimeSource().getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail(event.source + " was fired before the configured waitFor period");
          }
          triggerFiredLatch.countDown();
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
      super.init();
    }
  }

  public static class TestTriggerListener extends TriggerListenerBase {
    private TimeSource timeSource;
    @Override
    public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) throws TriggerValidationException {
      super.configure(loader, cloudManager, config);
      timeSource = cloudManager.getTimeSource();
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
      lst.add(new CapturedEvent(timeSource.getTimeNs(),
                                context, config, stage, actionName, event, message));
    }
  }
}
