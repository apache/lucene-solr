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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.autoscaling.sim.SimCloudManager;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class ScheduledMaintenanceTriggerTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static SolrCloudManager cloudManager;
  private static SolrClient solrClient;
  private static TimeSource timeSource;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    if (random().nextBoolean()) {
      cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
      solrClient = cluster.getSolrClient();
    } else {
      cloudManager = SimCloudManager.createCluster(1, TimeSource.get("simTime:50"));
      // wait for defaults to be applied - due to accelerated time sometimes we may miss this
      cloudManager.getTimeSource().sleep(10000);
      AutoScalingConfig cfg = cloudManager.getDistribStateManager().getAutoScalingConfig();
      assertFalse("autoscaling config is empty", cfg.isEmpty());
      solrClient = ((SimCloudManager)cloudManager).simGetSolrClient();
    }
    timeSource = cloudManager.getTimeSource();
  }

  @After
  public void restoreDefaults() throws Exception {
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST,
        "{'set-trigger' : " + AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_DSL + "}");
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
    if (autoScalingConfig.getTriggerListenerConfigs().containsKey("foo")) {
      String cmd = "{" +
          "'remove-listener' : {'name' : 'foo'}" +
          "}";
      response = solrClient.request(createAutoScalingRequest(SolrRequest.METHOD.POST, cmd));
      assertEquals(response.get("result").toString(), "success");
    }
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (cloudManager instanceof SimCloudManager) {
      cloudManager.close();
    }
    solrClient = null;
    cloudManager = null;
  }

  @Test
  public void testTriggerDefaults() throws Exception {
    AutoScalingConfig autoScalingConfig = cloudManager.getDistribStateManager().getAutoScalingConfig();
    log.info(autoScalingConfig.toString());
    AutoScalingConfig.TriggerConfig triggerConfig = autoScalingConfig.getTriggerConfigs().get(AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME);
    assertNotNull(triggerConfig);
    assertEquals(2, triggerConfig.actions.size());
    assertTrue(triggerConfig.actions.get(0).actionClass.endsWith(InactiveShardPlanAction.class.getSimpleName()));
    assertTrue(triggerConfig.actions.get(1).actionClass.endsWith(ExecutePlanAction.class.getSimpleName()));
    AutoScalingConfig.TriggerListenerConfig listenerConfig = autoScalingConfig.getTriggerListenerConfigs().get(AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME + ".system");
    assertNotNull(listenerConfig);
    assertEquals(AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME, listenerConfig.trigger);
    assertTrue(listenerConfig.listenerClass.endsWith(SystemLogListener.class.getSimpleName()));
  }

  static Map<String, List<CapturedEvent>> listenerEvents = new ConcurrentHashMap<>();
  static CountDownLatch listenerCreated = new CountDownLatch(1);

  public static class CapturingTriggerListener extends TriggerListenerBase {
    @Override
    public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) throws TriggerValidationException {
      super.configure(loader, cloudManager, config);
      listenerCreated.countDown();
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
      CapturedEvent ev = new CapturedEvent(timeSource.getTimeNs(), context, config, stage, actionName, event, message);
      log.info("=======> " + ev);
      lst.add(ev);
    }
  }

  static CountDownLatch triggerFired = new CountDownLatch(1);

  public static class TestTriggerAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      if (context.getProperties().containsKey("inactive_shard_plan")) {
        triggerFired.countDown();
      }
    }
  }

  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 17-Mar-2018
  public void testInactiveShardCleanup() throws Exception {
    String collection1 = getClass().getSimpleName() + "_collection1";
    CollectionAdminRequest.Create create1 = CollectionAdminRequest.createCollection(collection1,
        "conf", 1, 1);

    create1.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "failed to create " + collection1, collection1,
        CloudTestUtils.clusterShape(1, 1));

    CollectionAdminRequest.SplitShard split1 = CollectionAdminRequest.splitShard(collection1)
        .setShardName("shard1");
    split1.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, "failed to split " + collection1, collection1,
        CloudTestUtils.clusterShape(3, 1, true, true));

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'foo'," +
        "'trigger' : '" + AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME + "'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED','FAILED']," +
        "'beforeAction' : 'inactive_shard_plan'," +
        "'afterAction' : 'inactive_shard_plan'," +
        "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : '" + AutoScaling.SCHEDULED_MAINTENANCE_TRIGGER_NAME + "'," +
        "'event' : 'scheduled'," +
        "'startTime' : 'NOW+3SECONDS'," +
        "'every' : '+2SECONDS'," +
        "'enabled' : true," +
        "'actions' : [{'name' : 'inactive_shard_plan', 'class' : 'solr.InactiveShardPlanAction', 'ttl' : '10'}," +
        "{'name' : 'execute_plan', 'class' : '" + ExecutePlanAction.class.getName() + "'}," +
        "{'name' : 'test', 'class' : '" + TestTriggerAction.class.getName() + "'}]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    boolean await = listenerCreated.await(10, TimeUnit.SECONDS);
    assertTrue("listener not created in time", await);
    await = triggerFired.await(60, TimeUnit.SECONDS);
    assertTrue("cleanup action didn't run", await);

    // cleanup should have occurred
    assertFalse("no events captured!", listenerEvents.isEmpty());
    List<CapturedEvent> events = new ArrayList<>(listenerEvents.get("foo"));
    listenerEvents.clear();

    assertFalse(events.isEmpty());
    int inactiveEvents = 0;
    CapturedEvent ce = null;
    for (CapturedEvent e : events) {
      if (e.stage != TriggerEventProcessorStage.AFTER_ACTION) {
        continue;
      }
      if (e.context.containsKey("properties.inactive_shard_plan")) {
        ce = e;
        break;
      } else {
        inactiveEvents++;
      }
    }
    assertTrue("should be at least one inactive event", inactiveEvents > 0);
    assertNotNull("missing cleanup event", ce);
    Map<String, Object> map = (Map<String, Object>)ce.context.get("properties.inactive_shard_plan");
    assertNotNull(map);

    Map<String, List<String>> inactive = (Map<String, List<String>>)map.get("inactive");
    assertEquals(1, inactive.size());
    assertNotNull(inactive.get(collection1));
    Map<String, List<String>> cleanup = (Map<String, List<String>>)map.get("cleanup");
    assertEquals(1, cleanup.size());
    assertNotNull(cleanup.get(collection1));

    ClusterState state = cloudManager.getClusterStateProvider().getClusterState();

    CloudTestUtils.clusterShape(2, 1).matches(state.getLiveNodes(), state.getCollection(collection1));
  }
}
