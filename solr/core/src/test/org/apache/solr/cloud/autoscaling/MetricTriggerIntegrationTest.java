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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.WAIT_FOR_DELTA_NANOS;

/**
 * Integration test for {@link MetricTrigger}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
public class MetricTriggerIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final TimeSource timeSource = TimeSource.NANO_TIME;
  
  static final Map<String, List<CapturedEvent>> listenerEvents = new HashMap<>();
  private static CountDownLatch triggerFiredLatch;
  private static int waitForSeconds = 1;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cluster.getOpenOverseer().getSolrCloudManager(), ".scheduled_maintenance");

    listenerEvents.clear();
    triggerFiredLatch = new CountDownLatch(1);
  }

  @Test
  // commented 4-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 14-Oct-2018
  public void testMetricTrigger() throws Exception {
    String collectionName = "testMetricTrigger";
    CloudSolrClient solrClient = cluster.getSolrClient();
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 2).setMaxShardsPerNode(2);
    create.process(solrClient);
    solrClient.setDefaultCollection(collectionName);

    cluster.waitForActiveCollection(collectionName, 2, 4);

    DocCollection docCollection = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    String shardId = "shard1";
    Replica replica = docCollection.getSlice(shardId).getReplicas().iterator().next();
    String coreName = replica.getCoreName();
    String replicaName = Utils.parseMetricsReplicaName(collectionName, coreName);
    long waitForSeconds = 2 + random().nextInt(5);
    String registry = SolrCoreMetricManager.createRegistryName(true, collectionName, shardId, replicaName, null);
    String tag = "metrics:" + registry + ":INDEX.sizeInBytes";

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'metric_trigger'," +
        "'event' : 'metric'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'metric': '" + tag + "'" +
        "'above' : 100.0," +
        "'collection': '" + collectionName + "'" +
        "'shard':'" + shardId + "'" +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + MetricAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand1 = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'srt'," +
        "'trigger' : 'metric_trigger'," +
        "'stage' : ['FAILED','SUCCEEDED']," +
        "'afterAction': ['compute', 'execute', 'test']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand1);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // start more nodes so that we have at least 4
    for (int i = cluster.getJettySolrRunners().size(); i < 4; i++) {
      cluster.startJettySolrRunner();
    }
    cluster.waitForAllNodes(10);

    List<SolrInputDocument> docs = new ArrayList<>(500);
    for (int i = 0; i < 500; i++) {
      docs.add(new SolrInputDocument("id", String.valueOf(i), "x_s", "x" + i));
    }
    solrClient.add(docs);
    solrClient.commit();

    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    Thread.sleep(2000);
    assertEquals(listenerEvents.toString(), 4, listenerEvents.get("srt").size());
    CapturedEvent ev = listenerEvents.get("srt").get(0);
    long now = timeSource.getTimeNs();
    // verify waitFor
    assertTrue(TimeUnit.SECONDS.convert(waitForSeconds, TimeUnit.NANOSECONDS) - WAIT_FOR_DELTA_NANOS <= now - ev.event.getEventTime());
    assertEquals(collectionName, ev.event.getProperties().get("collection"));

    // find a new replica and create its metric name
    docCollection = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    replica = docCollection.getSlice(shardId).getReplicas().iterator().next();
    coreName = replica.getCoreName();
    replicaName = Utils.parseMetricsReplicaName(collectionName, coreName);
    registry = SolrCoreMetricManager.createRegistryName(true, collectionName, shardId, replicaName, null);
    tag = "metrics:" + registry + ":INDEX.sizeInBytes";

    triggerFiredLatch = new CountDownLatch(1);
    listenerEvents.clear();

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'metric_trigger'," +
        "'event' : 'metric'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'metric': '" + tag + "'," +
        "'above' : 100.0," +
        "'collection': '" + collectionName + "'," +
        "'shard':'" + shardId + "'," +
        "'preferredOperation':'addreplica'," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + MetricAction.class.getName() + "'}" +
        "]" +
        "}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    Thread.sleep(2000);
    assertEquals(listenerEvents.toString(), 4, listenerEvents.get("srt").size());
    ev = listenerEvents.get("srt").get(0);
    now = timeSource.getTimeNs();
    // verify waitFor
    assertTrue(TimeUnit.SECONDS.convert(waitForSeconds, TimeUnit.NANOSECONDS) - WAIT_FOR_DELTA_NANOS <= now - ev.event.getEventTime());
    assertEquals(collectionName, ev.event.getProperties().get("collection"));
    docCollection = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertEquals(5, docCollection.getReplicas().size());
  }

  public static class MetricAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      try {
        long currentTimeNanos = context.getCloudManager().getTimeSource().getTimeNs();
        long eventTimeNanos = event.getEventTime();
        long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
        if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
          fail(event.source + " was fired before the configured waitFor period");
        }
        triggerFiredLatch.countDown();
      } catch (Throwable t) {
        log.debug("--throwable", t);
        throw t;
      }
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
      lst.add(new CapturedEvent(timeSource.getTimeNs(), context, config, stage, actionName, event, message));
                                
    }
  }
}
