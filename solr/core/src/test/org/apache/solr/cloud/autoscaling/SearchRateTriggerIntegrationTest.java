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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;
import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.WAIT_FOR_DELTA_NANOS;
import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.timeSource;

/**
 * Integration test for {@link SearchRateTrigger}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
@LuceneTestCase.BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-12028")
public class SearchRateTriggerIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CountDownLatch triggerFiredLatch = new CountDownLatch(1);
  private static CountDownLatch listenerCreated = new CountDownLatch(1);
  private static int waitForSeconds = 1;
  private static Set<TriggerEvent> events = ConcurrentHashMap.newKeySet();
  private static Map<String, List<CapturedEvent>> listenerEvents = new HashMap<>();

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(5)
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

  @Test
  public void testSearchRate() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String COLL1 = "collection1";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 1, 2);
    create.process(solrClient);
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'search_rate_trigger'," +
        "'event' : 'searchRate'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'rate' : 1.0," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + TestSearchRateAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand1 = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'srt'," +
        "'trigger' : 'search_rate_trigger'," +
        "'stage' : ['FAILED','SUCCEEDED']," +
        "'afterAction': ['compute', 'execute', 'test']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand1);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    SolrParams query = params(CommonParams.Q, "*:*");
    for (int i = 0; i < 500; i++) {
      solrClient.query(COLL1, query);
    }
    boolean await = triggerFiredLatch.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    Thread.sleep(5000);
    List<CapturedEvent> events = listenerEvents.get("srt");
    assertEquals(listenerEvents.toString(), 4, events.size());
    assertEquals("AFTER_ACTION", events.get(0).stage.toString());
    assertEquals("compute", events.get(0).actionName);
    assertEquals("AFTER_ACTION", events.get(1).stage.toString());
    assertEquals("execute", events.get(1).actionName);
    assertEquals("AFTER_ACTION", events.get(2).stage.toString());
    assertEquals("test", events.get(2).actionName);
    assertEquals("SUCCEEDED", events.get(3).stage.toString());
    assertNull(events.get(3).actionName);

    CapturedEvent ev = events.get(0);
    long now = timeSource.getTimeNs();
    // verify waitFor
    assertTrue(TimeUnit.SECONDS.convert(waitForSeconds, TimeUnit.NANOSECONDS) - WAIT_FOR_DELTA_NANOS <= now - ev.event.getEventTime());
    Map<String, Double> nodeRates = (Map<String, Double>) ev.event.getProperties().get("node");
    assertNotNull("nodeRates", nodeRates);
    assertTrue(nodeRates.toString(), nodeRates.size() > 0);
    AtomicDouble totalNodeRate = new AtomicDouble();
    nodeRates.forEach((n, r) -> totalNodeRate.addAndGet(r));
    List<ReplicaInfo> replicaRates = (List<ReplicaInfo>) ev.event.getProperties().get("replica");
    assertNotNull("replicaRates", replicaRates);
    assertTrue(replicaRates.toString(), replicaRates.size() > 0);
    AtomicDouble totalReplicaRate = new AtomicDouble();
    replicaRates.forEach(r -> {
      assertTrue(r.toString(), r.getVariable("rate") != null);
      totalReplicaRate.addAndGet((Double) r.getVariable("rate"));
    });
    Map<String, Object> shardRates = (Map<String, Object>) ev.event.getProperties().get("shard");
    assertNotNull("shardRates", shardRates);
    assertEquals(shardRates.toString(), 1, shardRates.size());
    shardRates = (Map<String, Object>) shardRates.get(COLL1);
    assertNotNull("shardRates", shardRates);
    assertEquals(shardRates.toString(), 1, shardRates.size());
    AtomicDouble totalShardRate = new AtomicDouble();
    shardRates.forEach((s, r) -> totalShardRate.addAndGet((Double) r));
    Map<String, Double> collectionRates = (Map<String, Double>) ev.event.getProperties().get("collection");
    assertNotNull("collectionRates", collectionRates);
    assertEquals(collectionRates.toString(), 1, collectionRates.size());
    Double collectionRate = collectionRates.get(COLL1);
    assertNotNull(collectionRate);
    assertTrue(collectionRate > 5.0);
    assertEquals(collectionRate, totalNodeRate.get(), 5.0);
    assertEquals(collectionRate, totalShardRate.get(), 5.0);
    assertEquals(collectionRate, totalReplicaRate.get(), 5.0);

    // check operations
    List<Map<String, Object>> ops = (List<Map<String, Object>>) ev.context.get("properties.operations");
    assertNotNull(ops);
    assertTrue(ops.size() > 1);
    for (Map<String, Object> m : ops) {
      assertEquals("ADDREPLICA", m.get("params.action"));
    }
  }

  public static class TestSearchRateAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      try {
        events.add(event);
        long currentTimeNanos = timeSource.getTimeNs();
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
    @Override
    public void init(SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) {
      super.init(cloudManager, config);
      listenerCreated.countDown();
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
      lst.add(new CapturedEvent(timeSource.getTimeNs(), context, config, stage, actionName, event, message));
    }
  }
}
