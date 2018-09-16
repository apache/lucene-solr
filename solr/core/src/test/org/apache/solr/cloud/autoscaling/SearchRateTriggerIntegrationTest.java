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
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;
import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.WAIT_FOR_DELTA_NANOS;
import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.timeSource;
import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 * Integration test for {@link SearchRateTrigger}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
@LuceneTestCase.Slow
public class SearchRateTriggerIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CountDownLatch listenerCreated = new CountDownLatch(1);
  private static Map<String, List<CapturedEvent>> listenerEvents = new HashMap<>();
  private static CountDownLatch finished = new CountDownLatch(1);
  private static CountDownLatch started = new CountDownLatch(1);
  private static SolrCloudManager cloudManager;

  private int waitForSeconds;

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
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
  }

  @Before
  public void beforeTest() throws Exception {
    cluster.deleteAllCollections();
    // clear any persisted auto scaling configuration
    Stat stat = zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);
    log.info(SOLR_AUTOSCALING_CONF_PATH + " reset, new znode version {}", stat.getVersion());
    timeSource.sleep(5000);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);

    finished = new CountDownLatch(1);
    started = new CountDownLatch(1);
    listenerEvents = new HashMap<>();
    waitForSeconds = 3 + random().nextInt(5);
  }

  private void deleteChildrenRecursively(String path) throws Exception {
    cloudManager.getDistribStateManager().removeRecursively(path, true, false);
  }

  @Test
  public void testAboveSearchRate() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String COLL1 = "aboveRate_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 1, 2);
    create.process(solrClient);

    CloudTestUtils.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(1, 2));

    // the trigger is initially disabled so that we have the time to set up listeners
    // and generate the traffic
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'search_rate_trigger1'," +
        "'event' : 'searchRate'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : false," +
        "'collections' : '" + COLL1 + "'," +
        "'aboveRate' : 1.0," +
        "'belowRate' : 0.1," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'started'," +
        "'trigger' : 'search_rate_trigger1'," +
        "'stage' : ['STARTED']," +
        "'class' : '" + StartedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'srt'," +
        "'trigger' : 'search_rate_trigger1'," +
        "'stage' : ['FAILED','SUCCEEDED']," +
        "'afterAction': ['compute', 'execute']," +
        "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'finished'," +
        "'trigger' : 'search_rate_trigger1'," +
        "'stage' : ['SUCCEEDED']," +
        "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    SolrParams query = params(CommonParams.Q, "*:*");
    for (int i = 0; i < 500; i++) {
      solrClient.query(COLL1, query);
    }

    // enable the trigger
    String resumeTriggerCommand = "{" +
        "'resume-trigger' : {" +
        "'name' : 'search_rate_trigger1'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    boolean await = started.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);

    await = finished.await(60, TimeUnit.SECONDS);
    assertTrue("The trigger did not finish processing", await);

    // suspend the trigger
    String suspendTriggerCommand = "{" +
        "'suspend-trigger' : {" +
        "'name' : 'search_rate_trigger1'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(5000);

    List<CapturedEvent> events = listenerEvents.get("srt");
    assertEquals(listenerEvents.toString(), 3, events.size());
    assertEquals("AFTER_ACTION", events.get(0).stage.toString());
    assertEquals("compute", events.get(0).actionName);
    assertEquals("AFTER_ACTION", events.get(1).stage.toString());
    assertEquals("execute", events.get(1).actionName);
    assertEquals("SUCCEEDED", events.get(2).stage.toString());
    assertNull(events.get(2).actionName);

    CapturedEvent ev = events.get(0);
    long now = timeSource.getTimeNs();
    // verify waitFor
    assertTrue(TimeUnit.SECONDS.convert(waitForSeconds, TimeUnit.NANOSECONDS) - WAIT_FOR_DELTA_NANOS <= now - ev.event.getEventTime());
    Map<String, Double> nodeRates = (Map<String, Double>) ev.event.getProperties().get(SearchRateTrigger.HOT_NODES);
    assertNotNull("nodeRates", nodeRates);
    // no node violations because node rates weren't set in the config
    assertTrue(nodeRates.toString(), nodeRates.isEmpty());
    List<ReplicaInfo> replicaRates = (List<ReplicaInfo>) ev.event.getProperties().get(SearchRateTrigger.HOT_REPLICAS);
    assertNotNull("replicaRates", replicaRates);
    assertTrue(replicaRates.toString(), replicaRates.size() > 0);
    AtomicDouble totalReplicaRate = new AtomicDouble();
    replicaRates.forEach(r -> {
      assertTrue(r.toString(), r.getVariable("rate") != null);
      totalReplicaRate.addAndGet((Double) r.getVariable("rate"));
    });
    Map<String, Object> shardRates = (Map<String, Object>) ev.event.getProperties().get(SearchRateTrigger.HOT_SHARDS);
    assertNotNull("shardRates", shardRates);
    assertEquals(shardRates.toString(), 1, shardRates.size());
    shardRates = (Map<String, Object>) shardRates.get(COLL1);
    assertNotNull("shardRates", shardRates);
    assertEquals(shardRates.toString(), 1, shardRates.size());
    AtomicDouble totalShardRate = new AtomicDouble();
    shardRates.forEach((s, r) -> totalShardRate.addAndGet((Double) r));
    Map<String, Double> collectionRates = (Map<String, Double>) ev.event.getProperties().get(SearchRateTrigger.HOT_COLLECTIONS);
    assertNotNull("collectionRates", collectionRates);
    assertEquals(collectionRates.toString(), 1, collectionRates.size());
    Double collectionRate = collectionRates.get(COLL1);
    assertNotNull(collectionRate);
    assertTrue(collectionRate > 5.0);
    // two replicas - the trigger calculates average over all searchable replicas
    assertEquals(collectionRate / 2, totalShardRate.get(), 5.0);
    assertEquals(collectionRate, totalReplicaRate.get(), 5.0);

    // check operations
    List<Map<String, Object>> ops = (List<Map<String, Object>>) ev.context.get("properties.operations");
    assertNotNull(ops);
    assertTrue(ops.size() > 1);
    for (Map<String, Object> m : ops) {
      assertEquals("ADDREPLICA", m.get("params.action"));
    }
  }

  @Test
  //17-Aug-2018 commented  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 21-May-2018
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 15-Sep-2018
  public void testBelowSearchRate() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String COLL1 = "belowRate_collection";
    // replicationFactor == 2
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 1, 2);
    create.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(1, 2));

    // add a couple of spare replicas above RF. Use different types.
    // these additional replicas will be placed on other nodes in the cluster
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.NRT));
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.TLOG));
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.PULL));

    CloudTestUtils.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(1, 5));

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'search_rate_trigger2'," +
        "'event' : 'searchRate'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : false," +
        "'collections' : '" + COLL1 + "'," +
        "'aboveRate' : 1.0," +
        // RecoveryStrategy calls /admin/ping, which calls /select so this may not be zero
        // even when no external requests were made
        "'belowRate' : 0.3," +
        "'aboveNodeRate' : 1.0," +
        "'belowNodeRate' : 0.3," +
        // do nothing but generate an op
        "'belowNodeOp' : 'none'," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'started'," +
        "'trigger' : 'search_rate_trigger2'," +
        "'stage' : ['STARTED']," +
        "'class' : '" + StartedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'srt'," +
        "'trigger' : 'search_rate_trigger2'," +
        "'stage' : ['FAILED','SUCCEEDED']," +
        "'afterAction': ['compute', 'execute']," +
        "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'finished'," +
        "'trigger' : 'search_rate_trigger2'," +
        "'stage' : ['SUCCEEDED']," +
        "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    // enable the trigger
    String resumeTriggerCommand = "{" +
        "'resume-trigger' : {" +
        "'name' : 'search_rate_trigger2'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    boolean await = started.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    await = finished.await(60, TimeUnit.SECONDS);
    assertTrue("The trigger did not finish processing", await);

    // suspend the trigger
    String suspendTriggerCommand = "{" +
        "'suspend-trigger' : {" +
        "'name' : 'search_rate_trigger2'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    List<CapturedEvent> events = listenerEvents.get("srt");
    assertEquals(events.toString(), 3, events.size());
    CapturedEvent ev = events.get(0);
    assertEquals(ev.toString(), "compute", ev.actionName);
    List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>)ev.event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("there should be some requestedOps: " + ev.toString(), ops);
    // 4 cold nodes, 3 cold replicas
    assertEquals(ops.toString(), 7, ops.size());
    AtomicInteger coldNodes = new AtomicInteger();
    AtomicInteger coldReplicas = new AtomicInteger();
    ops.forEach(op -> {
      if (op.getAction().equals(CollectionParams.CollectionAction.NONE)) {
        coldNodes.incrementAndGet();
      } else if (op.getAction().equals(CollectionParams.CollectionAction.DELETEREPLICA)) {
        coldReplicas.incrementAndGet();
      } else {
        fail("unexpected op: " + op);
      }
    });
    assertEquals("cold nodes", 4, coldNodes.get());
    assertEquals("cold replicas", 3, coldReplicas.get());

    // now the collection should be down to RF = 2
    CloudTestUtils.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(1, 2));

    listenerEvents.clear();
    finished = new CountDownLatch(1);
    started = new CountDownLatch(1);

    // resume trigger
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // there should be only coldNode ops now, and no coldReplica ops since searchable RF == collection RF
    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    await = started.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    await = finished.await(60, TimeUnit.SECONDS);
    assertTrue("The trigger did not finish processing", await);

    // suspend trigger
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(5000);

    events = listenerEvents.get("srt");
    assertEquals(events.toString(), 3, events.size());

    ev = events.get(0);
    assertEquals(ev.toString(), "compute", ev.actionName);
    ops = (List<TriggerEvent.Op>)ev.event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("there should be some requestedOps: " + ev.toString(), ops);
    assertEquals(ops.toString(), 2, ops.size());
    assertEquals(ops.toString(), CollectionParams.CollectionAction.NONE, ops.get(0).getAction());
    assertEquals(ops.toString(), CollectionParams.CollectionAction.NONE, ops.get(1).getAction());

    // wait for waitFor to elapse for all types of violations
    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds * 2, TimeUnit.SECONDS));

    listenerEvents.clear();
    finished = new CountDownLatch(1);
    started = new CountDownLatch(1);

    log.info("## test single replicas.");

    // now allow single replicas
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'search_rate_trigger2'," +
        "'event' : 'searchRate'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'collections' : '" + COLL1 + "'," +
        "'aboveRate' : 1.0," +
        "'belowRate' : 0.3," +
        "'aboveNodeRate' : 1.0," +
        "'belowNodeRate' : 0.3," +
        "'minReplicas' : 1," +
        "'belowNodeOp' : 'none'," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}" +
        "]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    await = started.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    await = finished.await(60, TimeUnit.SECONDS);
    assertTrue("The trigger did not finish processing", await);

    // suspend trigger
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(5000);

    events = listenerEvents.get("srt");
    assertEquals(events.toString(), 3, events.size());

    ev = events.get(0);
    assertEquals(ev.toString(), "compute", ev.actionName);
    ops = (List<TriggerEvent.Op>)ev.event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("there should be some requestedOps: " + ev.toString(), ops);

    assertTrue(ops.toString(), ops.size() > 0);
    AtomicInteger coldNodes2 = new AtomicInteger();
    ops.forEach(op -> {
      if (op.getAction().equals(CollectionParams.CollectionAction.NONE)) {
        coldNodes2.incrementAndGet();
      } else if (op.getAction().equals(CollectionParams.CollectionAction.DELETEREPLICA)) {
        // ignore
      } else {
        fail("unexpected op: " + op);
      }
    });

    assertEquals("coldNodes: " +ops.toString(), 2, coldNodes2.get());

    // now the collection should be at RF == 1, with one additional PULL replica
    CloudTestUtils.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(1, 1));
  }

  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 21-May-2018
  public void testDeleteNode() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String COLL1 = "deleteNode_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 1, 2);

    create.process(solrClient);
    CloudTestUtils.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(1, 2));

    // add a couple of spare replicas above RF. Use different types to verify that only
    // searchable replicas are considered
    // these additional replicas will be placed on other nodes in the cluster
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.NRT));
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.TLOG));
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.PULL));

    CloudTestUtils.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(1, 5));

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'search_rate_trigger3'," +
        "'event' : 'searchRate'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : false," +
        "'collections' : '" + COLL1 + "'," +
        "'aboveRate' : 1.0," +
        "'belowRate' : 0.1," +
        // set limits to node rates
        "'aboveNodeRate' : 1.0," +
        "'belowNodeRate' : 0.1," +
        // allow deleting all spare replicas
        "'minReplicas' : 1," +
        // allow requesting all deletions in one event
        "'maxOps' : 10," +
        // delete underutilised nodes
        "'belowNodeOp' : 'DELETENODE'," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'started'," +
        "'trigger' : 'search_rate_trigger3'," +
        "'stage' : ['STARTED']," +
        "'class' : '" + StartedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'srt'," +
        "'trigger' : 'search_rate_trigger3'," +
        "'stage' : ['FAILED','SUCCEEDED']," +
        "'afterAction': ['compute', 'execute']," +
        "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'finished'," +
        "'trigger' : 'search_rate_trigger3'," +
        "'stage' : ['SUCCEEDED']," +
        "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    // enable the trigger
    String resumeTriggerCommand = "{" +
        "'resume-trigger' : {" +
        "'name' : 'search_rate_trigger3'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    boolean await = started.await(20, TimeUnit.SECONDS);
    assertTrue("The trigger did not fire at all", await);
    await = finished.await(90, TimeUnit.SECONDS);
    assertTrue("The trigger did not finish processing", await);

    // suspend the trigger
    String suspendTriggerCommand = "{" +
        "'suspend-trigger' : {" +
        "'name' : 'search_rate_trigger3'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(5000);

    List<CapturedEvent> events = listenerEvents.get("srt");
    assertEquals(events.toString(), 3, events.size());

    CapturedEvent ev = events.get(0);
    assertEquals(ev.toString(), "compute", ev.actionName);
    List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>)ev.event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("there should be some requestedOps: " + ev.toString(), ops);
    // 4 DELETEREPLICA, 4 DELETENODE
    assertEquals(ops.toString(), 8, ops.size());
    AtomicInteger replicas = new AtomicInteger();
    AtomicInteger nodes = new AtomicInteger();
    ops.forEach(op -> {
      if (op.getAction().equals(CollectionParams.CollectionAction.DELETEREPLICA)) {
        replicas.incrementAndGet();
      } else if (op.getAction().equals(CollectionParams.CollectionAction.DELETENODE)) {
        nodes.incrementAndGet();
      } else {
        fail("unexpected op: " + op);
      }
    });
    assertEquals(ops.toString(), 4, replicas.get());
    assertEquals(ops.toString(), 4, nodes.get());
    // check status
    ev = events.get(1);
    assertEquals(ev.toString(), "execute", ev.actionName);
    List<NamedList<Object>> responses = (List<NamedList<Object>>)ev.context.get("properties.responses");
    assertNotNull(ev.toString(), responses);
    assertEquals(responses.toString(), 8, responses.size());
    replicas.set(0);
    nodes.set(0);
    responses.forEach(m -> {
      if (m.get("success") != null) {
        replicas.incrementAndGet();
      } else if (m.get("status") != null) {
        NamedList<Object> status = (NamedList<Object>)m.get("status");
        if ("completed".equals(status.get("state"))) {
          nodes.incrementAndGet();
        } else {
          fail("unexpected DELETENODE status: " + m);
        }
      } else {
        fail("unexpected status: " + m);
      }
    });

    assertEquals(responses.toString(), 4, replicas.get());
    assertEquals(responses.toString(), 4, nodes.get());

    // we are left with one searchable replica
    CloudTestUtils.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(1, 1));
  }

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

  public static class StartedProcessingListener extends TriggerListenerBase {

    @Override
    public void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName, ActionContext context, Throwable error, String message) throws Exception {
      started.countDown();
    }
  }

  public static class FinishedProcessingListener extends TriggerListenerBase {

    @Override
    public void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName, ActionContext context, Throwable error, String message) throws Exception {
      finished.countDown();
    }
  }

}
