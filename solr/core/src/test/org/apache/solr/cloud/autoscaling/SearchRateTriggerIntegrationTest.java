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

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.TriggerIntegrationTest.WAIT_FOR_DELTA_NANOS;
import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 * Integration test for {@link SearchRateTrigger}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling=DEBUG")
@LuceneTestCase.Slow
@Nightly // this test is too long for non nightly right now
public class SearchRateTriggerIntegrationTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final TimeSource timeSource = TimeSource.NANO_TIME;
  private static volatile CountDownLatch listenerCreated = new CountDownLatch(1);
  private static volatile CountDownLatch listenerEventLatch = new CountDownLatch(0);
  private static volatile Map<String, List<CapturedEvent>> listenerEvents = new HashMap<>();
  private static volatile CountDownLatch finished = new CountDownLatch(1);
  private static volatile CountDownLatch started = new CountDownLatch(1);
  private static SolrCloudManager cloudManager;

  private int waitForSeconds;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(5)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    
    cloudManager = cluster.getOpenOverseer().getSolrCloudManager();
    
    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cloudManager, ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cloudManager, ".scheduled_maintenance");

  }

  @AfterClass
  public static void cleanUpAfterClass() throws Exception {
    cloudManager = null;
  }

  @Before
  public void beforeTest() throws Exception {
    cluster.deleteAllCollections();
    // clear any persisted auto scaling configuration
    Stat stat = zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);
    if (log.isInfoEnabled()) {
      log.info("{} reset, new znode version {}", SOLR_AUTOSCALING_CONF_PATH, stat.getVersion());
    }
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
    deleteChildrenRecursively(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);
    
    finished = new CountDownLatch(1);
    started = new CountDownLatch(1);
    listenerCreated = new CountDownLatch(1);
    listenerEvents = new HashMap<>();
    listenerEventLatch = new CountDownLatch(0);
    
    waitForSeconds = 3 + random().nextInt(5);
  }

  private void deleteChildrenRecursively(String path) throws Exception {
    cloudManager.getDistribStateManager().removeRecursively(path, true, false);
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testAboveSearchRate() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String COLL1 = "aboveRate_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 1, 2);
    create.process(solrClient);

    CloudUtil.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 2));

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       // the trigger is initially disabled so that we have the time to set up listeners
       // and generate the traffic
       "{" +
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
       "}}");

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'started'," +
       "'trigger' : 'search_rate_trigger1'," +
       "'stage' : ['STARTED']," +
       "'class' : '" + StartedProcessingListener.class.getName() + "'" +
       "}" +
       "}");

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'srt'," +
       "'trigger' : 'search_rate_trigger1'," +
       "'stage' : ['FAILED','SUCCEEDED']," +
       "'afterAction': ['compute', 'execute']," +
       "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
       "}" +
       "}");
    listenerEventLatch = new CountDownLatch(3);

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'finished'," +
       "'trigger' : 'search_rate_trigger1'," +
       "'stage' : ['SUCCEEDED']," +
       "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
       "}" +
       "}");

    SolrParams query = params(CommonParams.Q, "*:*");
    for (int i = 0; i < 500; i++) {
      solrClient.query(COLL1, query);
    }
    
    // enable the trigger
    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'resume-trigger' : {" +
       "'name' : 'search_rate_trigger1'" +
       "}" +
       "}");

    assertTrue("The trigger did not start in a reasonable amount of time",
               started.await(60, TimeUnit.SECONDS));
    
    assertTrue("The trigger did not finish in a reasonable amount of time",
               started.await(60, TimeUnit.SECONDS));
    
    assertTrue("the listener should have recorded all events w/in a reasonable amount of time",
               listenerEventLatch.await(60, TimeUnit.SECONDS));

    // suspend the trigger
    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'suspend-trigger' : {" +
       "'name' : 'search_rate_trigger1'" +
       "}" +
       "}");

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
    List<MapWriter> ops = (List<MapWriter>) ev.context.get("properties.operations");
    assertNotNull(ops);
    assertTrue(ops.size() > 1);
    for (MapWriter m : ops) {
      assertEquals("ADDREPLICA", m._get("params.action",null));
    }
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testBelowSearchRate() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String COLL1 = "belowRate_collection";
    // replicationFactor == 2
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 1, 2);
    create.process(solrClient);
    CloudUtil.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 2));

    // add a couple of spare replicas above RF. Use different types.
    // these additional replicas will be placed on other nodes in the cluster
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.NRT));
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.TLOG));
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.PULL));

    CloudUtil.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 5));

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-trigger' : {" +
       "'name' : 'search_rate_trigger2'," +
       "'event' : 'searchRate'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : false," +
       "'collections' : '" + COLL1 + "'," +
       "'aboveRate' : 1.0," +
       "'aboveNodeRate' : 1.0," +
       // RecoveryStrategy calls /admin/ping, which calls /select so the rate may not be zero
       // even when no external requests were made .. but it's hard to predict exactly
       // what it will be.  use an insanely high rate so all shards/nodes are suspect
       // and produce an Op regardless of how much internal traffic is produced...
       "'belowRate' : 1.0," +
       "'belowNodeRate' : 1.0," +
       // ...but do absolutely nothing to nodes except generate an 'NONE' Op
       "'belowNodeOp' : 'none'," +
       "'actions' : [" +
       "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
       "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}" +
       "]" +
       "}}");

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'started'," +
       "'trigger' : 'search_rate_trigger2'," +
       "'stage' : ['STARTED']," +
       "'class' : '" + StartedProcessingListener.class.getName() + "'" +
       "}" +
       "}");

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'srt'," +
       "'trigger' : 'search_rate_trigger2'," +
       "'stage' : ['FAILED','SUCCEEDED']," +
       "'afterAction': ['compute', 'execute']," +
       "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
       "}" +
       "}");
    listenerEventLatch = new CountDownLatch(3);
    
    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'finished'," +
       "'trigger' : 'search_rate_trigger2'," +
       "'stage' : ['SUCCEEDED']," +
       "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
       "}" +
       "}");

    // Explicitly Do Nothing Here

    // enable the trigger
    final String resumeTriggerCommand = "{ 'resume-trigger' : { 'name' : 'search_rate_trigger2' } }";
    CloudTestUtils.assertAutoScalingRequest(cloudManager, resumeTriggerCommand);

    assertTrue("The trigger did not start in a reasonable amount of time",
               started.await(60, TimeUnit.SECONDS));
    
    assertTrue("The trigger did not finish in a reasonable amount of time",
               started.await(60, TimeUnit.SECONDS));
    
    assertTrue("the listener should have recorded all events w/in a reasonable amount of time",
               listenerEventLatch.await(60, TimeUnit.SECONDS));

    // suspend the trigger
    final String suspendTriggerCommand = "{ 'suspend-trigger' : { 'name' : 'search_rate_trigger2' } }";
    CloudTestUtils.assertAutoScalingRequest(cloudManager, suspendTriggerCommand);

    List<CapturedEvent> events = listenerEvents.get("srt");
    assertEquals(events.toString(), 3, events.size());
    CapturedEvent ev = events.get(0);
    assertEquals(ev.toString(), "compute", ev.actionName);
    List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>)ev.event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("there should be some requestedOps: " + ev.toString(), ops);
    // 5 cold nodes, 3 cold replicas
    assertEquals(ops.toString(), 5 + 3, ops.size());
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
    assertEquals("cold nodes", 5, coldNodes.get());
    assertEquals("cold replicas", 3, coldReplicas.get());

    // now the collection should be down to RF = 2
    CloudUtil.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 2));

    listenerEvents.clear();
    listenerEventLatch = new CountDownLatch(3);
    finished = new CountDownLatch(1);
    started = new CountDownLatch(1);

    // resume trigger
    CloudTestUtils.assertAutoScalingRequest(cloudManager, resumeTriggerCommand);

    assertTrue("The trigger did not start in a reasonable amount of time",
               started.await(60, TimeUnit.SECONDS));
    
    assertTrue("The trigger did not finish in a reasonable amount of time",
               started.await(60, TimeUnit.SECONDS));
    
    assertTrue("the listener should have recorded all events w/in a reasonable amount of time",
               listenerEventLatch.await(60, TimeUnit.SECONDS));

    // suspend the trigger
    CloudTestUtils.assertAutoScalingRequest(cloudManager, suspendTriggerCommand);

    // there should be only coldNode ops now, and no coldReplica ops since searchable RF == collection RF

    events = listenerEvents.get("srt");
    assertEquals(events.toString(), 3, events.size());

    ev = events.get(0);
    assertEquals(ev.toString(), "compute", ev.actionName);
    ops = (List<TriggerEvent.Op>)ev.event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("there should be some requestedOps: " + ev.toString(), ops);
    assertEquals(ops.toString(), 2, ops.size());
    assertEquals(ops.toString(), CollectionParams.CollectionAction.NONE, ops.get(0).getAction());
    assertEquals(ops.toString(), CollectionParams.CollectionAction.NONE, ops.get(1).getAction());


    listenerEvents.clear();
    listenerEventLatch = new CountDownLatch(3);
    finished = new CountDownLatch(1);
    started = new CountDownLatch(1);

    log.info("## test single replicas.");

    // now allow single replicas
    CloudTestUtils.assertAutoScalingRequest
      (cloudManager,
       "{" +
       "'set-trigger' : {" +
       "'name' : 'search_rate_trigger2'," +
       "'event' : 'searchRate'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : true," +
       "'collections' : '" + COLL1 + "'," +
       "'aboveRate' : 1.0," +
       "'aboveNodeRate' : 1.0," +
       "'belowRate' : 1.0," + // same excessively high values
       "'belowNodeRate' : 1.0," +
       "'minReplicas' : 1," + // NEW: force lower replicas
       "'belowNodeOp' : 'none'," + // still do nothing to nodes
       "'actions' : [" +
       "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
       "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}" +
       "]" +
       "}}");

    assertTrue("The trigger did not start in a reasonable amount of time",
               started.await(60, TimeUnit.SECONDS));
    
    assertTrue("The trigger did not finish in a reasonable amount of time",
               started.await(60, TimeUnit.SECONDS));
    
    assertTrue("the listener should have recorded all events w/in a reasonable amount of time",
               listenerEventLatch.await(60, TimeUnit.SECONDS));

    // suspend the trigger
    CloudTestUtils.assertAutoScalingRequest(cloudManager, suspendTriggerCommand);

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
    CloudUtil.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 1));
  }

  @Test
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13163") 
  public void testDeleteNode() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String COLL1 = "deleteNode_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(COLL1,
        "conf", 1, 2);

    create.process(solrClient);
    CloudUtil.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 2));

    // add a couple of spare replicas above RF. Use different types to verify that only
    // searchable replicas are considered
    // these additional replicas will be placed on other nodes in the cluster
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.NRT));
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.TLOG));
    solrClient.request(CollectionAdminRequest.addReplicaToShard(COLL1, "shard1", Replica.Type.PULL));

    CloudUtil.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 5));

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-trigger' : {" +
       "'name' : 'search_rate_trigger3'," +
       "'event' : 'searchRate'," +
       "'waitFor' : '" + waitForSeconds + "s'," +
       "'enabled' : false," +
       "'collections' : '" + COLL1 + "'," +
       "'aboveRate' : 1.0," +
       "'aboveNodeRate' : 1.0," +
       // RecoveryStrategy calls /admin/ping, which calls /select so the rate may not be zero
       // even when no external requests were made .. but it's hard to predict exactly
       // what it will be.  use an insanely high rate so all shards/nodes are suspect
       // and produce an Op regardless of how much internal traffic is produced...
       "'belowRate' : 1.0," +
       "'belowNodeRate' : 1.0," +
       // ...our Ops should be to delete underutilised nodes...
       "'belowNodeOp' : 'DELETENODE'," +
       // ...allow deleting all spare replicas...
       "'minReplicas' : 1," +
       // ...and allow requesting all deletions in one event.
       "'maxOps' : 10," +
       "'actions' : [" +
       "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
       "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}" +
       "]" +
       "}}");

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'started'," +
       "'trigger' : 'search_rate_trigger3'," +
       "'stage' : ['STARTED']," +
       "'class' : '" + StartedProcessingListener.class.getName() + "'" +
       "}" +
       "}");

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'srt'," +
       "'trigger' : 'search_rate_trigger3'," +
       "'stage' : ['FAILED','SUCCEEDED']," +
       "'afterAction': ['compute', 'execute']," +
       "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
       "}" +
       "}");
    listenerEventLatch = new CountDownLatch(3);

    CloudTestUtils.assertAutoScalingRequest
      (cloudManager, 
       "{" +
       "'set-listener' : " +
       "{" +
       "'name' : 'finished'," +
       "'trigger' : 'search_rate_trigger3'," +
       "'stage' : ['SUCCEEDED']," +
       "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
       "}" +
       "}");
    
    // Explicitly Do Nothing Here
    
    // enable the trigger
    CloudTestUtils.assertAutoScalingRequest
      (cloudManager,
       "{" +
       "'resume-trigger' : {" +
       "'name' : 'search_rate_trigger3'" +
       "}" +
       "}");

    assertTrue("The trigger did not start in a reasonable amount of time",
               started.await(60, TimeUnit.SECONDS));
    
    assertTrue("The trigger did not finish in a reasonable amount of time",
               started.await(60, TimeUnit.SECONDS));
    
    assertTrue("the listener should have recorded all events w/in a reasonable amount of time",
               listenerEventLatch.await(60, TimeUnit.SECONDS));
    
    // suspend the trigger
    CloudTestUtils.assertAutoScalingRequest
      (cloudManager,
       "{" +
       "'suspend-trigger' : {" +
       "'name' : 'search_rate_trigger3'" +
       "}" +
       "}");

    List<CapturedEvent> events = listenerEvents.get("srt");
    assertEquals(events.toString(), 3, events.size());

    CapturedEvent ev = events.get(0);
    assertEquals(ev.toString(), "compute", ev.actionName);
    @SuppressWarnings({"unchecked"})
    List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>)ev.event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("there should be some requestedOps: " + ev.toString(), ops);
    // 4 DELETEREPLICA, 4 DELETENODE (minReplicas==1 & leader should be protected)
    assertEquals(ops.toString(), 4 + 4, ops.size());
    // The above assert can fail with actual==9 because all 5 nodes are resulting in a DELETENODE
    // Which is problemtatic for 2 reasons:
    //  1) it means that the leader node has not been protected from the 'belowNodeOp':'DELETENODE'
    //     - definitely a bug that needs fixed
    //  2) it suggests that minReplicas isn't being respected by 'belowNodeOp':'DELETENODE'
    //     - something that needs more rigerous testing
    //     - ie: if belowRate==0 && belowNodeRate==1 && minReplicas==2, will leader + 1 be protected?
    //
    // In general, to adequately trust testing of 'belowNodeOp':'DELETENODE' we should also test:
    //  - some nodes with multiple replicas of the shard to ensure best nodes are picked
    //  - node nodes hosting replicas of multiple shards/collection, only some of which are belowNodeRate



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
    @SuppressWarnings({"unchecked"})
    List<NamedList<Object>> responses = (List<NamedList<Object>>)ev.context.get("properties.responses");
    assertNotNull(ev.toString(), responses);
    assertEquals(responses.toString(), 8, responses.size());
    replicas.set(0);
    nodes.set(0);
    responses.forEach(m -> {
      if (m.get("success") != null) {
        replicas.incrementAndGet();
      } else if (m.get("status") != null) {
        Object status = m.get("status");
        String state;
        if (status instanceof Map) {
          state = (String)((Map)status).get("state");
        } else if (status instanceof NamedList) {
          state = (String)((NamedList)status).get("state");
        } else {
          throw new IllegalArgumentException("unsupported status format: " + status.getClass().getName() + ", " + status);
        }
        if ("completed".equals(state)) {
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
    CloudUtil.waitForState(cloudManager, COLL1, 60, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 1));
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
      CapturedEvent ev = new CapturedEvent(timeSource.getTimeNs(), context, config, stage, actionName, event, message);
      final CountDownLatch latch = listenerEventLatch;
      synchronized (latch) {
        if (0 == latch.getCount()) {
          log.warn("Ignoring captured event since latch is 'full': {}", ev);
        } else {
          List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
          log.info("=======> {}", ev);
          lst.add(ev);
          latch.countDown();
        }
      }
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
