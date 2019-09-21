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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.autoscaling.sim.SimCloudManager;
import org.apache.solr.cloud.autoscaling.sim.SimUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.update.SolrIndexSplitter;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
@LuceneTestCase.Slow
public class IndexSizeTriggerTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static SolrCloudManager cloudManager;
  private static SolrClient solrClient;
  private static TimeSource timeSource;
  private static SolrResourceLoader loader;

  private static int SPEED;

  private AutoScaling.TriggerEventProcessor noFirstRunProcessor = event -> {
    fail("Did not expect the processor to fire on first run! event=" + event);
    return true;
  };
  private static final long WAIT_FOR_DELTA_NANOS = TimeUnit.MILLISECONDS.toNanos(2);

  static Map<String, List<CapturedEvent>> listenerEvents = new ConcurrentHashMap<>();
  static CountDownLatch listenerCreated = new CountDownLatch(1);
  static CountDownLatch finished = new CountDownLatch(1);
  static boolean realCluster;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    realCluster = random().nextBoolean();
    realCluster = true;
    if (realCluster) {
      cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
      solrClient = cluster.getSolrClient();
      loader = cluster.getJettySolrRunner(0).getCoreContainer().getResourceLoader();
      SPEED = 1;
    } else {
      SPEED = 50;
      cloudManager = SimCloudManager.createCluster(2, TimeSource.get("simTime:" + SPEED));
      // wait for defaults to be applied - due to accelerated time sometimes we may miss this
      cloudManager.getTimeSource().sleep(10000);
      AutoScalingConfig cfg = cloudManager.getDistribStateManager().getAutoScalingConfig();
      assertFalse("autoscaling config is empty", cfg.isEmpty());
      solrClient = ((SimCloudManager)cloudManager).simGetSolrClient();
      loader = ((SimCloudManager) cloudManager).getLoader();
    }
    timeSource = cloudManager.getTimeSource();
  }

  @After
  public void restoreDefaults() throws Exception {
    if (!realCluster) {
      log.info(((SimCloudManager) cloudManager).dumpClusterState(true));
      ((SimCloudManager) cloudManager).getSimClusterStateProvider().simDeleteAllCollections();
      ((SimCloudManager) cloudManager).simClearSystemCollection();
      ((SimCloudManager) cloudManager).getSimClusterStateProvider().simResetLeaderThrottles();
      ((SimCloudManager) cloudManager).simRestartOverseer(null);
      cloudManager.getTimeSource().sleep(500);
      ((SimCloudManager) cloudManager).simResetOpCounts();
    } else {
      cluster.deleteAllCollections();
    }
    cloudManager.getDistribStateManager().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), -1);
    cloudManager.getTimeSource().sleep(5000);
    listenerEvents.clear();
    listenerCreated = new CountDownLatch(1);
    finished = new CountDownLatch(1);
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (cloudManager != null && !realCluster) {
      cloudManager.close();
    }
    solrClient = null;
    cloudManager = null;
  }

  @Test
  public void testTrigger() throws Exception {
    String collectionName = "testTrigger_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 2).setMaxShardsPerNode(2);
    create.process(solrClient);
    
    if (SPEED == 1) {
      cluster.waitForActiveCollection(collectionName, 2, 4);
    } else {
      CloudUtil.waitForState(cloudManager, "failed to create " + collectionName, collectionName,
          CloudUtil.clusterShape(2, 2, false, true));
    }

    long waitForSeconds = 3 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);
    try (IndexSizeTrigger trigger = new IndexSizeTrigger("index_size_trigger1")) {
      trigger.configure(loader, cloudManager, props);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();

      for (int i = 0; i < 25; i++) {
        SolrInputDocument doc = new SolrInputDocument("id", "id-" + i);
        solrClient.add(collectionName, doc);
      }
      solrClient.commit(collectionName);

      AtomicBoolean fired = new AtomicBoolean(false);
      AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();
      trigger.setProcessor(event -> {
        if (fired.compareAndSet(false, true)) {
          eventRef.set(event);
          long currentTimeNanos = timeSource.getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail("processor was fired before the configured waitFor period: currentTimeNanos=" + currentTimeNanos + ", eventTimeNanos=" +  eventTimeNanos + ",waitForNanos=" + waitForNanos);
          }
        } else {
          fail("IndexSizeTrigger was fired more than once!");
        }
        return true;
      });
      trigger.run();
      TriggerEvent ev = eventRef.get();
      // waitFor delay - should not produce any event yet
      assertNull("waitFor not elapsed but produced an event", ev);
      timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));
      trigger.run();
      ev = eventRef.get();
      assertNotNull("should have fired an event", ev);
      List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>) ev.getProperty(TriggerEvent.REQUESTED_OPS);
      assertNotNull("should contain requestedOps", ops);
      assertEquals("number of ops: " + ops, 2, ops.size());
      boolean shard1 = false;
      boolean shard2 = false;
      for (TriggerEvent.Op op : ops) {
        assertEquals(CollectionParams.CollectionAction.SPLITSHARD, op.getAction());
        Set<Pair<String, String>> hints = (Set<Pair<String, String>>)op.getHints().get(Suggester.Hint.COLL_SHARD);
        assertNotNull("hints", hints);
        assertEquals("hints", 1, hints.size());
        Pair<String, String> p = hints.iterator().next();
        assertEquals(collectionName, p.first());
        if (p.second().equals("shard1")) {
          shard1 = true;
        } else if (p.second().equals("shard2")) {
          shard2 = true;
        } else {
          fail("unexpected shard name " + p.second());
        }
        Map<String, Object> params = (Map<String, Object>)op.getHints().get(Suggester.Hint.PARAMS);
        assertNotNull("params are null: " + op, params);
        
        // verify default split configs
        assertEquals("splitMethod: " + op, SolrIndexSplitter.SplitMethod.LINK.toLower(),
            params.get(CommonAdminParams.SPLIT_METHOD));
        assertEquals("splitByPrefix: " + op, false, params.get(CommonAdminParams.SPLIT_BY_PREFIX));
      }
      assertTrue("shard1 should be split", shard1);
      assertTrue("shard2 should be split", shard2);
    }
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

  public static class FinishedProcessingListener extends TriggerListenerBase {

    @Override
    public void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName, ActionContext context, Throwable error, String message) throws Exception {
      finished.countDown();
    }
  }

  @Test
  public void testSplitIntegration() throws Exception {
    String collectionName = "testSplitIntegration_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 2).setMaxShardsPerNode(2);
    create.process(solrClient);
    
    if (SPEED == 1) {
      cluster.waitForActiveCollection(collectionName, 2, 4);
    } else {
      CloudUtil.waitForState(cloudManager, "failed to create " + collectionName, collectionName,
          CloudUtil.clusterShape(2, 2, false, true));
    }

    long waitForSeconds = 6 + random().nextInt(5);
    // add disabled trigger
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'index_size_trigger2'," +
        "'event' : 'indexSize'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'aboveDocs' : 10," +
        "'belowDocs' : 4," +
        "'enabled' : false," +
        "'actions' : [{'name' : 'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name' : 'execute_plan', 'class' : '" + ExecutePlanAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'capturing2'," +
        "'trigger' : 'index_size_trigger2'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED','FAILED']," +
        "'beforeAction' : ['compute_plan','execute_plan']," +
        "'afterAction' : ['compute_plan','execute_plan']," +
        "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'finished'," +
        "'trigger' : 'index_size_trigger2'," +
        "'stage' : ['SUCCEEDED']," +
        "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");


    for (int i = 0; i < 50; i++) {
      SolrInputDocument doc = new SolrInputDocument("id", "id-" + i);
      solrClient.add(collectionName, doc);
    }
    solrClient.commit(collectionName);

    // enable the trigger
    String resumeTriggerCommand = "{" +
        "'resume-trigger' : {" +
        "'name' : 'index_size_trigger2'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    boolean await = finished.await(60000, TimeUnit.MILLISECONDS);
    assertTrue("did not finish processing in time", await);
    CloudUtil.waitForState(cloudManager, collectionName, 20, TimeUnit.SECONDS, CloudUtil.clusterShape(6, 2, true, true));
    assertEquals(1, listenerEvents.size());
    List<CapturedEvent> events = listenerEvents.get("capturing2");
    assertNotNull("'capturing2' events not found", events);
    assertEquals("events: " + events, 6, events.size());
    assertEquals(TriggerEventProcessorStage.STARTED, events.get(0).stage);
    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, events.get(1).stage);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, events.get(2).stage);
    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, events.get(3).stage);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, events.get(4).stage);
    assertEquals(TriggerEventProcessorStage.SUCCEEDED, events.get(5).stage);
    // check ops
    List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>) events.get(4).event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("should contain requestedOps", ops);
    assertEquals("number of ops", 2, ops.size());
    boolean shard1 = false;
    boolean shard2 = false;
    for (TriggerEvent.Op op : ops) {
      assertEquals(CollectionParams.CollectionAction.SPLITSHARD, op.getAction());
      Set<Pair<String, String>> hints = (Set<Pair<String, String>>)op.getHints().get(Suggester.Hint.COLL_SHARD);
      assertNotNull("hints", hints);
      assertEquals("hints", 1, hints.size());
      Pair<String, String> p = hints.iterator().next();
      assertEquals(collectionName, p.first());
      if (p.second().equals("shard1")) {
        shard1 = true;
      } else if (p.second().equals("shard2")) {
        shard2 = true;
      } else {
        fail("unexpected shard name " + p.second());
      }
    }

    
    if (events.size() == 6) {
      assertTrue("shard1 should be split", shard1);
      assertTrue("shard2 should be split", shard2);
    } else {
      assertTrue("shard1 or shard2 should be split", shard1 || shard2);
    }

  }

  @Test
  public void testMergeIntegration() throws Exception {
    String collectionName = "testMergeIntegration_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 2).setMaxShardsPerNode(2);
    create.process(solrClient);
    
    if (SPEED == 1) {
      cluster.waitForActiveCollection(collectionName, 2, 4);
    } else {
      CloudUtil.waitForState(cloudManager, "failed to create " + collectionName, collectionName,
          CloudUtil.clusterShape(2, 2, false, true));
    }

    for (int i = 0; i < 20; i++) {
      SolrInputDocument doc = new SolrInputDocument("id", "id-" + (i * 100));
      solrClient.add(collectionName, doc);
    }
    solrClient.commit(collectionName);

    long waitForSeconds = 3 + random().nextInt(5);
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'index_size_trigger3'," +
        "'event' : 'indexSize'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'aboveDocs' : 40," +
        "'belowDocs' : 4," +
        "'enabled' : false," +
        "'actions' : [{'name' : 'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name' : 'execute_plan', 'class' : '" + ExecutePlanAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'capturing3'," +
        "'trigger' : 'index_size_trigger3'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED','FAILED']," +
        "'beforeAction' : ['compute_plan','execute_plan']," +
        "'afterAction' : ['compute_plan','execute_plan']," +
        "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'finished'," +
        "'trigger' : 'index_size_trigger3'," +
        "'stage' : ['SUCCEEDED']," +
        "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // delete some docs to trigger a merge
    for (int i = 0; i < 15; i++) {
      solrClient.deleteById(collectionName, "id-" + (i * 100));
    }
    solrClient.commit(collectionName);

    // enable the trigger
    String resumeTriggerCommand = "{" +
        "'resume-trigger' : {" +
        "'name' : 'index_size_trigger3'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals("success", response.get("result").toString());

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    boolean await = finished.await(90000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("did not finish processing in time", await);
    assertEquals(1, listenerEvents.size());
    List<CapturedEvent> events = listenerEvents.get("capturing3");
    assertNotNull("'capturing3' events not found", events);
    assertEquals("events: " + events, 6, events.size());
    assertEquals(TriggerEventProcessorStage.STARTED, events.get(0).stage);
    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, events.get(1).stage);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, events.get(2).stage);
    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, events.get(3).stage);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, events.get(4).stage);
    assertEquals(TriggerEventProcessorStage.SUCCEEDED, events.get(5).stage);
    // check ops
    List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>) events.get(4).event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("should contain requestedOps", ops);
    assertTrue("number of ops: " + ops, ops.size() > 0);
    for (TriggerEvent.Op op : ops) {
      assertEquals(CollectionParams.CollectionAction.MERGESHARDS, op.getAction());
      Set<Pair<String, String>> hints = (Set<Pair<String, String>>)op.getHints().get(Suggester.Hint.COLL_SHARD);
      assertNotNull("hints", hints);
      assertEquals("hints", 2, hints.size());
      Pair<String, String> p = hints.iterator().next();
      assertEquals(collectionName, p.first());
    }

    // TODO: fix this once MERGESHARDS is supported
    List<TriggerEvent.Op> unsupportedOps = (List<TriggerEvent.Op>)events.get(2).context.get("properties.unsupportedOps");
    assertNotNull("should have unsupportedOps", unsupportedOps);
    assertEquals(unsupportedOps.toString() + "\n" + ops, ops.size(), unsupportedOps.size());
    unsupportedOps.forEach(op -> assertEquals(CollectionParams.CollectionAction.MERGESHARDS, op.getAction()));
  }

  @Test
  public void testMixedBounds() throws Exception {
    if (!realCluster) {
      log.info("This test doesn't work with a simulated cluster");
      return;
    }

    String collectionName = "testMixedBounds_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 2).setMaxShardsPerNode(2);
    create.process(solrClient);
    CloudUtil.waitForState(cloudManager, "failed to create " + collectionName, collectionName,
        CloudUtil.clusterShape(2, 2, false, true));

    for (int j = 0; j < 10; j++) {
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParam("collection", collectionName);
      for (int i = 0; i < 100; i++) {
        SolrInputDocument doc = new SolrInputDocument("id", "id-" + (i * 100) + "-" + j);
        doc.addField("foo", TestUtil.randomSimpleString(random(), 130, 130));
        ureq.add(doc);
      }
      solrClient.request(ureq);
    }
    solrClient.commit(collectionName);

    // check the actual size of shard to set the threshold
    QueryResponse rsp = solrClient.query(params(CommonParams.QT, "/admin/metrics", "group", "core"));
    NamedList<Object> nl = rsp.getResponse();
    nl = (NamedList<Object>)nl.get("metrics");
    int maxSize = 0;
    for (Iterator<Map.Entry<String, Object>> it = nl.iterator(); it.hasNext(); ) {
      Map.Entry<String, Object> e = it.next();
      NamedList<Object> metrics = (NamedList<Object>)e.getValue();
      Object o = metrics.get("INDEX.sizeInBytes");
      assertNotNull("INDEX.sizeInBytes missing: " + metrics, o);
      assertTrue("not a number", o instanceof Number);
      if (maxSize < ((Number)o).intValue()) {
        maxSize = ((Number)o).intValue();
      }
    }
    assertTrue("maxSize should be non-zero", maxSize > 0);

    int aboveBytes = maxSize * 2 / 3;

    // need to wait for recovery after splitting
    long waitForSeconds = 10 + random().nextInt(5);

    // the trigger is initially disabled so that we have time to add listeners
    // and have them capture all events once the trigger is enabled
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'index_size_trigger4'," +
        "'event' : 'indexSize'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        // don't hit this limit when indexing
        "'aboveDocs' : 10000," +
        // hit this limit when deleting
        "'belowDocs' : 100," +
        // hit this limit when indexing
        "'aboveBytes' : " + aboveBytes + "," +
        // don't hit this limit when deleting
        "'belowBytes' : 10," +
        "'enabled' : false," +
        "'actions' : [{'name' : 'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name' : 'execute_plan', 'class' : '" + ExecutePlanAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'capturing4'," +
        "'trigger' : 'index_size_trigger4'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED','FAILED']," +
        "'beforeAction' : ['compute_plan','execute_plan']," +
        "'afterAction' : ['compute_plan','execute_plan']," +
        "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'finished'," +
        "'trigger' : 'index_size_trigger4'," +
        "'stage' : ['SUCCEEDED']," +
        "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // now enable the trigger
    String resumeTriggerCommand = "{" +
        "'resume-trigger' : {" +
        "'name' : 'index_size_trigger4'" +
        "}" +
        "}";
    log.info("-- resuming trigger");
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    boolean await = finished.await(90000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("did not finish processing in time", await);
    log.info("-- suspending trigger");
    // suspend the trigger to avoid generating more events
    String suspendTriggerCommand = "{" +
        "'suspend-trigger' : {" +
        "'name' : 'index_size_trigger4'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertEquals(1, listenerEvents.size());
    List<CapturedEvent> events = listenerEvents.get("capturing4");
    assertNotNull("'capturing4' events not found", events);
    assertEquals("events: " + events, 6, events.size());
    assertEquals(TriggerEventProcessorStage.STARTED, events.get(0).stage);
    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, events.get(1).stage);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, events.get(2).stage);
    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, events.get(3).stage);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, events.get(4).stage);
    assertEquals(TriggerEventProcessorStage.SUCCEEDED, events.get(5).stage);

    // collection should have 2 inactive and 4 active shards
    CloudUtil.waitForState(cloudManager, "failed to create " + collectionName, collectionName,
        CloudUtil.clusterShape(6, 2, true, true));

    // check ops
    List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>) events.get(4).event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("should contain requestedOps", ops);
    assertEquals("number of ops", 2, ops.size());
    boolean shard1 = false;
    boolean shard2 = false;
    for (TriggerEvent.Op op : ops) {
      assertEquals(CollectionParams.CollectionAction.SPLITSHARD, op.getAction());
      Set<Pair<String, String>> hints = (Set<Pair<String, String>>)op.getHints().get(Suggester.Hint.COLL_SHARD);
      assertNotNull("hints", hints);
      assertEquals("hints", 1, hints.size());
      Pair<String, String> p = hints.iterator().next();
      assertEquals(collectionName, p.first());
      if (p.second().equals("shard1")) {
        shard1 = true;
      } else if (p.second().equals("shard2")) {
        shard2 = true;
      } else {
        fail("unexpected shard name " + p.second());
      }
    }
    assertTrue("shard1 should be split", shard1);
    assertTrue("shard2 should be split", shard2);

    // now delete most of docs to trigger belowDocs condition
    listenerEvents.clear();
    finished = new CountDownLatch(1);

    // suspend the trigger first so that we can safely delete all docs
    suspendTriggerCommand = "{" +
        "'suspend-trigger' : {" +
        "'name' : 'index_size_trigger4'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    log.info("-- deleting documents");
    for (int j = 0; j < 10; j++) {
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParam("collection", collectionName);
      for (int i = 0; i < 98; i++) {
        ureq.deleteById("id-" + (i * 100) + "-" + j);
      }
      solrClient.request(ureq);
    }
    cloudManager.getTimeSource().sleep(5000);
    // make sure the actual index size is reduced by deletions, otherwise we may still violate aboveBytes
    UpdateRequest ur = new UpdateRequest();
    ur.setParam(UpdateParams.COMMIT, "true");
    ur.setParam(UpdateParams.EXPUNGE_DELETES, "true");
    ur.setParam(UpdateParams.OPTIMIZE, "true");
    ur.setParam(UpdateParams.MAX_OPTIMIZE_SEGMENTS, "1");
    ur.setParam(UpdateParams.WAIT_SEARCHER, "true");
    ur.setParam(UpdateParams.OPEN_SEARCHER, "true");
    log.info("-- requesting optimize / expungeDeletes / commit");
    solrClient.request(ur, collectionName);

    // wait for the segments to merge to reduce the index size
    cloudManager.getTimeSource().sleep(50000);

    // add some docs so that every shard gets an update
    // we can reduce the number of docs here but this also works
    for (int j = 0; j < 1; j++) {
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParam("collection", collectionName);
      for (int i = 0; i < 98; i++) {
        ureq.add("id", "id-" + (i * 100) + "-" + j);
      }
      solrClient.request(ureq);
    }

    log.info("-- requesting commit");
    solrClient.commit(collectionName, true, true);

    // resume the trigger
    log.info("-- resuming trigger");
    // resume trigger
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    await = finished.await(90000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("did not finish processing in time", await);
    log.info("-- suspending trigger");
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertEquals(1, listenerEvents.size());
    events = listenerEvents.get("capturing4");
    assertNotNull("'capturing4' events not found", events);
    assertEquals("events: " + events, 6, events.size());
    assertEquals(TriggerEventProcessorStage.STARTED, events.get(0).stage);
    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, events.get(1).stage);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, events.get(2).stage);
    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, events.get(3).stage);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, events.get(4).stage);
    assertEquals(TriggerEventProcessorStage.SUCCEEDED, events.get(5).stage);

    // check ops
    ops = (List<TriggerEvent.Op>) events.get(4).event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("should contain requestedOps", ops);
    assertTrue("number of ops: " + ops, ops.size() > 0);
    for (TriggerEvent.Op op : ops) {
      assertEquals(CollectionParams.CollectionAction.MERGESHARDS, op.getAction());
      Set<Pair<String, String>> hints = (Set<Pair<String, String>>)op.getHints().get(Suggester.Hint.COLL_SHARD);
      assertNotNull("hints", hints);
      assertEquals("hints", 2, hints.size());
      Pair<String, String> p = hints.iterator().next();
      assertEquals(collectionName, p.first());
    }

    // TODO: fix this once MERGESHARDS is supported
    List<TriggerEvent.Op> unsupportedOps = (List<TriggerEvent.Op>)events.get(2).context.get("properties.unsupportedOps");
    assertNotNull("should have unsupportedOps", unsupportedOps);
    assertEquals(unsupportedOps.toString() + "\n" + ops, ops.size(), unsupportedOps.size());
    unsupportedOps.forEach(op -> assertEquals(CollectionParams.CollectionAction.MERGESHARDS, op.getAction()));
  }

  @Test
  public void testMaxOps() throws Exception {
    String collectionName = "testMaxOps_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 5, 2).setMaxShardsPerNode(10);
    create.process(solrClient);
    
    CloudUtil.waitForState(cloudManager, "failed to create " + collectionName, collectionName,
        CloudUtil.clusterShape(5, 2, false, true));

    long waitForSeconds = 3 + random().nextInt(5);
    // add disabled trigger
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'index_size_trigger5'," +
        "'event' : 'indexSize'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'aboveDocs' : 10," +
        "'enabled' : false," +
        "'actions' : [{'name' : 'compute_plan', 'class' : 'solr.ComputePlanAction'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'capturing5'," +
        "'trigger' : 'index_size_trigger5'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED','FAILED']," +
        "'beforeAction' : ['compute_plan']," +
        "'afterAction' : ['compute_plan']," +
        "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'finished'," +
        "'trigger' : 'index_size_trigger5'," +
        "'stage' : ['SUCCEEDED']," +
        "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");


    for (int i = 0; i < 200; i++) {
      SolrInputDocument doc = new SolrInputDocument("id", "id-" + i);
      solrClient.add(collectionName, doc);
    }
    solrClient.commit(collectionName);

    // enable the trigger
    String resumeTriggerCommand = "{" +
        "'resume-trigger' : {" +
        "'name' : 'index_size_trigger5'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    boolean await = finished.await(60000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("did not finish processing in time", await);

    // suspend the trigger
    String suspendTriggerCommand = "{" +
        "'suspend-trigger' : {" +
        "'name' : 'index_size_trigger5'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertEquals(1, listenerEvents.size());
    List<CapturedEvent> events = listenerEvents.get("capturing5");
    assertNotNull("'capturing5' events not found", events);
    assertEquals("events: " + events, 4, events.size());
    assertEquals(TriggerEventProcessorStage.STARTED, events.get(0).stage);
    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, events.get(1).stage);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, events.get(2).stage);
    assertEquals(TriggerEventProcessorStage.SUCCEEDED, events.get(3).stage);
    // check ops
    List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>) events.get(2).event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("should contain requestedOps", ops);
    assertEquals("number of ops: " + ops, 5, ops.size());

    listenerEvents.clear();
    finished = new CountDownLatch(1);

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'index_size_trigger5'," +
        "'event' : 'indexSize'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'aboveDocs' : 10," +
        "'maxOps' : 3," +
        "'enabled' : true," +
        "'actions' : [{'name' : 'compute_plan', 'class' : 'solr.ComputePlanAction'}]" +
        "}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    await = finished.await(60000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("did not finish processing in time", await);

    // suspend the trigger
    suspendTriggerCommand = "{" +
        "'suspend-trigger' : {" +
        "'name' : 'index_size_trigger5'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertEquals(1, listenerEvents.size());
    events = listenerEvents.get("capturing5");
    assertNotNull("'capturing5' events not found", events);
    assertEquals("events: " + events, 4, events.size());
    assertEquals(TriggerEventProcessorStage.STARTED, events.get(0).stage);
    assertEquals(TriggerEventProcessorStage.BEFORE_ACTION, events.get(1).stage);
    assertEquals(TriggerEventProcessorStage.AFTER_ACTION, events.get(2).stage);
    assertEquals(TriggerEventProcessorStage.SUCCEEDED, events.get(3).stage);
    // check ops
    ops = (List<TriggerEvent.Op>) events.get(2).event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull("should contain requestedOps", ops);
    assertEquals("number of ops: " + ops, 3, ops.size());
  }

  //test that split parameters can be overridden
  @Test
  public void testSplitConfig() throws Exception {
    String collectionName = "testSplitConfig_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 2).setMaxShardsPerNode(2);
    create.process(solrClient);
    CloudUtil.waitForState(cloudManager, "failed to create " + collectionName, collectionName,
        CloudUtil.clusterShape(2, 2, false, true));

    long waitForSeconds = 3 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);
    props.put(CommonAdminParams.SPLIT_METHOD, SolrIndexSplitter.SplitMethod.REWRITE.toLower());
    props.put(IndexSizeTrigger.SPLIT_BY_PREFIX, true);
    
    try (IndexSizeTrigger trigger = new IndexSizeTrigger("index_size_trigger6")) {
      trigger.configure(loader, cloudManager, props);
      trigger.init();
      trigger.setProcessor(noFirstRunProcessor);
      trigger.run();

      for (int i = 0; i < 25; i++) {
        SolrInputDocument doc = new SolrInputDocument("id", "id-" + i);
        solrClient.add(collectionName, doc);
      }
      solrClient.commit(collectionName);

      AtomicBoolean fired = new AtomicBoolean(false);
      AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();
      trigger.setProcessor(event -> {
        if (fired.compareAndSet(false, true)) {
          eventRef.set(event);
          long currentTimeNanos = timeSource.getTimeNs();
          long eventTimeNanos = event.getEventTime();
          long waitForNanos = TimeUnit.NANOSECONDS.convert(waitForSeconds, TimeUnit.SECONDS) - WAIT_FOR_DELTA_NANOS;
          if (currentTimeNanos - eventTimeNanos <= waitForNanos) {
            fail("processor was fired before the configured waitFor period: currentTimeNanos=" + currentTimeNanos + ", eventTimeNanos=" +  eventTimeNanos + ",waitForNanos=" + waitForNanos);
          }
        } else {
          fail("IndexSizeTrigger was fired more than once!");
        }
        return true;
      });
      trigger.run();
      TriggerEvent ev = eventRef.get();
      // waitFor delay - should not produce any event yet
      assertNull("waitFor not elapsed but produced an event", ev);
      timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));
      trigger.run();
      ev = eventRef.get();
      assertNotNull("should have fired an event", ev);
      List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>) ev.getProperty(TriggerEvent.REQUESTED_OPS);
      assertNotNull("should contain requestedOps", ops);
      assertEquals("number of ops: " + ops, 2, ops.size());
      boolean shard1 = false;
      boolean shard2 = false;
      for (TriggerEvent.Op op : ops) {
        assertEquals(CollectionParams.CollectionAction.SPLITSHARD, op.getAction());
        Set<Pair<String, String>> hints = (Set<Pair<String, String>>)op.getHints().get(Suggester.Hint.COLL_SHARD);
        assertNotNull("hints", hints);
        assertEquals("hints", 1, hints.size());
        Pair<String, String> p = hints.iterator().next();
        assertEquals(collectionName, p.first());
        if (p.second().equals("shard1")) {
          shard1 = true;
        } else if (p.second().equals("shard2")) {
          shard2 = true;
        } else {
          fail("unexpected shard name " + p.second());
        }
        Map<String, Object> params = (Map<String, Object>)op.getHints().get(Suggester.Hint.PARAMS);
        assertNotNull("params are null: " + op, params);
        
        // verify overrides for split config
        assertEquals("splitMethod: " + op, SolrIndexSplitter.SplitMethod.REWRITE.toLower(),
            params.get(CommonAdminParams.SPLIT_METHOD));
        assertEquals("splitByPrefix: " + op, true, params.get(CommonAdminParams.SPLIT_BY_PREFIX));
      }
      assertTrue("shard1 should be split", shard1);
      assertTrue("shard2 should be split", shard2);
    }

  }
  
  //validates that trigger configuration will fail for invalid split configs
  @Test
  public void testInvalidSplitConfig() throws Exception {
    long waitForSeconds = 3 + random().nextInt(5);
    Map<String, Object> props = createTriggerProps(waitForSeconds);
    props.put(IndexSizeTrigger.SPLIT_BY_PREFIX, "hello");

    try (IndexSizeTrigger trigger = new IndexSizeTrigger("index_size_trigger7")) {
      trigger.configure(loader, cloudManager, props);
      fail("Trigger configuration should have failed with invalid property.");
    } catch (TriggerValidationException e) {
      assertTrue(e.getDetails().containsKey(IndexSizeTrigger.SPLIT_BY_PREFIX));
    }

    props.put(IndexSizeTrigger.SPLIT_BY_PREFIX, true);
    props.put(CommonAdminParams.SPLIT_METHOD, "hello");
    try (IndexSizeTrigger trigger = new IndexSizeTrigger("index_size_trigger8")) {
      trigger.configure(loader, cloudManager, props);
      fail("Trigger configuration should have failed with invalid property.");
    } catch (TriggerValidationException e) {
      assertTrue(e.getDetails().containsKey(IndexSizeTrigger.SPLIT_METHOD_PROP));
    }
  }


  @Test
  public void testEstimatedIndexSize() throws Exception {
    if (!realCluster) {
      log.info("This test doesn't work with a simulated cluster");
      return;
    }
    String collectionName = "testEstimatedIndexSize_collection";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 2).setMaxShardsPerNode(2);
    create.process(solrClient);

    CloudUtil.waitForState(cloudManager, "failed to create " + collectionName, collectionName,
        CloudUtil.clusterShape(2, 2, false, true));

    int NUM_DOCS = 20;
    for (int i = 0; i < NUM_DOCS; i++) {
      SolrInputDocument doc = new SolrInputDocument("id", "id-" + (i * 100));
      solrClient.add(collectionName, doc);
    }
    solrClient.commit(collectionName);

    // get the size of the leader's index
    DocCollection coll = cloudManager.getClusterStateProvider().getCollection(collectionName);
    Replica leader = coll.getSlice("shard1").getLeader();
    String replicaName = Utils.parseMetricsReplicaName(collectionName, leader.getCoreName());
    assertNotNull("replicaName could not be constructed from " + leader, replicaName);
    final String registry = SolrCoreMetricManager.createRegistryName(true, collectionName, "shard1", replicaName, null);
    Set<String> tags = SimUtils.COMMON_REPLICA_TAGS.stream()
        .map(s -> "metrics:" + registry + ":" + s).collect(Collectors.toSet());
    Map<String, Object> sizes = cloudManager.getNodeStateProvider().getNodeValues(leader.getNodeName(), tags);
    String commitSizeTag = "metrics:" + registry + ":SEARCHER.searcher.indexCommitSize";
    String numDocsTag = "metrics:" + registry + ":SEARCHER.searcher.numDocs";
    String maxDocTag = "metrics:" + registry + ":SEARCHER.searcher.maxDoc";
    assertNotNull(sizes.toString(), sizes.get(commitSizeTag));
    assertNotNull(sizes.toString(), sizes.get(numDocsTag));
    assertNotNull(sizes.toString(), sizes.get(maxDocTag));
    long commitSize = ((Number)sizes.get(commitSizeTag)).longValue();
    long maxDoc = ((Number)sizes.get(maxDocTag)).longValue();
    long numDocs = ((Number)sizes.get(numDocsTag)).longValue();

    assertEquals("maxDoc != numDocs", maxDoc, numDocs);
    assertTrue("unexpected numDocs=" + numDocs, numDocs > NUM_DOCS / 3);

    long aboveBytes = commitSize * 9 / 10;
    long waitForSeconds = 3 + random().nextInt(5);
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'index_size_trigger7'," +
        "'event' : 'indexSize'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'splitMethod' : 'link'," +
        "'aboveBytes' : " + aboveBytes + "," +
        "'enabled' : false," +
        "'actions' : [{'name' : 'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name' : 'execute_plan', 'class' : '" + ExecutePlanAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'capturing7'," +
        "'trigger' : 'index_size_trigger7'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED','FAILED']," +
        "'beforeAction' : ['compute_plan','execute_plan']," +
        "'afterAction' : ['compute_plan','execute_plan']," +
        "'class' : '" + CapturingTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'finished'," +
        "'trigger' : 'index_size_trigger7'," +
        "'stage' : ['SUCCEEDED']," +
        "'class' : '" + FinishedProcessingListener.class.getName() + "'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // enable the trigger
    String resumeTriggerCommand = "{" +
        "'resume-trigger' : {" +
        "'name' : 'index_size_trigger7'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals("success", response.get("result").toString());

    // aboveBytes was set to be slightly lower than the actual size of at least one shard, so
    // we're expecting a SPLITSHARD - but with 'link' method the actual size of the resulting shards
    // will likely not go down. However, the estimated size of the latest commit point will go down
    // (see SOLR-12941).

    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    boolean await = finished.await(90000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("did not finish processing in time", await);
    // suspend the trigger
    String suspendTriggerCommand = "{" +
        "'suspend-trigger' : {" +
        "'name' : 'index_size_trigger7'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals("success", response.get("result").toString());

    assertEquals(1, listenerEvents.size());
    List<CapturedEvent> events = listenerEvents.get("capturing7");
    assertNotNull(listenerEvents.toString(), events);
    assertFalse("empty events?", events.isEmpty());
    CapturedEvent ev = events.get(0);
    List<TriggerEvent.Op> ops = (List< TriggerEvent.Op>)ev.event.properties.get(TriggerEvent.REQUESTED_OPS);
    assertNotNull("no requested ops in " + ev, ops);
    assertFalse("empty list of ops in " + ev, ops.isEmpty());
    Set<String> parentShards = new HashSet<>();
    ops.forEach(op -> {
      assertTrue(op.toString(), op.getAction() == CollectionParams.CollectionAction.SPLITSHARD);
      Collection<Pair<String, String>> hints = (Collection<Pair<String, String>>)op.getHints().get(Suggester.Hint.COLL_SHARD);
      assertNotNull("no hints in op " + op, hints);
      hints.forEach(h -> parentShards.add(h.second()));
    });

    // allow for recovery of at least some sub-shards
    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    coll = cloudManager.getClusterStateProvider().getCollection(collectionName);

    int checkedSubShards = 0;

    for (String parentShard : parentShards) {
      for (String subShard : Arrays.asList(parentShard + "_0", parentShard + "_1")) {
        leader = coll.getSlice(subShard).getLeader();
        if (leader == null) {
          // no leader yet - skip it
        }
        checkedSubShards++;
        replicaName = Utils.parseMetricsReplicaName(collectionName, leader.getCoreName());
        assertNotNull("replicaName could not be constructed from " + leader, replicaName);
        final String subregistry = SolrCoreMetricManager.createRegistryName(true, collectionName, subShard, replicaName, null);
        Set<String> subtags = SimUtils.COMMON_REPLICA_TAGS.stream()
            .map(s -> "metrics:" + subregistry + ":" + s).collect(Collectors.toSet());
        sizes = cloudManager.getNodeStateProvider().getNodeValues(leader.getNodeName(), subtags);
        commitSizeTag = "metrics:" + subregistry + ":SEARCHER.searcher.indexCommitSize";
        numDocsTag = "metrics:" + subregistry + ":SEARCHER.searcher.numDocs";
        maxDocTag = "metrics:" + subregistry + ":SEARCHER.searcher.maxDoc";
        assertNotNull(sizes.toString(), sizes.get(commitSizeTag));
        assertNotNull(sizes.toString(), sizes.get(numDocsTag));
        assertNotNull(sizes.toString(), sizes.get(maxDocTag));
        long subCommitSize = ((Number)sizes.get(commitSizeTag)).longValue();
        long subMaxDoc = ((Number)sizes.get(maxDocTag)).longValue();
        long subNumDocs = ((Number)sizes.get(numDocsTag)).longValue();
        assertTrue("subNumDocs=" + subNumDocs + " should be less than subMaxDoc=" + subMaxDoc +
            " due to link split", subNumDocs < subMaxDoc);
        assertTrue("subCommitSize=" + subCommitSize + " should be still greater than aboveBytes=" + aboveBytes +
            " due to link split", subCommitSize > aboveBytes);
        // calculate estimated size using the same formula
        long estimatedSize = IndexSizeTrigger.estimatedSize(subMaxDoc, subNumDocs, subCommitSize);
        assertTrue("estimatedSize=" + estimatedSize + " should be lower than aboveBytes=" + aboveBytes,
            estimatedSize < aboveBytes);
      }
    }

    assertTrue("didn't find any leaders in new sub-shards", checkedSubShards > 0);

    // reset & resume
    listenerEvents.clear();
    finished = new CountDownLatch(1);
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals("success", response.get("result").toString());
    timeSource.sleep(TimeUnit.MILLISECONDS.convert(waitForSeconds + 1, TimeUnit.SECONDS));

    // estimated shard size should fall well below the aboveBytes, even though the real commitSize
    // still remains larger due to the splitMethod=link side-effects
    await = finished.await(10000 / SPEED, TimeUnit.MILLISECONDS);
    assertFalse("should not fire the trigger again! " + listenerEvents, await);

  }

  private Map<String, Object> createTriggerProps(long waitForSeconds) {
    Map<String, Object> props = new HashMap<>();
    props.put("event", "indexSize");
    props.put("waitFor", waitForSeconds);
    props.put("enabled", true);
    props.put(IndexSizeTrigger.ABOVE_DOCS_PROP, 10);
    props.put(IndexSizeTrigger.BELOW_DOCS_PROP, 2);
    List<Map<String, String>> actions = new ArrayList<>(3);
    Map<String, String> map = new HashMap<>(2);
    map.put("name", "compute_plan");
    map.put("class", "solr.ComputePlanAction");
    actions.add(map);
    map = new HashMap<>(2);
    map.put("name", "execute_plan");
    map.put("class", "solr.ExecutePlanAction");
    actions.add(map);
    props.put("actions", actions);
    return props;
  }
}
