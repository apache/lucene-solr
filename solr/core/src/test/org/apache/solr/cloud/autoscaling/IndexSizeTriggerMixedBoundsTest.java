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

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
@LuceneTestCase.Slow
public class IndexSizeTriggerMixedBoundsTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static SolrCloudManager cloudManager;
  private static SolrClient solrClient;
  private static TimeSource timeSource;

  private static int SPEED = 1;

  static Map<String, List<CapturedEvent>> listenerEvents = new ConcurrentHashMap<>();
  static CountDownLatch listenerCreated = new CountDownLatch(1);
  static CountDownLatch finished = new CountDownLatch(1);

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(2)
    .addConfig("conf", configset("cloud-minimal"))
    .configure();
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    solrClient = cluster.getSolrClient();
    timeSource = cloudManager.getTimeSource();
  }

  @After
  public void restoreDefaults() throws Exception {
    cluster.deleteAllCollections();
    cloudManager.getDistribStateManager().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), -1);
    cloudManager.getTimeSource().sleep(5000);
    listenerEvents.clear();
    listenerCreated = new CountDownLatch(1);
    finished = new CountDownLatch(1);
  }

  @AfterClass
  public static void teardown() throws Exception {
    solrClient = null;
    cloudManager = null;
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
      log.info("=======> {}", ev);
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
  @SuppressWarnings({"unchecked"})
  public void testMixedBounds() throws Exception {
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
    @SuppressWarnings({"rawtypes"})
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

}
