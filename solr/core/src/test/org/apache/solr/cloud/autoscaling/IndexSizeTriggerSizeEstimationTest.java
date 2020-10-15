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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.autoscaling.sim.SimUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.SolrCoreMetricManager;
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
public class IndexSizeTriggerSizeEstimationTest extends SolrCloudTestCase {
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
  public void testEstimatedIndexSize() throws Exception {
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
    @SuppressWarnings({"rawtypes"})
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
    @SuppressWarnings({"unchecked"})
    List<TriggerEvent.Op> ops = (List< TriggerEvent.Op>)ev.event.properties.get(TriggerEvent.REQUESTED_OPS);
    assertNotNull("no requested ops in " + ev, ops);
    assertFalse("empty list of ops in " + ev, ops.isEmpty());
    Set<String> parentShards = new HashSet<>();
    ops.forEach(op -> {
      assertTrue(op.toString(), op.getAction() == CollectionParams.CollectionAction.SPLITSHARD);
      @SuppressWarnings({"unchecked"})
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
}
