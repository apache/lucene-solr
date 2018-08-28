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

package org.apache.solr.cloud.autoscaling.sim;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.ComputePlanAction;
import org.apache.solr.cloud.autoscaling.ExecutePlanAction;
import org.apache.solr.cloud.autoscaling.SearchRateTrigger;
import org.apache.solr.cloud.autoscaling.TriggerActionBase;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerListenerBase;
import org.apache.solr.cloud.autoscaling.CapturedEvent;
import org.apache.solr.cloud.autoscaling.TriggerValidationException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

/**
 *
 */
@TimeoutSuite(millis = 4 * 3600 * 1000)
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
@ThreadLeakLingering(linger = 20000) // ComputePlanAction may take significant time to complete
//05-Jul-2018 @LuceneTestCase.BadApple(bugUrl = "https://issues.apache.org/jira/browse/SOLR-12075")
public class TestSimLargeCluster extends SimSolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int SPEED = 50;

  public static final int NUM_NODES = 100;

  static Map<String, List<CapturedEvent>> listenerEvents = new ConcurrentHashMap<>();
  static AtomicInteger triggerFinishedCount = new AtomicInteger();
  static AtomicInteger triggerStartedCount = new AtomicInteger();
  static CountDownLatch triggerStartedLatch;
  static CountDownLatch triggerFinishedLatch;
  static int waitForSeconds;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_NODES, TimeSource.get("simTime:" + SPEED));
  }

  @Before
  public void setupTest() throws Exception {
    waitForSeconds = 5;
    triggerStartedCount.set(0);
    triggerFinishedCount.set(0);
    triggerStartedLatch = new CountDownLatch(1);
    triggerFinishedLatch = new CountDownLatch(1);
    listenerEvents.clear();
    // disable .scheduled_maintenance and .auto_add_replicas
    String suspendTriggerCommand = "{" +
        "'suspend-trigger' : {'name' : '.scheduled_maintenance'}" +
        "}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
    SolrClient solrClient = cluster.simGetSolrClient();
    NamedList<Object> response;
    try {
      response = solrClient.request(req);
      assertEquals(response.get("result").toString(), "success");
    } catch (Exception e) {
      if (!e.toString().contains("No trigger exists")) {
        throw e;
      }
    }
    suspendTriggerCommand = "{" +
        "'suspend-trigger' : {'name' : '.auto_add_replicas'}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
    try {
      response = solrClient.request(req);
      assertEquals(response.get("result").toString(), "success");
    } catch (Exception e) {
      if (!e.toString().contains("No trigger exists")) {
        throw e;
      }
    }

    // do this in advance if missing
    if (!cluster.getSimClusterStateProvider().simListCollections().contains(CollectionAdminParams.SYSTEM_COLL)) {
      cluster.getSimClusterStateProvider().createSystemCollection();
      CloudTestUtils.waitForState(cluster, CollectionAdminParams.SYSTEM_COLL, 120, TimeUnit.SECONDS,
          CloudTestUtils.clusterShape(1, 1, false, true));
    }
  }

  public static class TestTriggerListener extends TriggerListenerBase {
    @Override
    public void configure(SolrResourceLoader loader, SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) throws TriggerValidationException {
      super.configure(loader, cloudManager, config);
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
      lst.add(new CapturedEvent(cluster.getTimeSource().getTimeNs(), context, config, stage, actionName, event, message));
    }
  }

  public static class FinishTriggerAction extends TriggerActionBase {
    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      triggerFinishedCount.incrementAndGet();
      triggerFinishedLatch.countDown();
    }
  }

  public static class StartTriggerAction extends TriggerActionBase {
    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      triggerStartedLatch.countDown();
      triggerStartedCount.incrementAndGet();
    }
  }

  @Test
  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2018-06-18
  public void testBasic() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger1'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'start','class':'" + StartTriggerAction.class.getName() + "'}," +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + FinishTriggerAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'foo'," +
        "'trigger' : 'node_lost_trigger1'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED', 'FAILED']," +
        "'beforeAction' : ['compute', 'execute']," +
        "'afterAction' : ['compute', 'execute']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    cluster.getTimeSource().sleep(5000);

    // pick a few random nodes
    List<String> nodes = new ArrayList<>();
    int limit = 75;
    for (String node : cluster.getClusterStateProvider().getLiveNodes()) {
      nodes.add(node);
      if (nodes.size() > limit) {
        break;
      }
    }
    Collections.shuffle(nodes, random());
    // create collection on these nodes
    String collectionName = "testBasic";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 5, 5, 5, 5);
    create.setMaxShardsPerNode(1);
    create.setAutoAddReplicas(false);
    create.setCreateNodeSet(String.join(",", nodes));
    create.process(solrClient);

    log.info("Ready after " + CloudTestUtils.waitForState(cluster, collectionName, 30 * nodes.size(), TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(5, 15, false, true)) + "ms");

    int KILL_NODES = 8;
    // kill off a number of nodes
    for (int i = 0; i < KILL_NODES; i++) {
      cluster.simRemoveNode(nodes.get(i), false);
    }
    // should fully recover
    log.info("Ready after " + CloudTestUtils.waitForState(cluster, collectionName, 90 * KILL_NODES, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(5, 15, false, true)) + "ms");

    log.info("OP COUNTS: " + cluster.simGetOpCounts());
    long moveReplicaOps = cluster.simGetOpCount(CollectionParams.CollectionAction.MOVEREPLICA.name());

    // simulate a number of flaky nodes
    int FLAKY_NODES = 10;
    int flakyReplicas = 0;
    for (int cnt = 0; cnt < 10; cnt++) {
      for (int i = KILL_NODES; i < KILL_NODES + FLAKY_NODES; i++) {
        flakyReplicas += cluster.getSimClusterStateProvider().simGetReplicaInfos(nodes.get(i))
            .stream().filter(r -> r.getState().equals(Replica.State.ACTIVE)).count();
        cluster.simRemoveNode(nodes.get(i), false);
      }
      cluster.getTimeSource().sleep(TimeUnit.SECONDS.toMillis(waitForSeconds) * 2);
      for (int i = KILL_NODES; i < KILL_NODES + FLAKY_NODES; i++) {
        final String nodeId = nodes.get(i);
        cluster.submit(() -> cluster.getSimClusterStateProvider().simRestoreNode(nodeId));
      }
    }

    // wait until started == finished
    TimeOut timeOut = new TimeOut(20 * waitForSeconds * NUM_NODES, TimeUnit.SECONDS, cluster.getTimeSource());
    while (!timeOut.hasTimedOut()) {
      if (triggerStartedCount.get() == triggerFinishedCount.get()) {
        break;
      }
      timeOut.sleep(1000);
    }
    if (timeOut.hasTimedOut()) {
      fail("did not finish processing all events in time: started=" + triggerStartedCount.get() + ", finished=" + triggerFinishedCount.get());
    }


    log.info("Ready after " + CloudTestUtils.waitForState(cluster, collectionName, 30 * nodes.size(), TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(5, 15, false, true)) + "ms");
    long newMoveReplicaOps = cluster.simGetOpCount(CollectionParams.CollectionAction.MOVEREPLICA.name());
    log.info("==== Flaky replicas: {}. Additional MOVEREPLICA count: {}", flakyReplicas, (newMoveReplicaOps - moveReplicaOps));
    // flaky nodes lead to a number of MOVEREPLICA that is non-zero but lower than the number of flaky replicas
    assertTrue("there should be new MOVERPLICA ops", newMoveReplicaOps - moveReplicaOps > 0);
    assertTrue("there should be less than flakyReplicas=" + flakyReplicas + " MOVEREPLICA ops",
        newMoveReplicaOps - moveReplicaOps < flakyReplicas);
  }

  @Test
  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 28-June-2018
  public void testAddNode() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger2'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'start','class':'" + StartTriggerAction.class.getName() + "'}," +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + FinishTriggerAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // create a collection with more than 1 replica per node
    String collectionName = "testNodeAdded";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", NUM_NODES / 10, NUM_NODES / 8, NUM_NODES / 8, NUM_NODES / 8);
    create.setMaxShardsPerNode(5);
    create.setAutoAddReplicas(false);
    create.process(solrClient);

    log.info("Ready after " + CloudTestUtils.waitForState(cluster, collectionName, 20 * NUM_NODES, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(NUM_NODES / 10, NUM_NODES / 8 * 3, false, true)) + " ms");

    // start adding nodes
    int numAddNode = NUM_NODES / 5;
    List<String> addNodesList = new ArrayList<>(numAddNode);
    for (int i = 0; i < numAddNode; i++) {
      addNodesList.add(cluster.simAddNode());
      cluster.getTimeSource().sleep(5000);
    }
    // wait until at least one event is generated
    boolean await = triggerStartedLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("trigger did not fire", await);

    // wait until started == finished
    TimeOut timeOut = new TimeOut(20 * waitForSeconds * NUM_NODES, TimeUnit.SECONDS, cluster.getTimeSource());
    while (!timeOut.hasTimedOut()) {
      if (triggerStartedCount.get() == triggerFinishedCount.get()) {
        break;
      }
      timeOut.sleep(1000);
    }
    if (timeOut.hasTimedOut()) {
      fail("did not finish processing all events in time: started=" + triggerStartedCount.get() + ", finished=" + triggerFinishedCount.get());
    }

    List<SolrInputDocument> systemColl = cluster.simGetSystemCollection();
    int startedEventPos = -1;
    for (int i = 0; i < systemColl.size(); i++) {
      SolrInputDocument d = systemColl.get(i);
      if (!"node_added_trigger2".equals(d.getFieldValue("event.source_s"))) {
        continue;
      }
      if ("NODEADDED".equals(d.getFieldValue("event.type_s")) &&
          "STARTED".equals(d.getFieldValue("stage_s"))) {
        startedEventPos = i;
        break;
      }
    }
    assertTrue("no STARTED event", startedEventPos > -1);
    SolrInputDocument startedEvent = systemColl.get(startedEventPos);
    int lastIgnoredPos = startedEventPos;
    // make sure some replicas have been moved
    assertTrue("no MOVEREPLICA ops?", cluster.simGetOpCount("MOVEREPLICA") > 0);

    log.info("Ready after " + CloudTestUtils.waitForState(cluster, collectionName, 20 * NUM_NODES, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(NUM_NODES / 10, NUM_NODES / 8 * 3, false, true)) + " ms");

    int count = 50;
    SolrInputDocument finishedEvent = null;
    long lastNumOps = cluster.simGetOpCount("MOVEREPLICA");
    while (count-- > 0) {
      cluster.getTimeSource().sleep(10000);
      long currentNumOps = cluster.simGetOpCount("MOVEREPLICA");
      if (currentNumOps == lastNumOps) {
        int size = systemColl.size() - 1;
        for (int i = size; i > lastIgnoredPos; i--) {
          SolrInputDocument d = systemColl.get(i);
          if (!"node_added_trigger2".equals(d.getFieldValue("event.source_s"))) {
            continue;
          }
          if ("SUCCEEDED".equals(d.getFieldValue("stage_s"))) {
            finishedEvent = d;
            break;
          }
        }
        break;
      } else {
        lastNumOps = currentNumOps;
      }
    }

    assertTrue("did not finish processing changes", finishedEvent != null);
    long delta = (Long)finishedEvent.getFieldValue("event.time_l") - (Long)startedEvent.getFieldValue("event.time_l");
    log.info("#### System stabilized after " + TimeUnit.NANOSECONDS.toMillis(delta) + " ms");
    assertTrue("unexpected number of MOVEREPLICA ops", cluster.simGetOpCount("MOVEREPLICA") > 1);
  }

  @Test
  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2018-06-18
  public void testNodeLost() throws Exception {
    doTestNodeLost(waitForSeconds, 5000, 0);
  }

  // Renard R5 series - evenly covers a log10 range
  private static final int[] renard5 = new int[] {
      1, 2, 3, 4, 6,
      10
  };
  private static final int[] renard5x = new int[] {
      1, 2, 3, 4, 6,
      10, 16, 25, 40, 63,
      100
  };
  private static final int[] renard5xx = new int[] {
      1, 2, 3, 4, 6,
      10, 16, 25, 40, 63,
      100, 158, 251, 398, 631,
      1000, 1585, 2512, 3981, 6310,
      10000
  };
  // Renard R10 series
  private static final double[] renard10 = new double[] {
      1, 1.3, 1.6, 2, 2.5, 3.2, 4, 5, 6.3, 7.9,
      10
  };
  private static final double[] renard10x = new double[] {
      1, 1.3, 1.6, 2, 2.5, 3.2, 4, 5, 6.3, 7.9,
      10, 12.6, 15.8, 20, 25.1, 31.6, 39.8, 50.1, 63.1, 79.4,
      100
  };

  private static final AtomicInteger ZERO = new AtomicInteger(0);

  //@Test
  public void benchmarkNodeLost() throws Exception {
    List<String> results = new ArrayList<>();
    for (int wait : renard5x) {
      for (int delay : renard5x) {
        SummaryStatistics totalTime = new SummaryStatistics();
        SummaryStatistics ignoredOurEvents = new SummaryStatistics();
        SummaryStatistics ignoredOtherEvents = new SummaryStatistics();
        SummaryStatistics startedOurEvents = new SummaryStatistics();
        SummaryStatistics startedOtherEvents = new SummaryStatistics();
        for (int i = 0; i < 5; i++) {
          if (cluster != null) {
            cluster.close();
          }
          setupCluster();
          setUp();
          setupTest();
          long total = doTestNodeLost(wait, delay * 1000, 0);
          totalTime.addValue(total);
          // get event counts
          Map<String, Map<String, AtomicInteger>> counts = cluster.simGetEventCounts();
          Map<String, AtomicInteger> map = counts.remove("node_lost_trigger");
          startedOurEvents.addValue(map.getOrDefault("STARTED", ZERO).get());
          ignoredOurEvents.addValue(map.getOrDefault("IGNORED", ZERO).get());
          int otherStarted = 0;
          int otherIgnored = 0;
          for (Map<String, AtomicInteger> m : counts.values()) {
            otherStarted += m.getOrDefault("STARTED", ZERO).get();
            otherIgnored += m.getOrDefault("IGNORED", ZERO).get();
          }
          startedOtherEvents.addValue(otherStarted);
          ignoredOtherEvents.addValue(otherIgnored);
        }
        results.add(String.format(Locale.ROOT, "%d\t%d\t%4.0f\t%4.0f\t%4.0f\t%4.0f\t%6.0f\t%6.0f\t%6.0f\t%6.0f\t%6.0f",
            wait, delay, startedOurEvents.getMean(), ignoredOurEvents.getMean(),
            startedOtherEvents.getMean(), ignoredOtherEvents.getMean(),
            totalTime.getMin(), totalTime.getMax(), totalTime.getMean(), totalTime.getStandardDeviation(), totalTime.getVariance()));
      }
    }
    log.info("===== RESULTS ======");
    log.info("waitFor\tdelay\tSTRT\tIGN\toSTRT\toIGN\tmin\tmax\tmean\tstdev\tvar");
    results.forEach(s -> log.info(s));
  }

  private long doTestNodeLost(int waitFor, long killDelay, int minIgnored) throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger3'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '" + waitFor + "s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'start','class':'" + StartTriggerAction.class.getName() + "'}," +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + FinishTriggerAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'failures'," +
        "'trigger' : 'node_lost_trigger3'," +
        "'stage' : ['FAILED']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");


    // create a collection with 1 replica per node
    String collectionName = "testNodeLost";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", NUM_NODES / 5, NUM_NODES / 10);
    create.setMaxShardsPerNode(5);
    create.setAutoAddReplicas(false);
    create.process(solrClient);

    log.info("Ready after " + CloudTestUtils.waitForState(cluster, collectionName, 20 * NUM_NODES, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(NUM_NODES / 5, NUM_NODES / 10, false, true)) + " ms");

    // start killing nodes
    int numNodes = NUM_NODES / 5;
    List<String> nodes = new ArrayList<>(cluster.getLiveNodesSet().get());
    for (int i = 0; i < numNodes; i++) {
      // this may also select a node where a replica is moved to, so the total number of
      // MOVEREPLICA may vary
      cluster.simRemoveNode(nodes.get(i), false);
      cluster.getTimeSource().sleep(killDelay);
    }
    // wait for the trigger to fire and complete at least once
    boolean await = triggerFinishedLatch.await(20 * waitFor * 1000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("trigger did not fire within timeout, " +
        "waitFor=" + waitFor + ", killDelay=" + killDelay + ", minIgnored=" + minIgnored,
        await);
    List<SolrInputDocument> systemColl = cluster.simGetSystemCollection();
    int startedEventPos = -1;
    for (int i = 0; i < systemColl.size(); i++) {
      SolrInputDocument d = systemColl.get(i);
      if (!"node_lost_trigger3".equals(d.getFieldValue("event.source_s"))) {
        continue;
      }
      if ("NODELOST".equals(d.getFieldValue("event.type_s")) &&
          "STARTED".equals(d.getFieldValue("stage_s"))) {
        startedEventPos = i;
        break;
      }
    }
    assertTrue("no STARTED event: " + systemColl + ", " +
            "waitFor=" + waitFor + ", killDelay=" + killDelay + ", minIgnored=" + minIgnored,
          startedEventPos > -1);
    SolrInputDocument startedEvent = systemColl.get(startedEventPos);
    // we can expect some failures when target node in MOVEREPLICA has been killed
    // between when the event processing started and the actual moment of MOVEREPLICA execution
    // wait until started == (finished + failed)
    TimeOut timeOut = new TimeOut(20 * waitFor * NUM_NODES, TimeUnit.SECONDS, cluster.getTimeSource());
    while (!timeOut.hasTimedOut()) {
      if (triggerStartedCount.get() == triggerFinishedCount.get()) {
        break;
      }
      log.debug("started=" + triggerStartedCount.get() + ", finished=" + triggerFinishedCount.get() +
          ", failed=" + listenerEvents.size());
      timeOut.sleep(1000);
    }
    if (timeOut.hasTimedOut()) {
      if (triggerStartedCount.get() > triggerFinishedCount.get() + listenerEvents.size()) {
        fail("did not finish processing all events in time: started=" + triggerStartedCount.get() + ", finished=" + triggerFinishedCount.get() +
            ", failed=" + listenerEvents.size());
      }
    }
    int ignored = 0;
    int lastIgnoredPos = startedEventPos;
    for (int i = startedEventPos + 1; i < systemColl.size(); i++) {
      SolrInputDocument d = systemColl.get(i);
      if (!"node_lost_trigger3".equals(d.getFieldValue("event.source_s"))) {
        continue;
      }
      if ("NODELOST".equals(d.getFieldValue("event.type_s"))) {
        if ("IGNORED".equals(d.getFieldValue("stage_s"))) {
          ignored++;
          lastIgnoredPos = i;
        }
      }
    }
    assertTrue("should be at least " + minIgnored + " IGNORED events, " +
            "waitFor=" + waitFor + ", killDelay=" + killDelay + ", minIgnored=" + minIgnored,
            ignored >= minIgnored);
    // make sure some replicas have been moved
    assertTrue("no MOVEREPLICA ops? " +
            "waitFor=" + waitFor + ", killDelay=" + killDelay + ", minIgnored=" + minIgnored,
            cluster.simGetOpCount("MOVEREPLICA") > 0);

    if (listenerEvents.isEmpty()) {
      // no failed movements - verify collection shape
      log.info("Ready after " + CloudTestUtils.waitForState(cluster, collectionName, 20 * NUM_NODES, TimeUnit.SECONDS,
          CloudTestUtils.clusterShape(NUM_NODES / 5, NUM_NODES / 10, false, true)) + " ms");
    } else {
      cluster.getTimeSource().sleep(NUM_NODES * 100);
    }

    int count = 50;
    SolrInputDocument finishedEvent = null;
    long lastNumOps = cluster.simGetOpCount("MOVEREPLICA");
    while (count-- > 0) {
      cluster.getTimeSource().sleep(waitFor * 10000);
      long currentNumOps = cluster.simGetOpCount("MOVEREPLICA");
      if (currentNumOps == lastNumOps) {
        int size = systemColl.size() - 1;
        for (int i = size; i > lastIgnoredPos; i--) {
          SolrInputDocument d = systemColl.get(i);
          if (!"node_lost_trigger3".equals(d.getFieldValue("event.source_s"))) {
            continue;
          }
          if ("SUCCEEDED".equals(d.getFieldValue("stage_s"))) {
            finishedEvent = d;
            break;
          }
        }
        break;
      } else {
        lastNumOps = currentNumOps;
      }
    }

    assertTrue("did not finish processing changes, " +
            "waitFor=" + waitFor + ", killDelay=" + killDelay + ", minIgnored=" + minIgnored,
            finishedEvent != null);
    long delta = (Long)finishedEvent.getFieldValue("event.time_l") - (Long)startedEvent.getFieldValue("event.time_l");
    delta = TimeUnit.NANOSECONDS.toMillis(delta);
    log.info("#### System stabilized after " + delta + " ms");
    long ops = cluster.simGetOpCount("MOVEREPLICA");
    long expectedMinOps = 40;
    if (!listenerEvents.isEmpty()) {
      expectedMinOps = 20;
    }
    assertTrue("unexpected number (" + expectedMinOps + ") of MOVEREPLICA ops: " + ops + ", " +
            "waitFor=" + waitFor + ", killDelay=" + killDelay + ", minIgnored=" + minIgnored,
            ops >= expectedMinOps);
    return delta;
  }

  @Test
  //commented 2-Aug-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2018-06-18
  public void testSearchRate() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String collectionName = "testSearchRate";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 10);
    create.process(solrClient);

    log.info("Ready after " + CloudTestUtils.waitForState(cluster, collectionName, 300, TimeUnit.SECONDS,
        CloudTestUtils.clusterShape(2, 10, false, true)) + " ms");

    // collect the node names for shard1
    Set<String> nodes = new HashSet<>();
    cluster.getSimClusterStateProvider().getClusterState().getCollection(collectionName)
        .getSlice("shard1")
        .getReplicas()
        .forEach(r -> nodes.add(r.getNodeName()));

    String metricName = "QUERY./select.requestTimes:1minRate";
    // simulate search traffic
    cluster.getSimClusterStateProvider().simSetShardValue(collectionName, "shard1", metricName, 40, false, true);

    // now define the trigger. doing it earlier may cause partial events to be generated (where only some
    // nodes / replicas exceeded the threshold).
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'search_rate_trigger'," +
        "'event' : 'searchRate'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'aboveRate' : 1.0," +
        "'aboveNodeRate' : 1.0," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + FinishTriggerAction.class.getName() + "'}" +
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
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand1);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");


    boolean await = triggerFinishedLatch.await(waitForSeconds * 20000 / SPEED, TimeUnit.MILLISECONDS);
    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    cluster.getTimeSource().sleep(5000);
    assertNotNull(listenerEvents.toString(), listenerEvents.get("srt"));
    assertEquals(listenerEvents.toString(), 1, listenerEvents.get("srt").size());
    CapturedEvent ev = listenerEvents.get("srt").get(0);
    assertEquals(TriggerEventType.SEARCHRATE, ev.event.getEventType());
    Map<String, Number> m = (Map<String, Number>)ev.event.getProperty(SearchRateTrigger.HOT_NODES);
    assertNotNull(m);
    assertEquals(nodes.size(), m.size());
    assertEquals(nodes, m.keySet());
    m.forEach((k, v) -> assertEquals(4.0, v.doubleValue(), 0.01));
    List<TriggerEvent.Op> ops = (List<TriggerEvent.Op>)ev.event.getProperty(TriggerEvent.REQUESTED_OPS);
    assertNotNull(ops);
    assertEquals(ops.toString(), 1, ops.size());
    ops.forEach(op -> {
      assertEquals(CollectionParams.CollectionAction.ADDREPLICA, op.getAction());
      assertEquals(1, op.getHints().size());
      Object o = op.getHints().get(Suggester.Hint.COLL_SHARD);
      // this may be a pair or a HashSet of pairs with size 1
      Pair<String, String> hint = null;
      if (o instanceof Pair) {
        hint = (Pair<String, String>)o;
      } else if (o instanceof Set) {
        assertEquals("unexpected number of hints: " + o, 1, ((Set)o).size());
        o = ((Set)o).iterator().next();
        assertTrue("unexpected hint: " + o, o instanceof Pair);
        hint = (Pair<String, String>)o;
      } else {
        fail("unexpected hints: " + o);
      }
      assertNotNull(hint);
      assertEquals(collectionName, hint.first());
      assertEquals("shard1", hint.second());
    });
  }
}
