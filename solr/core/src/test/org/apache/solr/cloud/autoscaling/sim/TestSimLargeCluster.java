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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.CapturedEvent;
import org.apache.solr.cloud.autoscaling.ComputePlanAction;
import org.apache.solr.cloud.autoscaling.ExecutePlanAction;
import org.apache.solr.cloud.autoscaling.SearchRateTrigger;
import org.apache.solr.cloud.autoscaling.TriggerActionBase;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerListenerBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class TestSimLargeCluster extends SimSolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int SPEED = 100;

  public static final int NUM_NODES = 100;
 
  static final Map<String, List<CapturedEvent>> listenerEvents = new ConcurrentHashMap<>();
  static final AtomicInteger triggerFinishedCount = new AtomicInteger();
  static final AtomicInteger triggerStartedCount = new AtomicInteger();
  static volatile CountDownLatch triggerStartedLatch;
  static volatile CountDownLatch triggerFinishedLatch;
  static volatile CountDownLatch listenerEventLatch;
  static volatile int waitForSeconds;

  @After
  public void tearDownTest() throws Exception {
    shutdownCluster();
  }
  
  @Before
  public void setupTest() throws Exception {
    configureCluster(NUM_NODES, TimeSource.get("simTime:" + SPEED));

    // disable metrics history collection
    cluster.disableMetricsHistory();

    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cluster, ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(cluster, ".scheduled_maintenance");
    // disable .auto_add_replicas (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(cluster, ".auto_add_replicas");
    CloudTestUtils.suspendTrigger(cluster, ".auto_add_replicas");
    cluster.getSimClusterStateProvider().createSystemCollection();

    waitForSeconds = 5;
    triggerStartedCount.set(0);
    triggerFinishedCount.set(0);
    triggerStartedLatch = new CountDownLatch(1);
    triggerFinishedLatch = new CountDownLatch(1);
    
    // by default assume we want to allow a (virtually) unbounded amount of events,
    // tests that expect a specific number can override
    listenerEventLatch = new CountDownLatch(Integer.MAX_VALUE);
    listenerEvents.clear();
  }

  public static class TestTriggerListener extends TriggerListenerBase {
    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      CapturedEvent ev = new CapturedEvent(cluster.getTimeSource().getTimeNs(), context, config, stage, actionName, event, message);
      final CountDownLatch latch = listenerEventLatch;
      synchronized (latch) {
        if (0 == latch.getCount()) {
          log.warn("Ignoring captured event since latch is 'full': {}", ev);
        } else {
          List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
          lst.add(ev);
          latch.countDown();
        }
      }
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
      triggerStartedCount.incrementAndGet();
      triggerStartedLatch.countDown();
    }
  }

  @Test
  @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // this test hits a timeout easily
  public void testBasic() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    assertAutoScalingRequest
      ( "{" +
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
        "}}");

    assertAutoScalingRequest
      ( "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'foo'," +
        "'trigger' : 'node_lost_trigger1'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED', 'FAILED']," +
        "'beforeAction' : ['compute', 'execute']," +
        "'afterAction' : ['compute', 'execute']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}");

    assertAutoscalingUpdateComplete();

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

    log.info("Ready after " + CloudUtil.waitForState(cluster, collectionName, 30 * nodes.size(), TimeUnit.SECONDS,
        CloudUtil.clusterShape(5, 15, false, true)) + "ms");

    int KILL_NODES = 8;
    // kill off a number of nodes
    for (int i = 0; i < KILL_NODES; i++) {
      cluster.simRemoveNode(nodes.get(i), false);
    }
    // should fully recover
    log.info("Ready after " + CloudUtil.waitForState(cluster, collectionName, 90 * KILL_NODES, TimeUnit.SECONDS,
        CloudUtil.clusterShape(5, 15, false, true)) + "ms");

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

    log.info("Ready after " + CloudUtil.waitForState(cluster, collectionName, 30 * nodes.size(), TimeUnit.SECONDS,
        CloudUtil.clusterShape(5, 15, false, true)) + "ms");
    long newMoveReplicaOps = cluster.simGetOpCount(CollectionParams.CollectionAction.MOVEREPLICA.name());
    log.info("==== Flaky replicas: {}. Additional MOVEREPLICA count: {}", flakyReplicas, (newMoveReplicaOps - moveReplicaOps));
    // flaky nodes lead to a number of MOVEREPLICA that is non-zero but lower than the number of flaky replicas
    assertTrue("there should be new MOVERPLICA ops", newMoveReplicaOps - moveReplicaOps > 0);
    assertTrue("there should be less than flakyReplicas=" + flakyReplicas + " MOVEREPLICA ops",
        newMoveReplicaOps - moveReplicaOps < flakyReplicas);
  }

  @Test
  public void testCreateLargeSimCollections() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();

    final int numCollections = atLeast(5);
    for (int i = 0; i < numCollections; i++) {
      // wide and shallow, or deep and narrow...
      final int numShards = TestUtil.nextInt(random(), 5, 20);
      final int nReps = TestUtil.nextInt(random(), 2, 25 - numShards);
      final int tReps = TestUtil.nextInt(random(), 2, 25 - numShards);
      final int pReps = TestUtil.nextInt(random(), 2, 25 - numShards);
      final int repsPerShard = (nReps + tReps + pReps);
      final int totalCores = repsPerShard * numShards;
      final int maxShardsPerNode = atLeast(2) + (totalCores / NUM_NODES);
      final String name = "large_sim_collection" + i;
      
      final CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection
        (name, "conf", numShards, nReps, tReps, pReps);
      create.setMaxShardsPerNode(maxShardsPerNode);
      create.setAutoAddReplicas(false);
      
      log.info("CREATE: {}", create);
      create.process(solrClient);

      // Since our current goal is to try and find situations where cores are just flat out missing
      // no matter how long we wait, let's be excessive and generous in our timeout.
      // (REMINDER: this uses the cluster's timesource, and ADDREPLICA has a hardcoded delay of 500ms)
      CloudUtil.waitForState(cluster, name, 2 * totalCores, TimeUnit.SECONDS,
          CloudUtil.clusterShape(numShards, repsPerShard, false, true));
      
      final CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(name);
      log.info("DELETE: {}", delete);
      delete.process(solrClient);
    }
  }
  
  @Test
  public void testAddNode() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    assertAutoScalingRequest
      ( "{" +
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
        "}}");

    assertAutoscalingUpdateComplete();

    // create a collection with more than 1 replica per node
    String collectionName = "testNodeAdded";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", NUM_NODES / 10, NUM_NODES / 8, NUM_NODES / 8, NUM_NODES / 8);
    create.setMaxShardsPerNode(5);
    create.setAutoAddReplicas(false);
    create.process(solrClient);

    log.info("Ready after " + CloudUtil.waitForState(cluster, collectionName, 90 * NUM_NODES, TimeUnit.SECONDS,
        CloudUtil.clusterShape(NUM_NODES / 10, NUM_NODES / 8 * 3, false, true)) + " ms");

    // start adding nodes
    int numAddNode = NUM_NODES / 5;
    List<String> addNodesList = new ArrayList<>(numAddNode);
    for (int i = 0; i < numAddNode; i++) {
      addNodesList.add(cluster.simAddNode());
    }
    // wait until at least one event is generated
    assertTrue("Trigger did not start even after await()ing an excessive amount of time",
               triggerStartedLatch.await(60, TimeUnit.SECONDS));

    // wait until started == finished
    TimeOut timeOut = new TimeOut(45 * waitForSeconds * NUM_NODES, TimeUnit.SECONDS, cluster.getTimeSource());
    while (!timeOut.hasTimedOut()) {
      final int started = triggerStartedCount.get();
      final int finished = triggerFinishedCount.get();
      log.info("started={} =?= finished={}", started, finished);
      if (triggerStartedCount.get() == triggerFinishedCount.get()) {
        log.info("started == finished: {} == {}", started, finished);
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
    long lastNumOps = cluster.simGetOpCount("MOVEREPLICA");
    log.info("1st check: lastNumOps (MOVEREPLICA) = {}", lastNumOps);
    assertTrue("no MOVEREPLICA ops?", lastNumOps > 0);

    log.info("Ready after " + CloudUtil.waitForState(cluster, collectionName, 20 * NUM_NODES, TimeUnit.SECONDS,
        CloudUtil.clusterShape(NUM_NODES / 10, NUM_NODES / 8 * 3, false, true)) + " ms");

    int count = 1000;
    SolrInputDocument finishedEvent = null;
    lastNumOps = cluster.simGetOpCount("MOVEREPLICA");
    log.info("2nd check: lastNumOps (MOVEREPLICA) = {}", lastNumOps);
    while (count-- > 0) {
      cluster.getTimeSource().sleep(10000);
      
      if (cluster.simGetOpCount("MOVEREPLICA") < 2) {
        log.info("MOVEREPLICA < 2");
        continue;
      }
      
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
            log.info("finishedEvent = {}", finishedEvent);
            break;
          }
        }
        log.info("breaking because currentNumOps == lastNumOps == {}", currentNumOps);
        break;
      } else {
        lastNumOps = currentNumOps;
      }
    }

    assertNotNull("did not finish processing changes", finishedEvent);
    long delta = (Long)finishedEvent.getFieldValue("event.time_l") - (Long)startedEvent.getFieldValue("event.time_l");
    log.info("#### System stabilized after " + TimeUnit.NANOSECONDS.toMillis(delta) + " ms");
    assertTrue("unexpected number of MOVEREPLICA ops: " + cluster.simGetOpCount("MOVEREPLICA"),
               cluster.simGetOpCount("MOVEREPLICA") > 1);
  }

  @Test
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
    assertAutoScalingRequest
      ( "{" +
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
        "}}");

    assertAutoScalingRequest
      ( "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'failures'," +
        "'trigger' : 'node_lost_trigger3'," +
        "'stage' : ['FAILED']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}");

    assertAutoscalingUpdateComplete();

    // create a collection with 1 replica per node
    String collectionName = "testNodeLost";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", NUM_NODES / 5, NUM_NODES / 10);
    create.setMaxShardsPerNode(5);
    create.setAutoAddReplicas(false);
    create.process(solrClient);

    log.info("Ready after " + CloudUtil.waitForState(cluster, collectionName, 60 * NUM_NODES, TimeUnit.SECONDS,
        CloudUtil.clusterShape(NUM_NODES / 5, NUM_NODES / 10, false, true)) + " ms");

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
    assertTrue("Trigger did not finish even after await()ing an excessive amount of time",
               triggerFinishedLatch.await(60, TimeUnit.SECONDS));
               
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
    
    // TODO we may not even have a .system collection because the message of node going down is interrupted on the executor
    // by the OverseerTriggerThread executors being interrupted on Overseer restart

      if (systemColl.size() > 0) {
        return 0;
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
      log.info("Ready after " + CloudUtil.waitForState(cluster, collectionName, 20 * NUM_NODES, TimeUnit.SECONDS,
          CloudUtil.clusterShape(NUM_NODES / 5, NUM_NODES / 10, false, true)) + " ms");
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
    Long delta = 0L;
    if (startedEvent != null) {
      delta = (Long) finishedEvent.getFieldValue("event.time_l")
          - (Long) startedEvent.getFieldValue("event.time_l");
      delta = TimeUnit.NANOSECONDS.toMillis(delta);
      log.info("#### System stabilized after " + delta + " ms");
    }
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
  public void testSearchRate() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String collectionName = "testSearchRate";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 10);
    create.process(solrClient);

    log.info("Ready after " + CloudUtil.waitForState(cluster, collectionName, 300, TimeUnit.SECONDS,
        CloudUtil.clusterShape(2, 10, false, true)) + " ms");

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
    assertAutoScalingRequest
      ( "{" +
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
        "}}");


    // we're going to expect our trigger listener to process exactly one captured event
    listenerEventLatch = new CountDownLatch(1);
    assertAutoScalingRequest
      ( "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'srt'," +
        "'trigger' : 'search_rate_trigger'," +
        "'stage' : ['FAILED','SUCCEEDED']," +
        "'class' : '" + TestTriggerListener.class.getName() + "'" +
        "}" +
        "}");

    assertAutoscalingUpdateComplete();

    assertTrue("Trigger did not finish even after await()ing an excessive amount of time",
               triggerFinishedLatch.await(60, TimeUnit.SECONDS));
    
    assertTrue("The listener didn't record the event even after await()ing an excessive amount of time",
               listenerEventLatch.await(60, TimeUnit.SECONDS));
    List<CapturedEvent> events = listenerEvents.get("srt");
    assertNotNull("no srt events: " + listenerEvents.toString(), events);
    assertEquals(events.toString(), 1, events.size());

    CapturedEvent ev = events.get(0);
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

  @Test
  public void testFreediskTracking() throws Exception {
    int NUM_DOCS = 100000;
    String collectionName = "testFreeDisk";
    SolrClient solrClient = cluster.simGetSolrClient();
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf",2, 2);
    create.process(solrClient);

    CloudUtil.waitForState(cluster, "Timed out waiting for replicas of new collection to be active",
        collectionName, CloudUtil.clusterShape(2, 2, false, true));
    ClusterState clusterState = cluster.getClusterStateProvider().getClusterState();
    DocCollection coll = clusterState.getCollection(collectionName);
    Set<String> nodes = coll.getReplicas().stream()
        .map(r -> r.getNodeName())
        .collect(Collectors.toSet());
    Map<String, Number> initialFreedisk = getFreeDiskPerNode(nodes);

    // test small updates
    for (int i = 0; i < NUM_DOCS; i++) {
      SolrInputDocument doc = new SolrInputDocument("id", "id-" + i);
      solrClient.add(collectionName, doc);
    }
    Map<String, Number> updatedFreedisk = getFreeDiskPerNode(nodes);
    double delta = getDeltaFreeDiskBytes(initialFreedisk, updatedFreedisk);
    // 2 replicas - twice as much delta
    assertEquals(SimClusterStateProvider.DEFAULT_DOC_SIZE_BYTES * NUM_DOCS * 2, delta, delta * 0.1);

    // test small deletes - delete half of docs
    for (int i = 0; i < NUM_DOCS / 2; i++) {
      solrClient.deleteById(collectionName, "id-" + i);
    }
    Map<String, Number> updatedFreedisk1 = getFreeDiskPerNode(nodes);
    double delta1 = getDeltaFreeDiskBytes(initialFreedisk, updatedFreedisk1);
    // 2 replicas but half the docs
    assertEquals(SimClusterStateProvider.DEFAULT_DOC_SIZE_BYTES * NUM_DOCS * 2 / 2, delta1, delta1 * 0.1);

    // test bulk delete
    solrClient.deleteByQuery(collectionName, "*:*");
    Map<String, Number> updatedFreedisk2 = getFreeDiskPerNode(nodes);
    double delta2 = getDeltaFreeDiskBytes(initialFreedisk, updatedFreedisk2);
    // 0 docs - initial freedisk
    log.info(cluster.dumpClusterState(true));
    assertEquals(0.0, delta2, delta2 * 0.1);

    // test bulk update
    UpdateRequest ureq = new UpdateRequest();
    ureq.setDocIterator(new FakeDocIterator(0, NUM_DOCS));
    ureq.process(solrClient, collectionName);
    Map<String, Number> updatedFreedisk3 = getFreeDiskPerNode(nodes);
    double delta3 = getDeltaFreeDiskBytes(initialFreedisk, updatedFreedisk3);
    assertEquals(SimClusterStateProvider.DEFAULT_DOC_SIZE_BYTES * NUM_DOCS * 2, delta3, delta3 * 0.1);
  }

  private double getDeltaFreeDiskBytes(Map<String, Number> initial, Map<String, Number> updated) {
    double deltaGB = 0;
    for (String node : initial.keySet()) {
      double before = initial.get(node).doubleValue();
      double after = updated.get(node).doubleValue();
      assertTrue("freedisk after=" + after + " not smaller than before=" + before, after <= before);
      deltaGB += before - after;
    }
    return deltaGB * 1024.0 * 1024.0 * 1024.0;
  }

  private Map<String, Number> getFreeDiskPerNode(Collection<String> nodes) throws Exception {
    Map<String, Number> freediskPerNode = new HashMap<>();
    for (String node : nodes) {
      Map<String, Object> values = cluster.getNodeStateProvider().getNodeValues(node, Arrays.asList(Variable.Type.FREEDISK.tagName));
      freediskPerNode.put(node, (Number) values.get(Variable.Type.FREEDISK.tagName));
    }
    log.info("- freeDiskPerNode: " + Utils.toJSONString(freediskPerNode));
    return freediskPerNode;
  }
}
