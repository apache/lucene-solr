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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventProcessorStage;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.ComputePlanAction;
import org.apache.solr.cloud.autoscaling.ExecutePlanAction;
import org.apache.solr.cloud.autoscaling.TriggerActionBase;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.cloud.autoscaling.TriggerListenerBase;
import org.apache.solr.cloud.autoscaling.CapturedEvent;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.LogLevel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

/**
 *
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class TestLargeCluster extends SimSolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int SPEED = 50;

  public static final int NUM_NODES = 100;

  static Map<String, List<CapturedEvent>> listenerEvents = new ConcurrentHashMap<>();
  static AtomicInteger triggerFiredCount = new AtomicInteger();
  static CountDownLatch triggerFiredLatch;
  static int waitForSeconds;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NUM_NODES, TimeSource.get("simTime:" + SPEED));
  }

  @Before
  public void setupTest() throws Exception {

    waitForSeconds = 1 + random().nextInt(3);
    triggerFiredCount.set(0);
    triggerFiredLatch = new CountDownLatch(1);
    listenerEvents.clear();
    while (cluster.getClusterStateProvider().getLiveNodes().size() < NUM_NODES) {
      // perhaps a test stopped a node but didn't start it back
      // lets start a node
      cluster.simAddNode();
    }
  }

  public static class TestTriggerListener extends TriggerListenerBase {
    @Override
    public void init(SolrCloudManager cloudManager, AutoScalingConfig.TriggerListenerConfig config) {
      super.init(cloudManager, config);
    }

    @Override
    public synchronized void onEvent(TriggerEvent event, TriggerEventProcessorStage stage, String actionName,
                                     ActionContext context, Throwable error, String message) {
      List<CapturedEvent> lst = listenerEvents.computeIfAbsent(config.name, s -> new ArrayList<>());
      lst.add(new CapturedEvent(cluster.getTimeSource().getTime(), context, config, stage, actionName, event, message));
    }
  }

  public static class TestTriggerAction extends TriggerActionBase {
    @Override
    public void process(TriggerEvent event, ActionContext context) throws Exception {
      triggerFiredCount.incrementAndGet();
      triggerFiredLatch.countDown();
    }
  }

  @Test
  public void testBasic() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'foo'," +
        "'trigger' : 'node_lost_trigger'," +
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
    create.setCreateNodeSet(String.join(",", nodes));
    create.process(solrClient);

    log.info("Ready after " + waitForState(collectionName, 30 * nodes.size(), TimeUnit.SECONDS, clusterShape(5, 15)) + "ms");

    int KILL_NODES = 8;
    // kill off a number of nodes
    for (int i = 0; i < KILL_NODES; i++) {
      cluster.simRemoveNode(nodes.get(i), false);
    }
    // should fully recover
    log.info("Ready after " + waitForState(collectionName, 90 * KILL_NODES, TimeUnit.SECONDS, clusterShape(5, 15)) + "ms");

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

    log.info("Ready after " + waitForState(collectionName, 30 * nodes.size(), TimeUnit.SECONDS, clusterShape(5, 15)) + "ms");
    log.info("OP COUNTS: " + cluster.simGetOpCounts());
    long newMoveReplicaOps = cluster.simGetOpCount(CollectionParams.CollectionAction.MOVEREPLICA.name());
    log.info("==== Flaky replicas: {}. Additional MOVEREPLICA count: {}", flakyReplicas, (newMoveReplicaOps - moveReplicaOps));
    // flaky nodes lead to a number of MOVEREPLICA that is non-zero but lower than the number of flaky replicas
    assertTrue("there should be new MOVERPLICA ops", newMoveReplicaOps - moveReplicaOps > 0);
    assertTrue("there should be less than flakyReplicas=" + flakyReplicas + " MOVEREPLICA ops",
        newMoveReplicaOps - moveReplicaOps < flakyReplicas);
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-11714")
  public void testSearchRate() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'search_rate_trigger'," +
        "'event' : 'searchRate'," +
        "'waitFor' : '" + waitForSeconds + "s'," +
        "'rate' : 1.0," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'compute','class':'" + ComputePlanAction.class.getName() + "'}," +
        "{'name':'execute','class':'" + ExecutePlanAction.class.getName() + "'}," +
        "{'name':'test','class':'" + TestTriggerAction.class.getName() + "'}" +
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

    String collectionName = "testSearchRate";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 2, 10);
    create.process(solrClient);

    log.info("Ready after " + waitForState(collectionName, 300, TimeUnit.SECONDS, clusterShape(2, 10)) + " ms");

    // collect the node names
    Set<String> nodes = new HashSet<>();
    cluster.getSimClusterStateProvider().getClusterState().getCollection(collectionName)
        .getReplicas()
        .forEach(r -> nodes.add(r.getNodeName()));

    String metricName = "QUERY./select.requestTimes:1minRate";
    // simulate search traffic
    cluster.getSimClusterStateProvider().simSetShardValue(collectionName, "shard1", metricName, 40, true);

    Thread.sleep(1000000000);
//    boolean await = triggerFiredLatch.await(20000 / SPEED, TimeUnit.MILLISECONDS);
//    assertTrue("The trigger did not fire at all", await);
    // wait for listener to capture the SUCCEEDED stage
    cluster.getTimeSource().sleep(2000);
    assertEquals(listenerEvents.toString(), 1, listenerEvents.get("srt").size());
    CapturedEvent ev = listenerEvents.get("srt").get(0);
  }
}
