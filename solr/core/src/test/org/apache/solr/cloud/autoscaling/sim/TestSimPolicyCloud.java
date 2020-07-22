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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Row;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class TestSimPolicyCloud extends SimSolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  @org.junit.Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setupCluster() throws Exception {
    configureCluster(5, TimeSource.get("simTime:50"));
  }
  
  @After
  public void afterTest() throws Exception {
    shutdownCluster();
  }

  public void testDataProviderPerReplicaDetails() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    CollectionAdminRequest.createCollection("perReplicaDataColl", "conf", 1, 5)
        .process(solrClient);

    CloudUtil.waitForState(cluster, "Timeout waiting for collection to become active", "perReplicaDataColl",
        CloudUtil.clusterShape(1, 5, false, true));
    DocCollection coll = getCollectionState("perReplicaDataColl");
    String autoScaleJson = "{" +
        "  'cluster-preferences': [" +
        "    { maximize : freedisk , precision: 50}," +
        "    { minimize : cores, precision: 2}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { replica : '0' , 'nodeRole': 'overseer'}," +
        "    { 'replica': '<2', 'shard': '#ANY', 'node': '#ANY'" +
        "    }" +
        "  ]," +
        "  'policies': {" +
        "    'policy1': [" +
        "      { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      { 'replica': '<2', 'shard': '#EACH', 'sysprop.rack': 'rack1'}" +
        "    ]" +
        "  }" +
        "}";
    @SuppressWarnings({"unchecked"})
    AutoScalingConfig config = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScaleJson));
    Policy.Session session = config.getPolicy().createSession(cluster);

    AtomicInteger count = new AtomicInteger(0);
    for (Row row : session.getSortedNodes()) {
      row.collectionVsShardVsReplicas.forEach((c, shardVsReplicas) -> shardVsReplicas.forEach((s, replicaInfos) -> {
        for (ReplicaInfo replicaInfo : replicaInfos) {
          if (replicaInfo.getVariables().containsKey(Type.CORE_IDX.tagName)) count.incrementAndGet();
        }
      }));
    }
    assertTrue(count.get() > 0);

    CollectionAdminRequest.deleteCollection("perReplicaDataColl").process(solrClient);

  }

  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void testCreateCollectionAddReplica() throws Exception  {
    SolrClient solrClient = cluster.simGetSolrClient();
    String nodeId = cluster.getSimClusterStateProvider().simGetRandomNode();

    int port = (Integer)cluster.getSimNodeStateProvider().simGetNodeValue(nodeId, ImplicitSnitch.PORT);

    String commands =  "{set-policy :{c1 : [{replica:0 , shard:'#EACH', port: '!" + port + "'}]}}";
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, commands));

    String collectionName = "testCreateCollectionAddReplica";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
        .setPolicy("c1")
        .process(solrClient);
    CloudUtil.waitForState(cluster, collectionName, 120, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 1, false, true));

    getCollectionState(collectionName).forEachReplica((s, replica) -> assertEquals(nodeId, replica.getNodeName()));

    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1").process(solrClient);
    CloudUtil.waitForState(cluster,
        collectionName, 120l, TimeUnit.SECONDS,
        (liveNodes, collectionState) -> collectionState.getReplicas().size() == 2);

    getCollectionState(collectionName).forEachReplica((s, replica) -> assertEquals(nodeId, replica.getNodeName()));
  }

  public void testCreateCollectionSplitShard() throws Exception  {
    SolrClient solrClient = cluster.simGetSolrClient();
    String firstNode = cluster.getSimClusterStateProvider().simGetRandomNode();
    int firstNodePort = (Integer)cluster.getSimNodeStateProvider().simGetNodeValue(firstNode, ImplicitSnitch.PORT);

    String secondNode;
    int secondNodePort;
    while (true)  {
      secondNode = cluster.getSimClusterStateProvider().simGetRandomNode();
      secondNodePort = (Integer)cluster.getSimNodeStateProvider().simGetNodeValue(secondNode, ImplicitSnitch.PORT);
      if (secondNodePort != firstNodePort)  break;
    }

    String commands =  "{set-policy :{c1 : [{replica:1 , shard:'#EACH', port: '" + firstNodePort + "'}, {replica:1, shard:'#EACH', port:'" + secondNodePort + "'}]}}";
    NamedList<Object> response = solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, commands));
    assertEquals("success", response.get("result"));

    String collectionName = "testCreateCollectionSplitShard";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2)
        .setPolicy("c1")
        .process(solrClient);
    CloudUtil.waitForState(cluster, "Timeout waiting for collection to become active", collectionName,
        CloudUtil.clusterShape(1, 2, false, true));

    DocCollection docCollection = getCollectionState(collectionName);
    List<Replica> list = docCollection.getReplicas(firstNode);
    int replicasOnNode1 = list != null ? list.size() : 0;
    list = docCollection.getReplicas(secondNode);
    int replicasOnNode2 = list != null ? list.size() : 0;

    assertEquals("Expected exactly one replica of collection on node with port: " + firstNodePort, 1, replicasOnNode1);
    assertEquals("Expected exactly one replica of collection on node with port: " + secondNodePort, 1, replicasOnNode2);

    CollectionAdminRequest.splitShard(collectionName).setShardName("shard1").process(solrClient);

    CloudUtil.waitForState(cluster, "Timed out waiting to see 6 replicas for collection: " + collectionName,
        collectionName, (liveNodes, collectionState) -> collectionState.getReplicas().size() == 6);

    docCollection = getCollectionState(collectionName);
    list = docCollection.getReplicas(firstNode);
    replicasOnNode1 = list != null ? list.size() : 0;
    list = docCollection.getReplicas(secondNode);
    replicasOnNode2 = list != null ? list.size() : 0;

    assertEquals("Expected exactly three replica of collection on node with port: " + firstNodePort, 3, replicasOnNode1);
    assertEquals("Expected exactly three replica of collection on node with port: " + secondNodePort, 3, replicasOnNode2);
    CollectionAdminRequest.deleteCollection(collectionName).process(solrClient);

  }

  public void testMetricsTag() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'metrics:abc':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    try {
      solrClient.request(req);
      fail("expected exception");
    } catch (Exception e) {
      // expected
      assertTrue(e.toString().contains("Invalid metrics: param in"));
    }
    setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'metrics:solr.node:ADMIN./admin/authorization.clientErrors:count':'>58768765', 'replica':0}" +
        "    ]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    solrClient.request(req);

    //org.eclipse.jetty.server.handler.DefaultHandler.2xx-responses
    CollectionAdminRequest.createCollection("metricsTest", "conf", 1, 1)
        .process(solrClient);
    CloudUtil.waitForState(cluster, "Timeout waiting for collection to become active", "metricsTest",
        CloudUtil.clusterShape(1, 1));

    DocCollection collection = getCollectionState("metricsTest");
    List<String> tags = Arrays.asList("metrics:solr.node:ADMIN./admin/authorization.clientErrors:count",
        "metrics:solr.jvm:buffers.direct.Count");
    Map<String, Object> val = cluster.getNodeStateProvider().getNodeValues(collection.getReplicas().get(0).getNodeName(), tags);
    for (String tag : tags) {
      assertNotNull( "missing : "+ tag , val.get(tag));
    }


  }

  public void testCreateCollectionAddShardWithReplicaTypeUsingPolicy() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    List<String> nodes = new ArrayList<>(cluster.getClusterStateProvider().getLiveNodes());
    String nrtNodeName = nodes.get(0);
    int nrtPort = (Integer)cluster.getSimNodeStateProvider().simGetNodeValue(nrtNodeName, ImplicitSnitch.PORT);


    String pullNodeName = nodes.get(1);
    int pullPort = (Integer)cluster.getSimNodeStateProvider().simGetNodeValue(pullNodeName, ImplicitSnitch.PORT);

    String tlogNodeName = nodes.get(2);
    int tlogPort = (Integer)cluster.getSimNodeStateProvider().simGetNodeValue(tlogNodeName, ImplicitSnitch.PORT);
    log.info("NRT {} PULL {} , TLOG {} ", nrtNodeName, pullNodeName, tlogNodeName);

    String commands = "{set-cluster-policy :[" +
        "{replica:0 , shard:'#EACH', type: NRT, port: '!" + nrtPort + "'}" +
        "{replica:0 , shard:'#EACH', type: PULL, port: '!" + pullPort + "'}" +
        "{replica:0 , shard:'#EACH', type: TLOG, port: '!" + tlogPort + "'}" +
        "]}";


    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, commands));
    Map<String, Object> json = Utils.getJson(cluster.getDistribStateManager(), ZkStateReader.SOLR_AUTOSCALING_CONF_PATH);
    assertEquals("full json:" + Utils.toJSONString(json), "!" + nrtPort,
        Utils.getObjectByPath(json, true, "cluster-policy[0]/port"));
    assertEquals("full json:" + Utils.toJSONString(json), "!" + pullPort,
        Utils.getObjectByPath(json, true, "cluster-policy[1]/port"));
    assertEquals("full json:" + Utils.toJSONString(json), "!" + tlogPort,
        Utils.getObjectByPath(json, true, "cluster-policy[2]/port"));

    CollectionAdminRequest.createCollectionWithImplicitRouter("policiesTest", "conf", "s1", 1, 1, 1)
        .setMaxShardsPerNode(-1)
        .process(solrClient);
    CloudUtil.waitForState(cluster, "Timeout waiting for collection to become active", "policiesTest",
        CloudUtil.clusterShape(1, 3, false, true));

    DocCollection coll = getCollectionState("policiesTest");


    BiConsumer<String, Replica> verifyReplicas = (s, replica) -> {
      switch (replica.getType()) {
        case NRT: {
          assertTrue("NRT replica should be in " + nrtNodeName, replica.getNodeName().equals(nrtNodeName));
          break;
        }
        case TLOG: {
          assertTrue("TLOG replica should be in " + tlogNodeName, replica.getNodeName().equals(tlogNodeName));
          break;
        }
        case PULL: {
          assertTrue("PULL replica should be in " + pullNodeName, replica.getNodeName().equals(pullNodeName));
          break;
        }
      }

    };
    coll.forEachReplica(verifyReplicas);

    CollectionAdminRequest.createShard("policiesTest", "s3").
        process(solrClient);
    coll = getCollectionState("policiesTest");
    assertEquals(3, coll.getSlice("s3").getReplicas().size());
    coll.forEachReplica(verifyReplicas);
  }
  
  public void testCreateCollectionAddShardUsingPolicy() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String nodeId = cluster.getSimClusterStateProvider().simGetRandomNode();
    int port = (Integer)cluster.getSimNodeStateProvider().simGetNodeValue(nodeId, ImplicitSnitch.PORT);

    String commands =  "{set-policy :{c1 : [{replica:1 , shard:'#EACH', port: '" + port + "'}]}}";
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, commands));
    Map<String, Object> json = Utils.getJson(cluster.getDistribStateManager(), ZkStateReader.SOLR_AUTOSCALING_CONF_PATH);
    assertEquals("full json:"+ Utils.toJSONString(json) , "#EACH",
        Utils.getObjectByPath(json, true, "/policies/c1[0]/shard"));
    CollectionAdminRequest.createCollectionWithImplicitRouter("policiesTest", "conf", "s1,s2", 1)
        .setPolicy("c1")
        .process(solrClient);
    CloudUtil.waitForState(cluster, "Timeout waiting for collection to become active", "policiesTest",
        CloudUtil.clusterShape(2, 1));

    DocCollection coll = getCollectionState("policiesTest");
    assertEquals("c1", coll.getPolicyName());
    assertEquals(2,coll.getReplicas().size());
    coll.forEachReplica((s, replica) -> assertEquals(nodeId, replica.getNodeName()));
    CollectionAdminRequest.createShard("policiesTest", "s3").process(solrClient);
    CloudUtil.waitForState(cluster, "Timeout waiting for collection to become active", "policiesTest",
        CloudUtil.clusterShape(3, 1));

    coll = getCollectionState("policiesTest");
    assertEquals(1, coll.getSlice("s3").getReplicas().size());
    coll.getSlice("s3").forEach(replica -> assertEquals(nodeId, replica.getNodeName()));
  }

  public void testDataProvider() throws IOException, SolrServerException, KeeperException, InterruptedException {
    SolrClient solrClient = cluster.simGetSolrClient();
    CollectionAdminRequest.createCollectionWithImplicitRouter("policiesTest", "conf", "shard1", 2)
        .process(solrClient);
    CloudUtil.waitForState(cluster, "Timeout waiting for collection to become active", "policiesTest",
        CloudUtil.clusterShape(1, 2, false, true));
    DocCollection rulesCollection = getCollectionState("policiesTest");

    Map<String, Object> val = cluster.getNodeStateProvider().getNodeValues(rulesCollection.getReplicas().get(0).getNodeName(), Arrays.asList(
        "freedisk",
        "cores",
        "heapUsage",
        "sysLoadAvg"));
    assertNotNull(val.get("freedisk"));
    assertNotNull(val.get("heapUsage"));
    assertNotNull(val.get("sysLoadAvg"));
    assertTrue(((Number) val.get("cores")).intValue() > 0);
    assertTrue("freedisk value is " + ((Number) val.get("freedisk")).doubleValue(),  Double.compare(((Number) val.get("freedisk")).doubleValue(), 0.0d) > 0);
    assertTrue("heapUsage value is " + ((Number) val.get("heapUsage")).doubleValue(), Double.compare(((Number) val.get("heapUsage")).doubleValue(), 0.0d) > 0);
    if (!Constants.WINDOWS)  {
      // the system load average metrics is not available on windows platform
      assertTrue("sysLoadAvg value is " + ((Number) val.get("sysLoadAvg")).doubleValue(), Double.compare(((Number) val.get("sysLoadAvg")).doubleValue(), 0.0d) > 0);
    }
    // simulator doesn't have Overseer, so just pick a random node
    String overseerNode = cluster.getSimClusterStateProvider().simGetRandomNode();
    solrClient.request(CollectionAdminRequest.addRole(overseerNode, "overseer"));
    for (int i = 0; i < 10; i++) {
      Map<String, Object> data = Utils.getJson(cluster.getDistribStateManager(), ZkStateReader.ROLES);
      if (i >= 9 && data.isEmpty()) {
        throw new RuntimeException("NO overseer node created");
      }
      cluster.getTimeSource().sleep(100);
    }
    val = cluster.getNodeStateProvider().getNodeValues(overseerNode, Arrays.asList(
        "nodeRole",
        "ip_1", "ip_2", "ip_3", "ip_4",
        "sysprop.java.version",
        "sysprop.java.vendor"));
    assertEquals("overseer", val.get("nodeRole"));
    assertNotNull(val.get("ip_1"));
    assertNotNull(val.get("ip_2"));
    assertNotNull(val.get("ip_3"));
    assertNotNull(val.get("ip_4"));
    assertNotNull(val.get("sysprop.java.version"));
    assertNotNull(val.get("sysprop.java.vendor"));
  }
}
