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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Row;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.OverseerTaskProcessor;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.ZkDistributedQueueFactory;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;
import static org.apache.solr.common.util.Utils.getObjectByPath;

@LuceneTestCase.Slow
public class TestPolicyCloud extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  @org.junit.Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(5)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @After
  public void after() throws Exception {
    cluster.deleteAllCollections();
    cluster.getSolrClient().getZkStateReader().getZkClient().setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH,
        "{}".getBytes(StandardCharsets.UTF_8), true);
  }

  public void testDataProviderPerReplicaDetails() throws Exception {
    CollectionAdminRequest.createCollection("perReplicaDataColl", "conf", 1, 5)
        .process(cluster.getSolrClient());

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
    AutoScalingConfig config = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScaleJson));
    AtomicInteger count = new AtomicInteger(0);
    SolrCloudManager cloudManager = new SolrClientCloudManager(new ZkDistributedQueueFactory(cluster.getZkClient()), cluster.getSolrClient());
    String nodeName = cloudManager.getClusterStateProvider().getLiveNodes().iterator().next();
    SolrClientNodeStateProvider nodeStateProvider = (SolrClientNodeStateProvider) cloudManager.getNodeStateProvider();
    Map<String, Map<String, List<ReplicaInfo>>> result = nodeStateProvider.getReplicaInfo(nodeName, Collections.singleton("UPDATE./update.requests"));
    nodeStateProvider.forEachReplica(nodeName, replicaInfo -> {
      if (replicaInfo.getVariables().containsKey("UPDATE./update.requests")) count.incrementAndGet();
    });
    assertTrue(count.get() > 0);

    Policy.Session session = config.getPolicy().createSession(cloudManager);

    for (Row row : session.getSortedNodes()) {
      Object val = row.getVal(Type.TOTALDISK.tagName, null);
      log.info("node: {} , totaldisk : {}, freedisk : {}", row.node, val, row.getVal("freedisk",null));
      assertTrue(val != null);

    }

    count .set(0);
    for (Row row : session.getSortedNodes()) {
      row.collectionVsShardVsReplicas.forEach((c, shardVsReplicas) -> shardVsReplicas.forEach((s, replicaInfos) -> {
        for (ReplicaInfo replicaInfo : replicaInfos) {
          if (replicaInfo.getVariables().containsKey(Type.CORE_IDX.tagName)) count.incrementAndGet();
        }
      }));
    }
    assertTrue(count.get() > 0);

    CollectionAdminRequest.deleteCollection("perReplicaDataColl").process(cluster.getSolrClient());

  }

  public void testCreateCollectionAddReplica() throws Exception  {
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    int port = jetty.getLocalPort();

    String commands =  "{set-policy :{c1 : [{replica:0 , shard:'#EACH', port: '!" + port + "'}]}}";
    cluster.getSolrClient().request(AutoScalingHandlerTest.createAutoScalingRequest(SolrRequest.METHOD.POST, commands));

    String collectionName = "testCreateCollectionAddReplica";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
        .setPolicy("c1")
        .process(cluster.getSolrClient());

    getCollectionState(collectionName).forEachReplica((s, replica) -> assertEquals(jetty.getNodeName(), replica.getNodeName()));

    CollectionAdminRequest.addReplicaToShard(collectionName, "shard1").process(cluster.getSolrClient());
    waitForState("Timed out waiting to see 2 replicas for collection: " + collectionName,
        collectionName, (liveNodes, collectionState) -> collectionState.getReplicas().size() == 2);

    getCollectionState(collectionName).forEachReplica((s, replica) -> assertEquals(jetty.getNodeName(), replica.getNodeName()));
  }

  public void testCreateCollectionSplitShard() throws Exception  {
    JettySolrRunner firstNode = cluster.getRandomJetty(random());
    int firstNodePort = firstNode.getLocalPort();

    JettySolrRunner secondNode = null;
    while (true)  {
      secondNode = cluster.getRandomJetty(random());
      if (secondNode.getLocalPort() != firstNodePort)  break;
    }
    int secondNodePort = secondNode.getLocalPort();

    String commands =  "{set-policy :{c1 : [{replica:1 , shard:'#EACH', port: '" + firstNodePort + "'}, {replica:1, shard:'#EACH', port:'" + secondNodePort + "'}]}}";
    NamedList<Object> response = cluster.getSolrClient().request(AutoScalingHandlerTest.createAutoScalingRequest(SolrRequest.METHOD.POST, commands));
    assertEquals("success", response.get("result"));

    String collectionName = "testCreateCollectionSplitShard";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2)
        .setPolicy("c1")
        .process(cluster.getSolrClient());

    DocCollection docCollection = getCollectionState(collectionName);
    List<Replica> list = docCollection.getReplicas(firstNode.getNodeName());
    int replicasOnNode1 = list != null ? list.size() : 0;
    list = docCollection.getReplicas(secondNode.getNodeName());
    int replicasOnNode2 = list != null ? list.size() : 0;

    assertEquals("Expected exactly one replica of collection on node with port: " + firstNodePort, 1, replicasOnNode1);
    assertEquals("Expected exactly one replica of collection on node with port: " + secondNodePort, 1, replicasOnNode2);

    CollectionAdminRequest.splitShard(collectionName).setShardName("shard1").process(cluster.getSolrClient());

    waitForState("Timed out waiting to see 6 replicas for collection: " + collectionName,
        collectionName, (liveNodes, collectionState) -> collectionState.getReplicas().size() == 6);

    docCollection = getCollectionState(collectionName);
    list = docCollection.getReplicas(firstNode.getNodeName());
    replicasOnNode1 = list != null ? list.size() : 0;
    list = docCollection.getReplicas(secondNode.getNodeName());
    replicasOnNode2 = list != null ? list.size() : 0;

    assertEquals("Expected exactly three replica of collection on node with port: " + firstNodePort, 3, replicasOnNode1);
    assertEquals("Expected exactly three replica of collection on node with port: " + secondNodePort, 3, replicasOnNode2);
    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());

  }

  public void testMetricsTag() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'metrics:abc':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    try {
      solrClient.request(req);
      fail("expected exception");
    } catch (HttpSolrClient.RemoteExecutionException e) {
      // expected
      assertTrue(String.valueOf(getObjectByPath(e.getMetaData(),
          false, "error/details[0]/errorMessages[0]")).contains("Invalid metrics: param in"));
    }
    setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'metrics:solr.node:ADMIN./admin/authorization.clientErrors:count':'>58768765', 'replica':0}" +
        "    ]" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    solrClient.request(req);

    //org.eclipse.jetty.server.handler.DefaultHandler.2xx-responses
    CollectionAdminRequest.createCollection("metricsTest", "conf", 1, 1)
        .process(cluster.getSolrClient());
    DocCollection collection = getCollectionState("metricsTest");
    DistributedQueueFactory queueFactory = new ZkDistributedQueueFactory(cluster.getZkClient());
    try (SolrCloudManager provider = new SolrClientCloudManager(queueFactory, solrClient)) {
      List<String> tags = Arrays.asList("metrics:solr.node:ADMIN./admin/authorization.clientErrors:count",
          "metrics:solr.jvm:buffers.direct.Count");
      Map<String, Object> val = provider.getNodeStateProvider().getNodeValues(collection.getReplicas().get(0).getNodeName(), tags);
      for (String tag : tags) {
        assertNotNull("missing : " + tag, val.get(tag));
      }
      val = provider.getNodeStateProvider().getNodeValues(collection.getReplicas().get(0).getNodeName(), Collections.singleton("diskType"));

      Set<String> diskTypes = ImmutableSet.of("rotational", "ssd");
      assertTrue(diskTypes.contains(val.get("diskType")));
    }
  }

  public void testCreateCollectionAddShardWithReplicaTypeUsingPolicy() throws Exception {
    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);
    String nrtNodeName = jetty.getNodeName();
    int nrtPort = jetty.getLocalPort();

    jetty = cluster.getJettySolrRunners().get(1);
    String pullNodeName = jetty.getNodeName();
    int pullPort = jetty.getLocalPort();

    jetty = cluster.getJettySolrRunners().get(2);
    String tlogNodeName = jetty.getNodeName();
    int tlogPort = jetty.getLocalPort();
    log.info("NRT {} PULL {} , TLOG {} ", nrtNodeName, pullNodeName, tlogNodeName);

    String commands = "{set-cluster-policy :[" +
        "{replica:0 , shard:'#EACH', type: NRT, port: '!" + nrtPort + "'}" +
        "{replica:0 , shard:'#EACH', type: PULL, port: '!" + pullPort + "'}" +
        "{replica:0 , shard:'#EACH', type: TLOG, port: '!" + tlogPort + "'}" +
        "]}";


    cluster.getSolrClient().request(AutoScalingHandlerTest.createAutoScalingRequest(SolrRequest.METHOD.POST, commands));
    Map<String, Object> json = Utils.getJson(cluster.getZkClient(), ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, true);
    assertEquals("full json:" + Utils.toJSONString(json), "!" + nrtPort,
        Utils.getObjectByPath(json, true, "cluster-policy[0]/port"));
    assertEquals("full json:" + Utils.toJSONString(json), "!" + pullPort,
        Utils.getObjectByPath(json, true, "cluster-policy[1]/port"));
    assertEquals("full json:" + Utils.toJSONString(json), "!" + tlogPort,
        Utils.getObjectByPath(json, true, "cluster-policy[2]/port"));

    CollectionAdminRequest.createCollectionWithImplicitRouter("policiesTest", "conf", "s1", 1, 1, 1)
        .process(cluster.getSolrClient());

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
        process(cluster.getSolrClient());
    coll = getCollectionState("policiesTest");
    assertEquals(3, coll.getSlice("s3").getReplicas().size());
    coll.forEachReplica(verifyReplicas);
  }

  public void testCreateCollectionAddShardUsingPolicy() throws Exception {
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    int port = jetty.getLocalPort();

    String commands =  "{set-policy :{c1 : [{replica:1 , shard:'#EACH', port: '" + port + "'}]}}";
    cluster.getSolrClient().request(AutoScalingHandlerTest.createAutoScalingRequest(SolrRequest.METHOD.POST, commands));
    Map<String, Object> json = Utils.getJson(cluster.getZkClient(), ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, true);
    assertEquals("full json:"+ Utils.toJSONString(json) , "#EACH",
        Utils.getObjectByPath(json, true, "/policies/c1[0]/shard"));
    CollectionAdminRequest.createCollectionWithImplicitRouter("policiesTest", "conf", "s1,s2", 1)
        .setPolicy("c1")
        .process(cluster.getSolrClient());

    DocCollection coll = getCollectionState("policiesTest");
    assertEquals("c1", coll.getPolicyName());
    assertEquals(2,coll.getReplicas().size());
    coll.forEachReplica((s, replica) -> assertEquals(jetty.getNodeName(), replica.getNodeName()));
    CollectionAdminRequest.createShard("policiesTest", "s3").process(cluster.getSolrClient());
    coll = getCollectionState("policiesTest");
    assertEquals(1, coll.getSlice("s3").getReplicas().size());
    coll.getSlice("s3").forEach(replica -> assertEquals(jetty.getNodeName(), replica.getNodeName()));
  }

  public void testDataProvider() throws IOException, SolrServerException, KeeperException, InterruptedException {
    CollectionAdminRequest.createCollectionWithImplicitRouter("policiesTest", "conf", "shard1", 2)
        .process(cluster.getSolrClient());
    DocCollection rulesCollection = getCollectionState("policiesTest");

    try (SolrCloudManager cloudManager = new SolrClientCloudManager(new ZkDistributedQueueFactory(cluster.getZkClient()), cluster.getSolrClient())) {
      Map<String, Object> val = cloudManager.getNodeStateProvider().getNodeValues(rulesCollection.getReplicas().get(0).getNodeName(), Arrays.asList(
          "freedisk",
          "cores",
          "heapUsage",
          "sysLoadAvg"));
      assertNotNull(val.get("freedisk"));
      assertNotNull(val.get("heapUsage"));
      assertNotNull(val.get("sysLoadAvg"));
      assertTrue(((Number) val.get("cores")).intValue() > 0);
      assertTrue("freedisk value is " + ((Number) val.get("freedisk")).doubleValue(), Double.compare(((Number) val.get("freedisk")).doubleValue(), 0.0d) > 0);
      assertTrue("heapUsage value is " + ((Number) val.get("heapUsage")).doubleValue(), Double.compare(((Number) val.get("heapUsage")).doubleValue(), 0.0d) > 0);
      if (!Constants.WINDOWS) {
        // the system load average metrics is not available on windows platform
        assertTrue("sysLoadAvg value is " + ((Number) val.get("sysLoadAvg")).doubleValue(), Double.compare(((Number) val.get("sysLoadAvg")).doubleValue(), 0.0d) > 0);
      }
      String overseerNode = OverseerTaskProcessor.getLeaderNode(cluster.getZkClient());
      cluster.getSolrClient().request(CollectionAdminRequest.addRole(overseerNode, "overseer"));
      for (int i = 0; i < 10; i++) {
        Map<String, Object> data = Utils.getJson(cluster.getZkClient(), ZkStateReader.ROLES, true);
        if (i >= 9 && data.isEmpty()) {
          throw new RuntimeException("NO overseer node created");
        }
        Thread.sleep(100);
      }
      val = cloudManager.getNodeStateProvider().getNodeValues(overseerNode, Arrays.asList(
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
}
