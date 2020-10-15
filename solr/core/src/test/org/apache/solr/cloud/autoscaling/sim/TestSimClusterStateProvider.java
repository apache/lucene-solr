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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Preference;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.Watcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test compares the cluster state of a real cluster and a simulated one.
 */
public class TestSimClusterStateProvider extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static int NODE_COUNT = 3;
  private static boolean simulated;

  private static SolrCloudManager cloudManager;

  private static Collection<String> liveNodes;
  private static Map<String, Object> clusterProperties;
  private static AutoScalingConfig autoScalingConfig;
  private static Map<String, Map<String, Map<String, List<ReplicaInfo>>>> replicas;
  private static Map<String, Map<String, Object>> nodeValues;
  private static ClusterState realState;

  // set up a real cluster as the source of test data
  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    simulated = random().nextBoolean();
    simulated = true;
    log.info("####### Using simulated components? {}", simulated);

    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 2, 0, 1)
        .process(cluster.getSolrClient());
    init();
  }

  @AfterClass
  public static void closeCloudManager() throws Exception {
    if (simulated && cloudManager != null) {
      cloudManager.close();
    }
    cloudManager = null;
  }

  private static void init() throws Exception {
    SolrCloudManager realManager = cluster.getJettySolrRunner(cluster.getJettySolrRunners().size() - 1).getCoreContainer()
        .getZkController().getSolrCloudManager();
    liveNodes = realManager.getClusterStateProvider().getLiveNodes();
    clusterProperties = realManager.getClusterStateProvider().getClusterProperties();
    autoScalingConfig = realManager.getDistribStateManager().getAutoScalingConfig();
    replicas = new HashMap<>();
    nodeValues = new HashMap<>();
    liveNodes.forEach(n -> {
      replicas.put(n, realManager.getNodeStateProvider().getReplicaInfo(n, Collections.emptySet()));
      nodeValues.put(n, realManager.getNodeStateProvider().getNodeValues(n, ImplicitSnitch.tags));
    });
    realState = realManager.getClusterStateProvider().getClusterState();

    if (simulated) {
      // initialize simulated provider
      cloudManager = SimCloudManager.createCluster(realManager, null, TimeSource.get("simTime:10"));
//      simCloudManager.getSimClusterStateProvider().simSetClusterProperties(clusterProperties);
//      simCloudManager.getSimDistribStateManager().simSetAutoScalingConfig(autoScalingConfig);
//      nodeValues.forEach((n, values) -> {
//        try {
//          simCloudManager.getSimNodeStateProvider().simSetNodeValues(n, values);
//        } catch (InterruptedException e) {
//          fail("Interrupted:" + e);
//        }
//      });
//      simCloudManager.getSimClusterStateProvider().simSetClusterState(realState);
      ClusterState simState = cloudManager.getClusterStateProvider().getClusterState();
      assertClusterStateEquals(realState, simState);
    } else {
      cloudManager = realManager;
    }
  }

  private static void assertClusterStateEquals(ClusterState one, ClusterState two) {
    assertEquals(one.getLiveNodes(), two.getLiveNodes());
    assertEquals(one.getCollectionsMap().keySet(), two.getCollectionsMap().keySet());
    one.forEachCollection(oneColl -> {
      DocCollection twoColl = two.getCollection(oneColl.getName());
      Map<String, Slice> oneSlices = oneColl.getSlicesMap();
      Map<String, Slice> twoSlices = twoColl.getSlicesMap();
      assertEquals(oneSlices.keySet(), twoSlices.keySet());
      oneSlices.forEach((s, slice) -> {
        Slice sTwo = twoSlices.get(s);
        for (Replica oneReplica : slice.getReplicas()) {
          Replica twoReplica = sTwo.getReplica(oneReplica.getName());
          assertNotNull(twoReplica);
          SimSolrCloudTestCase.assertReplicaEquals(oneReplica, twoReplica);
        }
      });
    });
  }

  private String addNode() throws Exception {
    JettySolrRunner solr = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    String nodeId = solr.getNodeName();
    if (simulated) {
      ((SimCloudManager) cloudManager).getSimClusterStateProvider().simAddNode(nodeId);
    }
    return nodeId;
  }

  private String deleteNode() throws Exception {
    String nodeId = cluster.getJettySolrRunner(0).getNodeName();
    JettySolrRunner stoppedServer = cluster.stopJettySolrRunner(0);
    cluster.waitForJettyToStop(stoppedServer);
    if (simulated) {
      ((SimCloudManager) cloudManager).getSimClusterStateProvider().simRemoveNode(nodeId);
    }
    return nodeId;
  }

  private void setAutoScalingConfig(AutoScalingConfig cfg) throws Exception {
    cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getZkClient().setData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH,
        Utils.toJSON(cfg), -1, true);
    if (simulated) {
      ((SimCloudManager) cloudManager).getSimDistribStateManager().simSetAutoScalingConfig(cfg);
    }
  }

  @Test
  public void testAddRemoveNode() throws Exception {
    Set<String> lastNodes = new HashSet<>(cloudManager.getClusterStateProvider().getLiveNodes());
    List<String> liveNodes = cloudManager.getDistribStateManager().listData(ZkStateReader.LIVE_NODES_ZKNODE);
    assertEquals(lastNodes.size(), liveNodes.size());
    liveNodes.removeAll(lastNodes);
    assertTrue(liveNodes.isEmpty());

    String node = addNode();
    cloudManager.getTimeSource().sleep(2000);
    assertFalse(lastNodes.contains(node));
    lastNodes = new HashSet<>(cloudManager.getClusterStateProvider().getLiveNodes());
    assertTrue(lastNodes.contains(node));
    liveNodes = cloudManager.getDistribStateManager().listData(ZkStateReader.LIVE_NODES_ZKNODE);
    assertEquals(lastNodes.size(), liveNodes.size());
    liveNodes.removeAll(lastNodes);
    assertTrue(liveNodes.isEmpty());

    node = deleteNode();
    cloudManager.getTimeSource().sleep(2000);
    assertTrue(lastNodes.contains(node));
    lastNodes = new HashSet<>(cloudManager.getClusterStateProvider().getLiveNodes());
    assertFalse(lastNodes.contains(node));
    liveNodes = cloudManager.getDistribStateManager().listData(ZkStateReader.LIVE_NODES_ZKNODE);
    assertEquals(lastNodes.size(), liveNodes.size());
    liveNodes.removeAll(lastNodes);
    assertTrue(liveNodes.isEmpty());  }

  @Test
  public void testAutoScalingConfig() throws Exception {
    final CountDownLatch triggered = new CountDownLatch(1);
    Watcher w = ev -> {
      if (triggered.getCount() == 0) {
        fail("already triggered once!");
      }
      triggered.countDown();
    };
    AutoScalingConfig cfg = cloudManager.getDistribStateManager().getAutoScalingConfig(w);
    assertEquals(autoScalingConfig, cfg);
    Preference p = new Preference(Collections.singletonMap("maximize", "freedisk"));
    cfg = cfg.withPolicy(cfg.getPolicy().withClusterPreferences(Collections.singletonList(p)));
    setAutoScalingConfig(cfg);
    if (!triggered.await(10, TimeUnit.SECONDS)) {
      fail("Watch should be triggered on update!");
    }
    AutoScalingConfig cfg1 = cloudManager.getDistribStateManager().getAutoScalingConfig(null);
    assertEquals(cfg, cfg1);

    // restore
    setAutoScalingConfig(autoScalingConfig);
    cfg1 = cloudManager.getDistribStateManager().getAutoScalingConfig(null);
    assertEquals(autoScalingConfig, cfg1);
  }
}
