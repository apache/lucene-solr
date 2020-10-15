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

import java.io.File;
import java.io.FileInputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggestion;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestSnapshotCloudManager extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static int NODE_COUNT = 3;

  private static SolrCloudManager realManager;

  // set up a real cluster as the source of test data
  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 2, 0, 1)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("coll1", null, 1, 1)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection("coll10", null, 1, 1)
        .process(cluster.getSolrClient());
    realManager = cluster.getJettySolrRunner(cluster.getJettySolrRunners().size() - 1).getCoreContainer()
        .getZkController().getSolrCloudManager();
    // disable .scheduled_maintenance (once it exists)
    CloudTestUtils.waitForTriggerToBeScheduled(realManager, ".scheduled_maintenance");
    CloudTestUtils.suspendTrigger(realManager, ".scheduled_maintenance");
  }

  @AfterClass
  public static void cleanUpAfterClass() throws Exception {
    realManager = null;
  }

  @Test
  public void testSnapshots() throws Exception {
    SnapshotCloudManager snapshotCloudManager = new SnapshotCloudManager(realManager, null);
    Map<String, Object> snapshot = snapshotCloudManager.getSnapshot(true, false);
    SnapshotCloudManager snapshotCloudManager1 = new SnapshotCloudManager(snapshot);
    SimSolrCloudTestCase.assertClusterStateEquals(realManager.getClusterStateProvider().getClusterState(), snapshotCloudManager.getClusterStateProvider().getClusterState());
    SimSolrCloudTestCase.assertClusterStateEquals(realManager.getClusterStateProvider().getClusterState(), snapshotCloudManager1.getClusterStateProvider().getClusterState());
    // this will always fail because the metrics will be already different
    // assertNodeStateProvider(realManager, snapshotCloudManager);
    assertNodeStateProvider(snapshotCloudManager, snapshotCloudManager1);
    assertDistribStateManager(snapshotCloudManager.getDistribStateManager(), snapshotCloudManager1.getDistribStateManager());
  }

  @Test
  public void testPersistance() throws Exception {
    Path tmpPath = createTempDir();
    File tmpDir = tmpPath.toFile();
    SnapshotCloudManager snapshotCloudManager = new SnapshotCloudManager(realManager, null);
    snapshotCloudManager.saveSnapshot(tmpDir, true, false);
    SnapshotCloudManager snapshotCloudManager1 = SnapshotCloudManager.readSnapshot(tmpDir);
    SimSolrCloudTestCase.assertClusterStateEquals(snapshotCloudManager.getClusterStateProvider().getClusterState(), snapshotCloudManager1.getClusterStateProvider().getClusterState());
    assertNodeStateProvider(snapshotCloudManager, snapshotCloudManager1);
    assertDistribStateManager(snapshotCloudManager.getDistribStateManager(), snapshotCloudManager1.getDistribStateManager());
  }

  @Test
  public void testRedaction() throws Exception {
    Path tmpPath = createTempDir();
    File tmpDir = tmpPath.toFile();
    Set<String> redacted = new HashSet<>(realManager.getClusterStateProvider().getLiveNodes());
    try (SnapshotCloudManager snapshotCloudManager = new SnapshotCloudManager(realManager, null)) {
      redacted.addAll(realManager.getClusterStateProvider().getClusterState().getCollectionStates().keySet());
      snapshotCloudManager.saveSnapshot(tmpDir, true, true);
    }
    for (String key : SnapshotCloudManager.REQUIRED_KEYS) {
      File src = new File(tmpDir, key + ".json");
      assertTrue(src.toString() + " doesn't exist", src.exists());
      try (FileInputStream is = new FileInputStream(src)) {
        String data = IOUtils.toString(is, Charset.forName("UTF-8"));
        assertFalse("empty data in " + src, data.trim().isEmpty());
        for (String redactedName : redacted) {
          assertFalse("redacted name " + redactedName + " found in " + src, data.contains(redactedName));
        }
      }
    }
  }

  @Test
  public void testComplexSnapshot() throws Exception {
    File snapshotDir = new File(TEST_HOME(), "simSnapshot");
    SnapshotCloudManager snapshotCloudManager = SnapshotCloudManager.readSnapshot(snapshotDir);
    assertEquals(48, snapshotCloudManager.getClusterStateProvider().getLiveNodes().size());
    assertEquals(16, snapshotCloudManager.getClusterStateProvider().getClusterState().getCollectionStates().size());
    try (SimCloudManager simCloudManager = SimCloudManager.createCluster(snapshotCloudManager, null, TimeSource.get("simTime:50"))) {
      List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(simCloudManager.getDistribStateManager().getAutoScalingConfig(), simCloudManager);
      //assertEquals(1, suggestions.size());
      if (suggestions.size() > 0) {
        Suggester.SuggestionInfo suggestion = suggestions.get(0);
        assertEquals(Suggestion.Type.improvement.toString(), suggestion.toMap(new HashMap<>()).get("type").toString());
      }
    }
  }

  @Test
  public void testSimulatorFromSnapshot() throws Exception {
    Path tmpPath = createTempDir();
    File tmpDir = tmpPath.toFile();
    SnapshotCloudManager snapshotCloudManager = new SnapshotCloudManager(realManager, null);
    snapshotCloudManager.saveSnapshot(tmpDir, true, false);
    SnapshotCloudManager snapshotCloudManager1 = SnapshotCloudManager.readSnapshot(tmpDir);
    try (SimCloudManager simCloudManager = SimCloudManager.createCluster(snapshotCloudManager1, null, TimeSource.get("simTime:50"))) {
      SimSolrCloudTestCase.assertClusterStateEquals(snapshotCloudManager.getClusterStateProvider().getClusterState(), simCloudManager.getClusterStateProvider().getClusterState());
      assertNodeStateProvider(snapshotCloudManager, simCloudManager, "freedisk");
      assertDistribStateManager(snapshotCloudManager.getDistribStateManager(), simCloudManager.getDistribStateManager());
      ClusterState state = simCloudManager.getClusterStateProvider().getClusterState();
      Replica r = state.getCollection(CollectionAdminParams.SYSTEM_COLL).getReplicas().get(0);
      // get another node
      String target = null;
      for (String node : simCloudManager.getClusterStateProvider().getLiveNodes()) {
        if (!node.equals(r.getNodeName())) {
          target = node;
          break;
        }
      }
      if (target == null) {
        fail("can't find suitable target node for replica " + r + ", liveNodes=" + simCloudManager.getClusterStateProvider().getLiveNodes());
      }
      CollectionAdminRequest.MoveReplica moveReplica = CollectionAdminRequest
          .moveReplica(CollectionAdminParams.SYSTEM_COLL, r.getName(), target);
      log.info("################");
      simCloudManager.simGetSolrClient().request(moveReplica);
    }
  }

  @SuppressWarnings({"unchecked"})
  private static void assertNodeStateProvider(SolrCloudManager oneMgr, SolrCloudManager twoMgr, String... ignorableNodeValues) throws Exception {
    NodeStateProvider one = oneMgr.getNodeStateProvider();
    NodeStateProvider two = twoMgr.getNodeStateProvider();
    for (String node : oneMgr.getClusterStateProvider().getLiveNodes()) {
      Map<String, Object> oneVals = one.getNodeValues(node, SimUtils.COMMON_NODE_TAGS);
      Map<String, Object> twoVals = two.getNodeValues(node, SimUtils.COMMON_NODE_TAGS);
      oneVals = new TreeMap<>(Utils.getDeepCopy(oneVals, 10, false, true));
      twoVals = new TreeMap<>(Utils.getDeepCopy(twoVals, 10, false, true));
      if (ignorableNodeValues != null) {
        for (String key : ignorableNodeValues) {
          oneVals.remove(key);
          twoVals.remove(key);
        }
      }
      assertEquals(Utils.toJSONString(oneVals), Utils.toJSONString(twoVals));
      Map<String, Map<String, List<ReplicaInfo>>> oneInfos = one.getReplicaInfo(node, SimUtils.COMMON_REPLICA_TAGS);
      Map<String, Map<String, List<ReplicaInfo>>> twoInfos = two.getReplicaInfo(node, SimUtils.COMMON_REPLICA_TAGS);
      assertEquals("collections on node" + node, oneInfos.keySet(), twoInfos.keySet());
      oneInfos.forEach((coll, oneShards) -> {
        Map<String, List<ReplicaInfo>> twoShards = twoInfos.get(coll);
        assertEquals("shards on node " + node, oneShards.keySet(), twoShards.keySet());
        oneShards.forEach((shard, oneReplicas) -> {
          List<ReplicaInfo> twoReplicas = twoShards.get(shard);
          assertEquals("num replicas on node " + node, oneReplicas.size(), twoReplicas.size());
          Map<String, ReplicaInfo> oneMap = oneReplicas.stream()
              .collect(Collectors.toMap(ReplicaInfo::getName, Function.identity()));
          Map<String, ReplicaInfo> twoMap = twoReplicas.stream()
              .collect(Collectors.toMap(ReplicaInfo::getName, Function.identity()));
          assertEquals("replica coreNodeNames on node " + node, oneMap.keySet(), twoMap.keySet());
          oneMap.forEach((coreNode, oneReplica) -> {
            ReplicaInfo twoReplica = twoMap.get(coreNode);
            SimSolrCloudTestCase.assertReplicaInfoEquals(oneReplica, twoReplica);
          });
        });
      });
    }
  }

  // ignore these because SimCloudManager always modifies them
  private static final Set<Pattern> IGNORE_DISTRIB_STATE_PATTERNS = new HashSet<>(Arrays.asList(
      Pattern.compile("/autoscaling/triggerState/.*"),
      // some triggers may have run after the snapshot was taken
      Pattern.compile("/autoscaling/events/.*"),
      // we always use format 1 in SimClusterStateProvider
      Pattern.compile("/clusterstate\\.json"),
      // depending on the startup sequence leaders may differ
      Pattern.compile("/collections/[^/]+?/leader_elect/.*"),
      Pattern.compile("/collections/[^/]+?/leaders/.*"),
      Pattern.compile("/collections/[^/]+?/terms/.*"),
      Pattern.compile("/overseer_elect/election/.*"),
      Pattern.compile("/live_nodes/.*")
  ));

  private static final Predicate<String> STATE_FILTER_FUN = p -> {
    for (Pattern pattern : IGNORE_DISTRIB_STATE_PATTERNS) {
      if (pattern.matcher(p).matches()) {
        return false;
      }
    }
    return true;
  };

  private static void assertDistribStateManager(DistribStateManager one, DistribStateManager two) throws Exception {
    List<String> treeOne = new ArrayList<>(one.listTree("/").stream()
        .filter(STATE_FILTER_FUN).collect(Collectors.toList()));
    List<String> treeTwo = new ArrayList<>(two.listTree("/").stream()
        .filter(STATE_FILTER_FUN).collect(Collectors.toList()));
    Collections.sort(treeOne);
    Collections.sort(treeTwo);
    assertEquals(treeOne, treeTwo);
    for (String path : treeOne) {
      VersionedData vd1 = one.getData(path);
      VersionedData vd2 = two.getData(path);
      assertEquals(path, vd1, vd2);
    }
  }


}
