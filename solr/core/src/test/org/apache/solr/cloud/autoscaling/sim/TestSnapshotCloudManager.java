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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
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
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 2, 0, 1)
        .process(cluster.getSolrClient());
    realManager = cluster.getJettySolrRunner(cluster.getJettySolrRunners().size() - 1).getCoreContainer()
        .getZkController().getSolrCloudManager();
  }

  @Test
  public void testSnapshots() throws Exception {
    SnapshotCloudManager snapshotCloudManager = new SnapshotCloudManager(realManager, null);
    Map<String, Object> snapshot = snapshotCloudManager.getSnapshot(true);
    SnapshotCloudManager snapshotCloudManager1 = new SnapshotCloudManager(snapshot);
    assertClusterStateEquals(realManager.getClusterStateProvider().getClusterState(), snapshotCloudManager.getClusterStateProvider().getClusterState());
    assertClusterStateEquals(realManager.getClusterStateProvider().getClusterState(), snapshotCloudManager1.getClusterStateProvider().getClusterState());
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
    snapshotCloudManager.saveSnapshot(tmpDir, true);
    SnapshotCloudManager snapshotCloudManager1 = SnapshotCloudManager.readSnapshot(tmpDir);
    assertClusterStateEquals(snapshotCloudManager.getClusterStateProvider().getClusterState(), snapshotCloudManager1.getClusterStateProvider().getClusterState());
    assertNodeStateProvider(snapshotCloudManager, snapshotCloudManager1);
    assertDistribStateManager(snapshotCloudManager.getDistribStateManager(), snapshotCloudManager1.getDistribStateManager());
  }

  @Test
  public void testSimulatorFromSnapshot() throws Exception {
    Path tmpPath = createTempDir();
    File tmpDir = tmpPath.toFile();
    SnapshotCloudManager snapshotCloudManager = new SnapshotCloudManager(realManager, null);
    snapshotCloudManager.saveSnapshot(tmpDir, true);
    SnapshotCloudManager snapshotCloudManager1 = SnapshotCloudManager.readSnapshot(tmpDir);
    try (SimCloudManager simCloudManager = SimCloudManager.createCluster(snapshotCloudManager1, null, TimeSource.get("simTime:50"))) {
      assertClusterStateEquals(snapshotCloudManager.getClusterStateProvider().getClusterState(), simCloudManager.getClusterStateProvider().getClusterState());
      assertNodeStateProvider(snapshotCloudManager, simCloudManager);
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

  private static void assertNodeStateProvider(SolrCloudManager oneMgr, SolrCloudManager twoMgr) throws Exception {
    NodeStateProvider one = oneMgr.getNodeStateProvider();
    NodeStateProvider two = twoMgr.getNodeStateProvider();
    for (String node : oneMgr.getClusterStateProvider().getLiveNodes()) {
      Map<String, Object> oneVals = one.getNodeValues(node, SimUtils.COMMON_NODE_TAGS);
      Map<String, Object> twoVals = two.getNodeValues(node, SimUtils.COMMON_NODE_TAGS);
      oneVals = Utils.getDeepCopy(oneVals, 10, false, true);
      twoVals = Utils.getDeepCopy(twoVals, 10, false, true);
      assertEquals(Utils.toJSONString(oneVals), Utils.toJSONString(twoVals));
      Map<String, Map<String, List<ReplicaInfo>>> oneInfos = one.getReplicaInfo(node, SimUtils.COMMON_REPLICA_TAGS);
      Map<String, Map<String, List<ReplicaInfo>>> twoInfos = two.getReplicaInfo(node, SimUtils.COMMON_REPLICA_TAGS);
      assertEquals(Utils.fromJSON(Utils.toJSON(oneInfos)), Utils.fromJSON(Utils.toJSON(twoInfos)));
    }
  }

  // ignore these because SimCloudManager always modifies them
  private static final Set<Pattern> IGNORE_PATTERNS = new HashSet<>(Arrays.asList(
      Pattern.compile("/autoscaling/triggerState.*"),
      Pattern.compile("/clusterstate\\.json"), // different format in SimClusterStateProvider
      Pattern.compile("/collections/[^/]+?/leader_elect/.*"),
      Pattern.compile("/collections/[^/]+?/leaders/.*"),
      Pattern.compile("/live_nodes/.*")
  ));

  private static final Predicate<String> FILTER_FUN = p -> {
    for (Pattern pattern : IGNORE_PATTERNS) {
      if (pattern.matcher(p).matches()) {
        return false;
      }
    }
    return true;
  };

  private static void assertDistribStateManager(DistribStateManager one, DistribStateManager two) throws Exception {
    List<String> treeOne = new ArrayList<>(one.listTree("/").stream()
        .filter(FILTER_FUN).collect(Collectors.toList()));
    List<String> treeTwo = new ArrayList<>(two.listTree("/").stream()
        .filter(FILTER_FUN).collect(Collectors.toList()));
    Collections.sort(treeOne);
    Collections.sort(treeTwo);
    assertEquals(treeOne, treeTwo);
    for (String path : treeOne) {
      VersionedData vd1 = one.getData(path);
      VersionedData vd2 = two.getData(path);
      assertEquals(path, vd1, vd2);
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
          assertReplicaEquals(oneReplica, twoReplica);
        }
      });
    });
  }

  private static void assertReplicaEquals(Replica one, Replica two) {
    assertEquals(one.getName(), two.getName());
    assertEquals(one.getNodeName(), two.getNodeName());
    assertEquals(one.getState(), two.getState());
    assertEquals(one.getType(), two.getType());
    Map<String, Object> filteredPropsOne = one.getProperties().entrySet().stream()
        .filter(e -> !(e.getKey().startsWith("INDEX") || e.getKey().startsWith("QUERY") || e.getKey().startsWith("UPDATE")))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<String, Object> filteredPropsTwo = two.getProperties().entrySet().stream()
        .filter(e -> !(e.getKey().startsWith("INDEX") || e.getKey().startsWith("QUERY") || e.getKey().startsWith("UPDATE")))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    assertEquals(filteredPropsOne, filteredPropsTwo);
  }

}
