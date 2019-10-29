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

package org.apache.solr.client.solrj.cloud.autoscaling;


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.CLUSTER_PREFERENCES;
import static org.apache.solr.client.solrj.cloud.autoscaling.TestPolicy2.loadFromResource;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.CORES;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.FREEDISK;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.REPLICA;
import static org.apache.solr.common.cloud.ZkStateReader.CLUSTER_STATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.V2RequestSupport;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester.Hint;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JsonTextWriter;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SolrJSONWriter;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.response.JSONWriter;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class TestPolicy extends SolrTestCaseJ4 {
  boolean useNodeset ;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public TestPolicy(){
    useNodeset = true;
  }
  static Suggester createSuggester(SolrCloudManager cloudManager, Map jsonObj, Suggester seed) throws IOException, InterruptedException {
    Policy.Session session = null;
    if (seed != null) session = seed.session;
    else {
      session = cloudManager.getDistribStateManager().getAutoScalingConfig().getPolicy().createSession(cloudManager);
    }

    Map m = (Map) jsonObj.get("suggester");
    Suggester result = session.getSuggester(CollectionParams.CollectionAction.get((String) m.get("action")));
    m = (Map) m.get("hints");
    m.forEach((k, v) -> {
      Hint hint = Hint.get(k.toString());
      result.hint(hint, hint.parse(v));
    });
    return result;
  }

  static SolrCloudManager createCloudManager(Map jsonObj) {
    return cloudManagerWithData(jsonObj);
  }

  public static String clusterState = Utils.toJSONString(loadFromResource("testPolicy.json"));

  public static Map<String, Map<String, List<ReplicaInfo>>> getReplicaDetails(String node, Map clusterState) {
    ValidatingJsonMap m = ValidatingJsonMap
        .getDeepCopy(clusterState, 6, true);
    Map<String, Map<String, List<ReplicaInfo>>> result = new LinkedHashMap<>();

    m.forEach((collName, o) -> {
      ValidatingJsonMap coll = (ValidatingJsonMap) o;
      coll.getMap("shards").forEach((shard, o1) -> {
        ValidatingJsonMap sh = (ValidatingJsonMap) o1;
        sh.getMap("replicas").forEach((replicaName, o2) -> {
          ValidatingJsonMap r = (ValidatingJsonMap) o2;
          String node_name = (String) r.get("node_name");
          if (!node_name.equals(node)) return;
          Map<String, List<ReplicaInfo>> shardVsReplicaStats = result.computeIfAbsent(collName, k -> new HashMap<>());
          List<ReplicaInfo> replicaInfos = shardVsReplicaStats.computeIfAbsent(shard, k -> new ArrayList<>());
          replicaInfos.add(new ReplicaInfo(replicaName, (String) r.get("core"), collName, shard,
              Replica.Type.get((String) r.get(ZkStateReader.REPLICA_TYPE)), node, r));
        });
      });
    });
    return result;
  }


  public void testWithCollection() {
    ClusterState clusterState = ClusterState.load(1,
        (Map) loadFromResource("testWithCollection.json"),
        ImmutableSet.of("node1", "node2", "node3", "node4", "node5"), CLUSTER_STATE);
    DelegatingClusterStateProvider clusterStateProvider = new DelegatingClusterStateProvider(null) {
      @Override
      public ClusterState getClusterState() throws IOException {
        return clusterState;
      }

      @Override
      public Set<String> getLiveNodes() {
        return clusterState.getLiveNodes();
      }
    };

    SolrClientNodeStateProvider solrClientNodeStateProvider = new SolrClientNodeStateProvider(null) {
      @Override
      protected Map<String, Object> fetchTagValues(String node, Collection<String> tags) {
        Map<String, Object> result = new HashMap<>();
        AtomicInteger cores = new AtomicInteger();
        forEachReplica(node, replicaInfo -> cores.incrementAndGet());
        if (tags.contains(ImplicitSnitch.CORES)) result.put(ImplicitSnitch.CORES, cores.get());
        if (tags.contains(ImplicitSnitch.DISK)) result.put(ImplicitSnitch.DISK, 100);
        return result;
      }

      @Override
      protected Map<String, Object> fetchReplicaMetrics(String solrNode, Map<String, Pair<String, ReplicaInfo>> metricsKeyVsTagReplica) {
        //e.g: solr.core.perReplicaDataColl.shard1.replica_n4:INDEX.sizeInBytes
        Map<String, Object> result = new HashMap<>();
        metricsKeyVsTagReplica.forEach((k, v) -> {
          if (k.endsWith(":INDEX.sizeInBytes")) result.put(k, 100);
        });
        return result;
      }

      @Override
      protected ClusterStateProvider getClusterStateProvider() {
        return clusterStateProvider;
      }
    };
    Map m = solrClientNodeStateProvider.getNodeValues("node1", ImmutableSet.of("cores", "withCollection"));
    assertNotNull(m.get("withCollection"));

    Map policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'minimize': 'cores'}," +
        "    { 'maximize': 'freedisk', 'precision': 50}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}" +
        "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "  ]" +
        "}");
    AutoScalingConfig config = new AutoScalingConfig(policies);
    Policy policy = config.getPolicy();
    Policy.Session session = policy.createSession(new DelegatingCloudManager(null) {
      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return clusterStateProvider;
      }

      @Override
      public NodeStateProvider getNodeStateProvider() {
        return solrClientNodeStateProvider;
      }
    });
    Suggester suggester = session.getSuggester(CollectionAction.ADDREPLICA);
    suggester.hint(Hint.COLL_SHARD, new Pair<>("comments_coll", "shard1"));
    SolrRequest op = suggester.getSuggestion();
    assertNotNull(op);
    Set<String> nodes = new HashSet<>(2);
    nodes.add(op.getParams().get("node"));
    session = suggester.getSession();
    suggester = session.getSuggester(ADDREPLICA);
    suggester.hint(Hint.COLL_SHARD, new Pair<>("comments_coll", "shard1"));
    op = suggester.getSuggestion();
    assertNotNull(op);
    nodes.add(op.getParams().get("node"));
    assertEquals(2, nodes.size());
    assertTrue("node1 should have been selected by add replica", nodes.contains("node1"));
    assertTrue("node2 should have been selected by add replica", nodes.contains("node2"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA);
    suggester.hint(Hint.COLL_SHARD, new Pair<>("comments_coll", "shard1"));
    op = suggester.getSuggestion();
    assertNull(op);
  }

  public void testWithCollectionSuggestions() {
    ClusterState clusterState =
        ClusterState.load(1,
            (Map) loadFromResource("testWithCollectionSuggestions.json"),
            ImmutableSet.of("node1", "node2", "node3", "node4", "node5"), CLUSTER_STATE);
    DelegatingClusterStateProvider clusterStateProvider = new DelegatingClusterStateProvider(null) {
      @Override
      public ClusterState getClusterState() throws IOException {
        return clusterState;
      }

      @Override
      public Set<String> getLiveNodes() {
        return clusterState.getLiveNodes();
      }
    };

    SolrClientNodeStateProvider solrClientNodeStateProvider = new SolrClientNodeStateProvider(null) {
      @Override
      protected Map<String, Object> fetchTagValues(String node, Collection<String> tags) {
        Map<String, Object> result = new HashMap<>();
        AtomicInteger cores = new AtomicInteger();
        forEachReplica(node, replicaInfo -> cores.incrementAndGet());
        if (tags.contains(ImplicitSnitch.CORES)) result.put(ImplicitSnitch.CORES, cores.get());
        if (tags.contains(ImplicitSnitch.DISK)) result.put(ImplicitSnitch.DISK, 100);
        return result;
      }

      @Override
      protected Map<String, Object> fetchReplicaMetrics(String solrNode, Map<String, Pair<String, ReplicaInfo>> metricsKeyVsTagReplica) {
        //e.g: solr.core.perReplicaDataColl.shard1.replica_n4:INDEX.sizeInBytes
        Map<String, Object> result = new HashMap<>();
        metricsKeyVsTagReplica.forEach((k, v) -> {
          if (k.endsWith(":INDEX.sizeInBytes")) result.put(k, 100);
        });
        return result;
      }

      @Override
      protected ClusterStateProvider getClusterStateProvider() {
        return clusterStateProvider;
      }
    };
    Map m = solrClientNodeStateProvider.getNodeValues("node1", ImmutableSet.of("cores", "withCollection"));
    assertNotNull(m.get("withCollection"));

    Map policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores'}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}" +
        "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "  ]" +
        "}");

    List<Suggester.SuggestionInfo> l = PolicyHelper.getSuggestions(new AutoScalingConfig(policies),
        new DelegatingCloudManager(null) {
          @Override
          public ClusterStateProvider getClusterStateProvider() {
            return clusterStateProvider;
          }

          @Override
          public NodeStateProvider getNodeStateProvider() {
            return solrClientNodeStateProvider;
          }
        });
    assertNotNull(l);
    assertEquals(2, l.size());

    // collect the set of nodes to which replicas are being added
    Set<String> nodes = new HashSet<>(2);

    assertEquals(1.0d, l.get(0)._get("violation/violation/delta", null));
    assertEquals("POST", l.get(0)._get("operation/method", null));
    assertEquals("/c/articles_coll/shards", l.get(0)._get("operation/path", null));
    assertNotNull(l.get(0)._get("operation/command/add-replica", null));
    nodes.add((String) l.get(0)._get("operation/command/add-replica/node", null));

    assertEquals(1.0d, l.get(1)._get("violation/violation/delta", null));
    assertEquals("POST", l.get(1)._get("operation/method", null));
    assertEquals("/c/articles_coll/shards", l.get(1)._get("operation/path", null));
    assertNotNull(l.get(1)._get("operation/command/add-replica", null));
    nodes.add((String) l.get(1)._get("operation/command/add-replica/node", null));

    assertEquals(2, nodes.size());
    assertTrue(nodes.contains("node1"));
    assertTrue(nodes.contains("node2"));
  }

  public void testWithCollectionMoveVsAddSuggestions() throws IOException {
    ClusterState clusterState = ClusterState.load(1,
        (Map) loadFromResource("testWithCollectionMoveVsAddSuggestions.json"),
        ImmutableSet.of("node1", "node2", "node3", "node4", "node5", "node6"),
        CLUSTER_STATE
    );
    DelegatingClusterStateProvider clusterStateProvider = new DelegatingClusterStateProvider(null) {
      @Override
      public ClusterState getClusterState() {
        return clusterState;
      }

      @Override
      public Set<String> getLiveNodes() {
        return clusterState.getLiveNodes();
      }
    };

    SolrClientNodeStateProvider solrClientNodeStateProvider = new SolrClientNodeStateProvider(null) {
      @Override
      protected Map<String, Object> fetchTagValues(String node, Collection<String> tags) {
        Map<String, Object> result = new HashMap<>();
        AtomicInteger cores = new AtomicInteger();
        forEachReplica(node, replicaInfo -> cores.incrementAndGet());
        if (tags.contains(ImplicitSnitch.CORES)) result.put(ImplicitSnitch.CORES, cores.get());
        if (tags.contains(ImplicitSnitch.DISK)) result.put(ImplicitSnitch.DISK, 100);
        return result;
      }

      @Override
      protected Map<String, Object> fetchReplicaMetrics(String solrNode, Map<String, Pair<String, ReplicaInfo>> metricsKeyVsTagReplica) {
        //e.g: solr.core.perReplicaDataColl.shard1.replica_n4:INDEX.sizeInBytes
        Map<String, Object> result = new HashMap<>();
        metricsKeyVsTagReplica.forEach((k, v) -> {
          if (k.endsWith(":INDEX.sizeInBytes")) result.put(k, 100);
        });
        return result;
      }

      @Override
      protected ClusterStateProvider getClusterStateProvider() {
        return clusterStateProvider;
      }
    };
    Map m = solrClientNodeStateProvider.getNodeValues("node1", ImmutableSet.of("cores", "withCollection"));
    assertNotNull(m.get("withCollection"));

    Map policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores'}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}" +
        "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "  ]" +
        "}");

    List<Suggester.SuggestionInfo> l = PolicyHelper.getSuggestions(new AutoScalingConfig(policies),
        new DelegatingCloudManager(null) {
          @Override
          public ClusterStateProvider getClusterStateProvider() {
            return clusterStateProvider;
          }

          @Override
          public NodeStateProvider getNodeStateProvider() {
            return solrClientNodeStateProvider;
          }
        });
    assertNotNull(l);
    assertEquals(3, l.size());

    // collect the set of nodes to which replicas are being added
    Set<String> nodes = new HashSet<>(2);

    int numMoves = 0, numAdds = 0;
    Set<String> addNodes = new HashSet<>();
    Set<String> targetNodes = new HashSet<>();
    Set<String> movedReplicas = new HashSet<>();
    for (Suggester.SuggestionInfo suggestionInfo : l) {
      assertEquals("POST", suggestionInfo._get("operation/method", null));
      if (suggestionInfo._get("operation/command/add-replica", null) != null) {
        numAdds++;
        assertEquals(1.0d, suggestionInfo._get("violation/violation/delta", null));
        assertEquals("/c/articles_coll/shards", suggestionInfo._get("operation/path", null));
        addNodes.add((String) suggestionInfo._get("operation/command/add-replica/node", null));
      } else if (suggestionInfo._get("operation/command/move-replica", null) != null) {
        numMoves++;
        assertEquals("/c/articles_coll", suggestionInfo._get("operation/path", null));
        targetNodes.add((String) suggestionInfo._get("operation/command/move-replica/targetNode", null));
        movedReplicas.add((String) suggestionInfo._get("operation/command/move-replica/replica", null));
      } else {
        fail("Unexpected operation type suggested for suggestion: " + suggestionInfo);
      }
    }

    assertEquals(2, targetNodes.size());
    assertEquals(1, addNodes.size());
    assertEquals(2, movedReplicas.size());
    Set<String> allTargetNodes = new HashSet<>(targetNodes);
    allTargetNodes.addAll(addNodes);
    assertEquals(3, allTargetNodes.size());
    assertTrue(allTargetNodes.contains("node3"));
    assertTrue(allTargetNodes.contains("node4"));
    assertTrue(allTargetNodes.contains("node5"));
  }

  public void testWithCollectionMoveReplica() {
    ClusterState clusterState = ClusterState.load(1,
        (Map) loadFromResource("testWithCollectionMoveReplica.json"),
        ImmutableSet.of("node2", "node3", "node4", "node5"), CLUSTER_STATE);
    DelegatingClusterStateProvider clusterStateProvider = new DelegatingClusterStateProvider(null) {
      @Override
      public ClusterState getClusterState() throws IOException {
        return clusterState;
      }

      @Override
      public Set<String> getLiveNodes() {
        return clusterState.getLiveNodes();
      }
    };

    SolrClientNodeStateProvider solrClientNodeStateProvider = new SolrClientNodeStateProvider(null) {
      @Override
      protected Map<String, Object> fetchTagValues(String node, Collection<String> tags) {
        Map<String, Object> result = new HashMap<>();
        AtomicInteger cores = new AtomicInteger();
        forEachReplica(node, replicaInfo -> cores.incrementAndGet());
        if (tags.contains(ImplicitSnitch.CORES)) result.put(ImplicitSnitch.CORES, cores.get());
        if (tags.contains(ImplicitSnitch.DISK)) result.put(ImplicitSnitch.DISK, 100);
        return result;
      }

      @Override
      protected Map<String, Object> fetchReplicaMetrics(String solrNode, Map<String, Pair<String, ReplicaInfo>> metricsKeyVsTagReplica) {
        //e.g: solr.core.perReplicaDataColl.shard1.replica_n4:INDEX.sizeInBytes
        Map<String, Object> result = new HashMap<>();
        metricsKeyVsTagReplica.forEach((k, v) -> {
          if (k.endsWith(":INDEX.sizeInBytes")) result.put(k, 100);
        });
        return result;
      }

      @Override
      protected ClusterStateProvider getClusterStateProvider() {
        return clusterStateProvider;
      }
    };
    Map m = solrClientNodeStateProvider.getNodeValues("node1", ImmutableSet.of("cores", "withCollection"));
    assertNotNull(m.get("withCollection"));

    Map policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'minimize': 'cores'}," +
        "    { 'maximize': 'freedisk', 'precision': 50}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}" +
        "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "  ]" +
        "}");
    AutoScalingConfig config = new AutoScalingConfig(policies);
    Policy policy = config.getPolicy();
    Policy.Session session = policy.createSession(new DelegatingCloudManager(null) {
      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return clusterStateProvider;
      }

      @Override
      public NodeStateProvider getNodeStateProvider() {
        return solrClientNodeStateProvider;
      }
    });
    Suggester suggester = session.getSuggester(CollectionAction.MOVEREPLICA);
    suggester.hint(Hint.COLL_SHARD, new Pair<>("comments_coll", "shard1"));
    suggester.hint(Hint.SRC_NODE, "node1");
    SolrRequest op = suggester.getSuggestion();
    assertNotNull(op);
    assertEquals("node2 should have been selected by move replica", "node2",
        op.getParams().get("targetNode"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA);
    suggester.hint(Hint.COLL_SHARD, new Pair<>("comments_coll", "shard1"));
    suggester.hint(Hint.SRC_NODE, "node1");
    op = suggester.getSuggestion();
    assertNull(op);
  }

  public void testValidate() {
    expectError("replica", -1, "must be greater than");
    expectError("replica", "hello", "not a valid number");
    assertEquals(1d, Clause.validate("replica", "1", true));
    assertEquals("c", Clause.validate("collection", "c", true));
    assertEquals("s", Clause.validate("shard", "s", true));
    assertEquals("overseer", Clause.validate("nodeRole", "overseer", true));

    expectError("nodeRole", "wrong", "must be one of");

    expectError("sysLoadAvg", "101", "must be less than ");
    expectError("sysLoadAvg", 101, "must be less than ");
    expectError("sysLoadAvg", "-1", "must be greater than");
    expectError("sysLoadAvg", -1, "must be greater than");

    assertEquals(12.46d, Clause.validate("sysLoadAvg", "12.46", true));
    assertEquals(12.46, Clause.validate("sysLoadAvg", 12.46d, true));


    expectError("ip_1", "300", "must be less than ");
    expectError("ip_1", 300, "must be less than ");
    expectError("ip_1", "-1", "must be greater than");
    expectError("ip_1", -1, "must be greater than");

    assertEquals(1L, Clause.validate("ip_1", "1", true));

    expectError("heapUsage", "-1", "must be greater than");
    expectError("heapUsage", -1, "must be greater than");
    assertEquals(69.9d, Clause.validate("heapUsage", "69.9", true));
    assertEquals(69.9d, Clause.validate("heapUsage", 69.9d, true));

    expectError("port", "70000", "must be less than ");
    expectError("port", 70000, "must be less than ");
    expectError("port", "0", "must be greater than");
    expectError("port", 0, "must be greater than");

    expectError("cores", "-1", "must be greater than");

    assertEquals(Operand.EQUAL, REPLICA.getOperand(Operand.EQUAL, "2.0", null));
    assertEquals(Operand.NOT_EQUAL, REPLICA.getOperand(Operand.NOT_EQUAL, "2.0", null));
    assertEquals(Operand.EQUAL, REPLICA.getOperand(Operand.EQUAL, "2", null));
    assertEquals(Operand.NOT_EQUAL, REPLICA.getOperand(Operand.NOT_EQUAL, "2", null));
    assertEquals(Operand.RANGE_EQUAL, REPLICA.getOperand(Operand.EQUAL, "2.1", null));
    assertEquals(Operand.RANGE_EQUAL, REPLICA.getOperand(Operand.EQUAL, "2.01", null));

    Clause clause = Clause.create("{replica: '1.23', node:'#ANY'}");
    assertTrue(clause.getReplica().isPass(2));
    assertTrue(clause.getReplica().isPass(1));
    assertFalse(clause.getReplica().isPass(0));
    assertFalse(clause.getReplica().isPass(3));

    clause = Clause.create("{replica: '<1.23', node:'#ANY'}");
    assertTrue(clause.getReplica().isPass(1));
    assertTrue(clause.getReplica().isPass(0));
    assertFalse(clause.getReplica().isPass(2));

    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: '!1.23', node:'#ANY'}"));


    clause = Clause.create("{replica: 1.23, node:'#ANY'}");
    assertTrue(clause.getReplica().isPass(2));
    assertTrue(clause.getReplica().isPass(1));
    assertFalse(clause.getReplica().isPass(0));
    assertFalse(clause.getReplica().isPass(3));

    clause = Clause.create("{replica: '33%', node:'#ANY'}");
    assertEquals(Operand.RANGE_EQUAL, clause.getReplica().op);
    clause = clause.getSealedClause(condition -> {
      if (condition.name.equals("replica")) {
        return 2.0d;
      }
      throw new RuntimeException("");
    });
    assertTrue(clause.getReplica().isPass(2));

    clause = Clause.create("{replica: '3 - 5', node:'#ANY'}");
    assertEquals(Operand.RANGE_EQUAL, clause.getReplica().getOperand());
    RangeVal range = (RangeVal) clause.getReplica().getValue();
    assertEquals(3.0, range.min);
    assertEquals(5.0, range.max);
    assertTrue(clause.replica.isPass(3));
    assertTrue(clause.replica.isPass(4));
    assertTrue(clause.replica.isPass(5));
    assertFalse(clause.replica.isPass(6));
    assertFalse(clause.replica.isPass(2));

    assertEquals(Double.valueOf(1.0), clause.replica.delta(6));
    assertEquals(Double.valueOf(-1.0), clause.replica.delta(2));
    assertEquals(Double.valueOf(0.0), clause.replica.delta(4));

    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: '-33%', node:'#ANY'}"));
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: 'x%', node:'#ANY'}"));
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: '20%-33%', node:'#ANY'}"));

    clause = Clause.create("{replica: '#EQUAL', shard:'#EACH', node:'#ANY'}");
    assertEquals(Operand.RANGE_EQUAL, clause.replica.op);
    clause = Clause.create("{replica: '#EQUAL', node:'#ANY'}");
    assertEquals(Operand.RANGE_EQUAL, clause.replica.op);
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: '#EQUAL', node:'node_1'}"));
    clause = Clause.create("{replica : 0, freedisk:'<20%'}");
    assertEquals(clause.tag.computedType, ComputedType.PERCENT);
    assertEquals(clause.tag.op, Operand.LESS_THAN);
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica : 0, INDEX.sizeInGB:'>300'}"));
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica:'<3', shard: '#ANV', node:'#ANY'}"));
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica:'<3', shard: '#EACH', node:'#E4CH'}"));
    try {
      Clause.create("{replica:0, 'ip_1':'<30%'}");
      fail("Expected exception");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("'%' is not allowed for variable :  'ip_1'"));
    }

    clause = Clause.create("{replica: '#ALL',  freedisk:'>20%'}");
    clause = Clause.create("{replica: '#ALL',  sysprop.zone :'west'}");
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: [3,4] ,  freedisk:'>20'}"));
    clause = Clause.create("{replica: 3 ,  port:[8983, 7574]}");
    assertEquals(Operand.IN, clause.tag.op);
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: 3 ,  sysprop.zone :['east', ' ', 'west']}"));
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: 3 ,  sysprop.zone :[]}"));
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: 3 ,  sysprop.zone :['!east','west']}"));
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: '#ALL' , shard: '#EACH' , sysprop.zone:[east, west]}"));
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: '#ALL' , shard: '#EACH' , sysprop.zone:'#EACH'}"));
    clause = Clause.create("{replica: '#EQUAL' , shard: '#EACH' , sysprop.zone:[east, west]}");
    assertEquals(ComputedType.EQUAL, clause.replica.computedType);
    assertEquals(Operand.IN, clause.tag.op);
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: '#EQUAL' , shard: '#EACH' , sysprop.zone:[east]}"));

    clause = Clause.create("{cores: '#EQUAL' , node:'#ANY'}");
    assertEquals(ComputedType.EQUAL, clause.globalTag.computedType);
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{cores: '#EQUAL' , node:'node1'}"));

    clause = Clause.create("{cores: '#EQUAL' , node:[node1 , node2 , node3]}");
    assertEquals(Operand.IN, clause.getTag().op);
    assertEquals(ComputedType.EQUAL, clause.getGlobalTag().computedType);

    clause = Clause.create("{cores: '3-5' , node:'#ANY'}");
    assertEquals(Operand.RANGE_EQUAL, clause.globalTag.op);
    assertEquals(3.0d, ((RangeVal) clause.globalTag.val).min.doubleValue(), 0.001);
    assertEquals(5.0d, ((RangeVal) clause.globalTag.val).max.doubleValue(), 0.001);

    clause = Clause.create("{cores: 1.66 , node:'#ANY'}");
    assertEquals(Operand.RANGE_EQUAL, clause.globalTag.op);
    assertEquals(1.0d, ((RangeVal) clause.globalTag.val).min.doubleValue(), 0.001);
    assertEquals(2.0d, ((RangeVal) clause.globalTag.val).max.doubleValue(), 0.001);
    assertEquals(1.66d, ((RangeVal) clause.globalTag.val).actual.doubleValue(), 0.001);

    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{cores:5, sysprop.zone : west}"));

    clause = Clause.create("{cores: '14%' , node:'#ANY'}");
    assertEquals(ComputedType.PERCENT, clause.getGlobalTag().computedType);

    clause = Clause.create("{cores: '14%' , node:[node1, node2, node3]}");
    assertEquals(Operand.IN, clause.getTag().op);

    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: '!14%' , node:'#ANY'}"));

    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{cores: '!14%' , node:[node1, node2, node3]}"));

    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{cores: '!1.66' , node:'#ANY'}"));
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: '<14%' , node:'#ANY'}"));
    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica: '>14%' , node:'#ANY'}"));

    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{cores: '>14%' , node:'#ANY'}"));
     clause  = Clause.create("{replica:1, nodeset : {sysprop.zone : east}}");
     assertEquals(Variable.Type.SYSPROP, clause.tag.varType);
     clause  =Clause.create("{replica:1, nodeset : [node1, node2, node3]}");
     assertEquals(Variable.Type.NODE, clause.tag.varType);
     assertEquals(Operand.IN, clause.tag.op);

    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica:1, nodeset : {sysprop.zone : '#EACH'}}"));

    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica:1, nodeset : {host : '#EACH'}}"));

    expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica:1, node: n1, nodeset : {sysprop.zone : east}}"));

    IllegalArgumentException exp = expectThrows(IllegalArgumentException.class,
        () -> Clause.create("{replica:1, nodeset : {sysprop.zone : east  , port: 8983 }}"));
    assertTrue(exp.getMessage().contains("nodeset must only have one and only one key"));
    clause = Clause.create("{'replica': '#ALL', 'nodeset': {'freedisk': '>700'}, 'strict': false}");
    assertEquals(clause.put, Clause.Put.ON_ALL);
    assertEquals(Operand.GREATER_THAN , clause.tag.op);
    clause = Clause.create("{'replica': '#ALL', put: on-each-node,  'nodeset': {sysprop.zone : east}}");
    assertEquals(clause.put, Clause.Put.ON_EACH);
    exp = expectThrows(IllegalArgumentException.class, ()-> Clause.create("{'replica': '#ALL', put: on-Each,  'nodeset': {sysprop.zone : east}}"));
    assertTrue(exp.getMessage().contains("invalid value for put : on-Each"));

    clause = Clause.create("{replica : '#EQUAL', nodeset : [{sysprop.zone : east}, {sysprop.zone : west}]}");
    assertTrue(((List)clause.tag.val).get(0) instanceof Condition);
    assertTrue( Utils.fromJSONString(Utils.toJSONString(clause)) instanceof Map);
    exp = expectThrows(IllegalArgumentException.class, ()-> Clause.create("{replica : '#EQUAL', nodeset : [{sysprop.zone : east}, {port : '8983'}]}"));
    assertTrue(exp.getMessage().contains("all element must have same key"));
    exp = expectThrows(IllegalArgumentException.class, ()-> Clause.create("{replica : '#EQUAL', nodeset : [{sysprop.zone : east}, {sysprop.zone : '#EACH'}]}"));
    assertTrue(exp.getMessage().contains("computed  value #EACH not allowed in nodeset"));
    exp = expectThrows(IllegalArgumentException.class, ()->  Clause.create("{replica : '#EQUAL', nodeset : {sysprop.zone : east}}"));
    assertTrue(exp.getMessage().contains("'nodeset' must have an array value when 'replica': '#EQUAL` is used"));
    exp = expectThrows(IllegalArgumentException.class, ()-> Clause.create("{replica : '#ALL', nodeset : [{sysprop.zone : east}, {sysprop.zone : west}]}"));
    assertTrue(exp.getMessage().contains("cannot use array value for nodeset if replica : '#EQUAL' is not used"));
    exp = expectThrows(IllegalArgumentException.class, ()-> Clause.create("{replica : '50%', nodeset : [{sysprop.zone : east}, {sysprop.zone : west}]}"));
    assertTrue(exp.getMessage().contains("cannot use array value for nodeset if replica : '#EQUAL' is not used"));
    exp = expectThrows(IllegalArgumentException.class, ()-> Clause.create("{replica : 3, nodeset : [{sysprop.zone : east}, {sysprop.zone : west}]}"));
    assertTrue(exp.getMessage().contains("cannot use array value for nodeset if replica : '#EQUAL' is not used"));
    exp = expectThrows(IllegalArgumentException.class, ()-> Clause.create("{replica : '#EQUAL', put: on-each-node, nodeset : [{sysprop.zone : east}, {sysprop.zone : west}]}"));
    assertTrue(exp.getMessage().contains("cannot use put: 'on-each-node'  with an array value in nodeset "));
  }


  public void testEqualFunction() {

    String clusterStateStr = "{" +
        "  'coll1': {" +
        "    'router': {" +
        "      'name': 'compositeId'" +
        "    }," +
        "    'shards': {" +
        "      'shard1': {" +
        "        'range': '80000000-ffffffff'," +
        "        'replicas': {" +
        "          'r1': {" +
        "            'core': 'r1'," +
        "            'base_url': 'http://10.0.0.4:8983/solr'," +
        "            'node_name': 'node1'," +
        "            'state': 'active'," +
        "            'leader': 'true'" +
        "          }," +
        "          'r2': {" +
        "            'core': 'r2'," +
        "            'base_url': 'http://10.0.0.4:7574/solr'," +
        "            'node_name': 'node2'," +
        "            'state': 'active'" +
        "          }" +
        "        }" +
        "      }," +
        "      'shard2': {" +
        "        'range': '0-7fffffff'," +
        "        'replicas': {" +
        "          'r3': {" +
        "            'core': 'r3'," +
        "            'base_url': 'http://10.0.0.4:8983/solr'," +
        "            'node_name': 'node1'," +
        "            'state': 'active'," +
        "            'leader': 'true'" +
        "          }," +
        "          'r4': {" +
        "            'core': 'r4'," +
        "            'base_url': 'http://10.0.0.4:8987/solr'," +
        "            'node_name': 'node4'," +
        "            'state': 'active'" +
        "          }," +
        "          'r6': {" +
        "            'core': 'r6'," +
        "            'base_url': 'http://10.0.0.4:8989/solr'," +
        "            'node_name': 'node3'," +
        "            'state': 'active'" +
        "          }," +
        "          'r5': {" +
        "            'core': 'r5'," +
        "            'base_url': 'http://10.0.0.4:8983/solr'," +
        "            'node_name': 'node1'," +
        "            'state': 'active'" +
        "          }" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}";


    ClusterState clusterState = ClusterState.load(1, clusterStateStr.getBytes(UTF_8),
        ImmutableSet.of("node1", "node2", "node3", "node4", "node5"));
    DelegatingClusterStateProvider clusterStateProvider = new DelegatingClusterStateProvider(null) {
      @Override
      public ClusterState getClusterState() throws IOException {
        return clusterState;
      }

      @Override
      public Set<String> getLiveNodes() {
        return clusterState.getLiveNodes();
      }
    };

    SolrClientNodeStateProvider solrClientNodeStateProvider = new SolrClientNodeStateProvider(null) {
      @Override
      protected Map<String, Object> fetchTagValues(String node, Collection<String> tags) {
        Map<String, Object> result = new HashMap<>();
        AtomicInteger cores = new AtomicInteger();
        forEachReplica(node, replicaInfo -> cores.incrementAndGet());
        if (tags.contains(ImplicitSnitch.CORES)) result.put(ImplicitSnitch.CORES, cores.get());
        if (tags.contains(ImplicitSnitch.DISK)) result.put(ImplicitSnitch.DISK, 100);
        return result;
      }

      @Override
      protected Map<String, Object> fetchReplicaMetrics(String node, Map<String, Pair<String, ReplicaInfo>> metricsKeyVsTagReplica) {
        //e.g: solr.core.perReplicaDataColl.shard1.replica_n4:INDEX.sizeInBytes
        Map<String, Object> result = new HashMap<>();
        metricsKeyVsTagReplica.forEach((k, v) -> {
          if (k.endsWith(":INDEX.sizeInBytes")) result.put(k, 100);
        });

        return result;
      }

      @Override
      protected ClusterStateProvider getClusterStateProvider() {
        return clusterStateProvider;
      }
    };

    Map policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'minimize': 'cores', 'precision': 50}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': '#EQUAL', 'node': '#ANY'}," +
        "  ]" +
        "}");
    AutoScalingConfig config = new AutoScalingConfig(policies);
    Policy policy = config.getPolicy();
    Policy.Session session = policy.createSession(new DelegatingCloudManager(null) {
      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return clusterStateProvider;
      }

      @Override
      public NodeStateProvider getNodeStateProvider() {
        return solrClientNodeStateProvider;
      }
    });
    List<Violation> violations = session.getViolations();
    assertEquals(2, violations.size());
    for (Violation violation : violations) {
      if (violation.node.equals("node1")) {
        RangeVal val = (RangeVal) violation.getClause().replica.val;
        assertEquals(1.0d, val.min.doubleValue(), 0.01);
        assertEquals(2.0, val.max.doubleValue(), 0.01);
        assertEquals(1.2d, val.actual.doubleValue(), 0.01d);
        assertEquals(1, violation.replicaCountDelta.doubleValue(), 0.01);
        assertEquals(3, violation.getViolatingReplicas().size());
        Set<String> expected = ImmutableSet.of("r1", "r3", "r5");
        for (Violation.ReplicaInfoAndErr replicaInfoAndErr : violation.getViolatingReplicas()) {
          assertTrue(expected.contains(replicaInfoAndErr.replicaInfo.getCore()));
        }
      } else if (violation.node.equals("node5")) {
        assertEquals(-1, violation.replicaCountDelta.doubleValue(), 0.01);

      } else {
        fail();
      }
    }
//    Violation violation = violations.get(0);
//    assertEquals("node1", violation.node);


  }

  private static void expectError(String name, Object val, String msg) {
    Exception e = expectThrows(Exception.class, () -> Clause.validate(name, val, true));
    assertTrue("expected exception containing " + msg, e.getMessage().contains(msg));
  }

  public void testOperands() {
    Clause c = Clause.create("{replica:'<2', node:'#ANY'}");
    assertFalse(c.replica.isPass(3));
    assertFalse(c.replica.isPass(2));
    assertTrue(c.replica.isPass(1));
    assertEquals("{\"replica\":\"<2.0\"}", c.replica.toString());

    c = Clause.create("{replica:'>2', node:'#ANY'}");
    assertTrue(c.replica.isPass(3));
    assertFalse(c.replica.isPass(2));
    assertFalse(c.replica.isPass(1));
    assertEquals("{\"replica\":\">2.0\"}", c.replica.toString());


    c = Clause.create("{replica:0, nodeRole:'!overseer'}");
    assertTrue(c.tag.isPass("OVERSEER"));
    assertFalse(c.tag.isPass("overseer"));

    c = Clause.create("{replica:0, sysLoadAvg:'<12.7'}");
    assertTrue(c.tag.isPass("12.6"));
    assertTrue(c.tag.isPass(12.6d));
    assertFalse(c.tag.isPass("12.9"));
    assertFalse(c.tag.isPass(12.9d));

    c = Clause.create("{replica:0, sysLoadAvg:'>12.7'}");
    assertTrue(c.tag.isPass("12.8"));
    assertTrue(c.tag.isPass(12.8d));
    assertFalse(c.tag.isPass("12.6"));
    assertFalse(c.tag.isPass(12.6d));

    c = Clause.create("{replica:0, 'metrics:x:y:z':'>12.7'}");
    assertTrue(c.tag.val instanceof String);
    assertTrue(c.tag.isPass("12.8"));
    assertTrue(c.tag.isPass(12.8d));
    assertFalse(c.tag.isPass("12.6"));
    assertFalse(c.tag.isPass(12.6d));

    c = Clause.create("{replica: '<3', sysprop.zone : [east, west]}");
    assertTrue(c.tag.isPass("east"));
    assertTrue(c.tag.isPass("west"));
    assertFalse(c.tag.isPass("south"));

  }

  public void testNodeLost() {
    String dataproviderdata = " {'liveNodes':[" +
        "    '127.0.0.1:65417_solr'," +
        "    '127.0.0.1:65434_solr']," +
        "  'replicaInfo':{" +
        "    '127.0.0.1:65427_solr':{'testNodeLost':{'shard1':[{'core_node2':{type: NRT}}]}}," +
        "    '127.0.0.1:65417_solr':{'testNodeLost':{'shard1':[{'core_node1':{type: NRT}}]}}," +
        "    '127.0.0.1:65434_solr':{}}," +
        "  'nodeValues':{" +
        "    '127.0.0.1:65417_solr':{" +
        "      'node':'127.0.0.1:65417_solr'," +
        "      'cores':1," +
        "      'freedisk':884.7097854614258}," +
        "    '127.0.0.1:65434_solr':{" +
        "      'node':'127.0.0.1:65434_solr'," +
        "      'cores':0," +
        "      'freedisk':884.7097854614258}}}";
    String autoScalingjson = "{" +
        "       'cluster-policy':[" +
        "         {" +
        "           'cores':'<10'," +
        "           'node':'#ANY'}," +
        "         {" +
        "           'replica':'<2'," +
        "           'shard':'#EACH'," +
        "           'node':'#ANY'}," +
        "         {" +
        "           'nodeRole':'overseer'," +
        "           'replica':0}]," +
        "       'cluster-preferences':[" +
        "         {" +
        "           'minimize':'cores'," +
        "           'precision':3}," +
        "         {" +
        "           'maximize':'freedisk'," +
        "           'precision':100}]}";

    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    Policy.Session session = policy.createSession(cloudManagerWithData(dataproviderdata));
    SolrRequest op = session.getSuggester(MOVEREPLICA).hint(Hint.SRC_NODE, "127.0.0.1:65427_solr").getSuggestion();
    assertNotNull(op);
    assertEquals("127.0.0.1:65434_solr", op.getParams().get("targetNode"));
  }

  public void testNodeLostMultipleReplica() {
    String nodeValues = " {" +
        "    'node4':{" +
        "      'node':'10.0.0.4:8987_solr'," +
        "      'cores':1," +
        "      'freedisk':884.7097854614258}," +
        "    'node3':{" +
        "      'node':'10.0.0.4:8989_solr'," +
        "      'cores':1," +
        "      'freedisk':884.7097854614258}," +
        "    'node2':{" +
        "      'node':'10.0.0.4:7574_solr'," +
        "      'cores':1," +
        "      'freedisk':884.7097854614258}," +
        "}";

    SolrCloudManager provider = getSolrCloudManager((Map<String, Map>) Utils.fromJSONString(nodeValues), clusterState);
    Map policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 50}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}" +
        "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "  ]" +
        "}");

    if(useNodeset) {
      policies = (Map) Utils.fromJSONString("{" +
          "  'cluster-preferences': [" +
          "    { 'maximize': 'freedisk', 'precision': 50}," +
          "    { 'minimize': 'cores', 'precision': 50}" +
          "  ]," +
          "  'cluster-policy': [" +
          "    { 'replica': 0, nodeset:{'nodeRole': 'overseer'}}" +
          "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
          "  ]" +
          "}");
    }
    AutoScalingConfig config = new AutoScalingConfig(policies);
    Policy policy = config.getPolicy();
    Policy.Session session = policy.createSession(provider);
    Suggester suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");

    SolrRequest operation = suggester.getSuggestion();
    assertNotNull(operation);
    assertEquals("node2", operation.getParams().get("targetNode"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getSuggestion();
    assertNotNull(operation);
    assertEquals("node3", operation.getParams().get("targetNode"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getSuggestion();
    assertNull(operation);

    // lets change the policy such that all replicas that were on node1
    // can now fit on node2
    policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 50}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}" +
        "    { 'replica': '<3', 'shard': '#EACH', 'node': '#ANY'}," +
        "  ]" +
        "}");
    if(useNodeset){
      policies = (Map) Utils.fromJSONString("{" +
          "  'cluster-preferences': [" +
          "    { 'maximize': 'freedisk', 'precision': 50}," +
          "    { 'minimize': 'cores', 'precision': 50}" +
          "  ]," +
          "  'cluster-policy': [" +
          "    { 'replica': 0, nodeset: {'nodeRole': 'overseer'}}" +
          "    { 'replica': '<3', 'shard': '#EACH', 'node': '#ANY'}," +
          "  ]" +
          "}");

    }
    config = new AutoScalingConfig(policies);
    policy = config.getPolicy();
    session = null;
    for (String expectedReplica : new String[] { "r1", "r3", "r5", null }) {
      if (session == null) {
        session = policy.createSession(provider);
      } else {
        session = suggester.getSession();
      }
      suggester = session.getSuggester(MOVEREPLICA)
          .hint(Hint.SRC_NODE, "node1");
      operation = suggester.getSuggestion();
      if (expectedReplica == null) {
        assertNull(operation);
      } else {
        assertNotNull(operation);
        assertEquals("node2", operation.getParams().get("targetNode"));
        assertEquals(expectedReplica, operation.getParams().get("replica"));
      }
    }

    // now lets change the policy such that a node can have 2 shard2 replicas
    policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 50}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}" +
        "    { 'replica': '<2', 'shard': 'shard1', 'node': '#ANY'}," +
        "    { 'replica': '<3', 'shard': 'shard2', 'node': '#ANY'}," +
        "  ]" +
        "}");
    if(useNodeset){
      policies = (Map) Utils.fromJSONString("{" +
          "  'cluster-preferences': [" +
          "    { 'maximize': 'freedisk', 'precision': 50}," +
          "    { 'minimize': 'cores', 'precision': 50}" +
          "  ]," +
          "  'cluster-policy': [" +
          "    { 'replica': 0, nodeset:{'nodeRole': 'overseer'}}" +
          "    { 'replica': '<2', 'shard': 'shard1', 'node': '#ANY'}," +
          "    { 'replica': '<3', 'shard': 'shard2', 'node': '#ANY'}," +
          "  ]" +
          "}");

    }
    config = new AutoScalingConfig(policies);
    policy = config.getPolicy();

    session = null;
    final String[] expectedReplica = new String[] { "r1", "r3", "r5" };
    final String[] expectedTargetNode = new String[] { "node3", "node3", "node2" };
    for (int ii = 0; ii < expectedReplica.length; ++ii) {
      if (session == null) {
        session = policy.createSession(provider);
      } else {
        session = suggester.getSession();
      }
      suggester = session.getSuggester(MOVEREPLICA)
          .hint(Hint.SRC_NODE, "node1");
      operation = suggester.getSuggestion();
      assertNotNull(operation);
      assertEquals(expectedTargetNode[ii], operation.getParams().get("targetNode"));
      assertEquals(expectedReplica[ii], operation.getParams().get("replica"));
    }
  }

  private static SolrCloudManager cloudManagerWithData(String data) {
    return cloudManagerWithData((Map) Utils.fromJSONString(data));
  }

  static SolrCloudManager cloudManagerWithData(Map m) {
    Map replicaInfo = (Map) m.get("replicaInfo");
    replicaInfo.forEach((node, val) -> {
      Map m1 = (Map) val;
      m1.forEach((coll, val2) -> {
        Map m2 = (Map) val2;
        m2.forEach((shard, val3) -> {
          List l3 = (List) val3;
          for (int i = 0; i < l3.size(); i++) {
            Object o = l3.get(i);
            Map m3 = (Map) o;
            String name = m3.keySet().iterator().next().toString();
            m3 = (Map) m3.get(name);
            Replica.Type type = Replica.Type.get((String) m3.get("type"));
            l3.set(i, new ReplicaInfo(name, name
                , coll.toString(), shard.toString(), type, (String) node, m3));
          }
        });

      });
    });
    AutoScalingConfig asc = m.containsKey("autoscalingJson") ? new AutoScalingConfig((Map<String, Object>) m.get("autoscalingJson")) : null;
    return new DelegatingCloudManager(null) {

      @Override
      public DistribStateManager getDistribStateManager() {
        return new DelegatingDistribStateManager(null) {
          @Override
          public AutoScalingConfig getAutoScalingConfig() {
            return asc;
          }
        };
      }

      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public ClusterState getClusterState() throws IOException {
            return ClusterState.load(0, new HashMap<>(), getLiveNodes(), CLUSTER_STATE);
          }

          @Override
          public Set<String> getLiveNodes() {
            return new HashSet<>((Collection<String>) m.get("liveNodes"));
          }
        };
      }

      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new DelegatingNodeStateProvider(null) {
          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
            Map<String, Object> result = (Map<String, Object>) Utils.getObjectByPath(m, false, Arrays.asList("nodeValues", node));
            return result == null ? new HashMap<>() : result;
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            Map<String, Map<String, List<ReplicaInfo>>> result = (Map<String, Map<String, List<ReplicaInfo>>>) Utils.getObjectByPath(m, false, Arrays.asList("replicaInfo", node));
            return result == null ? new HashMap<>() : result;
          }
        };
      }
    };
  }

  public void testPolicyWithReplicaType() {
    Map policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 50}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}" +
        "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "    { 'replica': 0, 'shard': '#EACH', sysprop.fs : '!ssd',  type : TLOG }" +
        "    { 'replica': 0, 'shard': '#EACH', sysprop.fs : '!slowdisk' ,  type : PULL }" +
        "  ]" +
        "}");
    if(useNodeset){
      policies = (Map) Utils.fromJSONString("{" +
          "  'cluster-preferences': [" +
          "    { 'maximize': 'freedisk', 'precision': 50}," +
          "    { 'minimize': 'cores', 'precision': 50}" +
          "  ]," +
          "  'cluster-policy': [" +
          "    { 'replica': 0, nodeset : {'nodeRole': 'overseer'}}" +
          "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
          "    { 'replica': 0, 'shard': '#EACH',  nodeset : { sysprop.fs : '!ssd'},  type : TLOG }" +
          "    { 'replica': 0, 'shard': '#EACH', put:'on-each-node' nodeset : {sysprop.fs : '!slowdisk'} ,  type : PULL }" +
          "  ]" +
          "}");

    }
    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480, rack: rack4, sysprop.fs: slowdisk}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873, rack: rack3, sysprop.fs: unknown }," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834, rack: rack2, sysprop.fs : ssd}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer, rack: rack1, sysprop.fs: unknown}" +
        "}");
    Policy policy = new Policy(policies);
    Suggester suggester = policy.createSession(getSolrCloudManager(nodeValues, clusterState))
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair("newColl", "shard1"))
        .hint(Hint.REPLICATYPE, Replica.Type.PULL);
    SolrRequest op = suggester.getSuggestion();
    assertNotNull(op);
    assertEquals(Replica.Type.PULL.name(), op.getParams().get("type"));
    assertEquals("PULL type node must be in 'slowdisk' node", "node1", op.getParams().get("node"));

    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair<>("newColl", "shard2"))
        .hint(Hint.REPLICATYPE, Replica.Type.PULL);
    op = suggester.getSuggestion();
    assertNotNull(op);
    assertEquals(Replica.Type.PULL.name(), op.getParams().get("type"));
    assertEquals("PULL type node must be in 'slowdisk' node", "node1", op.getParams().get("node"));

    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair("newColl", "shard1"))
        .hint(Hint.REPLICATYPE, Replica.Type.TLOG);
    op = suggester.getSuggestion();
    assertNotNull(op);
    assertEquals(Replica.Type.TLOG.name(), op.getParams().get("type"));
    assertEquals("TLOG type node must be in 'ssd' node", "node3", op.getParams().get("node"));

    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair("newColl", "shard2"))
        .hint(Hint.REPLICATYPE, Replica.Type.TLOG);
    op = suggester.getSuggestion();
    assertNotNull(op);
    assertEquals(Replica.Type.TLOG.name(), op.getParams().get("type"));
    assertEquals("TLOG type node must be in 'ssd' node", "node3", op.getParams().get("node"));

    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair<>("newColl", "shard2"))
        .hint(Hint.REPLICATYPE, Replica.Type.TLOG);
    op = suggester.getSuggestion();
    assertNull("No node should qualify for this", op);

  }


  public void testMoveReplicasInMultipleCollections() throws IOException {
    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:2}," +
        "node3:{cores:4}" +
        "}");
    Policy policy = new Policy(new HashMap<>());
    Suggester suggester = policy.createSession(getSolrCloudManager(nodeValues,
        (Map<String, Object>) loadFromResource("testMoveReplicasInMultipleCollections.json")))
        .getSuggester(MOVEREPLICA)
        .hint(Hint.COLL, "collection1")
        .hint(Hint.COLL, "collection2")
        .hint(Suggester.Hint.SRC_NODE, "node2");
    SolrRequest op = suggester.getSuggestion();
    assertNotNull(op);
    assertEquals("collection2", op.getParams().get("collection"));
    assertEquals("node1", op.getParams().get("targetNode"));
    String coreNodeName = op.getParams().get("replica");
    assertTrue(coreNodeName.equals("core_node3") || coreNodeName.equals("core_node1"));

    suggester = suggester.getSession()
        .getSuggester(MOVEREPLICA)
        .hint(Hint.COLL, "collection1")
        .hint(Hint.COLL, "collection2")
        .hint(Suggester.Hint.SRC_NODE, "node2");
    op = suggester.getSuggestion();
    assertNotNull(op);
    assertEquals("collection2", op.getParams().get("collection"));
    assertEquals("node1", op.getParams().get("targetNode"));
    coreNodeName = op.getParams().get("replica");
    assertTrue(coreNodeName.equals("core_node3") || coreNodeName.equals("core_node1"));

    suggester = suggester.getSession()
        .getSuggester(MOVEREPLICA)
        .hint(Hint.COLL, "collection1")
        .hint(Hint.COLL, "collection2")
        .hint(Suggester.Hint.SRC_NODE, "node2");
    op = suggester.getSuggestion();
    assertNull(op);
  }


  public void testMultipleCollections() {
    Map policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 1}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}" +
        "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY', 'collection':'newColl'}," +
        "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY', 'collection':'newColl2', type : PULL}," +
        "    { 'replica': '<3', 'shard': '#EACH', 'node': '#ANY', 'collection':'newColl2'}," +
        "    { 'replica': 0, 'shard': '#EACH', sysprop.fs : '!ssd',  type : TLOG }" +
        "    { 'replica': 0, 'shard': '#EACH', sysprop.fs : '!slowdisk' ,  type : PULL }" +
        "  ]" +
        "}");
    if(useNodeset){
      policies = (Map) Utils.fromJSONString("{" +
          "  'cluster-preferences': [" +
          "    { 'maximize': 'freedisk', 'precision': 50}," +
          "    { 'minimize': 'cores', 'precision': 1}" +
          "  ]," +
          "  'cluster-policy': [" +
          "    { 'replica': 0, nodeset : {'nodeRole': 'overseer'}}" +
          "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY', 'collection':'newColl'}," +
          "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY', 'collection':'newColl2', type : PULL}," +
          "    { 'replica': '<3', 'shard': '#EACH', 'node': '#ANY', 'collection':'newColl2'}," +
          "    { 'replica': 0, 'shard': '#EACH', put: on-each-node , nodeset:{ sysprop.fs : '!ssd'},  type : TLOG }" +
          "    { 'replica': 0, 'shard': '#EACH', put: on-each-node ,nodeset : {sysprop.fs : '!slowdisk'} ,  type : PULL }" +
          "  ]" +
          "}");

    }
    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480, rack: rack4, sysprop.fs: slowdisk}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873, rack: rack3, sysprop.fs: unknown}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834, rack: rack2, sysprop.fs : ssd}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer, rack: rack1, sysprop.fs: unknown}" +
        "}");
    Policy policy = new Policy(policies);
    Suggester suggester = policy.createSession(getSolrCloudManager(nodeValues, clusterState))
        .getSuggester(ADDREPLICA)
        .hint(Hint.REPLICATYPE, Replica.Type.PULL)
        .hint(Hint.COLL_SHARD, new Pair<>("newColl", "shard1"))
        .hint(Hint.COLL_SHARD, new Pair<>("newColl2", "shard1"));
    SolrRequest op;
    int countOp = 0;
    int countNewCollOp = 0;
    int countNewColl2Op = 0;
    while ((op = suggester.getSuggestion()) != null) {
      countOp++;
      assertEquals(Replica.Type.PULL.name(), op.getParams().get("type"));
      String collection = op.getParams().get("collection");
      assertTrue("Collection for replica is not as expected " + collection, collection.equals("newColl") || collection.equals("newColl2"));
      if (collection.equals("newColl")) countNewCollOp++;
      else countNewColl2Op++;
      assertEquals("PULL type node must be in 'slowdisk' node, countOp : " + countOp, "node1", op.getParams().get("node"));
      suggester = suggester.getSession().getSuggester(ADDREPLICA)
          .hint(Hint.REPLICATYPE, Replica.Type.PULL)
          .hint(Hint.COLL_SHARD, new Pair<>("newColl", "shard1"))
          .hint(Hint.COLL_SHARD, new Pair<>("newColl2", "shard1"));
    }
    assertEquals(2, countOp);
    assertEquals(1, countNewCollOp);
    assertEquals(1, countNewColl2Op);

    countOp = 0;
    countNewCollOp = 0;
    countNewColl2Op = 0;
    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair<>("newColl", "shard2"))
        .hint(Hint.COLL_SHARD, new Pair<>("newColl2", "shard2"))
        .hint(Hint.REPLICATYPE, Replica.Type.TLOG);
    while ((op = suggester.getSuggestion()) != null) {
      countOp++;
      assertEquals(Replica.Type.TLOG.name(), op.getParams().get("type"));
      String collection = op.getParams().get("collection");
      assertTrue("Collection for replica is not as expected " + collection, collection.equals("newColl") || collection.equals("newColl2"));
      if (collection.equals("newColl")) countNewCollOp++;
      else countNewColl2Op++;
      assertEquals("TLOG type node must be in 'ssd' node", "node3", op.getParams().get("node"));
      suggester = suggester.getSession()
          .getSuggester(ADDREPLICA)
          .hint(Hint.COLL_SHARD, new Pair<>("newColl", "shard2"))
          .hint(Hint.COLL_SHARD, new Pair<>("newColl2", "shard2"))
          .hint(Hint.REPLICATYPE, Replica.Type.TLOG);
    }
    assertEquals(3, countOp);
    assertEquals(1, countNewCollOp);
    assertEquals(2, countNewColl2Op);
  }

  public void testRow() {
    Policy policy = new Policy();
    Policy.Session session = policy.createSession(new DelegatingCloudManager(null) {
      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new DelegatingNodeStateProvider(null) {
          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            Map<String, Map<String, List<ReplicaInfo>>> o = (Map<String, Map<String, List<ReplicaInfo>>>) Utils.fromJSONString("{c1: {s0:[{}]}}");
            Utils.setObjectByPath(o, "c1/s0[0]", new ReplicaInfo("r0", "c1.s0", "c1", "s0", Replica.Type.NRT, "nodex", new HashMap<>()));
            return o;
          }

          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
            return Utils.makeMap("node", "nodex", "cores", 1);
          }
        };
      }

      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public String getPolicyNameByCollection(String coll) {
            return null;
          }

          @Override
          public Set<String> getLiveNodes() {
            return Collections.singleton("nodex");
          }
        };
      }
    });

    Row row = session.getNode("nodex");
    Row r1 = row.addReplica("c1", "s1", null);
    Row r2 = r1.addReplica("c1", "s1", null);
    assertEquals(1, r1.collectionVsShardVsReplicas.get("c1").get("s1").size());
    assertEquals(2, r2.collectionVsShardVsReplicas.get("c1").get("s1").size());
    assertTrue(r2.collectionVsShardVsReplicas.get("c1").get("s1").get(0) instanceof ReplicaInfo);
    assertTrue(r2.collectionVsShardVsReplicas.get("c1").get("s1").get(1) instanceof ReplicaInfo);
  }

  public void testMerge() {

    Map map = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 50}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}," +
        "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}" +
        "  ]," +
        "  'policies': {" +
        "    'policy1': [" +
        "      { 'replica': '1', 'sysprop.fs': 'ssd', 'shard': '#EACH'}," +
        "      { 'replica': '<2', 'shard': '#ANY', 'node': '#ANY'}," +
        "      { 'replica': '<2', 'shard': '#EACH', 'sysprop.rack': 'rack1'}" +
        "    ]" +
        "  }" +
        "}");
    if(useNodeset){
      map = (Map) Utils.fromJSONString("{" +
          "  'cluster-preferences': [" +
          "    { 'maximize': 'freedisk', 'precision': 50}," +
          "    { 'minimize': 'cores', 'precision': 50}" +
          "  ]," +
          "  'cluster-policy': [" +
          "    { 'replica': 0, nodeset: {'nodeRole': 'overseer'}}," +
          "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}" +
          "  ]," +
          "  'policies': {" +
          "    'policy1': [" +
          "      { 'replica': '1', nodeset:{ 'sysprop.fs': 'ssd'}, 'shard': '#EACH'}," +
          "      { 'replica': '<2', 'shard': '#ANY', 'node': '#ANY'}," +
          "      { 'replica': '<2', 'shard': '#EACH',nodeset:{ 'sysprop.rack': 'rack1'}}" +
          "    ]" +
          "  }" +
          "}");

    }
    Policy policy = new Policy(map);
    List<Clause> clauses = Policy.mergePolicies("mycoll", policy.getPolicies().get("policy1"), policy.getClusterPolicy());
    Collections.sort(clauses);
    assertEquals(clauses.size(), 4);
    assertEquals("1", String.valueOf(clauses.get(0).original.get("replica")));
    assertEquals("0", String.valueOf(clauses.get(1).original.get("replica")));
    assertEquals("#ANY", clauses.get(3).original.get("shard"));
    assertEquals("rack1", clauses.get(2).tag.val);
    assertEquals("overseer",clauses.get(1).tag.val);
  }

  public void testConditionsSort() {
    String rules = "{" +
        "    'cluster-policy':[" +
        "      { 'nodeRole':'overseer', replica: 0,  'strict':false}," +
        "      { 'replica':'<1', 'node':'node3', 'shard':'#EACH'}," +
        "      { 'replica':'<2', 'node':'#ANY', 'shard':'#EACH'}," +
        "      { 'replica':1, 'sysprop.rack':'rack1'}]" +
        "  }";
    if(useNodeset){
      rules = "{" +
          "    'cluster-policy':[" +
          "      { 'nodeRole':'overseer', replica: 0,  'strict':false}," +
          "      { 'replica':'<1', 'node':'node3', 'shard':'#EACH'}," +
          "      { 'replica':'<2', 'node':'#ANY', 'shard':'#EACH'}," +
          "      { 'replica':1, nodeset: {'sysprop.rack':'rack1'}}]" +
          "  }";

    }
    Policy p = new Policy((Map<String, Object>) Utils.fromJSONString(rules));
    List<Clause> clauses = new ArrayList<>(p.getClusterPolicy());
    Collections.sort(clauses);
    assertEquals("nodeRole", clauses.get(1).tag.getName());
    assertEquals("sysprop.rack", clauses.get(0).tag.getName());
  }

  public void testRules() {
    String rules = "{" +
        "cluster-policy:[" +
        "{nodeRole:'overseer',replica : 0 , strict:false}," +
        "{replica:'<1',node:node3}," +
        "{replica:'<2',node:'#ANY', shard:'#EACH'}]," +
        " cluster-preferences:[" +
        "{minimize:cores , precision:2}," +
        "{maximize:freedisk, precision:50}, " +
        "{minimize:heapUsage, precision:1000}]}";
    if(useNodeset){
      rules = "{" +
          "cluster-policy:[" +
          "{nodeset:{nodeRole:'overseer'},replica : 0 , strict:false}," +
          "{replica:'<1',node:node3}," +
          "{replica:'<2',node:'#ANY', shard:'#EACH'}]," +
          " cluster-preferences:[" +
          "{minimize:cores , precision:2}," +
          "{maximize:freedisk, precision:50}, " +
          "{minimize:heapUsage, precision:1000}]}";
    }

    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer}" +
        "}");

    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(rules));
    Policy.Session session;
    session = policy.createSession(getSolrCloudManager(nodeValues, clusterState));

    List<Row> l = session.getSortedNodes();
    assertEquals("node1", l.get(0).node);
    assertEquals("node3", l.get(1).node);
    assertEquals("node4", l.get(2).node);
    assertEquals("node2", l.get(3).node);


    List<Violation> violations = session.getViolations();
    assertEquals(3, violations.size());
    assertTrue(violations.stream().anyMatch(violation -> "node3".equals(violation.getClause().tag.getValue())));
    assertTrue(violations.stream().anyMatch(violation -> "nodeRole".equals(violation.getClause().tag.getName())));
    assertTrue(violations.stream().anyMatch(violation -> (violation.getClause().replica.getOperand() == Operand.LESS_THAN && "node".equals(violation.getClause().tag.getName()))));

    Suggester suggester = session.getSuggester(ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair<>("gettingstarted", "r1"));
    SolrParams operation = suggester.getSuggestion().getParams();
    assertEquals("node2", operation.get("node"));

    nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834}," +
        "node5:{cores:0, freedisk: 895, heapUsage:17834}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer}" +
        "}");
    session = policy.createSession(getSolrCloudManager(nodeValues, clusterState));
    SolrRequest opReq = session.getSuggester(MOVEREPLICA)
        .hint(Hint.TARGET_NODE, "node5")
        .getSuggestion();
    assertNotNull(opReq);
    assertEquals("node5", opReq.getParams().get("targetNode"));
  }

  @Test
  public void testSessionCaching() throws IOException, InterruptedException {
//    PolicyHelper.SessionRef ref1 = new PolicyHelper.SessionRef();
    String autoScalingjson = "  '{cluster-policy':[" +
        "    {      'cores':'<10',      'node':'#ANY'}," +
        "    {      'replica':'<2',      'shard':'#EACH',      'node':'#ANY'}," +
        "    {      'nodeRole':'overseer','replica':0}]," +
        "  'cluster-preferences':[{'minimize':'cores'}]}";
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
//    PolicyHelper.SESSION_REF.set(ref1);
    String nodeValues = " {" +
        "    'node4':{" +
        "      'node':'10.0.0.4:8987_solr'," +
        "      'cores':1," +
        "      'freedisk':884.7097854614258}," +
        "    'node3':{" +
        "      'node':'10.0.0.4:8989_solr'," +
        "      'cores':1," +
        "      'freedisk':884.7097854614258}," +
        "    'node2':{" +
        "      'node':'10.0.0.4:7574_solr'," +
        "      'cores':1," +
        "      'freedisk':884.7097854614258}," +
        "}";


    Map policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 50}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, 'nodeRole': 'overseer'}" +
        "    { 'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "  ]" +
        "}");
    AutoScalingConfig config = new AutoScalingConfig(policies);
    final SolrCloudManager solrCloudManager = new DelegatingCloudManager(getSolrCloudManager((Map<String, Map>) Utils.fromJSONString(nodeValues),
        clusterState)) {
      @Override
      public DistribStateManager getDistribStateManager() {
        return delegatingDistribStateManager(config);
      }
    };

    List<ReplicaPosition> locations = PolicyHelper.getReplicaLocations("c", config, solrCloudManager, null,
        Arrays.asList("s1", "s2"), 1, 0, 0,
        null);

    PolicyHelper.SessionRef sessionRef = (PolicyHelper.SessionRef) solrCloudManager.getObjectCache().get(PolicyHelper.SessionRef.class.getName());
    assertNotNull(sessionRef);
    PolicyHelper.SessionWrapper sessionWrapper = PolicyHelper.getLastSessionWrapper(true);


    Policy.Session session = sessionWrapper.get();
    assertNotNull(session);
    assertTrue(session.getPolicy() == config.getPolicy());
    assertEquals(sessionWrapper.status, PolicyHelper.Status.EXECUTING);
    sessionWrapper.release();
    assertTrue(sessionRef.getSessionWrapper() == PolicyHelper.SessionWrapper.DEFAULT_INSTANCE);
    PolicyHelper.SessionWrapper s1 = PolicyHelper.getSession(solrCloudManager);
    assertEquals(sessionRef.getSessionWrapper().getCreateTime(), s1.getCreateTime());
    PolicyHelper.SessionWrapper[] s2 = new PolicyHelper.SessionWrapper[1];
    AtomicLong secondTime = new AtomicLong();
    Thread thread = new Thread(() -> {
      try {
        s2[0] = PolicyHelper.getSession(solrCloudManager);
        secondTime.set(System.nanoTime());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    thread.start();
    Thread.sleep(50);
    long beforeReturn = System.nanoTime();
    assertEquals(s1.getCreateTime(), sessionRef.getSessionWrapper().getCreateTime());
    s1.returnSession(s1.get());
    assertEquals(1, s1.getRefCount());
    thread.join();
    assertNotNull(s2[0]);
    assertTrue(secondTime.get() > beforeReturn);
    assertTrue(s1.getCreateTime() == s2[0].getCreateTime());

    s2[0].returnSession(s2[0].get());
    assertEquals(2, s1.getRefCount());

    s2[0].release();
    assertFalse(sessionRef.getSessionWrapper() == PolicyHelper.SessionWrapper.DEFAULT_INSTANCE);
    s1.release();
    assertTrue(sessionRef.getSessionWrapper() == PolicyHelper.SessionWrapper.DEFAULT_INSTANCE);


  }

  private DistribStateManager delegatingDistribStateManager(AutoScalingConfig config) {
    return new DelegatingDistribStateManager(null) {
      @Override
      public AutoScalingConfig getAutoScalingConfig() {
        return config;
      }
    };
  }

  public void testNegativeConditions() {
    String autoscaleJson = "{" +
        "      'cluster-policy':[" +
        "      {'replica':'<4','shard':'#EACH','node':'#ANY'}," +
        "      { 'replica': 0, 'sysprop.fs': '!ssd', 'shard': '#EACH'}," +//negative greedy condition
        "      {'nodeRole':'overseer','replica':'0'}]," +
        "      'cluster-preferences':[" +
        "      {'minimize':'cores', 'precision':3}," +
        "      {'maximize':'freedisk','precision':100}]}";
    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480, rack: rack4, sysprop.fs: slowdisk}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873, rack: rack3, sysprop.fs: slowdisk}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834, rack: rack2, sysprop.fs : ssd}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer, rack: rack1, sysprop.fs: slowdisk}" +
        "}");
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoscaleJson));
    SolrCloudManager cloudManager = getSolrCloudManager(nodeValues, clusterState);
    Policy.Session session = policy.createSession(cloudManager);
    for (int i = 0; i < 3; i++) {
      Suggester suggester = session.getSuggester(ADDREPLICA);
      SolrRequest op = suggester
          .hint(Hint.COLL_SHARD, new Pair<>("newColl", "shard1"))
          .getSuggestion();
      assertNotNull(op);
      assertEquals("node3", op.getParams().get("node"));
      session = suggester.getSession();
    }

  }

  public void testGreedyConditions() {
    String autoscaleJson = "{" +
        "      'cluster-policy':[" +
        "      {'cores':'<10','node':'#ANY'}," +
        "      {'replica':'<3','shard':'#EACH','node':'#ANY'}," +
        "      { 'replica': 2, 'sysprop.fs': 'ssd', 'shard': '#EACH'}," +//greedy condition
        "      {'nodeRole':'overseer','replica':'0'}]," +
        "      'cluster-preferences':[" +
        "      {'minimize':'cores', 'precision':3}," +
        "      {'maximize':'freedisk','precision':100}]}";
    if(useNodeset){
      autoscaleJson = "{" +
          "      'cluster-policy':[" +
          "      {'cores':'<10','node':'#ANY'}," +
          "      {'replica':'<3','shard':'#EACH','node':'#ANY'}," +
          "      { 'replica': 2, nodeset: {'sysprop.fs': 'ssd'}, 'shard': '#EACH'}," +//greedy condition
          "      {nodeset:{'nodeRole':'overseer'},'replica':'0'}]," +
          "      'cluster-preferences':[" +
          "      {'minimize':'cores', 'precision':3}," +
          "      {'maximize':'freedisk','precision':100}]}";

    }
    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480, rack: rack4}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873, rack: rack3}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834, rack: rack2, sysprop.fs : ssd}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer, rack: rack1}" +
        "}");

    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoscaleJson));
    SolrCloudManager cloudManager = getSolrCloudManager(nodeValues, clusterState);
    Policy.Session session = policy.createSession(cloudManager);
    Suggester suggester = session.getSuggester(ADDREPLICA);
    SolrRequest op = suggester
        .hint(Hint.COLL_SHARD, new Pair<>("newColl", "shard1"))
        .getSuggestion();
    assertNotNull(op);
    assertEquals("node3", op.getParams().get("node"));
    suggester = suggester
        .getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair<>("newColl", "shard1"));
    op = suggester.getSuggestion();
    assertNotNull(op);
    assertEquals("node3", op.getParams().get("node"));

    suggester = suggester
        .getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair<>("newColl", "shard1"));
    op = suggester.getSuggestion();
    assertNotNull(op);
    assertEquals("node2", op.getParams().get("node"));
  }

  public void testMoveReplica() {
    String autoscaleJson = "{" +
        "      'cluster-policy':[" +
        "      {'cores':'<10','node':'#ANY'}," +
        "      {'replica':'<3','shard':'#EACH','node':'#ANY'}," +
        "      {'nodeRole':'overseer','replica':'0'}]," +
        "      'cluster-preferences':[" +
        "      {'minimize':'cores', 'precision':3}," +
        "      {'maximize':'freedisk','precision':100}]}";

    if(useNodeset){
      autoscaleJson = "{" +
          "      'cluster-policy':[" +
          "      {'cores':'<10','node':'#ANY'}," +
          "      {'replica':'<3','shard':'#EACH','node':'#ANY'}," +
          "      {nodeset: {'nodeRole':'overseer'},'replica':'0'}]," +
          "      'cluster-preferences':[" +
          "      {'minimize':'cores', 'precision':3}," +
          "      {'maximize':'freedisk','precision':100}]}";

    }

    Map replicaInfoMap = (Map) Utils.fromJSONString("{ '127.0.0.1:60099_solr':{}," +
        " '127.0.0.1:60089_solr':{'compute_plan_action_test':{'shard1':[" +
        "      {'core_node1':{}}," +
        "      {'core_node2':{}}]}}}");
    Map m = (Map) Utils.getObjectByPath(replicaInfoMap, false, "127.0.0.1:60089_solr/compute_plan_action_test");
    m.put("shard1", Arrays.asList(
        new ReplicaInfo("core_node1", "core_node1", "compute_plan_action_test", "shard1", Replica.Type.NRT, "127.0.0.1:60089_solr", Collections.emptyMap()),
        new ReplicaInfo("core_node2", "core_node2", "compute_plan_action_test", "shard1", Replica.Type.NRT, "127.0.0.1:60089_solr", Collections.emptyMap())));

    Map<String, Map<String, Object>> tagsMap = (Map) Utils.fromJSONString("{" +
        "      '127.0.0.1:60099_solr':{" +
        "        'cores':0," +
        "            'freedisk':918005641216}," +
        "      '127.0.0.1:60089_solr':{" +
        "        'cores':2," +
        "            'freedisk':918005641216}}");

    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoscaleJson));
    Policy.Session session = policy.createSession(new DelegatingCloudManager(null) {
      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public Set<String> getLiveNodes() {
            return replicaInfoMap.keySet();
          }

        };
      }

      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new DelegatingNodeStateProvider(null) {
          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
            return tagsMap.get(node);
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            return (Map<String, Map<String, List<ReplicaInfo>>>) replicaInfoMap.get(node);
          }
        };
      }
    });
    Suggester suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.TARGET_NODE, "127.0.0.1:60099_solr");
    SolrRequest op = suggester.getSuggestion();
    assertNotNull("expect a non null operation", op);
  }

  public void testOtherTag() {
    String rules = "{" +
        "'cluster-preferences':[" +
        "{'minimize':'cores','precision':2}," +
        "{'maximize':'freedisk','precision':50}," +
        "{'minimize':'heapUsage','precision':1000}" +
        "]," +
        "'cluster-policy':[" +
        "{replica:0, 'nodeRole':'overseer','strict':false}," +
        "{'replica':'<1','node':'node3'}," +
        "{'replica':'<2','node':'#ANY','shard':'#EACH'}" +
        "]," +
        "'policies':{" +
        "'p1':[" +
        "{replica:0, 'nodeRole':'overseer','strict':false}," +
        "{'replica':'<1','node':'node3'}," +
        "{'replica':'<2','node':'#ANY','shard':'#EACH'}," +
        "{'replica':'<3','shard':'#EACH','sysprop.rack':'#EACH'}" +
        "]" +
        "}" +
        "}";

    if(useNodeset){
      rules = "{" +
          "'cluster-preferences':[" +
          "{'minimize':'cores','precision':2}," +
          "{'maximize':'freedisk','precision':50}," +
          "{'minimize':'heapUsage','precision':1000}" +
          "]," +
          "'cluster-policy':[" +
          "{replica:0, nodeset:{'nodeRole':'overseer'},'strict':false}," +
          "{'replica':'<1','node':'node3'}," +
          "{'replica':'<2','node':'#ANY','shard':'#EACH'}" +
          "]," +
          "'policies':{" +
          "'p1':[" +
          "{replica:0, nodeset:{'nodeRole':'overseer'},'strict':false}," +
          "{'replica':'<1','node':'node3'}," +
          "{'replica':'<2','node':'#ANY','shard':'#EACH'}," +
          "{'replica':'<3','shard':'#EACH', nodeset : { 'sysprop.rack':[rack1, rack2, rack3, rack4]}}" +
          "]" +
          "}" +
          "}";


    }

    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480, rack: rack4}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873, rack: rack3}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834, rack: rack2}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer, sysprop.rack: rack1}" +
        "}");
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(rules));
    SolrCloudManager cloudManager = getSolrCloudManager(nodeValues, clusterState);
    SolrCloudManager cdp = new DelegatingCloudManager(null) {
      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new DelegatingNodeStateProvider(null) {
          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
            return cloudManager.getNodeStateProvider().getNodeValues(node, tags);
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            return cloudManager.getNodeStateProvider().getReplicaInfo(node, keys);
          }
        };
      }

      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public Set<String> getLiveNodes() {
            return cloudManager.getClusterStateProvider().getLiveNodes();
          }

          @Override
          public String getPolicyNameByCollection(String coll) {
            return "p1";
          }
        };
      }


    };
    Policy.Session session = policy.createSession(cdp);

    CollectionAdminRequest.AddReplica op = (CollectionAdminRequest.AddReplica) session
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair<>("newColl", "s1")).getSuggestion();
    assertNotNull(op);
    assertEquals("node2", op.getNode());
  }
  static SolrCloudManager getSolrCloudManager(final Map<String, Map> nodeValues, String clusterS) {
    return getSolrCloudManager(nodeValues,(Map) Utils.fromJSONString(clusterS));

  }
  private static SolrCloudManager getSolrCloudManager(final Map<String, Map> nodeValues, Map clusterS) {
    return new SolrCloudManager() {
      ObjectCache objectCache = new ObjectCache();

      @Override
      public ObjectCache getObjectCache() {
        return objectCache;
      }

      @Override
      public TimeSource getTimeSource() {
        return TimeSource.NANO_TIME;
      }

      @Override
      public void close() {

      }

      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public Set<String> getLiveNodes() {
            return nodeValues.keySet();
          }

        };
      }

      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new DelegatingNodeStateProvider(null) {
          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
            Map<String, Object> result = new LinkedHashMap<>();
            tags.stream().forEach(s -> result.put(s, nodeValues.get(node).get(s)));
            return result;
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            return getReplicaDetails(node, clusterS);
          }
        };
      }

      @Override
      public DistribStateManager getDistribStateManager() {
        return null;
      }

      @Override
      public DistributedQueueFactory getDistributedQueueFactory() {
        return null;
      }

      @Override
      public SolrResponse request(SolrRequest req) {
        return null;
      }

      @Override
      public byte[] httpRequest(String url, SolrRequest.METHOD method, Map<String, String> headers, String payload, int timeout, boolean followRedirects) {
        return new byte[0];
      }
    };
  }

  public void testEmptyClusterState() {
    String autoScaleJson = " {'policies':{'c1':[{" +
        "        'replica':1," +
        "        'shard':'#EACH'," +
        "        'port':'50096'}]}}";
    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "    '127.0.0.1:50097_solr':{" +
        "      'cores':0," +
        "      'port':'50097'}," +
        "    '127.0.0.1:50096_solr':{" +
        "      'cores':0," +
        "      'port':'50096'}}");
    SolrCloudManager dataProvider = new DelegatingCloudManager(null) {
      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public Set<String> getLiveNodes() {
            return new HashSet<>(Arrays.asList("127.0.0.1:50097_solr", "127.0.0.1:50096_solr"));
          }
        };
      }

      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new DelegatingNodeStateProvider(null) {
          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> keys) {
            Map<String, Object> result = new LinkedHashMap<>();
            keys.stream().forEach(s -> result.put(s, nodeValues.get(node).get(s)));
            return result;
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            return getReplicaDetails(node, (Map)Utils.fromJSONString(clusterState));
          }
        };
      }
    };
    List<ReplicaPosition> locations = PolicyHelper.getReplicaLocations(
        "newColl", new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScaleJson)),
        dataProvider, Collections.singletonMap("newColl", "c1"), Arrays.asList("shard1", "shard2"), 1, 0, 0, null);

    assertTrue(locations.stream().allMatch(it -> it.node.equals("127.0.0.1:50096_solr")));
  }

  public void testMultiReplicaPlacement() {
    String autoScaleJson = "{" +
        "  cluster-preferences: [" +
        "    { maximize : freedisk , precision: 50}," +
        "    { minimize : cores, precision: 2}" +
        "  ]," +
        "  cluster-policy: [" +
        "    { replica : '0' , nodeRole: overseer}," +
        "    { replica: '<2', shard: '#ANY', node: '#ANY'" +
        "    }" +
        "  ]," +
        "  policies: {" +
        "    policy1: [" +
        "      { replica: '<2', shard: '#EACH', node: '#ANY'}," +
        "      { replica: '<2', shard: '#EACH', sysprop.rack: rack1}" +
        "    ]" +
        "  }" +
        "}";
    if(useNodeset){
      autoScaleJson = "{" +
          "  cluster-preferences: [" +
          "    { maximize : freedisk , precision: 50}," +
          "    { minimize : cores, precision: 2}" +
          "  ]," +
          "  cluster-policy: [" +
          "    { replica : '0' , nodeset: {nodeRole: overseer}}," +
          "    { replica: '<2', shard: '#ANY', node: '#ANY'" +
          "    }" +
          "  ]," +
          "  policies: {" +
          "    policy1: [" +
          "      { replica: '<2', shard: '#EACH', node: '#ANY'}," +
          "      { replica: '<2', shard: '#EACH', nodeset:{ sysprop.rack: rack1}}" +
          "    ]" +
          "  }" +
          "}";

    }


    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heap:10480, sysprop.rack:rack3}," +
        "node2:{cores:4, freedisk: 749, heap:6873, sysprop.fs : ssd, sysprop.rack:rack1}," +
        "node3:{cores:7, freedisk: 262, heap:7834, sysprop.rack:rack4}," +
        "node4:{cores:0, freedisk: 900, heap:16900, nodeRole:overseer, sysprop.rack:rack2}" +
        "}");

    SolrCloudManager cloudManager = new DelegatingCloudManager(null) {
      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new DelegatingNodeStateProvider(null) {
          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> keys) {
            Map<String, Object> result = new LinkedHashMap<>();
            keys.stream().forEach(s -> result.put(s, nodeValues.get(node).get(s)));
            return result;
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            return getReplicaDetails(node, (Map)Utils.fromJSONString(clusterState));
          }
        };
      }

      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public Set<String> getLiveNodes() {
            return new HashSet<>(Arrays.asList("node1", "node2", "node3", "node4"));
          }
        };
      }
    };
    List<ReplicaPosition> locations = PolicyHelper.getReplicaLocations(
        "newColl", new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScaleJson)),
        cloudManager, Collections.singletonMap("newColl", "policy1"), Arrays.asList("shard1", "shard2"), 3, 0, 0, null);
    assertTrue(locations.stream().allMatch(it -> ImmutableList.of("node2", "node1", "node3").contains(it.node)));
  }

  public void testMoveReplicaSuggester() {
    String autoScalingjson = "  '{cluster-policy':[" +
        "{'cores':'<10', 'node':'#ANY'}," +
        "{'replica':'<2', 'shard':'#EACH','node':'#ANY'}]," +
        "'cluster-preferences':[{'minimize':'cores'}]}";
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    Policy.Session session = policy.createSession(cloudManagerWithData((Map) loadFromResource("testMoveReplicaSuggester.json")));
    Suggester suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.TARGET_NODE, "10.0.0.6:7574_solr");
    SolrRequest op = suggester.getSuggestion();
    assertNotNull(op);
    suggester = suggester.getSession()
        .getSuggester(MOVEREPLICA)
        .hint(Hint.TARGET_NODE, "10.0.0.6:7574_solr");
    op = suggester.getSuggestion();
    assertNull(op);

    suggester = suggester.getSession()
        .getSuggester(MOVEREPLICA)
        .forceOperation(true)
        .hint(Hint.TARGET_NODE, "10.0.0.6:8983_solr");
    op = suggester.getSuggestion();
    assertNull(op);
  }

  public void testComputePlanAfterNodeAdded() {
    String autoScalingjson = "cluster-preferences:[" +
        "       {minimize : cores}," +
        "       {'maximize':freedisk , precision:100}],    " +
        " cluster-policy:[{cores:'<10',node:'#ANY'}," +
        "       {replica:'<2', shard:'#EACH',node:'#ANY'}," +
        "       { nodeRole:overseer,replica:0}]}";
    if(useNodeset){
      autoScalingjson = "cluster-preferences:[" +
          "       {minimize : cores}," +
          "       {'maximize':freedisk , precision:100}],    " +
          " cluster-policy:[{cores:'<10',node:'#ANY'}," +
          "       {replica:'<2', shard:'#EACH',node:'#ANY'}," +
          "       {nodeset:{ nodeRole:overseer},replica:0}]}";

    }
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    Policy.Session session = policy.createSession(cloudManagerWithData((Map) loadFromResource("testComputePlanAfterNodeAdded.json")));
    Suggester suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA)
        .hint(Hint.TARGET_NODE, "127.0.0.1:51147_solr");
    SolrRequest op = suggester.getSuggestion();
    log.info("" + op);
    assertNotNull("operation expected ", op);
  }

  public void testReplicaCountSuggestions() {
    String autoScalingjson = "  { cluster-policy:[" +
        "    { cores :'<10', node :'#ANY'}," +
        "    { replica :'<2',  node:'#ANY'}," +
        "    { nodeRole : overseer, replica :0}]," +
        "  cluster-preferences :[{ minimize : cores }]}";
    if(useNodeset){
      autoScalingjson = "  { cluster-policy:[" +
          "    { cores :'<10', node :'#ANY'}," +
          "    { replica :'<2',  node:'#ANY'}," +
          "    { nodeset:{nodeRole : overseer}, replica :0}]," +
          "  cluster-preferences :[{ minimize : cores }]}";

    }
    List<Suggester.SuggestionInfo> l = PolicyHelper.getSuggestions(new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson)),
        cloudManagerWithData((Map) loadFromResource("testReplicaCountSuggestions.json")));
    assertFalse(l.isEmpty());

    assertEquals(1.0d, l.get(0)._get( "violation/violation/delta",null));
    assertEquals("POST", l.get(0)._get("operation/method",null));
    assertEquals("/c/mycoll1", l.get(0)._get( "operation/path",null));
    assertNotNull(l.get(0)._get("operation/command/move-replica", null));
    assertEquals("10.0.0.6:7574_solr", l.get(0)._get( "operation/command/move-replica/targetNode",null));
    /*
     * one of the two cores on 10.0.0.6:8983_solr should move to 10.0.0.6:7574_solr and
     * (everything else being equal) core_node1 is chosen ahead of core_node2 based on its name
     */
    assertEquals("core_node1", l.get(0)._get("operation/command/move-replica/replica", null));
  }


  public void testReplicaPercentage() {
    List<Map> l = (List<Map>) loadFromResource("testReplicaPercentage.json");
    String autoScalingjson = "  { cluster-policy:[" +
        "    { replica :'51%', shard:'#EACH', node:'#ANY'}]," +
        "  cluster-preferences :[{ minimize : cores }]}";


    AutoScalingConfig autoScalingConfig = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    Policy.Session session = autoScalingConfig.getPolicy().createSession(cloudManagerWithData(l.get(0)));
    List<Violation> violations = session.getViolations();
    assertEquals(2, violations.size());
    for (Violation violation : violations) {
      if (violation.node.equals("10.0.0.6:8983_solr")) {
        assertEquals(1.0d, violation.replicaCountDelta, 0.01);
        assertEquals(1.53d, ((RangeVal) violation.getClause().getReplica().val).actual);
      } else if (violation.node.equals("10.0.0.6:7574_solr")) {
        assertEquals(-1.0d, violation.replicaCountDelta, 0.01);
      }

    }


    session = autoScalingConfig.getPolicy().createSession(cloudManagerWithData(Utils.getDeepCopy(l.get(1), 6)));
    violations = session.getViolations();
    assertEquals(0, violations.size());
    autoScalingjson = "  { cluster-policy:[" +
        "    { replica :'51%', shard: '#EACH' , node:'#ANY'}]," +
        "  cluster-preferences :[{ minimize : cores }]}";
    autoScalingConfig = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    session = autoScalingConfig.getPolicy().createSession(cloudManagerWithData(l.get(1)));
    violations = session.getViolations();
    assertEquals(0, violations.size());
    autoScalingjson = "  { cluster-policy:[" +
        "    { replica :'50%',node:'#ANY' , type: TLOG } ,{ replica :'50%',node:'#ANY' , type: PULL } ]," +
        "  cluster-preferences :[{ minimize : cores }]}";
    autoScalingConfig = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    session = autoScalingConfig.getPolicy().createSession(cloudManagerWithData(l.get(2)));
    violations = session.getViolations();
    assertEquals(2, violations.size());

  }

  public void testReplicaZonesPercentage() {
    String autoScalingjson = "  { cluster-policy:[" +
        "    { replica :'33%', shard: '#EACH', sysprop.az : east}," +
        "    { replica :'67%', shard: '#EACH', sysprop.az : west}" +
        "    ]," +
        "  cluster-preferences :[{ minimize : cores }]}";

    String COLL_NAME = "percentColl";
    AutoScalingConfig autoScalingConfig = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));

    Policy.Transaction txn = new Policy.Transaction(autoScalingConfig.getPolicy());
    txn.open(cloudManagerWithData((Map<String, Object>) loadFromResource("testReplicaZonesPercentage.json")));

    List<String> nodes = new ArrayList<>();

    int westCount = 0, eastCount = 0;
    for (int i = 0; i < 12; i++) {
      SolrRequest suggestion = txn.getCurrentSession()
          .getSuggester(ADDREPLICA)
          .hint(Hint.COLL_SHARD, new Pair<>(COLL_NAME, "shard1"))
          .getSuggestion();
      assertNotNull(suggestion);
      String node = suggestion.getParams().get("node");
      nodes.add(node);
      if ("10.0.0.6:8983_solr".equals(node)) eastCount++;
      if ("10.0.0.6:7574_solr".equals(node)) westCount++;
      if (i % 3 == 1) assertEquals("10.0.0.6:8983_solr", node);
      else assertEquals("10.0.0.6:7574_solr", node);
    }
    assertEquals(8, westCount);
    assertEquals(4, eastCount);

    List<Violation> violations = txn.close();
    assertTrue(violations.isEmpty());
    Policy.Session latestSession = txn.getCurrentSession();
    assertEquals("10.0.0.6:7574_solr", latestSession.matrix.get(0).node);
    AtomicInteger count = new AtomicInteger();
    latestSession.matrix.get(0).forEachReplica(replicaInfo -> count.incrementAndGet());
    assertEquals(8, count.get());

    assertEquals("10.0.0.6:8983_solr", latestSession.matrix.get(1).node);
    count.set(0);
    latestSession.matrix.get(1).forEachReplica(replicaInfo -> count.incrementAndGet());
    assertEquals(4, count.get());

  }

  public void testFreeDiskDeviation() {
    Map map = (Map) loadFromResource("testFreeDiskDeviation.json");
    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) map.get("config"));
    if(useNodeset){
      cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString("{" +
          "    'cluster-policy': [{'replica':'<2', 'shard':'#EACH', 'node':'#ANY'}," +
          "      {'replica': '#ALL', 'nodeset': {'freedisk': '>700'}, 'strict': false}]" +
          "  }"));
    }
    SolrCloudManager scm = cloudManagerWithData(map);
    Suggester suggester = cfg.getPolicy()
        .createSession(scm)
        .getSuggester(ADDREPLICA);

    MapWriter v2Request = (MapWriter) ((V2RequestSupport) suggester
        .hint(Hint.COLL_SHARD, new Pair<>("mycoll2", "shard1"))
        .getSuggestion()
        .setUseV2(true))
        .getV2Request();
    assertEquals("/c/mycoll2/shards", v2Request._get("path",null));
    assertEquals("add-replica", v2Request._get("command[0]/key",null));
    assertEquals("node1", v2Request._get("command/add-replica/node",null));


    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA);
    v2Request = (MapWriter) ((V2RequestSupport) suggester
        .hint(Hint.COLL_SHARD, new Pair<>("mycoll2", "shard1"))
        .getSuggestion()
        .setUseV2(true))
        .getV2Request();

    assertEquals("/c/mycoll2/shards", v2Request._get("path",null));
    assertEquals("add-replica", v2Request._get("command[0]/key",null));
    assertEquals("node2", v2Request._get("command/add-replica/node",null));


  }


  public void testFreeDiskSuggestions() {
    String autoScalingjson = "  { cluster-policy:[" +
        "    { replica :'0', freedisk:'<1000'}," +
        "    { nodeRole : overseer, replica :0}]," +
        "  cluster-preferences :[{ minimize : cores, precision : 2 }]}";
    if(useNodeset){
      autoScalingjson = "  { cluster-policy:[" +
          "    { replica :'0', put:on-each-node ,   nodeset:{ freedisk:'<1000'}}," +
          "    { replica :0, put : on-each-node , nodeset : {nodeRole : overseer}}]," +
          "  cluster-preferences :[{ minimize : cores, precision : 2 }]}";
    }
    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    List<Violation> violations = cfg.getPolicy().createSession(cloudManagerWithData((Map) loadFromResource("testFreeDiskSuggestions.json"))).getViolations();
    assertEquals(1, violations.size());
    assertEquals(4, violations.get(0).getViolatingReplicas().size());
    assertEquals(4, violations.get(0).replicaCountDelta, 0.1);
    for (Violation.ReplicaInfoAndErr r : violations.get(0).getViolatingReplicas()) {
      assertEquals(500d, r.delta, 0.1);

    }

    List<Suggester.SuggestionInfo> l = PolicyHelper.getSuggestions(cfg, cloudManagerWithData((Map) loadFromResource("testFreeDiskSuggestions.json")));
    assertEquals(3, l.size());
    assertEquals("r4", l.get(0)._get("operation/command/move-replica/replica", null));
    assertEquals("node1", l.get(0)._get("operation/command/move-replica/targetNode", null));

    assertEquals("r3", l.get(1)._get("operation/command/move-replica/replica", null));
    assertEquals("node1", l.get(1)._get("operation/command/move-replica/targetNode", null));

    assertEquals("r2", l.get(2)._get("operation/command/move-replica/replica", null));
    assertEquals("node1", l.get(2)._get("operation/command/move-replica/targetNode", null));


    autoScalingjson = "  { cluster-policy:[" +
        "    { replica :'#ALL', freedisk:'>1000'}," +
        "    { nodeRole : overseer, replica :0}]," +
        "  cluster-preferences :[{ minimize : cores, precision : 2 }]}";
    if(useNodeset){
      autoScalingjson =  "  { cluster-policy:[" +
          "    { replica :'#ALL', nodeset:{ freedisk:'>1000'}}," +
          "    { replica :0 , put: on-each-node , nodeset : {nodeRole : overseer}}]," +
          "  cluster-preferences :[{ minimize : cores, precision : 2 }]}";
    }
    cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    violations = cfg.getPolicy().createSession(cloudManagerWithData((Map) loadFromResource("testFreeDiskSuggestions.json"))).getViolations();
    assertEquals(1, violations.size());
    assertEquals(-4, violations.get(0).replicaCountDelta, 0.1);
    assertEquals(1, violations.size());
    assertEquals(0, violations.get(0).getViolatingReplicas().size());

    l = PolicyHelper.getSuggestions(cfg, cloudManagerWithData((Map) loadFromResource("testFreeDiskSuggestions.json")));
    assertEquals(3, l.size());
    assertEquals("r4", l.get(0)._get("operation/command/move-replica/replica", null));
    assertEquals("node1", l.get(0)._get("operation/command/move-replica/targetNode", null));

    assertEquals("r3", l.get(1)._get("operation/command/move-replica/replica", null));
    assertEquals("node1", l.get(1)._get("operation/command/move-replica/targetNode", null));

    assertEquals("r2", l.get(2)._get("operation/command/move-replica/replica", null));
    assertEquals("node1", l.get(2)._get("operation/command/move-replica/targetNode", null));


  }


  public void testCoresSuggestions() {
    String autoScalingjson = "  { cluster-policy:[" +
        "    { cores :'<3', node :'#ANY'}]," +
        "  cluster-preferences :[{ minimize : cores }]}";
    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    List<Violation> violations = cfg.getPolicy().createSession(cloudManagerWithData((Map) loadFromResource("testCoresSuggestions.json"))).getViolations();
    assertFalse(violations.isEmpty());
    assertEquals(2L, violations.get(0).replicaCountDelta.longValue());

    List<Suggester.SuggestionInfo> l = PolicyHelper.getSuggestions(cfg,
        cloudManagerWithData((Map) loadFromResource("testCoresSuggestions.json")));
    assertEquals(2, l.size());
    for (Suggester.SuggestionInfo suggestionInfo : l) {
      assertEquals("10.0.0.6:7574_solr", suggestionInfo._get("operation/command/move-replica/targetNode", null));
      assertEquals("POST", suggestionInfo._get("operation/method", null));
      assertEquals("/c/mycoll1", suggestionInfo._get("operation/path", null));
    }

  }

  public void testSyspropSuggestions1() {
    String autoScalingjson = "{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 3}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': '1', shard:'#EACH', sysprop.fs : 'ssd'}" +
        "  ]" +
        "}";
    if(useNodeset){
      autoScalingjson = "{" +
          "  'cluster-preferences': [" +
          "    { 'maximize': 'freedisk', 'precision': 50}," +
          "    { 'minimize': 'cores', 'precision': 3}" +
          "  ]," +
          "  'cluster-policy': [" +
          "    { 'replica': '1', shard:'#EACH', nodeset:{ sysprop.fs : 'ssd'}}" +
          "  ]" +
          "}";
    }


    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    List<Violation> violations = cfg.getPolicy().createSession(cloudManagerWithData((Map) loadFromResource("testSyspropSuggestions1.json"))).getViolations();
    assertEquals("expected 2 violations", 2, violations.size());
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(cfg, cloudManagerWithData((Map) loadFromResource("testSyspropSuggestions1.json")));
    assertEquals(2, suggestions.size());
    for (Suggester.SuggestionInfo suggestion : suggestions) {
      suggestion._get("operation/move-replica/targetNode", null);
    }
  }

  public void testPortSuggestions() {
    String autoScalingjson = "{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 3}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, shard:'#EACH', port : '8983'}" +
        "  ]" +
        "}";

    if(useNodeset){
      autoScalingjson = "{" +
          "  'cluster-preferences': [" +
          "    { 'maximize': 'freedisk', 'precision': 50}," +
          "    { 'minimize': 'cores', 'precision': 3}" +
          "  ]," +
          "  'cluster-policy': [" +
          "    { 'replica': 0, shard:'#EACH', nodeset :{ port : '8983'}}" +
          "  ]" +
          "}";

    }
    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    List<Violation> violations = cfg.getPolicy().createSession(cloudManagerWithData((Map) loadFromResource("testPortSuggestions.json"))).getViolations();
    assertEquals(2, violations.size());
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(cfg, cloudManagerWithData((Map) loadFromResource("testPortSuggestions.json")));
    assertEquals(4, suggestions.size());
    for (Suggester.SuggestionInfo suggestionInfo : suggestions) {
      assertEquals(suggestionInfo.operation.getPath(), "/c/c1");
    }
  }

  public void testDiskSpaceHint() {
    String autoScalingjson = "cluster-preferences:[" +
        "       {minimize : cores}]" +
        " cluster-policy:[{cores:'<10',node:'#ANY'}," +
        "       {replica:'<2', shard:'#EACH',node:'#ANY'}," +
        "       { nodeRole:overseer,replica:0}]}";
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    Policy.Session session = policy.createSession(cloudManagerWithData((Map) loadFromResource("testDiskSpaceHint.json")));
    Suggester suggester = session.getSuggester(CollectionAction.ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair<>("coll1", "shard1"))
        .hint(Hint.MINFREEDISK, 150);
    CollectionAdminRequest.AddReplica op = (CollectionAdminRequest.AddReplica) suggester.getSuggestion();

    assertEquals("127.0.0.1:51078_solr", op.getNode());

    suggester = session.getSuggester(CollectionAction.ADDREPLICA)
        .hint(Hint.COLL_SHARD, new Pair<>("coll1", "shard1"));
    op = (CollectionAdminRequest.AddReplica) suggester.getSuggestion();

    assertEquals("127.0.0.1:51147_solr", op.getNode());
  }

  public void testDiskSpaceReqd() {
    String autoScaleJson = "{" +
        "  cluster-preferences: [" +
        "    { minimize : cores, precision: 2}" +
        "  ]," +
        "  cluster-policy: [" +
        "    { replica : '0' , nodeRole: overseer}" +

        "  ]" +
        "}";


    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heap:10480, sysprop.rack:rack3}," +
        "node2:{cores:4, freedisk: 262, heap:6873, sysprop.fs : ssd, sysprop.rack:rack1}," +
        "node3:{cores:7, freedisk: 749, heap:7834, sysprop.rack:rack4}," +
        "node4:{cores:0, freedisk: 900, heap:16900, nodeRole:overseer, sysprop.rack:rack2}" +
        "}");

    SolrCloudManager cloudManager = new DelegatingCloudManager(null) {
      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new DelegatingNodeStateProvider(null) {
          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> keys) {
            Map<String, Object> result = new LinkedHashMap<>();
            keys.stream().forEach(s -> result.put(s, nodeValues.get(node).get(s)));
            return result;
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            if (node.equals("node1")) {
              Map m = Utils.makeMap("newColl",
                  Utils.makeMap("shard1", Collections.singletonList(new ReplicaInfo("r1", "shard1",
                      new Replica("r1", Utils.makeMap(ZkStateReader.NODE_NAME_PROP, "node1")),
                      Utils.makeMap(FREEDISK.perReplicaValue, 200)))));
              return m;
            } else if (node.equals("node2")) {
              Map m = Utils.makeMap("newColl",
                  Utils.makeMap("shard2", Collections.singletonList(new ReplicaInfo("r1", "shard2",
                      new Replica("r1", Utils.makeMap(ZkStateReader.NODE_NAME_PROP, "node2")),
                      Utils.makeMap(FREEDISK.perReplicaValue, 200)))));
              return m;
            }
            return new HashMap<>();
          }
        };
      }

      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public Set<String> getLiveNodes() {
            return new HashSet<>(Arrays.asList("node1", "node2", "node3", "node4"));
          }

          @Override
          public DocCollection getCollection(String name) {
            return new DocCollection(name, Collections.emptyMap(), Collections.emptyMap(), DocRouter.DEFAULT) {
              @Override
              public Replica getLeader(String sliceName) {
                if (sliceName.equals("shard1"))
                  return new Replica("r1", Utils.makeMap(ZkStateReader.NODE_NAME_PROP, "node1"));
                if (sliceName.equals("shard2"))
                  return new Replica("r2", Utils.makeMap(ZkStateReader.NODE_NAME_PROP, "node2"));
                return null;
              }
            };
          }
        };
      }
    };
    List<ReplicaPosition> locations = PolicyHelper.getReplicaLocations(
        "newColl", new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScaleJson)),
        cloudManager, null, Arrays.asList("shard1", "shard2"), 1, 0, 0, null);
    assertTrue(locations.stream().allMatch(it -> "node3".equals(it.node)));
  }

  public void testMoveReplicaLeaderlast() {

    List<Pair<ReplicaInfo, Row>> validReplicas = new ArrayList<>();
    Replica replica = new Replica("r1", Utils.makeMap("leader", "true"));
    ReplicaInfo replicaInfo = new ReplicaInfo("c1", "s1", replica, new HashMap<>());
    validReplicas.add(new Pair<>(replicaInfo, null));

    replicaInfo = new ReplicaInfo("r4", "c1_s2_r1", "c1", "s2", Replica.Type.NRT, "n1", Collections.singletonMap("leader", "true"));
    validReplicas.add(new Pair<>(replicaInfo, null));


    replica = new Replica("r2", Utils.makeMap("leader", false));
    replicaInfo = new ReplicaInfo("c1", "s1", replica, new HashMap<>());
    validReplicas.add(new Pair<>(replicaInfo, null));

    replica = new Replica("r3", Utils.makeMap("leader", false));
    replicaInfo = new ReplicaInfo("c1", "s1", replica, new HashMap<>());
    validReplicas.add(new Pair<>(replicaInfo, null));


    validReplicas.sort(MoveReplicaSuggester.leaderLast);
    assertEquals("r2", validReplicas.get(0).first().getName());
    assertEquals("r3", validReplicas.get(1).first().getName());
    assertEquals("r1", validReplicas.get(2).first().getName());
    assertEquals("r4", validReplicas.get(3).first().getName());

  }

  public void testScheduledTriggerFailure() throws Exception {
    Map jsonObj = (Map) loadFromResource("testScheduledTriggerFailure.json");
    SolrCloudManager cloudManager = createCloudManager(jsonObj);
    Suggester suggester = createSuggester(cloudManager, jsonObj, null);
    int count = 0;
    while (count < 10) {
      CollectionAdminRequest.MoveReplica op = (CollectionAdminRequest.MoveReplica) suggester.getSuggestion();
      if (op == null) break;
      count++;
      log.info("OP:{}", op.getParams());
      suggester = createSuggester(cloudManager, jsonObj, suggester);
    }

    assertEquals(0, count);
  }

  public void testUtilizeNodeFailure() throws Exception {
    Map jsonObj = (Map) loadFromResource("testUtilizeNodeFailure.json"); //(Map) Utils.fromJSONString(state);
    SolrCloudManager cloudManager = createCloudManager(jsonObj);
    Suggester suggester = createSuggester(cloudManager, jsonObj, null);
    int count = 0;
    while (count < 100) {
      CollectionAdminRequest.MoveReplica op = (CollectionAdminRequest.MoveReplica) suggester.getSuggestion();
      if (op == null) break;
      count++;
      log.info("OP:{}", op.getParams());
      suggester = createSuggester(cloudManager, jsonObj, suggester);
    }

    assertEquals("count = " + count, 0, count);
  }

  public void testUtilizeNodeFailure2() throws Exception {
    Map jsonObj = (Map) loadFromResource("testUtilizeNodeFailure2.json");
    SolrCloudManager cloudManager = createCloudManager(jsonObj);
    Suggester suggester = createSuggester(cloudManager, jsonObj, null);
    int count = 0;
    while (count < 100) {
      CollectionAdminRequest.MoveReplica op = (CollectionAdminRequest.MoveReplica) suggester.getSuggestion();
      if (op == null) break;
      count++;
      log.info("OP:{}", op.getParams());
      suggester = createSuggester(cloudManager, jsonObj, suggester);
    }

    assertEquals("count = " + count, 1, count);
  }

  //SOLR-12358
  public void testSortError() {
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString("{cluster-preferences: [{minimize : cores, precision:1}, " +
        "{maximize : freedisk, precision: 50}, " +
        "{minimize: sysLoadAvg}]}"));

    List l = (List) loadFromResource("testSortError.json");
    List<Variable.Type> params = new ArrayList<>();
    params.add(CORES);
    params.add(Variable.Type.FREEDISK);
    params.add(Variable.Type.SYSLOADAVG);
    params.add(Variable.Type.NODE);
    List<Row> rows = new ArrayList<>();
    for (Object o : l) {
      Map m = (Map) o;
      Cell[] c = new Cell[params.size()];
      List attrs = (List) m.get("attributes");
      for (int i = 0; i < params.size(); i++) {
        Variable.Type param = params.get(i);
        for (Object attr : attrs) {
          Object o1 = ((Map) attr).get(param.tagName);
          if (o1 != null) {
            o1 = param.validate(param.tagName, o1, false);
            c[i] = new Cell(i, param.tagName, o1, o1, param, null);
          }
        }
      }
      rows.add(new Row((String) m.get("node"), c, false,
          new HashMap<>(),
          (Boolean) m.get("isLive"), null, new HashMap(), new HashMap()));
    }
    int deadNodes = 0;
    for (Row row : rows) {
      if (!row.isLive) deadNodes++;
    }

    Policy.setApproxValuesAndSortNodes(policy.clusterPreferences, rows);

    for (int i = 0; i < deadNodes; i++) {
      assertFalse(rows.get(i).isLive);
    }

    for (int i = deadNodes; i < rows.size(); i++) {
      assertTrue(rows.get(i).isLive);
    }


  }

  public void testViolationOutput() throws IOException {
    String autoScalingjson = "{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 3}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0, shard:'#EACH', port : '8983'}" +
        "  ]" +
        "}";

    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    List<Violation> violations = cfg.getPolicy().createSession(cloudManagerWithData((Map) loadFromResource("testViolationOutput.json"))).getViolations();
    StringWriter writer = new StringWriter();
    NamedList<Object> val = new NamedList<>();
    val.add("violations", violations);
    
    if (random().nextBoolean()) {
      new SolrJSONWriter(writer)
          .writeObj(val)
          .close();
    } else {
      JSONWriter.write(writer, true, JsonTextWriter.JSON_NL_MAP, val);
    }
    
    Object root = Utils.fromJSONString(writer.toString());
    assertEquals(2l,
        Utils.getObjectByPath(root, true, "violations[0]/violation/replica/NRT"));
  }


  public void testFreediskPercentage() {

    String autoScalingjson = "{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 3}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': 0,  freedisk : '<30%'}" +
        "  ]" +
        "}";
    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    List<Violation> violations = cfg.getPolicy().createSession(cloudManagerWithData((Map) loadFromResource("testFreediskPercentage.json"))).getViolations();
    assertEquals(1, violations.size());
    assertEquals(4, violations.get(0).getViolatingReplicas().size());
    for (Violation.ReplicaInfoAndErr r : violations.get(0).getViolatingReplicas()) {
      assertEquals(10.0d, r.delta.doubleValue(), 0.1);
    }
    autoScalingjson = "{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 3}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica':'#ALL' ,  freedisk : '>30%'}" +
        "  ]" +
        "}";
    cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    violations = cfg.getPolicy().createSession(cloudManagerWithData((Map) loadFromResource("testFreediskPercentage.json"))).getViolations();
    assertEquals(1, violations.size());
    assertEquals(-4d, violations.get(0).replicaCountDelta, 0.01);
    for (Violation.ReplicaInfoAndErr r : violations.get(0).getViolatingReplicas()) {
      assertEquals(10.0d, r.delta.doubleValue(), 0.1);
    }

  }

  public void testAutoscalingPreferencesUsedWithNoPolicy() throws IOException, InterruptedException {
    Map m = (Map) loadFromResource("testAutoscalingPreferencesUsedWithNoPolicy.json");
    Map clusterState = (Map) m.remove("clusterstate");

    Map replicaInfo = (Map) m.get("replicaInfo");
    replicaInfo.forEach((node, val) -> {
      Map m1 = (Map) val;
      m1.forEach((coll, val2) -> {
        Map m2 = (Map) val2;
        m2.forEach((shard, val3) -> {
          List l3 = (List) val3;
          for (int i = 0; i < l3.size(); i++) {
            Object o = l3.get(i);
            Map m3 = (Map) o;
            String name = m3.keySet().iterator().next().toString();
            m3 = (Map) m3.get(name);
            Replica.Type type = Replica.Type.get((String) m3.get("type"));
            l3.set(i, new ReplicaInfo(name, name
                , coll.toString(), shard.toString(), type, (String) node, m3));
          }
        });

      });
    });
    AutoScalingConfig asc = m.containsKey("autoscalingJson") ? new AutoScalingConfig((Map<String, Object>) m.get("autoscalingJson")) : new AutoScalingConfig(Collections.emptyMap());
    DelegatingCloudManager cloudManager = new DelegatingCloudManager(null) {

      @Override
      public DistribStateManager getDistribStateManager() {
        return new DelegatingDistribStateManager(null) {
          @Override
          public AutoScalingConfig getAutoScalingConfig() {
            return asc;
          }
        };
      }

      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public Set<String> getLiveNodes() {
            return new HashSet<>((Collection<String>) m.get("liveNodes"));
          }

          @Override
          public ClusterState getClusterState() throws IOException {
            return ClusterState.load(0, clusterState, getLiveNodes(), ZkStateReader.getCollectionPath("c1"));
          }

          @Override
          public Map<String, Object> getClusterProperties() {
            return Collections.singletonMap("defaults", Collections.singletonMap("cluster", Collections.singletonMap(CollectionAdminParams.USE_LEGACY_REPLICA_ASSIGNMENT, false)));
          }
        };
      }

      @Override
      public NodeStateProvider getNodeStateProvider() {
        return new DelegatingNodeStateProvider(null) {
          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
            Map<String, Object> result = (Map<String, Object>) Utils.getObjectByPath(m, false, Arrays.asList("nodeValues", node));
            return result == null ? new HashMap<>() : result;
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            Map<String, Map<String, List<ReplicaInfo>>> result = (Map<String, Map<String, List<ReplicaInfo>>>) Utils.getObjectByPath(m, false, Arrays.asList("replicaInfo", node));
            return result == null ? new HashMap<>() : result;
          }
        };
      }
    };

    Assign.AssignRequest assignRequest = new Assign.AssignRequestBuilder()
        .forCollection("c1")
        .forShard(Collections.singletonList("s1"))
        .assignNrtReplicas(1)
        .build();
    Assign.AssignStrategyFactory assignStrategyFactory = new Assign.AssignStrategyFactory(cloudManager);
    ClusterState state = cloudManager.getClusterStateProvider().getClusterState();
    DocCollection collection = state.getCollection("c1");
    Assign.AssignStrategy assignStrategy = assignStrategyFactory.create(state, collection);
    List<ReplicaPosition> replicaPositions = assignStrategy.assign(cloudManager, assignRequest);

    assertEquals(1, replicaPositions.size());
    ReplicaPosition replicaPosition = replicaPositions.get(0);
    assertEquals("node3:8985", replicaPosition.node); // only node3:8985 has enough space to handle the new replica
    assertEquals("s1", replicaPosition.shard); // sanity check
  }

  public void testPolicyForEmptyCollection() throws IOException, InterruptedException {
    Map m = (Map) loadFromResource("testEmptyCollection.json");
    Map clusterStateMap = (Map) m.remove("clusterstate");
    Map replicaInfoMap = (Map) m.remove("replicaInfo");

    ClusterState clusterState = ClusterState.load(1, clusterStateMap, ImmutableSet.of("node1", "node2"), CLUSTER_STATE);

    List<String> shards = Arrays.asList("shard1", "shard2", "shard3");

    Assign.AssignRequest assignRequest = new Assign.AssignRequestBuilder()
        .forCollection("test_empty_collection")
        .forShard(shards)
        .assignNrtReplicas(1)
        .build();

    DelegatingCloudManager cloudManager = new DelegatingCloudManager(null) {
      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public ClusterState getClusterState() {
            return clusterState;
          }

          @Override
          public Set<String> getLiveNodes() {
            return clusterState.getLiveNodes();
          }
        };
      }

      @Override
      public DistribStateManager getDistribStateManager() {
        return new DelegatingDistribStateManager(null) {
          @Override
          public AutoScalingConfig getAutoScalingConfig() {
            return new AutoScalingConfig(new HashMap<>());
          }
        };
      }

      public NodeStateProvider getNodeStateProvider() {
        return new DelegatingNodeStateProvider(null) {
          @Override
          public Map<String, Object> getNodeValues(String node, Collection<String> keys) {
            return Collections.EMPTY_MAP;
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            //return Collections.EMPTY_MAP;
            return replicaInfoMap;
          }
        };
      }

    };

    Assign.AssignStrategyFactory assignStrategyFactory = new Assign.AssignStrategyFactory(cloudManager);
    ClusterState state = cloudManager.getClusterStateProvider().getClusterState();
    DocCollection collection = state.getCollection("test_empty_collection");

    Assign.AssignStrategy assignStrategy = assignStrategyFactory.create(state, collection);
    List<ReplicaPosition> replicaPositions = assignStrategy.assign(cloudManager, assignRequest);
    assertEquals(2,replicaPositions.stream().map((rp)-> rp.node).distinct().count());
    assertEquals(3,replicaPositions.stream().map((rp)-> rp.shard).distinct().count());
  }

  /**
   * Tests that an empty policy should not persist implicitly added keys to MapWriter
   * <p>
   * The reason behind doing this is to ensure that implicitly added cluster preferences do not ever
   * go to ZooKeeper so that we can decide whether to enable autoscaling policy framework or not.
   *
   * @see Assign#usePolicyFramework(DocCollection, SolrCloudManager)
   */
  public void testPolicyMapWriterWithEmptyPreferences() throws IOException {
    List<Map> defaultPreferences = Policy.DEFAULT_PREFERENCES
        .stream().map(preference -> preference.getOriginal()).collect(Collectors.toList());

    // first we create a completely empty policy
    Policy policy = new Policy();
    // sanity check that the default cluster preferences were added implicitly
    assertNotNull(policy.getClusterPreferences());
    // and they were the same as the default preferences
    assertEquals(policy.getClusterPreferences().size(), defaultPreferences.size());
    Set<String> writtenKeys = new HashSet<>();
    policy.writeMap(new MapWriter.EntryWriter() {
      @Override
      public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
        writtenKeys.add(k.toString());
        return this;
      }
    });
    // but those implicitly added cluster preferences are never written by MapWriter
    assertEquals(0, writtenKeys.size());

    // reset
    writtenKeys.clear();
    // now we create a policy that only has cluster preferences which happen to be the same as the default
    // preferences
    policy = new Policy(Utils.makeMap(CLUSTER_PREFERENCES, defaultPreferences));
    // sanity checks
    assertNotNull(policy.getClusterPreferences());
    assertEquals(policy.getClusterPreferences().size(), defaultPreferences.size());
    policy.writeMap(new MapWriter.EntryWriter() {
      @Override
      public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
        writtenKeys.add(k.toString());
        return this;
      }
    });
    // since the user explicitly added those preferences, they should be written by MapWriter
    assertEquals(1, writtenKeys.size());
    assertTrue(writtenKeys.contains(CLUSTER_PREFERENCES));
  }
}
