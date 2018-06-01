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


import java.io.IOException;
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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.DistributedQueueFactory;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester.Hint;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.cloud.autoscaling.Suggestion.ConditionType.FREEDISK;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

public class TestPolicy extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Suggester createSuggester(SolrCloudManager cloudManager, Map jsonObj, Suggester seed) throws IOException, InterruptedException {
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

  private SolrCloudManager createCloudManager(Map jsonObj) {
    return cloudManagerWithData(jsonObj);
  }

  public static String clusterState = "{'gettingstarted':{" +
      "    'router':{'name':'compositeId'}," +
      "    'shards':{" +
      "      'shard1':{" +
      "        'range':'80000000-ffffffff'," +
      "        'replicas':{" +
      "          'r1':{" +
      "            'core':r1," +
      "            'base_url':'http://10.0.0.4:8983/solr'," +
      "            'node_name':'node1'," +
      "            'state':'active'," +
      "            'leader':'true'}," +
      "          'r2':{" +
      "            'core':r2," +
      "            'base_url':'http://10.0.0.4:7574/solr'," +
      "            'node_name':'node2'," +
      "            'state':'active'}}}," +
      "      'shard2':{" +
      "        'range':'0-7fffffff'," +
      "        'replicas':{" +
      "          'r3':{" +
      "            'core':r3," +
      "            'base_url':'http://10.0.0.4:8983/solr'," +
      "            'node_name':'node1'," +
      "            'state':'active'," +
      "            'leader':'true'}," +
      "          'r4':{" +
      "            'core':r4," +
      "            'base_url':'http://10.0.0.4:8987/solr'," +
      "            'node_name':'node4'," +
      "            'state':'active'}," +
      "          'r6':{" +
      "            'core':r6," +
      "            'base_url':'http://10.0.0.4:8989/solr'," +
      "            'node_name':'node3'," +
      "            'state':'active'}," +
      "          'r5':{" +
      "            'core':r5," +
      "            'base_url':'http://10.0.0.4:8983/solr'," +
      "            'node_name':'node1'," +
      "            'state':'active'}}}}}}";

  public static Map<String, Map<String, List<ReplicaInfo>>> getReplicaDetails(String node, String clusterState) {
    ValidatingJsonMap m = ValidatingJsonMap
        .getDeepCopy((Map) Utils.fromJSONString(clusterState), 6, true);
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

  public void testValidate() {
    expectError("replica", -1, "must be greater than");
    expectError("replica", "hello", "not a valid number");
    assertEquals(1L, Clause.validate("replica", "1", true));
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


  }

  private static void expectError(String name, Object val, String msg) {
    try {
      Clause.validate(name, val, true);
      fail("expected exception containing " + msg);
    } catch (Exception e) {
      assertTrue("expected exception containing " + msg, e.getMessage().contains(msg));
    }

  }

  public void testOperands() {
    Clause c = new Clause((Map<String, Object>) Utils.fromJSONString("{replica:'<2', node:'#ANY'}"));
    assertFalse(c.replica.isPass(3));
    assertFalse(c.replica.isPass(2));
    assertTrue(c.replica.isPass(1));

    c = new Clause((Map<String, Object>) Utils.fromJSONString("{replica:'>2', node:'#ANY'}"));
    assertTrue(c.replica.isPass(3));
    assertFalse(c.replica.isPass(2));
    assertFalse(c.replica.isPass(1));

    c = new Clause((Map<String, Object>) Utils.fromJSONString("{replica:0, nodeRole:'!overseer'}"));
    assertTrue(c.tag.isPass("OVERSEER"));
    assertFalse(c.tag.isPass("overseer"));

    c = new Clause((Map<String, Object>) Utils.fromJSONString("{replica:0, sysLoadAvg:'<12.7'}"));
    assertTrue(c.tag.isPass("12.6"));
    assertTrue(c.tag.isPass(12.6d));
    assertFalse(c.tag.isPass("12.9"));
    assertFalse(c.tag.isPass(12.9d));

    c = new Clause((Map<String, Object>) Utils.fromJSONString("{replica:0, sysLoadAvg:'>12.7'}"));
    assertTrue(c.tag.isPass("12.8"));
    assertTrue(c.tag.isPass(12.8d));
    assertFalse(c.tag.isPass("12.6"));
    assertFalse(c.tag.isPass(12.6d));

    c = new Clause((Map<String, Object>) Utils.fromJSONString("{replica:0, 'metrics:x:y:z':'>12.7'}"));
    assertTrue(c.tag.val instanceof String);
    assertTrue(c.tag.isPass("12.8"));
    assertTrue(c.tag.isPass(12.8d));
    assertFalse(c.tag.isPass("12.6"));
    assertFalse(c.tag.isPass(12.6d));

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
   /* String stateJson = "{'testNodeLost':{" +
        "           'pullReplicas':'0'," +
        "           'replicationFactor':'2'," +
        "           'router':{'name':'compositeId'}," +
        "           'maxShardsPerNode':'1'," +
        "           'autoAddReplicas':'false'," +
        "           'nrtReplicas':'2'," +
        "           'tlogReplicas':'0'," +
        "           'shards':{'shard1':{" +
        "               'range':'80000000-7fffffff'," +
        "               'state':'active'," +
        "               'replicas':{" +
        "                 'core_node1':{" +
        "                   'core':'testNodeLost_shard1_replica_n1'," +
        "                   'base_url':'http://127.0.0.1:65417/solr'," +
        "                   'node_name':'127.0.0.1:65417_solr'," +
        "                   'state':'active'," +
        "                   'type':'NRT'," +
        "                   'leader':'true'}," +
        "                 'core_node2':{" +
        "                   'core':'testNodeLost_shard1_replica_n2'," +
        "                   'base_url':'http://127.0.0.1:65427/solr'," +
        "                   'node_name':'127.0.0.1:65427_solr'," +
        "                   'state':'down'," +
        "                   'type':'NRT'}}}}}}";*/

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
    config = new AutoScalingConfig(policies);
    policy = config.getPolicy();
    session = policy.createSession(provider);
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");

    operation = suggester.getSuggestion();
    assertNotNull(operation);
    assertEquals("node2", operation.getParams().get("targetNode"));
    assertEquals("r3", operation.getParams().get("replica"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getSuggestion();
    assertNotNull(operation);
    assertEquals("node2", operation.getParams().get("targetNode"));
    assertEquals("r5", operation.getParams().get("replica"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getSuggestion();
    assertEquals("node2", operation.getParams().get("targetNode"));
    assertEquals("r1", operation.getParams().get("replica"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getSuggestion();
    assertNull(operation);

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
    config = new AutoScalingConfig(policies);
    policy = config.getPolicy();
    session = policy.createSession(provider);
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");

    operation = suggester.getSuggestion();
    assertNotNull(operation);
    assertEquals("node2", operation.getParams().get("targetNode"));
    assertEquals("r3", operation.getParams().get("replica"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getSuggestion();
    assertNotNull(operation);
    assertEquals("node2", operation.getParams().get("targetNode"));
    assertEquals("r5", operation.getParams().get("replica"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getSuggestion();
    assertEquals("node3", operation.getParams().get("targetNode"));
    assertEquals("r1", operation.getParams().get("replica"));
  }

  private static SolrCloudManager cloudManagerWithData(String data) {
    return cloudManagerWithData((Map) Utils.fromJSONString(data));
  }

  private static SolrCloudManager cloudManagerWithData(Map m) {
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
            return result == null ? Collections.emptyMap() : result;
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            Map<String, Map<String, List<ReplicaInfo>>> result = (Map<String, Map<String, List<ReplicaInfo>>>) Utils.getObjectByPath(m, false, Arrays.asList("replicaInfo", node));
            return result == null ? Collections.emptyMap() : result;
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
    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480, rack: rack4, sysprop.fs: slowdisk}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873, rack: rack3}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834, rack: rack2, sysprop.fs : ssd}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer, rack: rack1}" +
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


  public void testMoveReplicasInMultipleCollections() {
    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:2}," +
        "node3:{cores:4}" +
        "}");
    String clusterState = "{\n" +
        "'collection1' : {\n" +
        "  'pullReplicas':'0',\n" +
        "  'replicationFactor':'2',\n" +
        "  'shards':{\n" +
        "    'shard1':{\n" +
        "      'range':'80000000-ffffffff',\n" +
        "      'state':'active',\n" +
        "      'replicas':{\n" +
        "        'core_node1':{\n" +
        "          'core':'collection1_shard1_replica_n1',\n" +
        "          'base_url':'http://127.0.0.1:51650/solr',\n" +
        "          'node_name':'node1',\n" +
        "          'state':'active',\n" +
        "          'type':'NRT',\n" +
        "          'leader':'true'},\n" +
        "        'core_node6':{\n" +
        "          'core':'collection1_shard1_replica_n3',\n" +
        "          'base_url':'http://127.0.0.1:51651/solr',\n" +
        "          'node_name':'node3',\n" +
        "          'state':'active',\n" +
        "          'type':'NRT'}}},\n" +
        "    'shard2':{\n" +
        "      'range':'0-7fffffff',\n" +
        "      'state':'active',\n" +
        "      'replicas':{\n" +
        "        'core_node3':{\n" +
        "          'core':'collection1_shard2_replica_n1',\n" +
        "          'base_url':'http://127.0.0.1:51650/solr',\n" +
        "          'node_name':'node1',\n" +
        "          'state':'active',\n" +
        "          'type':'NRT',\n" +
        "          'leader':'true'},\n" +
        "        'core_node5':{\n" +
        "          'core':'collection1_shard2_replica_n3',\n" +
        "          'base_url':'http://127.0.0.1:51651/solr',\n" +
        "          'node_name':'node3',\n" +
        "          'state':'active',\n" +
        "          'type':'NRT'}}}},\n" +
        "  'router':{'name':'compositeId'},\n" +
        "  'maxShardsPerNode':'2',\n" +
        "  'autoAddReplicas':'true',\n" +
        "  'nrtReplicas':'2',\n" +
        "  'tlogReplicas':'0'},\n" +
        "'collection2' : {\n" +
        "  'pullReplicas':'0',\n" +
        "  'replicationFactor':'2',\n" +
        "  'shards':{\n" +
        "    'shard1':{\n" +
        "      'range':'80000000-ffffffff',\n" +
        "      'state':'active',\n" +
        "      'replicas':{\n" +
        "        'core_node1':{\n" +
        "          'core':'collection2_shard1_replica_n1',\n" +
        "          'base_url':'http://127.0.0.1:51649/solr',\n" +
        "          'node_name':'node2',\n" +
        "          'state':'active',\n" +
        "          'type':'NRT'},\n" +
        "        'core_node2':{\n" +
        "          'core':'collection2_shard1_replica_n2',\n" +
        "          'base_url':'http://127.0.0.1:51651/solr',\n" +
        "          'node_name':'node3',\n" +
        "          'state':'active',\n" +
        "          'type':'NRT',\n" +
        "          'leader':'true'}}},\n" +
        "    'shard2':{\n" +
        "      'range':'0-7fffffff',\n" +
        "      'state':'active',\n" +
        "      'replicas':{\n" +
        "        'core_node3':{\n" +
        "          'core':'collection2_shard2_replica_n1',\n" +
        "          'base_url':'http://127.0.0.1:51649/solr',\n" +
        "          'node_name':'node2',\n" +
        "          'state':'active',\n" +
        "          'type':'NRT'},\n" +
        "        'core_node4':{\n" +
        "          'core':'collection2_shard2_replica_n2',\n" +
        "          'base_url':'http://127.0.0.1:51651/solr',\n" +
        "          'node_name':'node3',\n" +
        "          'state':'active',\n" +
        "          'type':'NRT',\n" +
        "          'leader':'true'}}}},\n" +
        "  'router':{'name':'compositeId'},\n" +
        "  'maxShardsPerNode':'2',\n" +
        "  'autoAddReplicas':'true',\n" +
        "  'nrtReplicas':'2',\n" +
        "  'tlogReplicas':'0'}\n" +
        "}";
    Policy policy = new Policy(new HashMap<>());
    Suggester suggester = policy.createSession(getSolrCloudManager(nodeValues, clusterState))
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
    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480, rack: rack4, sysprop.fs: slowdisk}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873, rack: rack3}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834, rack: rack2, sysprop.fs : ssd}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer, rack: rack1}" +
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
      assertEquals("PULL type node must be in 'slowdisk' node", "node1", op.getParams().get("node"));
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
    Policy policy = new Policy(map);
    List<Clause> clauses = Policy.mergePolicies("mycoll", policy.getPolicies().get("policy1"), policy.getClusterPolicy());
    Collections.sort(clauses);
    assertEquals(clauses.size(), 4);
    assertEquals("1", String.valueOf(clauses.get(0).original.get("replica")));
    assertEquals("0", String.valueOf(clauses.get(1).original.get("replica")));
    assertEquals("#ANY", clauses.get(3).original.get("shard"));
    assertEquals("rack1", clauses.get(2).original.get("sysprop.rack"));
    assertEquals("overseer", clauses.get(1).original.get("nodeRole"));
  }

  public void testConditionsSort() {
    String rules = "{" +
        "    'cluster-policy':[" +
        "      { 'nodeRole':'overseer', replica: 0,  'strict':false}," +
        "      { 'replica':'<1', 'node':'node3', 'shard':'#EACH'}," +
        "      { 'replica':'<2', 'node':'#ANY', 'shard':'#EACH'}," +
        "      { 'replica':1, 'sysprop.rack':'rack1'}]" +
        "  }";
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

    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer}" +
        "}");

    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(rules));
    Policy.Session session;
    session = policy.createSession(getSolrCloudManager(nodeValues, clusterState));

    List<Row> l = session.getSorted();
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
    assertTrue(sessionRef.getSessionWrapper() == PolicyHelper.SessionWrapper.DEF_INST);
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
    assertFalse(sessionRef.getSessionWrapper() == PolicyHelper.SessionWrapper.DEF_INST);
    s1.release();
    assertTrue(sessionRef.getSessionWrapper() == PolicyHelper.SessionWrapper.DEF_INST);


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
        "node1:{cores:12, freedisk: 334, heapUsage:10480, rack: rack4}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873, rack: rack3}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834, rack: rack2, sysprop.fs : ssd}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer, rack: rack1}" +
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
        "            'freedisk':918005641216}}}");

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
        "{'replica':'<3','shard':'#EACH','sysprop.rack':'#ANY'}" +
        "]" +
        "}" +
        "}";

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

  private SolrCloudManager getSolrCloudManager(final Map<String, Map> nodeValues, String clusterState) {
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
            return getReplicaDetails(node, clusterState);
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
            return getReplicaDetails(node, clusterState);
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
            return getReplicaDetails(node, clusterState);
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
    String dataproviderdata = "{" +
        "  'liveNodes':[" +
        "    '10.0.0.6:7574_solr'," +
        "    '10.0.0.6:8983_solr']," +
        "  'replicaInfo':{" +
        "    '10.0.0.6:7574_solr':{}," +
        "    '10.0.0.6:8983_solr':{'mycoll1':{" +
        "        'shard2':[{'core_node2':{'type':'NRT'}}]," +
        "        'shard1':[{'core_node1':{'type':'NRT'}}]}}}," +
        "  'nodeValues':{" +
        "    '10.0.0.6:7574_solr':{" +
        "      'node':'10.0.0.6:7574_solr'," +
        "      'cores':0}," +
        "    '10.0.0.6:8983_solr':{" +
        "      'node':'10.0.0.6:8983_solr'," +
        "      'cores':2}}}";
    String autoScalingjson = "  '{cluster-policy':[" +
        "    {      'cores':'<10',      'node':'#ANY'}," +
        "    {      'replica':'<2',      'shard':'#EACH',      'node':'#ANY'}," +
        "    {      'nodeRole':'overseer','replica':0}]," +
        "  'cluster-preferences':[{'minimize':'cores'}]}";
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    Policy.Session session = policy.createSession(cloudManagerWithData(dataproviderdata));
    Suggester suggester = session.getSuggester(MOVEREPLICA).hint(Hint.TARGET_NODE, "10.0.0.6:7574_solr");
    SolrRequest op = suggester.getSuggestion();
    assertNotNull(op);
    suggester = suggester.getSession().getSuggester(MOVEREPLICA).hint(Hint.TARGET_NODE, "10.0.0.6:7574_solr");
    op = suggester.getSuggestion();
    assertNull(op);
  }

  public void testComputePlanAfterNodeAdded() {

    String dataproviderdata = "{" +
        "     liveNodes:[" +
        "       '127.0.0.1:51078_solr'," +
        "       '127.0.0.1:51147_solr']," +
        "     replicaInfo:{" +
        "       '127.0.0.1:51147_solr':{}," +
        "       '127.0.0.1:51078_solr':{testNodeAdded:{shard1:[" +
        "             { core_node3 : { type : NRT}}," +
        "             { core_node4 : { type : NRT}}]}}}," +
        "     nodeValues:{" +
        "       '127.0.0.1:51147_solr':{" +
        "         node:'127.0.0.1:51147_solr'," +
        "         cores:0," +
        "         freedisk : 880.5428657531738}," +
        "       '127.0.0.1:51078_solr':{" +
        "         node:'127.0.0.1:51078_solr'," +
        "         cores:2," +
        "         freedisk:880.5428695678711}}}";

    String autoScalingjson = "cluster-preferences:[" +
        "       {minimize : cores}," +
        "       {'maximize':freedisk , precision:100}],    " +
        " cluster-policy:[{cores:'<10',node:'#ANY'}," +
        "       {replica:'<2', shard:'#EACH',node:'#ANY'}," +
        "       { nodeRole:overseer,replica:0}]}";
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    Policy.Session session = policy.createSession(cloudManagerWithData(dataproviderdata));
    Suggester suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA)
        .hint(Hint.TARGET_NODE, "127.0.0.1:51147_solr");
    SolrRequest op = suggester.getSuggestion();
    log.info("" + op);
    assertNotNull("operation expected ", op);
  }

  public void testReplicaCountSuggestions() {
    String dataproviderdata = "{" +
        "  'liveNodes':[" +
        "    '10.0.0.6:7574_solr'," +
        "    '10.0.0.6:8983_solr']," +
        "  'replicaInfo':{" +
        "    '10.0.0.6:7574_solr':{}," +
        "    '10.0.0.6:8983_solr':{'mycoll1':{" +
        "        'shard2':[{'core_node2':{'type':'NRT'}}]," +
        "        'shard1':[{'core_node1':{'type':'NRT'}}]}}}," +
        "  'nodeValues':{" +
        "    '10.0.0.6:7574_solr':{" +
        "      'node':'10.0.0.6:7574_solr'," +
        "      'cores':0}," +
        "    '10.0.0.6:8983_solr':{" +
        "      'node':'10.0.0.6:8983_solr'," +
        "      'cores':2}}}";
    String autoScalingjson = "  { cluster-policy:[" +
        "    { cores :'<10', node :'#ANY'}," +
        "    { replica :'<2',  node:'#ANY'}," +
        "    { nodeRole : overseer, replica :0}]," +
        "  cluster-preferences :[{ minimize : cores }]}";
    List<Suggester.SuggestionInfo> l = PolicyHelper.getSuggestions(new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson)),
        cloudManagerWithData(dataproviderdata));
    assertFalse(l.isEmpty());

    Map m = l.get(0).toMap(new LinkedHashMap<>());
    assertEquals(1L, Utils.getObjectByPath(m, true, "violation/violation/delta"));
    assertEquals("POST", Utils.getObjectByPath(m, true, "operation/method"));
    assertEquals("/c/mycoll1", Utils.getObjectByPath(m, true, "operation/path"));
    assertNotNull(Utils.getObjectByPath(m, false, "operation/command/move-replica"));
    assertEquals("10.0.0.6:7574_solr", Utils.getObjectByPath(m, true, "operation/command/move-replica/targetNode"));
    assertEquals("core_node2", Utils.getObjectByPath(m, true, "operation/command/move-replica/replica"));
  }

  public void testFreeDiskSuggestions() {
    String dataproviderdata = "{" +
        "  liveNodes:[node1,node2]," +
        "  replicaInfo : {" +
        "    node1:{}," +
        "    node2:{mycoll1:{" +
        "        shard1:[{r1:{type:NRT, INDEX.sizeInBytes:900}}]," +
        "        shard2:[{r2:{type:NRT, INDEX.sizeInBytes:300}}]," +
        "        shard3:[{r3:{type:NRT, INDEX.sizeInBytes:200}}]," +
        "        shard4:[{r4:{type:NRT, INDEX.sizeInBytes:100}}]}}}" +
        "    nodeValues : {" +
        "    node1: { node : node1 , cores:0 , freedisk : 2000}," +
        "    node2: { node : node2 , cores:4 , freedisk : 500}}}";


    String autoScalingjson = "  { cluster-policy:[" +
        "    { replica :'0', freedisk:'<1000'}," +
        "    { nodeRole : overseer, replica :0}]," +
        "  cluster-preferences :[{ minimize : cores, precision : 2 }]}";
    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    List<Violation> violations = cfg.getPolicy().createSession(cloudManagerWithData(dataproviderdata)).getViolations();
    assertEquals(1, violations.size());
    assertEquals(4, violations.get(0).getViolatingReplicas().size());
    List<Suggester.SuggestionInfo> l = PolicyHelper.getSuggestions(cfg, cloudManagerWithData(dataproviderdata));
    assertEquals(3, l.size());
    Map m = l.get(0).toMap(new LinkedHashMap<>());
    assertEquals("r4", Utils.getObjectByPath(m, true, "operation/command/move-replica/replica"));
    assertEquals("node1", Utils.getObjectByPath(m, true, "operation/command/move-replica/targetNode"));

    m = l.get(1).toMap(new LinkedHashMap<>());
    assertEquals("r3", Utils.getObjectByPath(m, true, "operation/command/move-replica/replica"));
    assertEquals("node1", Utils.getObjectByPath(m, true, "operation/command/move-replica/targetNode"));

    m = l.get(2).toMap(new LinkedHashMap<>());
    assertEquals("r2", Utils.getObjectByPath(m, true, "operation/command/move-replica/replica"));
    assertEquals("node1", Utils.getObjectByPath(m, true, "operation/command/move-replica/targetNode"));


  }


  public void testCoresSuggestions() {
    String dataproviderdata = "{" +
        "  'liveNodes':[" +
        "    '10.0.0.6:7574_solr'," +
        "    '10.0.0.6:8983_solr']," +
        "  'replicaInfo':{" +
        "    '10.0.0.6:7574_solr':{}," +
        "    '10.0.0.6:8983_solr':{'mycoll1':{" +
        "        'shard1':[{'core_node1':{'type':'NRT'}}]," +
        "        'shard2':[{'core_node2':{'type':'NRT'}}]," +
        "        'shard3':[{'core_node3':{'type':'NRT'}}]," +
        "        'shard4':[{'core_node4':{'type':'NRT'}}]}}}," +
        "  'nodeValues':{" +
        "    '10.0.0.6:7574_solr':{" +
        "      'node':'10.0.0.6:7574_solr'," +
        "      'cores':0}," +
        "    '10.0.0.6:8983_solr':{" +
        "      'node':'10.0.0.6:8983_solr'," +
        "      'cores':4}}}";
    String autoScalingjson = "  { cluster-policy:[" +
        "    { cores :'<3', node :'#ANY'}]," +
        "  cluster-preferences :[{ minimize : cores }]}";
    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    List<Violation> violations = cfg.getPolicy().createSession(cloudManagerWithData(dataproviderdata)).getViolations();
    assertFalse(violations.isEmpty());
    assertEquals(2L, violations.get(0).replicaCountDelta.longValue());

    List<Suggester.SuggestionInfo> l = PolicyHelper.getSuggestions(cfg,
        cloudManagerWithData(dataproviderdata));
    assertEquals(2, l.size());
    for (Suggester.SuggestionInfo suggestionInfo : l) {
      Map m = suggestionInfo.toMap(new LinkedHashMap<>());
      assertEquals("10.0.0.6:7574_solr", Utils.getObjectByPath(m, true, "operation/command/move-replica/targetNode"));
      assertEquals("POST", Utils.getObjectByPath(m, true, "operation/method"));
      assertEquals("/c/mycoll1", Utils.getObjectByPath(m, true, "operation/path"));
    }

  }

  public void testSyspropSuggestions() {
    String autoScalingjson = "{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 3}" +
        "  ]," +
        "  'cluster-policy': [" +
        "    { 'replica': '1', shard:'#EACH', sysprop.fs : 'ssd'}" +
        "  ]" +
        "}";


    String dataproviderdata = "{" +
        "  'liveNodes': [" +
        "    'node1'," +
        "    'node2'," +
        "    'node3'" +
        "  ]," +
        "  'replicaInfo': {" +
        "    'node1': {" +
        "      'c1': {'s1': [{'r1': {'type': 'NRT'}, 'r2': {'type': 'NRT'}}]," +
        "             's2': [{'r1': {'type': 'NRT'}, 'r2': {'type': 'NRT'}}]}," +
        "    }" +
        "  }," +
        "    'nodeValues': {" +
        "      'node1': {'cores': 2, 'freedisk': 334, 'sysprop.fs': 'slowdisk'}," +
        "      'node2': {'cores': 2, 'freedisk': 749, 'sysprop.fs': 'slowdisk'}," +
        "      'node3': {'cores': 0, 'freedisk': 262, 'sysprop.fs': 'ssd'}" +
        "    }" +
        "}";
    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    List<Violation> violations = cfg.getPolicy().createSession(cloudManagerWithData(dataproviderdata)).getViolations();
    assertEquals(2, violations.size());
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(cfg, cloudManagerWithData(dataproviderdata));
    assertEquals(2, suggestions.size());
    for (Suggester.SuggestionInfo suggestion : suggestions) {
      Utils.getObjectByPath(suggestion, true, "operation/move-replica/targetNode");
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


    String dataproviderdata = "{" +
        "  'liveNodes': [" +
        "    'node1:8983'," +
        "    'node2:8984'," +
        "    'node3:8985'" +
        "  ]," +
        "  'replicaInfo': {" +
        "    'node1:8983': {" +
        "      'c1': {" +
        "        's1': [" +
        "          {'r1': {'type': 'NRT'}}," +
        "          {'r2': {'type': 'NRT'}}" +
        "        ]," +
        "        's2': [" +
        "          {'r1': {'type': 'NRT'}}," +
        "          {'r2': {'type': 'NRT'}}" +
        "        ]" +
        "      }" +
        "    }" +
        "  }," +
        "  'nodeValues': {" +
        "    'node1:8983': {" +
        "      'cores': 4," +
        "      'freedisk': 334," +
        "      'port': 8983" +
        "    }," +
        "    'node2:8984': {" +
        "      'cores': 0," +
        "      'freedisk': 1000," +
        "      'port': 8984" +
        "    }," +
        "    'node3:8985': {" +
        "      'cores': 0," +
        "      'freedisk': 1500," +
        "      'port': 8985" +
        "    }" +
        "  }" +
        "}";
    AutoScalingConfig cfg = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    List<Violation> violations = cfg.getPolicy().createSession(cloudManagerWithData(dataproviderdata)).getViolations();
    assertEquals(2, violations.size());
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(cfg, cloudManagerWithData(dataproviderdata));
    assertEquals(4, suggestions.size());
    for (Suggester.SuggestionInfo suggestionInfo : suggestions) {
      assertEquals(suggestionInfo.operation.getPath(), "/c/c1");
    }
  }

  public void testDiskSpaceHint() {

    String dataproviderdata = "{" +
        "     liveNodes:[" +
        "       '127.0.0.1:51078_solr'," +
        "       '127.0.0.1:51147_solr']," +
        "     replicaInfo:{" +
        "       '127.0.0.1:51147_solr':{}," +
        "       '127.0.0.1:51078_solr':{testNodeAdded:{shard1:[" +
        "             { core_node3 : { type : NRT}}," +
        "             { core_node4 : { type : NRT}}]}}}," +
        "     nodeValues:{" +
        "       '127.0.0.1:51147_solr':{" +
        "         node:'127.0.0.1:51147_solr'," +
        "         cores:0," +
        "         freedisk : 100}," +
        "       '127.0.0.1:51078_solr':{" +
        "         node:'127.0.0.1:51078_solr'," +
        "         cores:2," +
        "         freedisk:200}}}";

    String autoScalingjson = "cluster-preferences:[" +
        "       {minimize : cores}]" +
        " cluster-policy:[{cores:'<10',node:'#ANY'}," +
        "       {replica:'<2', shard:'#EACH',node:'#ANY'}," +
        "       { nodeRole:overseer,replica:0}]}";
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    Policy.Session session = policy.createSession(cloudManagerWithData(dataproviderdata));
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
            return Collections.emptyMap();
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
    String state = "{" +
        "  'liveNodes': [" +
        "    '127.0.0.1:49221_solr'," +
        "    '127.0.0.1:49210_solr'" +
        "  ]," +
        "  'suggester': {" +
        "    'action': 'MOVEREPLICA'," +
        "    'hints': {}" +
        "  }," +
        "  'replicaInfo': {" +
        "    '127.0.0.1:49210_solr': {" +
        "      'testScheduledTrigger': {" +
        "        'shard1': [" +
        "          {" +
        "            'core_node3': {" +
        "              'base_url': 'http://127.0.0.1:49210/solr'," +
        "              'node_name': '127.0.0.1:49210_solr'," +
        "              'core': 'testScheduledTrigger_shard1_replica_n1'," +
        "              'state': 'active'," +
        "              'type': 'NRT'," +
        "              'INDEX.sizeInBytes': 6.426125764846802E-8," +
        "              'shard': 'shard1'," +
        "              'collection': 'testScheduledTrigger'" +
        "            }" +
        "          }," +
        "          {" +
        "            'core_node6': {" +
        "              'base_url': 'http://127.0.0.1:49210/solr'," +
        "              'node_name': '127.0.0.1:49210_solr'," +
        "              'core': 'testScheduledTrigger_shard1_replica_n4'," +
        "              'state': 'active'," +
        "              'type': 'NRT'," +
        "              'INDEX.sizeInBytes': 6.426125764846802E-8," +
        "              'shard': 'shard1'," +
        "              'collection': 'testScheduledTrigger'" +
        "            }" +
        "          }" +
        "        ]" +
        "      }" +
        "    }," +
        "    '127.0.0.1:49221_solr': {" +
        "      'testScheduledTrigger': {" +
        "        'shard1': [" +
        "          {" +
        "            'core_node5': {" +
        "              'core': 'testScheduledTrigger_shard1_replica_n2'," +
        "              'leader': 'true'," +
        "              'INDEX.sizeInBytes': 6.426125764846802E-8," +
        "              'base_url': 'http://127.0.0.1:49221/solr'," +
        "              'node_name': '127.0.0.1:49221_solr'," +
        "              'state': 'active'," +
        "              'type': 'NRT'," +
        "              'shard': 'shard1'," +
        "              'collection': 'testScheduledTrigger'" +
        "            }" +
        "          }" +
        "        ]" +
        "      }" +
        "    }" +
        "  }," +
        "  'nodeValues': {" +
        "    '127.0.0.1:49210_solr': {" +
        "      'node': '127.0.0.1:49210_solr'," +
        "      'cores': 2," +
        "      'freedisk': 197.39717864990234" +
        "    }," +
        "    '127.0.0.1:49221_solr': {" +
        "      'node': '127.0.0.1:49221_solr'," +
        "      'cores': 1," +
        "      'freedisk': 197.39717864990234" +
        "    }" +
        "  }," +
        "  'autoscalingJson': {" +
        "    'cluster-preferences': [" +
        "      {" +
        "        'minimize': 'cores'," +
        "        'precision': 1" +
        "      }," +
        "      {" +
        "        'maximize': 'freedisk'" +
        "      }" +
        "    ]," +
        "    'cluster-policy': [" +
        "      {" +
        "        'cores': '<3'," +
        "        'node': '#EACH'" +
        "      }" +
        "    ]" +
        "  }" +
        "}";
    Map jsonObj = (Map) Utils.fromJSONString(state);
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
    String state = "{'liveNodes': ['127.0.0.1:50417_solr', '127.0.0.1:50418_solr', '127.0.0.1:50419_solr', '127.0.0.1:50420_solr', '127.0.0.1:50443_solr']," +
        "  'suggester': {" +
        "    'action': 'MOVEREPLICA'," +
        "    'hints': {'TARGET_NODE': ['127.0.0.1:50443_solr']}" +
        "  }," +
        "  'replicaInfo': {" +
        "    '127.0.0.1:50418_solr': {" +
        "      'utilizenodecoll': {" +
        "        'shard2': [" +
        "          {" +
        "            'core_node7': {" +
        "              'core': 'utilizenodecoll_shard2_replica_n4'," +
        "              'leader': 'true'," +
        "              'INDEX.sizeInBytes': 6.426125764846802E-8," +
        "              'base_url': 'http://127.0.0.1:50418/solr'," +
        "              'node_name': '127.0.0.1:50418_solr'," +
        "              'state': 'active'," +
        "              'type': 'NRT'," +
        "              'shard': 'shard2'," +
        "              'collection': 'utilizenodecoll'" +
        "            }" +
        "          }" +
        "        ]" +
        "      }" +
        "    }," +
        "    '127.0.0.1:50417_solr': {" +
        "      'utilizenodecoll': {" +
        "        'shard2': [" +
        "          {" +
        "            'core_node8': {" +
        "              'base_url': 'http://127.0.0.1:50417/solr'," +
        "              'node_name': '127.0.0.1:50417_solr'," +
        "              'core': 'utilizenodecoll_shard2_replica_n6'," +
        "              'state': 'active'," +
        "              'type': 'NRT'," +
        "              'INDEX.sizeInBytes': 6.426125764846802E-8," +
        "              'shard': 'shard2'," +
        "              'collection': 'utilizenodecoll'" +
        "            }" +
        "          }" +
        "        ]" +
        "      }" +
        "    }," +
        "    '127.0.0.1:50419_solr': {" +
        "      'utilizenodecoll': {" +
        "        'shard1': [" +
        "          {" +
        "            'core_node5': {" +
        "              'base_url': 'http://127.0.0.1:50419/solr'," +
        "              'node_name': '127.0.0.1:50419_solr'," +
        "              'core': 'utilizenodecoll_shard1_replica_n2'," +
        "              'state': 'active'," +
        "              'type': 'NRT'," +
        "              'INDEX.sizeInBytes': 6.426125764846802E-8," +
        "              'shard': 'shard1'," +
        "              'collection': 'utilizenodecoll'" +
        "            }" +
        "          }" +
        "        ]" +
        "      }" +
        "    }," +
        "    '127.0.0.1:50420_solr': {" +
        "      'utilizenodecoll': {" +
        "        'shard1': [" +
        "          {" +
        "            'core_node3': {" +
        "              'core': 'utilizenodecoll_shard1_replica_n1'," +
        "              'leader': 'true'," +
        "              'INDEX.sizeInBytes': 6.426125764846802E-8," +
        "              'base_url': 'http://127.0.0.1:50420/solr'," +
        "              'node_name': '127.0.0.1:50420_solr'," +
        "              'state': 'active'," +
        "              'type': 'NRT'," +
        "              'shard': 'shard1'," +
        "              'collection': 'utilizenodecoll'" +
        "            }" +
        "          }" +
        "        ]" +
        "      }" +
        "    }," +
        "    '127.0.0.1:50443_solr': {}" +
        "  }," +
        "  'nodeValues': {" +
        "    '127.0.0.1:50418_solr': {" +
        "      'cores': 1," +
        "      'freedisk': 187.70782089233398" +
        "    }," +
        "    '127.0.0.1:50417_solr': {" +
        "      'cores': 1," +
        "      'freedisk': 187.70782089233398" +
        "    }," +
        "    '127.0.0.1:50419_solr': {" +
        "      'cores': 1," +
        "      'freedisk': 187.70782089233398" +
        "    }," +
        "    '127.0.0.1:50420_solr': {" +
        "      'cores': 1," +
        "      'freedisk': 187.70782089233398" +
        "    }," +
        "    '127.0.0.1:50443_solr': {" +
        "      'cores': 0," +
        "      'freedisk': 187.70782089233398" +
        "    }" +
        "  }," +
        "  'autoscalingJson': {" +
        "    'cluster-preferences': [" +
        "      {'minimize': 'cores', 'precision': 1}," +
        "      {'maximize': 'freedisk'}" +
        "    ]" +
        "  }" +
        "}";
    Map jsonObj = (Map) Utils.fromJSONString(state);
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

    assertEquals("count = "+count ,0,count);
  }
public void testUtilizeNodeFailure2() throws Exception {
    String state = "{  'liveNodes':[" +
        "  '127.0.0.1:51075_solr'," +
        "  '127.0.0.1:51076_solr'," +
        "  '127.0.0.1:51077_solr'," +
        "  '127.0.0.1:51097_solr']," +
        "  'suggester':{" +
        "    'action':'MOVEREPLICA'," +
        "    'hints':{'TARGET_NODE':['127.0.0.1:51097_solr']}}," +
        "  'replicaInfo':{" +
        "    '127.0.0.1:51076_solr':{'utilizenodecoll':{'shard1':[{'core_node5':{" +
        "      'base_url':'https://127.0.0.1:51076/solr'," +
        "      'node_name':'127.0.0.1:51076_solr'," +
        "      'core':'utilizenodecoll_shard1_replica_n2'," +
        "      'state':'active'," +
        "      'type':'NRT'," +
        "      'INDEX.sizeInBytes':6.426125764846802E-8," +
        "      'shard':'shard1'," +
        "      'collection':'utilizenodecoll'}}]}}," +
        "    '127.0.0.1:51077_solr':{'utilizenodecoll':{" +
        "      'shard2':[{'core_node8':{" +
        "        'base_url':'https://127.0.0.1:51077/solr'," +
        "        'node_name':'127.0.0.1:51077_solr'," +
        "        'core':'utilizenodecoll_shard2_replica_n6'," +
        "        'state':'active'," +
        "        'type':'NRT'," +
        "        'INDEX.sizeInBytes':6.426125764846802E-8," +
        "        'shard':'shard2'," +
        "        'collection':'utilizenodecoll'}}]," +
        "      'shard1':[{'core_node3':{" +
        "        'core':'utilizenodecoll_shard1_replica_n1'," +
        "        'leader':'true'," +
        "        'INDEX.sizeInBytes':6.426125764846802E-8," +
        "        'base_url':'https://127.0.0.1:51077/solr'," +
        "        'node_name':'127.0.0.1:51077_solr'," +
        "        'state':'active'," +
        "        'type':'NRT'," +
        "        'shard':'shard1'," +
        "        'collection':'utilizenodecoll'}}]}}," +
        "    '127.0.0.1:51097_solr':{}," +
        "    '127.0.0.1:51075_solr':{'utilizenodecoll':{'shard2':[{'core_node7':{" +
        "      'core':'utilizenodecoll_shard2_replica_n4'," +
        "      'leader':'true'," +
        "      'INDEX.sizeInBytes':6.426125764846802E-8," +
        "      'base_url':'https://127.0.0.1:51075/solr'," +
        "      'node_name':'127.0.0.1:51075_solr'," +
        "      'state':'active'," +
        "      'type':'NRT'," +
        "      'shard':'shard2'," +
        "      'collection':'utilizenodecoll'}}]}}}," +
        "  'nodeValues':{" +
        "    '127.0.0.1:51076_solr':{" +
        "      'cores':1," +
        "      'freedisk':188.7262191772461}," +
        "    '127.0.0.1:51077_solr':{" +
        "      'cores':2," +
        "      'freedisk':188.7262191772461}," +
        "    '127.0.0.1:51097_solr':{" +
        "      'cores':0," +
        "      'freedisk':188.7262191772461}," +
        "    '127.0.0.1:51075_solr':{" +
        "      'cores':1," +
        "      'freedisk':188.7262191772461}}," +
        "  'autoscalingJson':{" +
        "    'cluster-preferences':[" +
        "      {" +
        "        'minimize':'cores'," +
        "        'precision':1}," +
        "      {'maximize':'freedisk'}]" +
        "    }}";
    Map jsonObj = (Map) Utils.fromJSONString(state);
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

    assertEquals("count = "+count ,1,count);
  }

  //SOLR-12358
  public void testSortError() {
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString("{cluster-preferences: [{minimize : cores, precision:1}, " +
        "{maximize : freedisk, precision: 50}, " +
        "{minimize: sysLoadAvg}]}"));
    String rowsData = "{'sortedNodes':[" +
        "    {" +
        "      'node':'solr-01:8983_solr'," +
        "      'replicas':{}," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':2}," +
        "        {'freedisk':1734.5261459350586}," +
        "        {'sysLoadAvg':35.0}," +
        "        {'node':'solr-01:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-07:8983_solr'," +
        "      'replicas':{}," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1721.5669250488281}," +
        "        {'sysLoadAvg':10.0}," +
        "        {'node':'solr-07:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-08:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1764.9518203735352}," +
        "        {'sysLoadAvg':330.0}," +
        "        {'node':'solr-08:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-25:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1779.7792778015137}," +
        "        {'sysLoadAvg':304.0}," +
        "        {'node':'solr-25:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-15:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1697.5930519104004}," +
        "        {'sysLoadAvg':277.0}," +
        "        {'node':'solr-15:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-13:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':2}," +
        "        {'freedisk':1755.1909484863281}," +
        "        {'sysLoadAvg':265.0}," +
        "        {'node':'solr-13:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-14:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1757.6035423278809}," +
        "        {'sysLoadAvg':61.0}," +
        "        {'node':'solr-14:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-16:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1746.081386566162}," +
        "        {'sysLoadAvg':260.0}," +
        "        {'node':'solr-16:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-04:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':2}," +
        "        {'freedisk':1708.7230529785156}," +
        "        {'sysLoadAvg':216.0}," +
        "        {'node':'solr-04:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-06:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1688.3182678222656}," +
        "        {'sysLoadAvg':385.0}," +
        "        {'node':'solr-06:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-02:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':6}," +
        "        {'freedisk':1778.226963043213}," +
        "        {'sysLoadAvg':369.0}," +
        "        {'node':'solr-02:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-05:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1741.9401931762695}," +
        "        {'sysLoadAvg':354.0}," +
        "        {'node':'solr-05:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-23:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1718.854579925537}," +
        "        {'sysLoadAvg':329.0}," +
        "        {'node':'solr-23:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-24:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1733.6669311523438}," +
        "        {'sysLoadAvg':327.0}," +
        "        {'node':'solr-24:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-09:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1714.6191711425781}," +
        "        {'sysLoadAvg':278.0}," +
        "        {'node':'solr-09:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-10:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1755.3038482666016}," +
        "        {'sysLoadAvg':266.0}," +
        "        {'node':'solr-10:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-28:8983_solr'," +
        "      'isLive':false," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1691.3830909729004}," +
        "        {'sysLoadAvg':261.0}," +
        "        {'node':'solr-28:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-29:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':2}," +
        "        {'freedisk':1706.797966003418}," +
        "        {'sysLoadAvg':252.99999999999997}," +
        "        {'node':'solr-29:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-32:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1762.432300567627}," +
        "        {'sysLoadAvg':221.0}," +
        "        {'node':'solr-32:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-21:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1760.9801979064941}," +
        "        {'sysLoadAvg':213.0}," +
        "        {'node':'solr-21:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-22:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1780.5297241210938}," +
        "        {'sysLoadAvg':209.0}," +
        "        {'node':'solr-22:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-31:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1700.1481628417969}," +
        "        {'sysLoadAvg':211.0}," +
        "        {'node':'solr-31:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-33:8983_solr'," +
        "      'isLive':false," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1748.1132926940918}," +
        "        {'sysLoadAvg':199.0}," +
        "        {'node':'solr-33:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-36:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1776.197639465332}," +
        "        {'sysLoadAvg':193.0}," +
        "        {'node':'solr-36:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-35:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1746.7729606628418}," +
        "        {'sysLoadAvg':191.0}," +
        "        {'node':'solr-35:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-12:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1713.287540435791}," +
        "        {'sysLoadAvg':175.0}," +
        "        {'node':'solr-12:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-11:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1736.784511566162}," +
        "        {'sysLoadAvg':169.0}," +
        "        {'node':'solr-11:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-35:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1766.9416885375977}," +
        "        {'sysLoadAvg':155.0}," +
        "        {'node':'solr-35:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-17:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1764.3425407409668}," +
        "        {'sysLoadAvg':139.0}," +
        "        {'node':'solr-17:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-18:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':2}," +
        "        {'freedisk':1757.0613975524902}," +
        "        {'sysLoadAvg':132.0}," +
        "        {'node':'solr-18:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-20:8983_solr'," +
        "      'isLive':false," +
        "      'attributes':[" +
        "        {'cores':3}," +
        "        {'freedisk':1747.4205322265625}," +
        "        {'sysLoadAvg':126.0}," +
        "        {'node':'solr-20:8983_solr'}]}," +
        "    {" +
        "      'node':'solr-27:8983_solr'," +
        "      'isLive':true," +
        "      'attributes':[" +
        "        {'cores':4}," +
        "        {'freedisk':1721.0442085266113}," +
        "        {'sysLoadAvg':118.0}," +
        "        {'node':'solr-27:8983_solr'}]}]}";

    List l = (List) ((Map) Utils.fromJSONString(rowsData)).get("sortedNodes");
    List<Suggestion.ConditionType> params = new ArrayList<>();
    params.add(Suggestion.ConditionType.CORES);
    params.add(Suggestion.ConditionType.FREEDISK);
    params.add(Suggestion.ConditionType.SYSLOADAVG);
    params.add(Suggestion.ConditionType.NODE);
    List<Row> rows = new ArrayList<>();
    for (Object o : l) {
      Map m = (Map) o;
      Cell[] c = new Cell[params.size()];
      List attrs = (List) m.get("attributes");
      for (int i = 0; i < params.size(); i++) {
        Suggestion.ConditionType param = params.get(i);
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
          (Boolean) m.get("isLive"), null));
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


}
