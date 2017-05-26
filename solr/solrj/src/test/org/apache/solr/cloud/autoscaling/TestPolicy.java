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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.autoscaling.Clause.Violation;
import org.apache.solr.cloud.autoscaling.Policy.Suggester.Hint;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

public class TestPolicy extends SolrTestCaseJ4 {

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
  }

  public void testRow() {
    Row row = new Row("nodex", new Cell[]{new Cell(0, "node", "nodex")}, false, new HashMap<>(), new ArrayList<>());
    Row r1 = row.addReplica("c1", "s1");
    Row r2 = r1.addReplica("c1", "s1");
    assertEquals(1, r1.collectionVsShardVsReplicas.get("c1").get("s1").size());
    assertEquals(2, r2.collectionVsShardVsReplicas.get("c1").get("s1").size());
    assertTrue(r2.collectionVsShardVsReplicas.get("c1").get("s1").get(0) instanceof Policy.ReplicaInfo);
    assertTrue(r2.collectionVsShardVsReplicas.get("c1").get("s1").get(1) instanceof Policy.ReplicaInfo);
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
        "      { 'replica': '<2', 'shard': '#EACH', 'rack': 'rack1'}" +
        "    ]" +
        "  }" +
        "}");
    Policy policy = new Policy(map);
    List<Clause> clauses = Policy.mergePolicies("mycoll", policy.policies.get("policy1"), policy.clusterPolicy);
    Collections.sort(clauses);
    assertEquals(clauses.size(), 4);
    assertEquals("1", String.valueOf(clauses.get(0).original.get("replica")));
    assertEquals("0", String.valueOf(clauses.get(1).original.get("replica")));
    assertEquals("#ANY", clauses.get(3).original.get("shard"));
    assertEquals("rack1", clauses.get(2).original.get("rack"));
    assertEquals("overseer", clauses.get(1).original.get("nodeRole"));
  }


  public void testConditionsSort() {
    String rules = "{" +
        "    'cluster-policy':[" +
        "      { 'nodeRole':'overseer', replica: 0,  'strict':false}," +
        "      { 'replica':'<1', 'node':'node3', 'shard':'#EACH'}," +
        "      { 'replica':'<2', 'node':'#ANY', 'shard':'#EACH'}," +
        "      { 'replica':1, 'rack':'rack1'}]" +
        "  }";
    Policy p = new Policy((Map<String, Object>) Utils.fromJSONString(rules));
    List<Clause> clauses = new ArrayList<>(p.getClusterPolicy());
    Collections.sort(clauses);
    assertEquals("nodeRole", clauses.get(1).tag.name);
    assertEquals("rack", clauses.get(0).tag.name);
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
      "            'base_url':'http://10.0.0.4:7574/solr'," +
      "            'node_name':'node1'," +
      "            'state':'active'}}}}}}";

  public void testRules() throws IOException {
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
    session = policy.createSession(getClusterDataProvider(nodeValues, clusterState));

    List<Row> l = session.getSorted();
    assertEquals("node1", l.get(0).node);
    assertEquals("node3", l.get(1).node);
    assertEquals("node4", l.get(2).node);
    assertEquals("node2", l.get(3).node);


    List<Violation> violations = session.getViolations();
    assertEquals(3, violations.size());
    assertTrue(violations.stream().anyMatch(violation -> "node3".equals(violation.getClause().tag.val)));
    assertTrue(violations.stream().anyMatch(violation -> "nodeRole".equals(violation.getClause().tag.name)));
    assertTrue(violations.stream().anyMatch(violation -> (violation.getClause().replica.op == Operand.LESS_THAN && "node".equals(violation.getClause().tag.name))));

    Policy.Suggester suggester = session.getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "gettingstarted")
        .hint(Hint.SHARD, "r1");
    SolrParams operation = suggester.getOperation().getParams();
    assertEquals("node2", operation.get("node"));

    nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834}," +
        "node5:{cores:0, freedisk: 895, heapUsage:17834}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer}" +
        "}");
    session = policy.createSession(getClusterDataProvider(nodeValues, clusterState));
    SolrRequest opReq = session.getSuggester(MOVEREPLICA)
        .hint(Hint.TARGET_NODE, "node5")
        .getOperation();
    assertNotNull(opReq);
    assertEquals("node5", opReq.getParams().get("targetNode"));


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
    ClusterDataProvider clusterDataProvider = getClusterDataProvider(nodeValues, clusterState);
    ClusterDataProvider cdp = new ClusterDataProvider() {
      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
        return clusterDataProvider.getNodeValues(node, tags);
      }

      @Override
      public Map<String, Map<String, List<Policy.ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
        return clusterDataProvider.getReplicaInfo(node, keys);
      }

      @Override
      public Collection<String> getNodes() {
        return clusterDataProvider.getNodes();
      }

      @Override
      public String getPolicyNameByCollection(String coll) {
        return null;
      }
    };
    Policy.Session session = policy.createSession(cdp);
    Policy.Suggester suggester = session.getSuggester(ADDREPLICA);
    SolrRequest op = suggester
        .hint(Hint.COLL, "newColl")
        .hint(Hint.SHARD, "shard1")
        .getOperation();
    assertNotNull(op);
    assertEquals("node3", op.getParams().get("node"));
    suggester = suggester
        .getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "newColl")
        .hint(Hint.SHARD, "shard1");
    op = suggester.getOperation();
    assertNotNull(op);
    assertEquals("node3", op.getParams().get("node"));

    suggester = suggester
        .getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "newColl")
        .hint(Hint.SHARD, "shard1");
    op = suggester.getOperation();
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
        new Policy.ReplicaInfo("core_node1", "compute_plan_action_test", "shard1", Collections.emptyMap()),
        new Policy.ReplicaInfo("core_node2", "compute_plan_action_test", "shard1", Collections.emptyMap())
    ));

    Map<String, Map<String, Object>> tagsMap = (Map) Utils.fromJSONString("{" +
        "      '127.0.0.1:60099_solr':{" +
        "        'cores':0," +
        "            'freedisk':918005641216}," +
        "      '127.0.0.1:60089_solr':{" +
        "        'cores':2," +
        "            'freedisk':918005641216}}}");

    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoscaleJson));
    Policy.Session session = policy.createSession(new ClusterDataProvider() {
      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
        return tagsMap.get(node);
      }

      @Override
      public Map<String, Map<String, List<Policy.ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
        return (Map<String, Map<String, List<Policy.ReplicaInfo>>>) replicaInfoMap.get(node);
      }

      @Override
      public Collection<String> getNodes() {
        return replicaInfoMap.keySet();
      }

      @Override
      public String getPolicyNameByCollection(String coll) {
        return null;
      }
    });

    Policy.Suggester suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA)
        .hint(Hint.TARGET_NODE, "127.0.0.1:60099_solr");
    SolrParams op = suggester.getOperation().getParams();
    assertNotNull(op);
    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA).hint(Hint.TARGET_NODE, "127.0.0.1:60099_solr");
    op = suggester.getOperation().getParams();
    assertNotNull(op);
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
        "{'replica':'<3','shard':'#EACH','rack':'#ANY'}" +
        "]" +
        "}" +
        "}";

    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heapUsage:10480, rack: rack4}," +
        "node2:{cores:4, freedisk: 749, heapUsage:6873, rack: rack3}," +
        "node3:{cores:7, freedisk: 262, heapUsage:7834, rack: rack2}," +
        "node4:{cores:8, freedisk: 375, heapUsage:16900, nodeRole:overseer, rack: rack1}" +
        "}");
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(rules));
    ClusterDataProvider clusterDataProvider = getClusterDataProvider(nodeValues, clusterState);
    ClusterDataProvider cdp = new ClusterDataProvider() {
      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
        return clusterDataProvider.getNodeValues(node, tags);
      }

      @Override
      public Map<String, Map<String, List<Policy.ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
        return clusterDataProvider.getReplicaInfo(node, keys);
      }

      @Override
      public Collection<String> getNodes() {
        return clusterDataProvider.getNodes();
      }

      @Override
      public String getPolicyNameByCollection(String coll) {
        return "p1";
      }
    };
    Policy.Session session = policy.createSession(cdp);

    CollectionAdminRequest.AddReplica op = (CollectionAdminRequest.AddReplica) session
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "newColl")
        .hint(Hint.SHARD, "s1").getOperation();
    assertNotNull(op);
    assertEquals("node2", op.getNode());
  }


  private ClusterDataProvider getClusterDataProvider(final Map<String, Map> nodeValues, String clusterState) {
    return new ClusterDataProvider() {
      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
        Map<String, Object> result = new LinkedHashMap<>();
        tags.stream().forEach(s -> result.put(s, nodeValues.get(node).get(s)));
        return result;
      }

      @Override
      public Collection<String> getNodes() {
        return nodeValues.keySet();
      }

      @Override
      public String getPolicyNameByCollection(String coll) {
        return null;
      }

      @Override
      public Map<String, Map<String, List<Policy.ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
        return getReplicaDetails(node, clusterState);
      }

    };
  }

  public void testMultiReplicaPlacement() {
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
        "      { 'replica': '<2', 'shard': '#EACH', 'rack': 'rack1'}" +
//        "      { 'replica': '1', 'sysprop.fs': 'ssd', 'shard': '#EACH'}" +
        "    ]" +
        "  }" +
        "}";


    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heap:10480, rack:rack3}," +
        "node2:{cores:4, freedisk: 749, heap:6873, sysprop.fs : ssd, rack:rack1}," +
        "node3:{cores:7, freedisk: 262, heap:7834, rack:rack4}," +
        "node4:{cores:0, freedisk: 900, heap:16900, nodeRole:overseer, rack:rack2}" +
        "}");

    ClusterDataProvider dataProvider = new ClusterDataProvider() {
      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> keys) {
        Map<String, Object> result = new LinkedHashMap<>();
        keys.stream().forEach(s -> result.put(s, nodeValues.get(node).get(s)));
        return result;
      }

      @Override
      public Map<String, Map<String, List<Policy.ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
        return getReplicaDetails(node, clusterState);
      }

      @Override
      public String getPolicyNameByCollection(String coll) {
        return null;
      }

      @Override
      public Collection<String> getNodes() {
        return Arrays.asList("node1", "node2", "node3", "node4");
      }
    };
    Map<String, List<String>> locations = PolicyHelper.getReplicaLocations(
        "newColl", (Map<String, Object>) Utils.fromJSONString(autoScaleJson),
        dataProvider, Collections.singletonMap("newColl", "policy1"), Arrays.asList("shard1", "shard2"), 3);
    assertTrue(locations.get("shard1").containsAll(ImmutableList.of("node2", "node1", "node3")));
    assertTrue(locations.get("shard2").containsAll(ImmutableList.of("node2", "node1", "node3")));


  }

  public static Map<String, Map<String, List<Policy.ReplicaInfo>>> getReplicaDetails(String node, String s) {
    ValidatingJsonMap m = ValidatingJsonMap
        .getDeepCopy((Map) Utils.fromJSONString(s), 6, true);
    Map<String, Map<String, List<Policy.ReplicaInfo>>> result = new LinkedHashMap<>();

    m.forEach((collName, o) -> {
      ValidatingJsonMap coll = (ValidatingJsonMap) o;
      coll.getMap("shards").forEach((shard, o1) -> {
        ValidatingJsonMap sh = (ValidatingJsonMap) o1;
        sh.getMap("replicas").forEach((replicaName, o2) -> {
          ValidatingJsonMap r = (ValidatingJsonMap) o2;
          String node_name = (String) r.get("node_name");
          if (!node_name.equals(node)) return;
          Map<String, List<Policy.ReplicaInfo>> shardVsReplicaStats = result.get(collName);
          if (shardVsReplicaStats == null) result.put(collName, shardVsReplicaStats = new HashMap<>());
          List<Policy.ReplicaInfo> replicaInfos = shardVsReplicaStats.get(shard);
          if (replicaInfos == null) shardVsReplicaStats.put(shard, replicaInfos = new ArrayList<>());
          replicaInfos.add(new Policy.ReplicaInfo(replicaName, collName, shard, new HashMap<>()));
        });
      });
    });
    return result;
  }


}
