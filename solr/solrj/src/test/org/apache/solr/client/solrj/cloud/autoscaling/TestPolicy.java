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
import org.apache.solr.client.solrj.cloud.autoscaling.Clause.Violation;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy.Suggester.Hint;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;

import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;

public class TestPolicy extends SolrTestCaseJ4 {

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
          replicaInfos.add(new ReplicaInfo(replicaName, collName, shard, Replica.Type.get((String) r.get(ZkStateReader.REPLICA_TYPE)), new HashMap<>()));
        });
      });
    });
    return result;
  }

  public void testValidate() {
    expectError("replica", -1, "must be greater than" );
    expectError("replica","hello", "not a valid number" );
    assertEquals( 1l,   Clause.validate("replica", "1", true));
    assertEquals("c",   Clause.validate("collection", "c", true));
    assertEquals( "s",   Clause.validate("shard", "s",true));
    assertEquals( "overseer",   Clause.validate("nodeRole", "overseer",true));

    expectError("nodeRole", "wrong","must be one of");

    expectError("sysLoadAvg", "101","must be less than ");
    expectError("sysLoadAvg", 101,"must be less than ");
    expectError("sysLoadAvg", "-1","must be greater than");
    expectError("sysLoadAvg", -1,"must be greater than");

    assertEquals(12.46d,Clause.validate("sysLoadAvg", "12.46",true));
    assertEquals(12.46,Clause.validate("sysLoadAvg", 12.46d,true));


    expectError("ip_1", "300","must be less than ");
    expectError("ip_1", 300,"must be less than ");
    expectError("ip_1", "-1","must be greater than");
    expectError("ip_1", -1,"must be greater than");

    assertEquals(1l,Clause.validate("ip_1", "1",true));

    expectError("heapUsage", "-1","must be greater than");
    expectError("heapUsage", -1,"must be greater than");
    assertEquals(69.9d,Clause.validate("heapUsage", "69.9",true));
    assertEquals(69.9d,Clause.validate("heapUsage", 69.9d,true));

    expectError("port", "70000","must be less than ");
    expectError("port", 70000,"must be less than ");
    expectError("port", "0","must be greater than");
    expectError("port", 0,"must be greater than");

    expectError("cores", "-1","must be greater than");


  }

  private static void expectError(String name, Object val, String msg){
    try {
      Clause.validate(name, val,true);
      fail("expected exception containing "+msg);
    } catch (Exception e) {
      assertTrue("expected exception containing "+msg,e.getMessage().contains(msg));
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
    Policy.Session session = policy.createSession(dataProviderWithData(dataproviderdata));
    SolrRequest op = session.getSuggester(MOVEREPLICA).hint(Hint.SRC_NODE, "127.0.0.1:65427_solr").getOperation();
    assertNotNull(op);
    assertEquals( "127.0.0.1:65434_solr",op.getParams().get("targetNode") );
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

    ClusterDataProvider provider = getClusterDataProvider((Map<String, Map>) Utils.fromJSONString(nodeValues), clusterState);
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
    Policy.Suggester suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");

    SolrRequest operation = suggester.getOperation();
    assertNotNull(operation);
    assertEquals("node2", operation.getParams().get("targetNode"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getOperation();
    assertNotNull(operation);
    assertEquals("node3", operation.getParams().get("targetNode"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getOperation();
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

    operation = suggester.getOperation();
    assertNotNull(operation);
    assertEquals("node2", operation.getParams().get("targetNode"));
    assertEquals("r3", operation.getParams().get("replica"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getOperation();
    assertNotNull(operation);
    assertEquals("node2", operation.getParams().get("targetNode"));
    assertEquals("r5", operation.getParams().get("replica"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getOperation();
    assertEquals("node2", operation.getParams().get("targetNode"));
    assertEquals("r1", operation.getParams().get("replica"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getOperation();
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

    operation = suggester.getOperation();
    assertNotNull(operation);
    assertEquals("node2", operation.getParams().get("targetNode"));
    assertEquals("r3", operation.getParams().get("replica"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getOperation();
    assertNotNull(operation);
    assertEquals("node2", operation.getParams().get("targetNode"));
    assertEquals("r5", operation.getParams().get("replica"));

    session = suggester.getSession();
    suggester = session.getSuggester(MOVEREPLICA)
        .hint(Hint.SRC_NODE, "node1");
    operation = suggester.getOperation();
    assertEquals("node3", operation.getParams().get("targetNode"));
    assertEquals("r1", operation.getParams().get("replica"));
  }

  private static ClusterDataProvider dataProviderWithData(String data){
    final Map m = (Map) Utils.fromJSONString(data);
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
            l3.set(i, new ReplicaInfo(m3.keySet().iterator().next().toString()
                ,coll.toString(), shard.toString(), Replica.Type.get((String)m3.get("type")), new HashMap<>()));
          }
        });

      });

    });
    return new ClusterDataProvider(){
      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
        return (Map<String, Object>) Utils.getObjectByPath(m,false, Arrays.asList("nodeValues", node));
      }

      @Override
      public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
        return (Map<String, Map<String, List<ReplicaInfo>>>) Utils.getObjectByPath(m,false, Arrays.asList("replicaInfo", node));
      }

      @Override
      public Collection<String> getNodes() {
        return (Collection<String>) m.get("liveNodes");
      }

      @Override
      public String getPolicyNameByCollection(String coll) {
        return null;
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
    Policy.Suggester suggester = policy.createSession(getClusterDataProvider(nodeValues, clusterState))
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "newColl")
        .hint(Hint.REPLICATYPE, Replica.Type.PULL)
        .hint(Hint.SHARD, "shard1");
    SolrRequest op = suggester.getOperation();
    assertNotNull(op);
    assertEquals(Replica.Type.PULL.name(),  op.getParams().get("type"));
    assertEquals("PULL type node must be in 'slowdisk' node","node1", op.getParams().get("node"));

    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "newColl")
        .hint(Hint.REPLICATYPE, Replica.Type.PULL)
        .hint(Hint.SHARD, "shard2");
    op = suggester.getOperation();
    assertNotNull(op);
    assertEquals(Replica.Type.PULL.name(),  op.getParams().get("type"));
    assertEquals("PULL type node must be in 'slowdisk' node","node1", op.getParams().get("node"));

    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "newColl")
        .hint(Hint.REPLICATYPE, Replica.Type.TLOG)
        .hint(Hint.SHARD, "shard1");
    op = suggester.getOperation();
    assertNotNull(op);
    assertEquals(Replica.Type.TLOG.name(),  op.getParams().get("type"));
    assertEquals("TLOG type node must be in 'ssd' node","node3", op.getParams().get("node"));

    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "newColl")
        .hint(Hint.REPLICATYPE, Replica.Type.TLOG)
        .hint(Hint.SHARD, "shard2");
    op = suggester.getOperation();
    assertNotNull(op);
    assertEquals(Replica.Type.TLOG.name(),  op.getParams().get("type"));
    assertEquals("TLOG type node must be in 'ssd' node","node3", op.getParams().get("node"));

    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "newColl")
        .hint(Hint.REPLICATYPE, Replica.Type.TLOG)
        .hint(Hint.SHARD, "shard2");
    op = suggester.getOperation();
    assertNull("No node should qualify for this" ,op);

  }

  public void testMultipleCollections() {
    Map policies = (Map) Utils.fromJSONString("{" +
        "  'cluster-preferences': [" +
        "    { 'maximize': 'freedisk', 'precision': 50}," +
        "    { 'minimize': 'cores', 'precision': 50}" +
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
    Policy.Suggester suggester = policy.createSession(getClusterDataProvider(nodeValues, clusterState))
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "newColl")
        .hint(Hint.COLL, "newColl2")
        .hint(Hint.REPLICATYPE, Replica.Type.PULL)
        .hint(Hint.SHARD, "shard1");
    SolrRequest op;
    int countOp = 0;
    int countNewCollOp = 0;
    int countNewColl2Op = 0;
    while ((op = suggester.getOperation()) != null) {
      countOp++;
      suggester = suggester.getSession().getSuggester(ADDREPLICA)
          .hint(Hint.COLL, "newColl")
          .hint(Hint.COLL, "newColl2")
          .hint(Hint.REPLICATYPE, Replica.Type.PULL)
          .hint(Hint.SHARD, "shard1");
      assertEquals(Replica.Type.PULL.name(),  op.getParams().get("type"));
      String collection =  op.getParams().get("collection");
      assertTrue("Collection for replica is not as expected " + collection, collection.equals("newColl") || collection.equals("newColl2"));
      if (collection.equals("newColl")) countNewCollOp++;
      else countNewColl2Op++;
      assertEquals("PULL type node must be in 'slowdisk' node","node1", op.getParams().get("node"));
    }
    assertEquals(2, countOp);
    assertEquals(1, countNewCollOp);
    assertEquals(1, countNewColl2Op);

    countOp = 0;
    countNewCollOp = 0;
    countNewColl2Op = 0;
    suggester = suggester.getSession()
        .getSuggester(ADDREPLICA)
        .hint(Hint.COLL, "newColl")
        .hint(Hint.COLL, "newColl2")
        .hint(Hint.REPLICATYPE, Replica.Type.TLOG)
        .hint(Hint.SHARD, "shard2");
    while ((op = suggester.getOperation()) != null) {
      countOp++;
      suggester = suggester.getSession()
          .getSuggester(ADDREPLICA)
          .hint(Hint.COLL, "newColl")
          .hint(Hint.COLL, "newColl2")
          .hint(Hint.REPLICATYPE, Replica.Type.TLOG)
          .hint(Hint.SHARD, "shard2");
      assertEquals(Replica.Type.TLOG.name(),  op.getParams().get("type"));
      String collection =  op.getParams().get("collection");
      assertTrue("Collection for replica is not as expected " + collection, collection.equals("newColl") || collection.equals("newColl2"));
      if (collection.equals("newColl")) countNewCollOp++;
      else countNewColl2Op++;
      assertEquals("TLOG type node must be in 'ssd' node","node3", op.getParams().get("node"));
    }
    assertEquals(3, countOp);
    assertEquals(1, countNewCollOp);
    assertEquals(2, countNewColl2Op);
  }

  public void testRow() {
    Row row = new Row("nodex", new Cell[]{new Cell(0, "node", "nodex")}, false, new HashMap<>(), new ArrayList<>(), true);
    Row r1 = row.addReplica("c1", "s1", null);
    Row r2 = r1.addReplica("c1", "s1",null);
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
    assertEquals("node4", l.get(1).node);
    assertEquals("node3", l.get(2).node);
    assertEquals("node2", l.get(3).node);


    List<Violation> violations = session.getViolations();
    assertEquals(3, violations.size());
    assertTrue(violations.stream().anyMatch(violation -> "node3".equals(violation.getClause().tag.getValue())));
    assertTrue(violations.stream().anyMatch(violation -> "nodeRole".equals(violation.getClause().tag.getName())));
    assertTrue(violations.stream().anyMatch(violation -> (violation.getClause().replica.getOperand() == Operand.LESS_THAN && "node".equals(violation.getClause().tag.getName()))));

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
    ClusterDataProvider clusterDataProvider = getClusterDataProvider(nodeValues, clusterState);
    Policy.Session session = policy.createSession(clusterDataProvider);
    for (int i = 0; i < 3; i++) {
      Policy.Suggester suggester = session.getSuggester(ADDREPLICA);
      SolrRequest op = suggester
          .hint(Hint.COLL, "newColl")
          .hint(Hint.SHARD, "shard1")
          .getOperation();
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
    ClusterDataProvider clusterDataProvider = getClusterDataProvider(nodeValues, clusterState);
    ClusterDataProvider cdp = new ClusterDataProvider() {
      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
        return clusterDataProvider.getNodeValues(node, tags);
      }

      @Override
      public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
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
        new ReplicaInfo("core_node1", "compute_plan_action_test", "shard1", Replica.Type.NRT, Collections.emptyMap()),
        new ReplicaInfo("core_node2", "compute_plan_action_test", "shard1", Replica.Type.NRT, Collections.emptyMap())
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
      public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
        return (Map<String, Map<String, List<ReplicaInfo>>>) replicaInfoMap.get(node);
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
    ClusterDataProvider clusterDataProvider = getClusterDataProvider(nodeValues, clusterState);
    ClusterDataProvider cdp = new ClusterDataProvider() {
      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
        return clusterDataProvider.getNodeValues(node, tags);
      }

      @Override
      public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
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
      public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
        return getReplicaDetails(node, clusterState);
      }

    };
  }
  public void testEmptyClusterState(){
    String autoScaleJson =  " {'policies':{'c1':[{" +
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
    ClusterDataProvider dataProvider = new ClusterDataProvider() {
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

      @Override
      public String getPolicyNameByCollection(String coll) {
        return null;
      }

      @Override
      public Collection<String> getNodes() {
        return Arrays.asList( "127.0.0.1:50097_solr", "127.0.0.1:50096_solr");
      }
    };
    List<ReplicaPosition> locations = PolicyHelper.getReplicaLocations(
        "newColl", new AutoScalingConfig((Map<String, Object>)Utils.fromJSONString(autoScaleJson)),
        dataProvider, Collections.singletonMap("newColl", "c1"), Arrays.asList("shard1", "shard2"), 1, 0, 0, null);

    assertTrue(locations.stream().allMatch(it -> it.node.equals("127.0.0.1:50096_solr")) );
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
        "      { 'replica': '<2', 'shard': '#EACH', 'sysprop.rack': 'rack1'}" +
        "    ]" +
        "  }" +
        "}";


    Map<String, Map> nodeValues = (Map<String, Map>) Utils.fromJSONString("{" +
        "node1:{cores:12, freedisk: 334, heap:10480, sysprop.rack:rack3}," +
        "node2:{cores:4, freedisk: 749, heap:6873, sysprop.fs : ssd, sysprop.rack:rack1}," +
        "node3:{cores:7, freedisk: 262, heap:7834, sysprop.rack:rack4}," +
        "node4:{cores:0, freedisk: 900, heap:16900, nodeRole:overseer, sysprop.rack:rack2}" +
        "}");

    ClusterDataProvider dataProvider = new ClusterDataProvider() {
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

      @Override
      public String getPolicyNameByCollection(String coll) {
        return null;
      }

      @Override
      public Collection<String> getNodes() {
        return Arrays.asList("node1", "node2", "node3", "node4");
      }
    };
    List<ReplicaPosition> locations = PolicyHelper.getReplicaLocations(
        "newColl", new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScaleJson)),
        dataProvider, Collections.singletonMap("newColl", "policy1"), Arrays.asList("shard1", "shard2"), 3,0,0, null);
    assertTrue(locations.stream().allMatch(it -> ImmutableList.of("node2", "node1", "node3").contains(it.node)) );
  }


}
