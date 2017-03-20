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

package org.apache.solr.recipe;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;

public class TestRuleSorter extends SolrTestCaseJ4 {
  public void testRuleParsing() throws IOException {
    String rules = "{" +
        "conditions:[{nodeRole:'!overseer', strict:false}, " +
        "{replica:'<2',node:'*', shard:'#EACH'}]," +
        " preferences:[" +
        "{minimize:cores , precision:2}," +
        "{maximize:freedisk, precision:50}, " +
        "{minimize:heap, precision:1000}]}";


    Map<String,Map> nodeValues = (Map<String, Map>) Utils.fromJSONString( "{" +
        "node1:{cores:12, freedisk: 334, heap:10480}," +
        "node2:{cores:4, freedisk: 749, heap:6873}," +
        "node3:{cores:7, freedisk: 262, heap:7834}," +
        "node4:{cores:8, freedisk: 375, heap:16900}" +
        "}");
    String clusterState = "{'gettingstarted':{" +
        "    'router':{'name':'compositeId'}," +
        "    'shards':{" +
        "      'shard1':{" +
        "        'range':'80000000-ffffffff'," +
        "        'state':'active'," +
        "        'replicas':{" +
        "          'core_node1':{" +
        "            'core':'gettingstarted_shard1_replica1'," +
        "            'base_url':'http://10.0.0.4:8983/solr'," +
        "            'node_name':'node1'," +
        "            'state':'active'," +
        "            'leader':'true'}," +
        "          'core_node4':{" +
        "            'core':'gettingstarted_shard1_replica2'," +
        "            'base_url':'http://10.0.0.4:7574/solr'," +
        "            'node_name':'node2'," +
        "            'state':'active'}}}," +
        "      'shard2':{" +
        "        'range':'0-7fffffff'," +
        "        'state':'active'," +
        "        'replicas':{" +
        "          'core_node2':{" +
        "            'core':'gettingstarted_shard2_replica1'," +
        "            'base_url':'http://10.0.0.4:8983/solr'," +
        "            'node_name':'node1'," +
        "            'state':'active'," +
        "            'leader':'true'}," +
        "          'core_node3':{" +
        "            'core':'gettingstarted_shard2_replica2'," +
        "            'base_url':'http://10.0.0.4:7574/solr'," +
        "            'node_name':'node2'," +
        "            'state':'active'}}}}}}";


    ValidatingJsonMap m = ValidatingJsonMap
        .getDeepCopy((Map) Utils.fromJSONString(clusterState), 6, true);


    RuleSorter ruleSorter = new RuleSorter((Map<String, Object>) Utils.fromJSONString(rules));
    RuleSorter.Session session;
    RuleSorter.NodeValueProvider snitch = new RuleSorter.NodeValueProvider() {
      @Override
      public Map<String,Object> getValues(String node, Collection<String> keys) {
        Map<String,Object> result = new LinkedHashMap<>();
        keys.stream().forEach(s -> result.put(s, nodeValues.get(node).get(s)));
        return result;

      }

      @Override
      public Map<String, Map<String, List<RuleSorter.ReplicaStat>>> getReplicaCounts(String node, Collection<String> keys) {
        Map<String, Map<String, List<RuleSorter.ReplicaStat>>> result = new LinkedHashMap<>();

        m.forEach((collName, o) -> {
          ValidatingJsonMap coll = (ValidatingJsonMap) o;
          coll.getMap("shards").forEach((shard, o1) -> {
            ValidatingJsonMap sh = (ValidatingJsonMap) o1;
            sh.getMap("replicas").forEach((replicaName, o2) -> {
              ValidatingJsonMap r = (ValidatingJsonMap) o2;
              String node_name = (String) r.get("node_name");
              if (!node_name.equals(node)) return;
              Map<String, List<RuleSorter.ReplicaStat>> shardVsReplicaStats = result.get(collName);
              if (shardVsReplicaStats == null) result.put(collName, shardVsReplicaStats = new HashMap<>());
              List<RuleSorter.ReplicaStat> replicaStats = shardVsReplicaStats.get(shard);
              if (replicaStats == null) shardVsReplicaStats.put(shard, replicaStats = new ArrayList<>());
              replicaStats.add(new RuleSorter.ReplicaStat(replicaName, new HashMap<>()));

            });
          });
        });

        return result;
      }


    };

    session = ruleSorter.createSession(Arrays.asList("node1", "node2", "node3", "node4"), snitch);

    session.applyRules();
    List<RuleSorter.Row> l = session.getSorted();
    assertEquals("node1",l.get(0).node);
    assertEquals("node3",l.get(1).node);
    assertEquals("node4",l.get(2).node);
    assertEquals("node2",l.get(3).node);


    System.out.printf(Utils.toJSONString(Utils.getDeepCopy(session.toMap(new LinkedHashMap<>()), 8)));
    System.out.println(Utils.getDeepCopy(session.getViolations(), 6));

  }


}
