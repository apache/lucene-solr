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
import java.io.InputStream;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.util.Utils;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.EMPTY_MAP;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.CORES;
import static org.apache.solr.common.util.Utils.MAPOBJBUILDER;
import static org.apache.solr.common.util.Utils.getObjectByPath;

public class TestPolicy2 extends SolrTestCaseJ4 {
  boolean useNodeset ;
  public TestPolicy2(){
    useNodeset = true;
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void testEqualOnNonNode() {
    List<Map> l = (List<Map>) loadFromResource("testEqualOnNonNode.json");
    String autoScalingjson = "{cluster-policy:[" +
        "    { replica : '<3' , shard : '#EACH', sysprop.zone: east}," +
        "{ replica : '<3' , shard : '#EACH', sysprop.zone: west} ]," +
        "  'cluster-preferences':[{ minimize : cores},{maximize : freedisk, precision : 50}]}";
    if(useNodeset){
      autoScalingjson = "{cluster-policy:[" +
          "{ replica : '<3' , shard : '#EACH', nodeset:{ sysprop.zone: east}}," +
          "{ replica : '<3' , shard : '#EACH', nodeset:{ sysprop.zone: west}} ]," +
          "  'cluster-preferences':[{ minimize : cores},{maximize : freedisk, precision : 50}]}";
      
    }
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    Policy.Session session = policy.createSession(createCloudManager(l.get(0), l.get(1)));
    List<Violation> violations = session.getViolations();
    assertEquals(1, violations.size());
    assertEquals(3, violations.get(0).getViolatingReplicas().size());
    assertEquals(1.0, violations.get(0).replicaCountDelta, 0.01);
    for (Violation.ReplicaInfoAndErr r : violations.get(0).getViolatingReplicas()) {
      assertEquals("shard2", r.replicaInfo.getShard());
    }

    l = (List<Map>) loadFromResource("testEqualOnNonNode.json");
    autoScalingjson = "{cluster-policy:[" +
        "    { replica : '<3' , shard : '#EACH', sysprop.zone: '#EACH' } ]," +
        "  'cluster-preferences':[{ minimize : cores},{maximize : freedisk, precision : 50}]}";
    if(useNodeset){
      autoScalingjson = "{cluster-policy:[" +
          "{ replica : '<3' , shard : '#EACH', nodeset:{sysprop.zone: east}} , " +
          "{ replica : '<3' , shard : '#EACH', nodeset:{sysprop.zone: west}}  ]," +
          "  'cluster-preferences':[{ minimize : cores},{maximize : freedisk, precision : 50}]}";
    }
    policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    session = policy.createSession(createCloudManager(l.get(0), l.get(1)));
    violations = session.getViolations();
    assertEquals(1, violations.size());
    assertEquals(3, violations.get(0).getViolatingReplicas().size());
    assertEquals(1.0, violations.get(0).replicaCountDelta, 0.01);
    for (Violation.ReplicaInfoAndErr r : violations.get(0).getViolatingReplicas()) {
      assertEquals("shard2", r.replicaInfo.getShard());
    }
    l = (List<Map>) loadFromResource("testEqualOnNonNode.json");
    autoScalingjson = "{cluster-policy:[" +
        "    { replica : '#EQUAL' , node: '#ANY' } ]," +
        "  'cluster-preferences':[{ minimize : cores},{maximize : freedisk, precision : 50}]}";
    policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    session = policy.createSession(createCloudManager(l.get(0), l.get(1)));
    violations = session.getViolations();
    List<Suggester.SuggestionInfo> suggestions = null;
    assertEquals(2, violations.size());
    for (Violation violation : violations) {
      if (violation.node.equals("node1")) {
        assertEquals(1.0d, violation.replicaCountDelta, 0.001);
        assertEquals(3, violation.getViolatingReplicas().size());
      } else if (violation.node.equals("node5")) {
        assertEquals(-1.0d, violation.replicaCountDelta, 0.001);
        assertEquals(0, violation.getViolatingReplicas().size());
      } else {
        fail();
      }
    }
    l = (List<Map>) loadFromResource("testEqualOnNonNode.json");
    suggestions = PolicyHelper.getSuggestions(new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson))
        , createCloudManager(l.get(0), l.get(1)));
    assertEquals(1, suggestions.size());
    String repName = (String) suggestions.get(0)._get("operation/command/move-replica/replica", null);

    AtomicBoolean found = new AtomicBoolean(false);
    session.getNode("node1").forEachReplica(replicaInfo -> {
      if (replicaInfo.getName().equals(repName)) {
        found.set(true);
      }
    });
    assertTrue(found.get());

    l = (List<Map>) loadFromResource("testEqualOnNonNode.json");
    autoScalingjson = "{cluster-policy:[" +
        "    { cores : '#EQUAL' , node: '#ANY' } ]," +
        "  'cluster-preferences':[{ minimize : cores},{minimize : freedisk, precision : 50}]}";
    policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    session = policy.createSession(createCloudManager(l.get(0), l.get(1)));
    violations = session.getViolations();
    assertEquals(2, violations.size());
    for (Violation violation : violations) {
      if(violation.node.equals("node1")) {
        assertEquals(1.0d, violation.replicaCountDelta, 0.001);
        assertEquals(3, violation.getViolatingReplicas().size());
      } else if(violation.node.equals("node5")){
        assertEquals(-1.0d, violation.replicaCountDelta, 0.001);
        assertEquals(0, violation.getViolatingReplicas().size());
      } else {
        fail();
      }

    }
    l = (List<Map>) loadFromResource("testEqualOnNonNode.json");
    suggestions = PolicyHelper.getSuggestions(new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson)),
        createCloudManager(l.get(0), l.get(1)));
    assertEquals(1, suggestions.size());
    assertEquals("node5", suggestions.get(0)._get("operation/command/move-replica/targetNode", null));

    String rName = (String) suggestions.get(0)._get("operation/command/move-replica/replica", null);

    found.set(false);
    session.getNode("node1").forEachReplica(replicaInfo -> {
      if (replicaInfo.getName().equals(rName)) {
        found.set(true);
      }
    });
    assertTrue(found.get());

  }

  static SolrCloudManager createCloudManager(Map m, Map meta) {
    Map nodeVals = (Map) meta.get("nodeValues");
    List<Map> replicaVals = (List<Map>) meta.get("replicaValues");
    ClusterState clusterState = ClusterState.load(0, m, Collections.emptySet(), null);
    Map<String, AtomicInteger> coreCount = new LinkedHashMap<>();
    Set<String> nodes = new HashSet<>(nodeVals.keySet());
    clusterState.getCollectionStates().forEach((s, collectionRef) -> collectionRef.get()
        .forEachReplica((s12, replica) -> {
          nodes.add(replica.getNodeName());
          coreCount.computeIfAbsent(replica.getNodeName(), s1 -> new AtomicInteger(0))
              .incrementAndGet();
        }));

    DelegatingClusterStateProvider delegatingClusterStateProvider = new DelegatingClusterStateProvider(null) {
      @Override
      public ClusterState getClusterState() throws IOException {
        return clusterState;
      }

      @Override
      public Set<String> getLiveNodes() {
        return nodes;
      }
    };

    return new DelegatingCloudManager(null) {

      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return delegatingClusterStateProvider;
      }

      @Override
      public NodeStateProvider getNodeStateProvider() {

        return new SolrClientNodeStateProvider(null) {
          @Override
          protected ClusterStateProvider getClusterStateProvider() {
            return delegatingClusterStateProvider;
          }

          @Override
          protected Map<String, Object> fetchTagValues(String node, Collection<String> tags) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (String tag : tags) {
              if (tag.equals(CORES.tagName))
                result.put(CORES.tagName, coreCount.getOrDefault(node, new AtomicInteger(0)).get());
              result.put(tag, getObjectByPath(nodeVals, true, Arrays.asList(node, tag)));
            }
            return result;
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            Map<String, Map<String, List<ReplicaInfo>>> result = nodeVsCollectionVsShardVsReplicaInfo.computeIfAbsent(node, Utils.NEW_HASHMAP_FUN);
            if (!keys.isEmpty()) {
              Row.forEachReplica(result, replicaInfo -> {
                for (String key : keys) {
                  if (!replicaInfo.getVariables().containsKey(key)) {
                    replicaVals.stream()
                        .filter(it -> replicaInfo.getCore().equals(it.get("core")))
                        .findFirst()
                        .ifPresent(map -> replicaInfo.getVariables().put(key, map.get(key)));
                  }
                }
              });
            }
            return result;
          }
        };
      }
    };
  }

  public void testAutoScalingHandlerFailure() {
    Map<String, Object> m = (Map<String, Object>) loadFromResource("testAutoScalingHandlerFailure.json");

    Policy policy = new Policy((Map<String, Object>) getObjectByPath(m, false, "diagnostics/config"));
    SolrCloudManager cloudManagerFromDiagnostics = createCloudManagerFromDiagnostics(m);
    Policy.Session session = policy.createSession(cloudManagerFromDiagnostics);
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(new AutoScalingConfig((Map<String, Object>) getObjectByPath(m, false, "diagnostics/config")), cloudManagerFromDiagnostics);
//    System.out.println(Utils.writeJson(suggestions, new StringWriter(),true).toString());
    assertEquals(4, suggestions.size());

  }

  static SolrCloudManager createCloudManagerFromDiagnostics(Map<String, Object> m) {
    List<Map> sortedNodes = (List<Map>) getObjectByPath(m, false, "diagnostics/sortedNodes");
    Set<String> liveNodes = new HashSet<>();
    SolrClientNodeStateProvider nodeStateProvider = new SolrClientNodeStateProvider(null) {
      @Override
      protected void readReplicaDetails() {
        for (Object o : sortedNodes) {
          String node = (String) ((Map) o).get("node");
          liveNodes.add(node);
          Map nodeDetails = nodeVsCollectionVsShardVsReplicaInfo.computeIfAbsent(node, s -> new LinkedHashMap<>());
          Map<String, Map<String, List<Map>>> replicas = (Map<String, Map<String, List<Map>>>) ((Map) o).get("replicas");
          replicas.forEach((coll, shardVsReplicas) -> shardVsReplicas
              .forEach((shard, repDetails) -> {
                List<ReplicaInfo> reps = (List) ((Map) nodeDetails
                    .computeIfAbsent(coll, o1 -> new LinkedHashMap<>()))
                    .computeIfAbsent(shard, o12 -> new ArrayList<ReplicaInfo>());
                for (Map map : repDetails) reps.add(new ReplicaInfo(map));
              }));
        }

      }

      @Override
      public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
        return nodeVsCollectionVsShardVsReplicaInfo.get(node) == null ?
            Collections.emptyMap() :
            nodeVsCollectionVsShardVsReplicaInfo.get(node);
      }

      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
        for (Map n : sortedNodes) if (n.get("node").equals(node)) return n;
        return Collections.emptyMap();
      }
    };
    return new DelegatingCloudManager(null) {
      ClusterState clusterState = null;
      @Override
      public NodeStateProvider getNodeStateProvider() {
        return nodeStateProvider;
      }

      @Override
      public ClusterStateProvider getClusterStateProvider() {
        if (clusterState == null) {
          Map map = (Map) getObjectByPath(m, false, "cluster/collections");
          if (map == null) map = new HashMap<>();
          clusterState = ClusterState.load(0, map, liveNodes, "/clusterstate.json");
        }

        return new DelegatingClusterStateProvider(null) {

          @Override
          public ClusterState getClusterState() {
            return clusterState;
          }

          @Override
          public ClusterState.CollectionRef getState(String collection) {
            return clusterState.getCollectionRef(collection);
          }

          @Override
          public Set<String> getLiveNodes() {
            return liveNodes;
          }
        };
      }
    };
  }

  public void testHostAttribute() {
    Map<String, Object> m = (Map<String, Object>) loadFromResource("testHostAttribute.json");
    Map<String, Object> conf = (Map<String, Object>) getObjectByPath(m, false, "diagnostics/config");
    Policy policy = new Policy(conf);
    SolrCloudManager cloudManagerFromDiagnostics = createCloudManagerFromDiagnostics(m);
    Policy.Session session = policy.createSession(cloudManagerFromDiagnostics);
    List<Violation> violations = session.getViolations();
    for (Violation violation : violations) {
      assertEquals(1.0d, violation.replicaCountDelta.doubleValue(), 0.0001);
    }
    assertEquals(2, violations.size());
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(new AutoScalingConfig(conf), cloudManagerFromDiagnostics);
    assertEquals(2, suggestions.size());
    for (Suggester.SuggestionInfo suggestion : suggestions) {
      assertTrue(ImmutableSet.of("127.0.0.219:63219_solr", "127.0.0.219:63229_solr").contains(
          suggestion._get("operation/command/move-replica/targetNode", null)));
    }
  }
  public void testSysPropSuggestions() {

    Map<String, Object> m = (Map<String, Object>) loadFromResource("testSysPropSuggestions.json");

    Map<String, Object> conf = (Map<String, Object>) getObjectByPath(m, false, "diagnostics/config");
    if(useNodeset){
      conf = (Map<String, Object>) Utils.fromJSONString("{" +
          "    'cluster-preferences':[{" +
          "      'minimize':'cores'," +
          "      'precision':1}," +
          "      {" +
          "        'maximize':'freedisk'," +
          "        'precision':100}," +
          "      {" +
          "        'minimize':'sysLoadAvg'," +
          "        'precision':10}]," +
          "    'cluster-policy':[" +
          "{'replica':'<3'," +
          "      'shard':'#EACH'," +
          "      nodeset: {'sysprop.zone':'east'}}, " +
          "{'replica':'<3'," +
          "      'shard':'#EACH'," +
          "      nodeset: {'sysprop.zone':'west'}} " +
          " ]}");
    }
    Policy policy = new Policy(conf);
    SolrCloudManager cloudManagerFromDiagnostics = createCloudManagerFromDiagnostics(m);
    Policy.Session session = policy.createSession(cloudManagerFromDiagnostics);
    List<Violation> violations = session.getViolations();
    for (Violation violation : violations) {
      assertEquals(1.0d, violation.replicaCountDelta.doubleValue(), 0.0001);
    }
    assertEquals(2, violations.size());
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(new AutoScalingConfig(conf), cloudManagerFromDiagnostics);
    assertEquals(2, suggestions.size());
    for (Suggester.SuggestionInfo suggestion : suggestions) {
      assertTrue(ImmutableSet.of("127.0.0.1:63219_solr", "127.0.0.1:63229_solr").contains(
          suggestion._get("operation/command/move-replica/targetNode", null)));

    }
  }

  public void testSuggestionsRebalanceOnly() {
    String conf = " {" +
        "    'cluster-preferences':[{" +
        "      'minimize':'cores'," +
        "      'precision':1}," +
        "      {'maximize':'freedisk','precision':100}," +
        "      {'minimize':'sysLoadAvg','precision':10}]," +
        "    'cluster-policy':[" +
        "{'replica':'<5','shard':'#EACH','sysprop.zone':['east','west']}]}";
    if(useNodeset){
      conf = " {" +
          "    'cluster-preferences':[{" +
          "      'minimize':'cores'," +
          "      'precision':1}," +
          "      {'maximize':'freedisk','precision':100}," +
          "      {'minimize':'sysLoadAvg','precision':10}]," +
          "    'cluster-policy':[" +
          "{'replica':'<5','shard':'#EACH', nodeset:{'sysprop.zone':['east','west']}}]}";

    }
    Map<String, Object> m = (Map<String, Object>) loadFromResource("testSuggestionsRebalanceOnly.json");
    SolrCloudManager cloudManagerFromDiagnostics = createCloudManagerFromDiagnostics(m);
    AutoScalingConfig autoScalingConfig = new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(conf));
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(autoScalingConfig, cloudManagerFromDiagnostics);

    assertEquals(2, suggestions.size());
    assertEquals("improvement", suggestions.get(0)._get("type",null));
    assertEquals("127.0.0.1:63229_solr", suggestions.get(0)._get("operation/command/move-replica/targetNode", null));
    assertEquals("improvement", suggestions.get(1)._get( "type",null));
    assertEquals("127.0.0.1:63219_solr", suggestions.get(1)._get("operation/command/move-replica/targetNode", null));
  }

  public void testSuggestionsRebalance2() {
    Map<String, Object> m = (Map<String, Object>) loadFromResource("testSuggestionsRebalance2.json");
    SolrCloudManager cloudManagerFromDiagnostics = createCloudManagerFromDiagnostics(m);

    AutoScalingConfig autoScalingConfig = new AutoScalingConfig((Map<String, Object>) getObjectByPath(m, false, "diagnostics/config"));
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(autoScalingConfig, cloudManagerFromDiagnostics);

    assertEquals(3, suggestions.size());

    for (Suggester.SuggestionInfo suggestion : suggestions) {
      assertEquals("improvement", suggestion._get("type", null));
      assertEquals("10.0.0.79:8983_solr", suggestion._get("operation/command/move-replica/targetNode",null));
    }

  }

  public void testAddMissingReplica() {
    Map<String, Object> m = (Map<String, Object>) loadFromResource("testAddMissingReplica.json");
    SolrCloudManager cloudManagerFromDiagnostics = createCloudManagerFromDiagnostics(m);
    AutoScalingConfig autoScalingConfig = new AutoScalingConfig((Map<String, Object>) getObjectByPath(m, false, "diagnostics/config"));

    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(autoScalingConfig, cloudManagerFromDiagnostics);

    assertEquals(1, suggestions.size());
    assertEquals("repair", suggestions.get(0)._get("type",null));
    assertEquals("add-replica", suggestions.get(0)._get("operation/command[0]/key",null));
    assertEquals("shard2", suggestions.get(0)._get("operation/command/add-replica/shard",null));
    assertEquals("NRT", suggestions.get(0)._get("operation/command/add-replica/type",null));

  }

  public void testCreateCollectionWithEmptyPolicy() {
    Map m = (Map) loadFromResource("testCreateCollectionWithEmptyPolicy.json");
    SolrCloudManager cloudManagerFromDiagnostics = createCloudManagerFromDiagnostics(m);
    AutoScalingConfig autoScalingConfig = new AutoScalingConfig(new HashMap());
    //POSITIONS : [shard1:1[NRT] @127.0.0.1:49469_solr, shard1:2[NRT] @127.0.0.1:49469_solr]
    List<ReplicaPosition> positions = PolicyHelper.getReplicaLocations("coll_new", autoScalingConfig, cloudManagerFromDiagnostics,
        EMPTY_MAP, Collections.singletonList("shard1"), 2, 0, 0, null);

    List<String> nodes = positions.stream().map(count -> count.node).collect(Collectors.toList());
    assertTrue(nodes.contains("127.0.0.1:49469_solr"));
    assertTrue(nodes.contains("127.0.0.1:49470_solr"));


  }

  public void testUnresolvedSuggestion() {
    Map<String, Object> m = (Map<String, Object>) loadFromResource("testUnresolvedSuggestion.json");

    SolrCloudManager cloudManagerFromDiagnostics = createCloudManagerFromDiagnostics(m);
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(new AutoScalingConfig((Map<String, Object>) getObjectByPath(m, false, "diagnostics/config"))
        , cloudManagerFromDiagnostics);
    for (Suggester.SuggestionInfo suggestion : suggestions) {
      assertEquals("unresolved_violation", suggestion._get("type", null));
      assertEquals("1.0", suggestion._getStr("violation/violation/delta", null));
    }
  }


  @Ignore("This takes too long to run. enable it for perf testing")
  public void testInfiniteLoop() {
    Row.cacheStats.clear();
    Map<String, Object> m = (Map<String, Object>) loadFromResource("testInfiniteLoop.json");
    SolrCloudManager cloudManagerFromDiagnostics = createCloudManagerFromDiagnostics(m);
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(
        new AutoScalingConfig((Map<String, Object>) getObjectByPath(m, false, "diagnostics/config"))
        , cloudManagerFromDiagnostics, 200, 1200, null);

    System.out.println(suggestions);
  }

  public static Object loadFromResource(String file)  {
    try (InputStream is = TestPolicy2.class.getResourceAsStream("/solrj/solr/autoscaling/" + file)) {
      return Utils.fromJSON(is, MAPOBJBUILDER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
