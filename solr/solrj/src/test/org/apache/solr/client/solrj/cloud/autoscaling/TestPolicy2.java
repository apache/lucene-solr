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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyMap;
import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.CORES;

public class TestPolicy2 extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void testEqualOnNonNode() {
    String state = "{" +
        "  'coll1': {" +
        "    'router': {" +
        "      'name': 'compositeId'" +
        "    }," +
        "    'shards': {" +
        "      'shard1': {" +
        "        'range': '80000000-ffffffff'," +
        "        'replicas': {" +
        "          'r1': {" +//east
        "            'core': 'r1'," +
        "            'base_url': 'http://10.0.0.4:8983/solr'," +
        "            'node_name': 'node1'," +
        "            'state': 'active'" +
        "          }," +
        "          'r2': {" +//west
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
        "          'r3': {" +//east
        "            'core': 'r3'," +
        "            'base_url': 'http://10.0.0.4:8983/solr'," +
        "            'node_name': 'node1'," +
        "            'state': 'active'" +
        "          }," +
        "          'r4': {" +//west
        "            'core': 'r4'," +
        "            'base_url': 'http://10.0.0.4:8987/solr'," +
        "            'node_name': 'node4'," +
        "            'state': 'active'" +
        "          }," +
        "          'r6': {" +//east
        "            'core': 'r6'," +
        "            'base_url': 'http://10.0.0.4:8989/solr'," +
        "            'node_name': 'node3'," +
        "            'state': 'active'" +
        "          }," +
        "          'r5': {" +//east
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
    String metaData =
        "  {'nodeValues':{" +
            "    'node1':{'cores' : 3, 'freedisk' : 700, 'totaldisk' :1000, 'sysprop.zone' : 'east'}," +
            "    'node2':{'cores' : 1, 'freedisk' : 900, 'totaldisk' :1000, 'sysprop.zone' : 'west'}," +
            "    'node3':{'cores' : 1, 'freedisk' : 900, 'totaldisk' :1000, 'sysprop.zone': 'east'}," +
            "    'node4':{'cores' : 1, 'freedisk' : 900, 'totaldisk' :1000, 'sysprop.zone': 'west'}," +
            "    'node5':{'cores' : 0, 'freedisk' : 1000, 'totaldisk' :1000, 'sysprop.zone': 'west'}" +
            "  }," +
            "  'replicaValues':[" +
            "    {'INDEX.sizeInGB': 100, core : r1}," +
            "    {'INDEX.sizeInGB': 100, core : r2}," +
            "    {'INDEX.sizeInGB': 100, core : r3}," +
            "    {'INDEX.sizeInGB': 100, core : r4}," +
            "    {'INDEX.sizeInGB': 100, core : r5}," +
            "    {'INDEX.sizeInGB': 100, core : r6}]}";

    String autoScalingjson = "{cluster-policy:[" +
        "    { replica : '<3' , shard : '#EACH', sysprop.zone: [east,west] } ]," +
        "  'cluster-preferences':[{ minimize : cores},{maximize : freedisk, precision : 50}]}";
    Policy policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    Policy.Session session = policy.createSession(createCloudManager(state, metaData));
    List<Violation> violations = session.getViolations();
    assertEquals(1, violations.size());
    assertEquals(3, violations.get(0).getViolatingReplicas().size());
    assertEquals(1.0, violations.get(0).replicaCountDelta, 0.01);
    for (Violation.ReplicaInfoAndErr r : violations.get(0).getViolatingReplicas()) {
      assertEquals("shard2", r.replicaInfo.getShard());
    }

    autoScalingjson = "{cluster-policy:[" +
        "    { replica : '<3' , shard : '#EACH', sysprop.zone: '#EACH' } ]," +
        "  'cluster-preferences':[{ minimize : cores},{maximize : freedisk, precision : 50}]}";
    policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    session = policy.createSession(createCloudManager(state, metaData));
    violations = session.getViolations();
    assertEquals(1, violations.size());
    assertEquals(3, violations.get(0).getViolatingReplicas().size());
    assertEquals(1.0, violations.get(0).replicaCountDelta, 0.01);
    for (Violation.ReplicaInfoAndErr r : violations.get(0).getViolatingReplicas()) {
      assertEquals("shard2", r.replicaInfo.getShard());
    }
    autoScalingjson = "{cluster-policy:[" +
        "    { replica : '#EQUAL' , node: '#ANY' } ]," +
        "  'cluster-preferences':[{ minimize : cores},{maximize : freedisk, precision : 50}]}";
    policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    session = policy.createSession(createCloudManager(state, metaData));
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
    suggestions = PolicyHelper.getSuggestions(new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson))
        , createCloudManager(state, metaData));
    assertEquals(1, suggestions.size());
    String repName = (String) Utils.getObjectByPath(suggestions.get(0).operation, true, "command/move-replica/replica");

    AtomicBoolean found = new AtomicBoolean(false);
    session.getNode("node1").forEachReplica(replicaInfo -> {
      if (replicaInfo.getName().equals(repName)) {
        found.set(true);
      }
    });
    assertTrue(found.get());

    autoScalingjson = "{cluster-policy:[" +
        "    { cores : '#EQUAL' , node: '#ANY' } ]," +
        "  'cluster-preferences':[{ minimize : cores},{minimize : freedisk, precision : 50}]}";
    policy = new Policy((Map<String, Object>) Utils.fromJSONString(autoScalingjson));
    session = policy.createSession(createCloudManager(state, metaData));
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

    suggestions = PolicyHelper.getSuggestions(new AutoScalingConfig((Map<String, Object>) Utils.fromJSONString(autoScalingjson)),
        createCloudManager(state, metaData));
    assertEquals(1, suggestions.size());
    assertEquals("node5", Utils.getObjectByPath(suggestions.get(0).operation, true, "command/move-replica/targetNode"));

    String rName = (String) Utils.getObjectByPath(suggestions.get(0).operation, true, "command/move-replica/replica");

    found.set(false);
    session.getNode("node1").forEachReplica(replicaInfo -> {
      if (replicaInfo.getName().equals(rName)) {
        found.set(true);
      }
    });
    assertTrue(found.get());

  }

  static SolrCloudManager createCloudManager(String clusterStateStr, String metadata) {
    Map m = (Map) Utils.fromJSONString(clusterStateStr);
    Map meta = (Map) Utils.fromJSONString(metadata);
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
              result.put(tag, Utils.getObjectByPath(nodeVals, true, Arrays.asList(node, tag)));
            }
            return result;
          }

          @Override
          public Map<String, Map<String, List<ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
            Map<String, Map<String, List<ReplicaInfo>>> result = nodeVsCollectionVsShardVsReplicaInfo.computeIfAbsent(node, s -> emptyMap());
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
    String diagnostics = "{" +
        "  'diagnostics': {" +
        "    'sortedNodes': [" +
        "      {" +
        "        'node': '127.0.0.1:63191_solr'," +
        "        'isLive': true," +
        "        'cores': 3.0," +
        "        'freedisk': 680.908073425293," +
        "        'heapUsage': 24.97510064011647," +
        "        'sysLoadAvg': 272.75390625," +
        "        'totaldisk': 1037.938980102539," +
        "        'replicas': {" +
        "          'readApiTestViolations': {" +
        "            'shard1': [" +
        "              {" +
        "                'core_node5': {" +
        "                  'core': 'readApiTestViolations_shard1_replica_n2'," +
        "                  'leader': 'true'," +
        "                  'base_url': 'https://127.0.0.1:63191/solr'," +
        "                  'node_name': '127.0.0.1:63191_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard1'," +
        "                  'collection': 'readApiTestViolations'" +
        "                }" +
        "              }," +
        "              {" +
        "                'core_node7': {" +
        "                  'core': 'readApiTestViolations_shard1_replica_n4'," +
        "                  'base_url': 'https://127.0.0.1:63191/solr'," +
        "                  'node_name': '127.0.0.1:63191_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard1'," +
        "                  'collection': 'readApiTestViolations'" +
        "                }" +
        "              }," +
        "              {" +
        "                'core_node12': {" +
        "                  'core': 'readApiTestViolations_shard1_replica_n10'," +
        "                  'base_url': 'https://127.0.0.1:63191/solr'," +
        "                  'node_name': '127.0.0.1:63191_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard1'," +
        "                  'collection': 'readApiTestViolations'" +
        "                }" +
        "              }" +
        "            ]" +
        "          }" +
        "        }" +
        "      }," +
        "      {" +
        "        'node': '127.0.0.1:63192_solr'," +
        "        'isLive': true," +
        "        'cores': 3.0," +
        "        'freedisk': 680.908073425293," +
        "        'heapUsage': 24.98878807983566," +
        "        'sysLoadAvg': 272.75390625," +
        "        'totaldisk': 1037.938980102539," +
        "        'replicas': {" +
        "          'readApiTestViolations': {" +
        "            'shard1': [" +
        "              {" +
        "                'core_node3': {" +
        "                  'core': 'readApiTestViolations_shard1_replica_n1'," +
        "                  'base_url': 'https://127.0.0.1:63192/solr'," +
        "                  'node_name': '127.0.0.1:63192_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard1'," +
        "                  'collection': 'readApiTestViolations'" +
        "                }" +
        "              }," +
        "              {" +
        "                'core_node9': {" +
        "                  'core': 'readApiTestViolations_shard1_replica_n6'," +
        "                  'base_url': 'https://127.0.0.1:63192/solr'," +
        "                  'node_name': '127.0.0.1:63192_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard1'," +
        "                  'collection': 'readApiTestViolations'" +
        "                }" +
        "              }," +
        "              {" +
        "                'core_node11': {" +
        "                  'core': 'readApiTestViolations_shard1_replica_n8'," +
        "                  'base_url': 'https://127.0.0.1:63192/solr'," +
        "                  'node_name': '127.0.0.1:63192_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard1'," +
        "                  'collection': 'readApiTestViolations'" +
        "                }" +
        "              }" +
        "            ]" +
        "          }" +
        "        }" +
        "      }," +
        "      {" +
        "        'node': '127.0.0.1:63219_solr'," +
        "        'isLive': true," +
        "        'cores': 0.0," +
        "        'freedisk': 680.908073425293," +
        "        'heapUsage': 24.98878807983566," +
        "        'sysLoadAvg': 272.75390625," +
        "        'totaldisk': 1037.938980102539," +
        "        'replicas': {}" +
        "      }" +
        "    ]," +
        "    'liveNodes': [" +
        "      '127.0.0.1:63191_solr'," +
        "      '127.0.0.1:63192_solr'," +
        "      '127.0.0.1:63219_solr'" +
        "    ]," +
        "    'violations': [" +
        "      {" +
        "        'collection': 'readApiTestViolations'," +
        "        'shard': 'shard1'," +
        "        'node': '127.0.0.1:63191_solr'," +
        "        'violation': {" +
        "          'replica': {'NRT': 3, 'count': 3}," +
        "          'delta': 2.0" +
        "        }," +
        "        'clause': {" +
        "          'replica': '<3'," +
        "          'shard': '#EACH'," +
        "          'node': '#ANY'," +
        "          'collection': 'readApiTestViolations'" +
        "        }" +
        "      }," +
        "      {" +
        "        'collection': 'readApiTestViolations'," +
        "        'shard': 'shard1'," +
        "        'node': '127.0.0.1:63192_solr'," +
        "        'violation': {" +
        "          'replica': {'NRT': 3, 'count': 3}," +
        "          'delta': 2.0" +
        "        }," +
        "        'clause': {" +
        "          'replica': '<2'," +
        "          'shard': '#EACH'," +
        "          'node': '#ANY'," +
        "          'collection': 'readApiTestViolations'" +
        "        }" +
        "      }" +
        "    ]," +
        "    'config': {" +
        "      'cluster-preferences': [" +
        "        {'minimize': 'cores', 'precision': 3}," +
        "        {'maximize': 'freedisk', 'precision': 100}," +
        "        {'minimize': 'sysLoadAvg', 'precision': 10}," +
        "        {'minimize': 'heapUsage', 'precision': 10}" +
        "      ]," +
        "      'cluster-policy': [" +
        "        {'cores': '<10', 'node': '#ANY'}," +
        "        {'replica': '<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "        {'nodeRole': 'overseer', 'replica': 0}" +
        "      ]" +
        "    }" +
        "  }}";
    Map<String, Object> m = (Map<String, Object>) Utils.fromJSONString(diagnostics);

    Policy policy = new Policy((Map<String, Object>) Utils.getObjectByPath(m, false, "diagnostics/config"));
    SolrCloudManager cloudManagerFromDiagnostics = createCloudManagerFromDiagnostics(m);
    Policy.Session session = policy.createSession(cloudManagerFromDiagnostics);
    List<Suggester.SuggestionInfo> suggestions = PolicyHelper.getSuggestions(new AutoScalingConfig((Map<String, Object>) Utils.getObjectByPath(m, false, "diagnostics/config")), cloudManagerFromDiagnostics);
    assertEquals(2, suggestions.size());

  }

  static SolrCloudManager createCloudManagerFromDiagnostics(Map<String, Object> m) {
    List<Map> sortedNodes = (List<Map>) Utils.getObjectByPath(m, false, "diagnostics/sortedNodes");
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
      @Override
      public NodeStateProvider getNodeStateProvider() {
        return nodeStateProvider;
      }

      @Override
      public ClusterStateProvider getClusterStateProvider() {
        return new DelegatingClusterStateProvider(null) {
          @Override
          public Set<String> getLiveNodes() {
            return liveNodes;
          }
        };
      }
    };
  }
  public void testSysPropSuggestions() {
    String diagnostics = "{" +
        "  'diagnostics': {" +
        "    'sortedNodes': [" +
        "      {" +
        "        'node': '127.0.0.1:63191_solr'," +
        "        'isLive': true," +
        "        'cores': 3.0," +
        "        'sysprop.zone': 'east'," +
        "        'freedisk': 1727.1459312438965," +
        "        'heapUsage': 24.97510064011647," +
        "        'sysLoadAvg': 272.75390625," +
        "        'totaldisk': 1037.938980102539," +
        "        'replicas': {" +
        "          'zonesTest': {" +
        "            'shard1': [" +
        "              {" +
        "                'core_node5': {" +
        "                  'core': 'zonesTest_shard1_replica_n2'," +
        "                  'leader': 'true'," +
        "                  'base_url': 'https://127.0.0.1:63191/solr'," +
        "                  'node_name': '127.0.0.1:63191_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard1'," +
        "                  'collection': 'zonesTest'" +
        "                }" +
        "              }," +
        "              {" +
        "                'core_node7': {" +
        "                  'core': 'zonesTest_shard1_replica_n4'," +
        "                  'base_url': 'https://127.0.0.1:63191/solr'," +
        "                  'node_name': '127.0.0.1:63191_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard1'," +
        "                  'collection': 'zonesTest'" +
        "                }" +
        "              }," +
        "              {" +
        "                'core_node12': {" +
        "                  'core': 'zonesTest_shard1_replica_n10'," +
        "                  'base_url': 'https://127.0.0.1:63191/solr'," +
        "                  'node_name': '127.0.0.1:63191_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard1'," +
        "                  'collection': 'zonesTest'" +
        "                }" +
        "              }" +
        "            ]" +
        "          }" +
        "        }" +
        "      }," +
        "      {" +
        "        'node': '127.0.0.1:63192_solr'," +
        "        'isLive': true," +
        "        'cores': 3.0," +
        "        'sysprop.zone': 'east'," +
        "        'freedisk': 1727.1459312438965," +
        "        'heapUsage': 24.98878807983566," +
        "        'sysLoadAvg': 272.75390625," +
        "        'totaldisk': 1037.938980102539," +
        "        'replicas': {" +
        "          'zonesTest': {" +
        "            'shard2': [" +
        "              {" +
        "                'core_node3': {" +
        "                  'core': 'zonesTest_shard1_replica_n1'," +
        "                  'base_url': 'https://127.0.0.1:63192/solr'," +
        "                  'node_name': '127.0.0.1:63192_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard2'," +
        "                  'collection': 'zonesTest'" +
        "                }" +
        "              }," +
        "              {" +
        "                'core_node9': {" +
        "                  'core': 'zonesTest_shard1_replica_n6'," +
        "                  'base_url': 'https://127.0.0.1:63192/solr'," +
        "                  'node_name': '127.0.0.1:63192_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard2'," +
        "                  'collection': 'zonesTest'" +
        "                }" +
        "              }," +
        "              {" +
        "                'core_node11': {" +
        "                  'core': 'zonesTest_shard1_replica_n8'," +
        "                  'base_url': 'https://127.0.0.1:63192/solr'," +
        "                  'node_name': '127.0.0.1:63192_solr'," +
        "                  'state': 'active'," +
        "                  'type': 'NRT'," +
        "                  'force_set_state': 'false'," +
        "                  'INDEX.sizeInGB': 6.426125764846802E-8," +
        "                  'shard': 'shard2'," +
        "                  'collection': 'zonesTest'" +
        "                }" +
        "              }" +
        "            ]" +
        "          }" +
        "        }" +
        "      }," +
        "      {" +
        "        'node': '127.0.0.1:63219_solr'," +
        "        'isLive': true," +
        "        'cores': 0.0," +
        "        'sysprop.zone': 'west'," +
        "        'freedisk': 1768.6174201965332," +
        "        'heapUsage': 24.98878807983566," +
        "        'sysLoadAvg': 272.75390625," +
        "        'totaldisk': 1037.938980102539," +
        "        'replicas': {}" +
        "      }," +
        "      {" +
        "        'node': '127.0.0.1:63229_solr'," +
        "        'isLive': true," +
        "        'cores': 0.0," +
        "        'sysprop.zone': 'west'," +
        "        'freedisk': 1768.6174201965332," +
        "        'heapUsage': 24.98878807983566," +
        "        'sysLoadAvg': 272.75390625," +
        "        'totaldisk': 1037.938980102539," +
        "        'replicas': {}" +
        "      }" +
        "    ]," +
        "    'liveNodes': [" +
        "      '127.0.0.1:63191_solr'," +
        "      '127.0.0.1:63192_solr'," +
        "      '127.0.0.1:63219_solr'," +
        "      '127.0.0.1:63229_solr'" +
        "    ]," +
        "    'config': {" +
        "      'cluster-preferences': [" +
        "        {'minimize': 'cores', 'precision': 1}," +
        "        {'maximize': 'freedisk', 'precision': 100}," +
        "        {'minimize': 'sysLoadAvg', 'precision': 10}" +
        "      ]," +
        "      'cluster-policy': [" +
        "        {'replica': '<3', 'shard': '#EACH', 'sysprop.zone': [east, west]}" +
        "      ]" +
        "    }" +
        "  }" +
        "}";

    Map<String, Object> m = (Map<String, Object>) Utils.fromJSONString(diagnostics);

    Map<String, Object> conf = (Map<String, Object>) Utils.getObjectByPath(m, false, "diagnostics/config");
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
          Utils.getObjectByPath(suggestion, true, "operation/command/move-replica/targetNode")));

    }
  }


}
