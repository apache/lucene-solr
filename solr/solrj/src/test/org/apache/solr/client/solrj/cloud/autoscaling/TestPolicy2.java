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
        "          'r1': {" +
        "            'core': 'r1'," +
        "            'base_url': 'http://10.0.0.4:8983/solr'," +
        "            'node_name': 'node1'," +
        "            'state': 'active'" +
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
        "            'state': 'active'" +
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
    assertEquals(4, violations.get(0).getViolatingReplicas().size());
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
    assertEquals(4, violations.get(0).getViolatingReplicas().size());
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

}
