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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.solr.client.solrj.cloud.NodeStateProvider;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable;
import org.apache.solr.common.cloud.Replica;

/**
 * Read-only snapshot of another {@link NodeStateProvider}.
 */
public class SnapshotNodeStateProvider implements NodeStateProvider {
  private Map<String, Map<String, Object>> nodeValues = new LinkedHashMap<>();
  private Map<String, Map<String, Map<String, List<Replica>>>> replicaInfos = new LinkedHashMap<>();

  private static double GB = 1024.0d * 1024.0d * 1024.0d;

  /**
   * Populate this instance from another instance of {@link SolrCloudManager}.
   * @param other another instance
   * @param config optional {@link AutoScalingConfig}, which will be used to determine what node and
   *               replica tags to retrieve. If this is null then the other instance's config will be used.
   */
  @SuppressWarnings({"unchecked"})
  public SnapshotNodeStateProvider(SolrCloudManager other, AutoScalingConfig config) throws Exception {
    if (config == null) {
      config = other.getDistribStateManager().getAutoScalingConfig();
    }
    Set<String> nodeTags = new HashSet<>(SimUtils.COMMON_NODE_TAGS);
    nodeTags.addAll(config.getPolicy().getParamNames());
    Set<String> replicaTags = new HashSet<>(SimUtils.COMMON_REPLICA_TAGS);
    replicaTags.addAll(config.getPolicy().getPerReplicaAttributes());
    for (String node : other.getClusterStateProvider().getLiveNodes()) {
      nodeValues.put(node, new LinkedHashMap<>(other.getNodeStateProvider().getNodeValues(node, nodeTags)));
      Map<String, Map<String, List<Replica>>> infos = other.getNodeStateProvider().getReplicaInfo(node, replicaTags);
      infos.forEach((collection, shards) -> {
        shards.forEach((shard, replicas) -> {
          replicas.forEach(r -> {
            List<Replica> myReplicas = replicaInfos
                .computeIfAbsent(node, n -> new LinkedHashMap<>())
                .computeIfAbsent(collection, c -> new LinkedHashMap<>())
                .computeIfAbsent(shard, s -> new ArrayList<>());
            Map<String, Object> rMap = new LinkedHashMap<>();
            r.toMap(rMap);
            if (r.isLeader()) { // ReplicaInfo.toMap doesn't write this!!!
              ((Map<String, Object>)rMap.values().iterator().next()).put("leader", "true");
            }
            Replica ri = new Replica(rMap);
            // put in "leader" again if present
            if (r.isLeader()) {
              ri.getProperties().put("leader", "true");
            }
            // externally produced snapshots may not include the right units
            if (ri.get(Variable.Type.CORE_IDX.metricsAttribute) == null) {
              if (ri.get(Variable.Type.CORE_IDX.tagName) != null) {
                Number indexSizeGB = (Number) ri.get(Variable.Type.CORE_IDX.tagName);
                ri.getProperties().put(Variable.Type.CORE_IDX.metricsAttribute, indexSizeGB.doubleValue() * GB);
              } else {
                throw new RuntimeException("Missing size information for replica: " + ri);
              }
            }
            myReplicas.add(ri);
          });
        });
      });
    }
  }

  /**
   * Populate this instance from a previously generated snapshot.
   * @param snapshot previous snapshot created using this class.
   */
  @SuppressWarnings({"unchecked"})
  public SnapshotNodeStateProvider(Map<String, Object> snapshot) {
    Objects.requireNonNull(snapshot);
    nodeValues = (Map<String, Map<String, Object>>)snapshot.getOrDefault("nodeValues", Collections.emptyMap());
    ((Map<String, Object>)snapshot.getOrDefault("replicaInfos", Collections.emptyMap())).forEach((node, v) -> {
      Map<String, Map<String, List<Replica>>> perNode = replicaInfos.computeIfAbsent(node, n -> new LinkedHashMap<>());
      ((Map<String, Object>)v).forEach((collection, shards) -> {
        Map<String, List<Replica>> perColl = perNode.computeIfAbsent(collection, c -> new LinkedHashMap<>());
        ((Map<String, Object>)shards).forEach((shard, replicas) -> {
          List<Replica> infos = perColl.computeIfAbsent(shard, s -> new ArrayList<>());
          ((List<Map<String, Object>>)replicas).forEach(replicaMap -> {
            Replica ri = new Replica(new LinkedHashMap<>(replicaMap)); // constructor modifies this map
            if (ri.isLeader()) {
              ri.getProperties().put("leader", "true");
            }
            // externally produced snapshots may not include the right units
            if (ri.get(Variable.Type.CORE_IDX.metricsAttribute) == null) {
                if (ri.get(Variable.Type.CORE_IDX.tagName) != null) {
                  Number indexSizeGB = (Number) ri.get(Variable.Type.CORE_IDX.tagName);
                  ri.getProperties().put(Variable.Type.CORE_IDX.metricsAttribute, indexSizeGB.doubleValue() * GB);
                } else {
                  throw new RuntimeException("Missing size information for replica: " + ri);
              }
            }
            infos.add(ri);
          });
        });
      });
    });
  }

  /**
   * Create a snapshot of all node and replica tag values available from the original source, per the original
   * autoscaling configuration. Note:
   */
  @SuppressWarnings({"unchecked"})
  public Map<String, Object> getSnapshot() {
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("nodeValues", nodeValues);
    Map<String, Map<String, Map<String, List<Map<String, Object>>>>> replicaInfosMap = new LinkedHashMap<>();
    snapshot.put("replicaInfos", replicaInfosMap);
    replicaInfos.forEach((node, perNode) -> {
      perNode.forEach((collection, shards) -> {
        shards.forEach((shard, replicas) -> {
          replicas.forEach(r -> {
            List<Map<String, Object>> myReplicas = replicaInfosMap
                .computeIfAbsent(node, n -> new LinkedHashMap<>())
                .computeIfAbsent(collection, c -> new LinkedHashMap<>())
                .computeIfAbsent(shard, s -> new ArrayList<>());
            Map<String, Object> rMap = new LinkedHashMap<>();
            r.toMap(rMap);
            if (r.isLeader()) { // ReplicaInfo.toMap doesn't write this!!!
              ((Map<String, Object>)rMap.values().iterator().next()).put("leader", "true");
            }
            myReplicas.add(rMap);
          });
        });
      });
    });
    return snapshot;
  }

  @Override
  public Map<String, Object> getNodeValues(String node, Collection<String> tags) {
    return new LinkedHashMap<>(nodeValues.getOrDefault(node, Collections.emptyMap()));
  }

  @Override
  public Map<String, Map<String, List<Replica>>> getReplicaInfo(String node, Collection<String> keys) {
    Map<String, Map<String, List<Replica>>> result = new LinkedHashMap<>();
    Map<String, Map<String, List<Replica>>> infos = replicaInfos.getOrDefault(node, Collections.emptyMap());
    // deep copy
    infos.forEach((coll, shards) -> {
      shards.forEach((shard, replicas) -> {
        replicas.forEach(ri -> {
          List<Replica> myReplicas = result
              .computeIfAbsent(coll, c -> new LinkedHashMap<>())
              .computeIfAbsent(shard, s -> new ArrayList<>());
          Replica myReplica = (Replica)ri.clone();
          myReplicas.add(myReplica);
        });
      });
    });
    return result;
  }

  public Replica getReplicaInfo(String collection, String coreNode) {
    for (Map<String, Map<String, List<Replica>>> perNode : replicaInfos.values()) {
      for (List<Replica> perShard : perNode.getOrDefault(collection, Collections.emptyMap()).values()) {
        for (Replica ri : perShard) {
          if (ri.getName().equals(coreNode)) {
            return (Replica)ri.clone();
          }
        }
      }
    }
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
