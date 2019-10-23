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

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.Cell;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.Row;
import org.apache.solr.client.solrj.cloud.autoscaling.Variable;
import org.apache.solr.client.solrj.request.CollectionApiMapping;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.RedactionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various utility methods useful for autoscaling simulations and snapshots.
 */
public class SimUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public static final Set<String> COMMON_REPLICA_TAGS = new HashSet<>(Arrays.asList(
      Variable.Type.CORE_IDX.metricsAttribute,
      Variable.Type.CORE_IDX.tagName,
      "SEARCHER.searcher.numDocs",
      "SEARCHER.searcher.maxDoc",
      "SEARCHER.searcher.indexCommitSize",
      "QUERY./select.requests",
      "UPDATE./update.requests"
  ));

  public static final Set<String> COMMON_NODE_TAGS = new HashSet<>(Arrays.asList(
      Variable.Type.CORES.tagName,
      Variable.Type.FREEDISK.tagName,
      Variable.Type.NODE.tagName,
      Variable.Type.NODE_ROLE.tagName,
      Variable.Type.TOTALDISK.tagName,
      Variable.Type.DISKTYPE.tagName,
      Variable.Type.HEAPUSAGE.tagName,
      Variable.Type.HOST.tagName,
      Variable.Type.IP_1.tagName,
      Variable.Type.IP_2.tagName,
      Variable.Type.IP_3.tagName,
      Variable.Type.IP_4.tagName,
      Variable.Type.PORT.tagName,
      Variable.Type.SYSLOADAVG.tagName,
      "withCollection"
  ));

  /**
   * Check consistency of data in a {@link SolrCloudManager}. This may be needed when constructing a simulated
   * instance from potentially inconsistent data (eg. partial snapshots taken at different points in time).
   * @param solrCloudManager source manager
   * @param config optional {@link AutoScalingConfig} instance used to determine what node and replica metrics to check.
   */
  public static void checkConsistency(SolrCloudManager solrCloudManager, AutoScalingConfig config) throws Exception {
    if (config == null) {
      config = solrCloudManager.getDistribStateManager().getAutoScalingConfig();
    }
    Set<String> replicaTags = new HashSet<>(COMMON_REPLICA_TAGS);
    replicaTags.addAll(config.getPolicy().getPerReplicaAttributes());

    // verify replicas are consistent and data is available
    Map<String, Map<String, Replica>> allReplicas = new HashMap<>();
    solrCloudManager.getClusterStateProvider().getClusterState().forEachCollection(coll -> {
      coll.getReplicas().forEach(r -> {
        if (allReplicas.containsKey(r.getName())) {
          throw new RuntimeException("duplicate core_node name in clusterState: " + allReplicas.get(r.getName()) + " versus " + r);
        } else {
          allReplicas.computeIfAbsent(coll.getName(), c -> new HashMap<>()).put(r.getName(), r);
        }
      });
    });
    Map<String, Map<String, ReplicaInfo>> allReplicaInfos = new HashMap<>();
    solrCloudManager.getClusterStateProvider().getLiveNodes().forEach(n -> {
      Map<String, Map<String, List<ReplicaInfo>>> infos = solrCloudManager.getNodeStateProvider().getReplicaInfo(n, replicaTags);
      infos.forEach((coll, shards) -> shards.forEach((shard, replicas) -> replicas.forEach(r -> {
        if (allReplicaInfos.containsKey(r.getName())) {
          throw new RuntimeException("duplicate core_node name in NodeStateProvider: " + allReplicaInfos.get(r.getName()) + " versus " + r);
        } else {
          allReplicaInfos.computeIfAbsent(coll, c -> new HashMap<>()).put(r.getName(), r);
        }
      })));
    });
    if (!allReplicaInfos.keySet().equals(allReplicas.keySet())) {
      Set<String> notInClusterState = allReplicaInfos.keySet().stream()
          .filter(k -> !allReplicas.containsKey(k))
          .collect(Collectors.toSet());
      Set<String> notInNodeProvider = allReplicas.keySet().stream()
          .filter(k -> !allReplicaInfos.containsKey(k))
          .collect(Collectors.toSet());
      throw new RuntimeException("Mismatched replica data between ClusterState and NodeStateProvider:\n\t" +
          "collection not in ClusterState: " + notInClusterState + "\n\t" +
          "collection not in NodeStateProvider: " + notInNodeProvider);
    }
    allReplicaInfos.keySet().forEach(collection -> {
      Set<String> infosCores = allReplicaInfos.getOrDefault(collection, Collections.emptyMap()).keySet();
      Map<String, Replica> replicas = allReplicas.getOrDefault(collection, Collections.emptyMap());
      Set<String> csCores = replicas.keySet();
      if (!infosCores.equals(csCores)) {
        Set<String> notInClusterState = infosCores.stream()
            .filter(k -> !csCores.contains(k))
            .collect(Collectors.toSet());
        Set<String> notInNodeProvider = csCores.stream()
            .filter(k -> !infosCores.contains(k) && replicas.get(k).isActive(solrCloudManager.getClusterStateProvider().getLiveNodes()))
            .collect(Collectors.toSet());
        if (!notInClusterState.isEmpty() || !notInNodeProvider.isEmpty()) {
          throw new RuntimeException("Mismatched replica data for collection " + collection + " between ClusterState and NodeStateProvider:\n\t" +
              "replica in NodeStateProvider but not in ClusterState: " + notInClusterState + "\n\t" +
              "replica in ClusterState but not in NodeStateProvider: " + notInNodeProvider);
        }
      }
    });
    // verify all replicas have size info
    allReplicaInfos.forEach((coll, replicas) -> replicas.forEach((core, ri) -> {
          Number size = (Number) ri.getVariable(Variable.Type.CORE_IDX.metricsAttribute);
          if (size == null) {
            size = (Number) ri.getVariable(Variable.Type.CORE_IDX.tagName);
            if (size == null) {
//              for (String node : solrCloudManager.getClusterStateProvider().getLiveNodes()) {
//                log.error("Check for missing values: {}: {}", node, solrCloudManager.getNodeStateProvider().getReplicaInfo(node, SnapshotNodeStateProvider.REPLICA_TAGS));
//              }
              throw new RuntimeException("missing replica size information: " + ri);
            }
          }
        }
    ));
  }

  /**
   * Calculate statistics of node / collection and replica layouts for the provided {@link SolrCloudManager}.
   * @param cloudManager manager
   * @param config autoscaling config, or null if the one from the provided manager should be used
   * @param verbose if true then add more details about replicas.
   * @return a map containing detailed statistics
   */
  public static Map<String, Object> calculateStats(SolrCloudManager cloudManager, AutoScalingConfig config, boolean verbose) throws Exception {
    ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
    Map<String, Map<String, Number>> collStats = new TreeMap<>();
    Policy.Session session = config.getPolicy().createSession(cloudManager);
    clusterState.forEachCollection(coll -> {
      Map<String, Number> perColl = collStats.computeIfAbsent(coll.getName(), n -> new LinkedHashMap<>());
      AtomicInteger numCores = new AtomicInteger();
      HashMap<String, Map<String, AtomicInteger>> nodes = new HashMap<>();
      coll.getSlices().forEach(s -> {
        numCores.addAndGet(s.getReplicas().size());
        s.getReplicas().forEach(r -> {
          nodes.computeIfAbsent(r.getNodeName(), n -> new HashMap<>())
              .computeIfAbsent(s.getName(), slice -> new AtomicInteger()).incrementAndGet();
        });
      });
      int maxCoresPerNode = 0;
      int minCoresPerNode = 0;
      int maxActualShardsPerNode = 0;
      int minActualShardsPerNode = 0;
      int maxShardReplicasPerNode = 0;
      int minShardReplicasPerNode = 0;
      if (!nodes.isEmpty()) {
        minCoresPerNode = Integer.MAX_VALUE;
        minActualShardsPerNode = Integer.MAX_VALUE;
        minShardReplicasPerNode = Integer.MAX_VALUE;
        for (Map<String, AtomicInteger> counts : nodes.values()) {
          int total = counts.values().stream().mapToInt(c -> c.get()).sum();
          for (AtomicInteger count : counts.values()) {
            if (count.get() > maxShardReplicasPerNode) {
              maxShardReplicasPerNode = count.get();
            }
            if (count.get() < minShardReplicasPerNode) {
              minShardReplicasPerNode = count.get();
            }
          }
          if (total > maxCoresPerNode) {
            maxCoresPerNode = total;
          }
          if (total < minCoresPerNode) {
            minCoresPerNode = total;
          }
          if (counts.size() > maxActualShardsPerNode) {
            maxActualShardsPerNode = counts.size();
          }
          if (counts.size() < minActualShardsPerNode) {
            minActualShardsPerNode = counts.size();
          }
        }
      }
      perColl.put("activeShards", coll.getActiveSlices().size());
      perColl.put("inactiveShards", coll.getSlices().size() - coll.getActiveSlices().size());
      perColl.put("rf", coll.getReplicationFactor());
      perColl.put("maxShardsPerNode", coll.getMaxShardsPerNode());
      perColl.put("maxActualShardsPerNode", maxActualShardsPerNode);
      perColl.put("minActualShardsPerNode", minActualShardsPerNode);
      perColl.put("maxShardReplicasPerNode", maxShardReplicasPerNode);
      perColl.put("minShardReplicasPerNode", minShardReplicasPerNode);
      perColl.put("numCores", numCores.get());
      perColl.put("numNodes", nodes.size());
      perColl.put("maxCoresPerNode", maxCoresPerNode);
      perColl.put("minCoresPerNode", minCoresPerNode);
    });
    Map<String, Map<String, Object>> nodeStats = new TreeMap<>();
    Map<Integer, AtomicInteger> coreStats = new TreeMap<>();
    List<Row> rows = session.getSortedNodes();
    // check consistency
    if (rows.size() != clusterState.getLiveNodes().size()) {
      throw new Exception("Mismatch between autoscaling matrix size (" + rows.size() + ") and liveNodes size (" + clusterState.getLiveNodes().size() + ")");
    }
    for (Row row : rows) {
      Map<String, Object> nodeStat = nodeStats.computeIfAbsent(row.node, n -> new LinkedHashMap<>());
      nodeStat.put("isLive", row.isLive());
      for (Cell cell : row.getCells()) {
        nodeStat.put(cell.getName(), cell.getValue());
      }
//      nodeStat.put("freedisk", row.getVal("freedisk", 0));
//      nodeStat.put("totaldisk", row.getVal("totaldisk", 0));
      int cores = ((Number)row.getVal("cores", 0)).intValue();
//      nodeStat.put("cores", cores);
      coreStats.computeIfAbsent(cores, num -> new AtomicInteger()).incrementAndGet();
      Map<String, Map<String, Map<String, Object>>> collReplicas = new TreeMap<>();
      // check consistency
      AtomicInteger rowCores = new AtomicInteger();
      row.forEachReplica(ri -> rowCores.incrementAndGet());
      if (cores != rowCores.get()) {
        throw new Exception("Mismatch between autoscaling matrix row replicas (" + rowCores.get() + ") and number of cores (" + cores + ")");
      }
      row.forEachReplica(ri -> {
        Map<String, Object> perReplica = collReplicas.computeIfAbsent(ri.getCollection(), c -> new TreeMap<>())
            .computeIfAbsent(ri.getCore().substring(ri.getCollection().length() + 1), core -> new LinkedHashMap<>());
//            if (ri.getVariable(Variable.Type.CORE_IDX.tagName) != null) {
//              perReplica.put(Variable.Type.CORE_IDX.tagName, ri.getVariable(Variable.Type.CORE_IDX.tagName));
//            }
        if (ri.getVariable(Variable.Type.CORE_IDX.metricsAttribute) != null) {
          perReplica.put(Variable.Type.CORE_IDX.metricsAttribute, ri.getVariable(Variable.Type.CORE_IDX.metricsAttribute));
          if (ri.getVariable(Variable.Type.CORE_IDX.tagName) != null) {
            perReplica.put(Variable.Type.CORE_IDX.tagName, ri.getVariable(Variable.Type.CORE_IDX.tagName));
          } else {
            perReplica.put(Variable.Type.CORE_IDX.tagName,
                Variable.Type.CORE_IDX.convertVal(ri.getVariable(Variable.Type.CORE_IDX.metricsAttribute)));
          }
        }
        perReplica.put("coreNode", ri.getName());
        if (ri.isLeader || ri.getBool("leader", false)) {
          perReplica.put("leader", true);
          Double totalSize = (Double)collStats.computeIfAbsent(ri.getCollection(), c -> new HashMap<>())
              .computeIfAbsent("avgShardSize", size -> 0.0);
          Number riSize = (Number)ri.getVariable(Variable.Type.CORE_IDX.metricsAttribute);
          if (riSize != null) {
            totalSize += riSize.doubleValue();
            collStats.get(ri.getCollection()).put("avgShardSize", totalSize);
            Double max = (Double)collStats.get(ri.getCollection()).get("maxShardSize");
            if (max == null) max = 0.0;
            if (riSize.doubleValue() > max) {
              collStats.get(ri.getCollection()).put("maxShardSize", riSize.doubleValue());
            }
            Double min = (Double)collStats.get(ri.getCollection()).get("minShardSize");
            if (min == null) min = Double.MAX_VALUE;
            if (riSize.doubleValue() < min) {
              collStats.get(ri.getCollection()).put("minShardSize", riSize.doubleValue());
            }
          } else {
            throw new RuntimeException("ReplicaInfo without size information: " + ri);
          }
        }
        if (verbose) {
          nodeStat.put("replicas", collReplicas);
        }
      });
    }

    // calculate average per shard and convert the units
    for (Map<String, Number> perColl : collStats.values()) {
      Number avg = perColl.get("avgShardSize");
      if (avg != null) {
        avg = avg.doubleValue() / perColl.get("activeShards").doubleValue();
        perColl.put("avgShardSize", (Number)Variable.Type.CORE_IDX.convertVal(avg));
      }
      Number num = perColl.get("maxShardSize");
      if (num != null) {
        perColl.put("maxShardSize", (Number)Variable.Type.CORE_IDX.convertVal(num));
      }
      num = perColl.get("minShardSize");
      if (num != null) {
        perColl.put("minShardSize", (Number)Variable.Type.CORE_IDX.convertVal(num));
      }
    }
    Map<String, Object> stats = new LinkedHashMap<>();
    stats.put("coresPerNodes", coreStats);
    stats.put("sortedNodeStats", nodeStats);
    stats.put("collectionStats", collStats);
    return stats;
  }

  private static final Map<String, String> v2v1Mapping = new HashMap<>();
  static {
    for (CollectionApiMapping.Meta meta : CollectionApiMapping.Meta.values()) {
      if (meta.action != null) v2v1Mapping.put(meta.commandName, meta.action.toLower());
    }
  }

  /**
   * Convert a V2 {@link org.apache.solr.client.solrj.request.CollectionAdminRequest} to regular {@link org.apache.solr.common.params.SolrParams}
   * @param req request
   * @return payload converted to V1 params
   */
  public static ModifiableSolrParams v2AdminRequestToV1Params(V2Request req) {
    Map<String, Object> reqMap = new HashMap<>();
    ((V2Request)req).toMap(reqMap);
    String path = (String)reqMap.get("path");
    if (!path.startsWith("/c/") || path.length() < 4) {
      throw new UnsupportedOperationException("Unsupported V2 request path: " + reqMap);
    }
    Map<String, Object> cmd = (Map<String, Object>)reqMap.get("command");
    if (cmd.size() != 1) {
      throw new UnsupportedOperationException("Unsupported multi-command V2 request: " + reqMap);
    }
    String a = cmd.keySet().iterator().next();
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (req.getParams() != null) {
      params.add(req.getParams());
    }
    params.add(CollectionAdminParams.COLLECTION, path.substring(3));
    if (req.getParams() != null) {
      params.add(req.getParams());
    }
    Map<String, Object> reqParams = (Map<String, Object>)cmd.get(a);
    for (Map.Entry<String, Object> e : reqParams.entrySet()) {
      params.add(e.getKey(), e.getValue().toString());
    }
    // re-map from v2 to v1 action
    a = v2v1Mapping.get(a);
    if (a == null) {
      throw new UnsupportedOperationException("Unsupported V2 request: " + reqMap);
    }
    params.add(CoreAdminParams.ACTION, a);
    return params;
  }

  /**
   * Prepare collection and node / host names for redaction.
   * @param clusterState cluster state
   */
  public static RedactionUtils.RedactionContext getRedactionContext(ClusterState clusterState) {
    RedactionUtils.RedactionContext ctx = new RedactionUtils.RedactionContext();
    TreeSet<String> names = new TreeSet<>(clusterState.getLiveNodes());
    for (String nodeName : names) {
      String urlString = Utils.getBaseUrlForNodeName(nodeName, "http");
      try {
        URL u = new URL(urlString);
        // protocol format
        String hostPort = u.getHost() + ":" + u.getPort();
        ctx.addName(u.getHost() + ":" + u.getPort(), RedactionUtils.NODE_REDACTION_PREFIX);
        // node name format
        ctx.addEquivalentName(hostPort, u.getHost() + "_" + u.getPort() + "_", RedactionUtils.NODE_REDACTION_PREFIX);
      } catch (MalformedURLException e) {
        log.warn("Invalid URL for node name " + nodeName + ", replacing including protocol and path", e);
        ctx.addName(urlString, RedactionUtils.NODE_REDACTION_PREFIX);
        ctx.addEquivalentName(urlString, Utils.getBaseUrlForNodeName(nodeName, "https"), RedactionUtils.NODE_REDACTION_PREFIX);
      }
    }
    names.clear();
    names.addAll(clusterState.getCollectionStates().keySet());
    names.forEach(n -> ctx.addName(n, RedactionUtils.COLL_REDACTION_PREFIX));
    return ctx;
  }
}
