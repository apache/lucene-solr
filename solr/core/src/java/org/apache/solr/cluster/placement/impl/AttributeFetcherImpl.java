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

package org.apache.solr.cluster.placement.impl;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.SolrClientNodeStateProvider;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.placement.CollectionMetrics;
import org.apache.solr.cluster.placement.ReplicaMetric;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.rule.ImplicitSnitch;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class AttributeFetcherImpl implements AttributeFetcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  boolean requestedNodeCoreCount;
  boolean requestedNodeFreeDisk;
  boolean requestedNodeTotalDisk;
  boolean requestedNodeHeapUsage;
  boolean requestedNodeSystemLoadAverage;
  Set<String> requestedNodeSystemSnitchTags = new HashSet<>();
  Set<String> requestedNodeMetricSnitchTags = new HashSet<>();
  Map<SolrCollection, Set<ReplicaMetric>> requestedCollectionMetrics = new HashMap<>();

  Set<Node> nodes = Collections.emptySet();

  private final SolrCloudManager cloudManager;

  AttributeFetcherImpl(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
  }

  @Override
  public AttributeFetcher requestNodeCoresCount() {
    requestedNodeCoreCount = true;
    return this;
  }

  @Override
  public AttributeFetcher requestNodeDiskType() {
    throw new UnsupportedOperationException("Not yet implemented...");
  }

  @Override
  public AttributeFetcher requestNodeFreeDisk() {
    requestedNodeFreeDisk = true;
    return this;
  }

  @Override
  public AttributeFetcher requestNodeTotalDisk() {
    requestedNodeTotalDisk = true;
    return this;
  }

  @Override
  public AttributeFetcher requestNodeHeapUsage() {
    requestedNodeHeapUsage = true;
    return this;
  }

  @Override
  public AttributeFetcher requestNodeSystemLoadAverage() {
    requestedNodeSystemLoadAverage = true;
    return this;
  }

  @Override
  public AttributeFetcher requestNodeSystemProperty(String name) {
    requestedNodeSystemSnitchTags.add(getSystemPropertySnitchTag(name));
    return this;
  }

  @Override
  public AttributeFetcher requestNodeEnvironmentVariable(String name) {
    requestedNodeSystemSnitchTags.add(getSystemEnvSnitchTag(name));
    return this;
  }

  @Override
  public AttributeFetcher requestNodeMetric(String metricName, NodeMetricRegistry registry) {
    requestedNodeMetricSnitchTags.add(getMetricSnitchTag(metricName, registry));
    return this;
  }

  @Override
  public AttributeFetcher requestNodeMetric(String metricKey) {
    requestedNodeMetricSnitchTags.add(getMetricKeySnitchTag(metricKey));
    return this;
  }

  @Override
  public AttributeFetcher requestCollectionMetrics(SolrCollection solrCollection, Set<ReplicaMetric> metricNames) {
    requestedCollectionMetrics.put(solrCollection, Set.copyOf(metricNames));
    return this;
  }

  @Override
  public AttributeFetcher fetchFrom(Set<Node> nodes) {
    this.nodes = nodes;
    return this;
  }

  private static final double GB = 1024 * 1024 * 1024;

  @Override
  public AttributeValues fetchAttributes() {
    // TODO Code here only supports node related attributes for now

    // Maps in which attribute values will be added
    Map<Node, Integer> nodeToCoreCount = new HashMap<>();
    Map<Node, DiskHardwareType> nodeToDiskType = new HashMap<>();
    Map<Node, Double> nodeToFreeDisk = new HashMap<>();
    Map<Node, Double> nodeToTotalDisk = new HashMap<>();
    Map<Node, Double> nodeToHeapUsage = new HashMap<>();
    Map<Node, Double> nodeToSystemLoadAverage = new HashMap<>();
    Map<String, Map<Node, String>> systemSnitchToNodeToValue = new HashMap<>();
    Map<String, Map<Node, Object>> metricSnitchToNodeToValue = new HashMap<>();
    Map<String, CollectionMetricsBuilder> collectionMetricsBuilders = new HashMap<>();
    Map<Node, Set<String>> nodeToReplicaInternalTags = new HashMap<>();
    Map<String, Set<ReplicaMetric>> requestedCollectionNamesMetrics = requestedCollectionMetrics.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getName(), e -> e.getValue()));

    // In order to match the returned values for the various snitches, we need to keep track of where each
    // received value goes. Given the target maps are of different types (the maps from Node to whatever defined
    // above) we instead pass a function taking two arguments, the node and the (non null) returned value,
    // that will cast the value into the appropriate type for the snitch tag and insert it into the appropriate map
    // with the node as the key.
    Map<String, BiConsumer<Node, Object>> allSnitchTagsToInsertion = new HashMap<>();
    if (requestedNodeCoreCount) {
      allSnitchTagsToInsertion.put(ImplicitSnitch.CORES, (node, value) -> nodeToCoreCount.put(node, ((Number) value).intValue()));
    }
    if (requestedNodeFreeDisk) {
      allSnitchTagsToInsertion.put(SolrClientNodeStateProvider.Variable.FREEDISK.tagName,
          // Convert from bytes to GB
          (node, value) -> nodeToFreeDisk.put(node, ((Number) value).doubleValue() / GB));
    }
    if (requestedNodeTotalDisk) {
      allSnitchTagsToInsertion.put(SolrClientNodeStateProvider.Variable.TOTALDISK.tagName,
          // Convert from bytes to GB
          (node, value) -> nodeToTotalDisk.put(node, ((Number) value).doubleValue() / GB));
    }
    if (requestedNodeHeapUsage) {
      allSnitchTagsToInsertion.put(ImplicitSnitch.HEAPUSAGE,
          (node, value) -> nodeToHeapUsage.put(node, ((Number) value).doubleValue()));
    }
    if (requestedNodeSystemLoadAverage) {
      allSnitchTagsToInsertion.put(ImplicitSnitch.SYSLOADAVG,
          (node, value) -> nodeToSystemLoadAverage.put(node, ((Number) value).doubleValue()));
    }
    for (String sysPropSnitch : requestedNodeSystemSnitchTags) {
      final Map<Node, String> sysPropMap = new HashMap<>();
      systemSnitchToNodeToValue.put(sysPropSnitch, sysPropMap);
      allSnitchTagsToInsertion.put(sysPropSnitch, (node, value) -> sysPropMap.put(node, (String) value));
    }
    for (String metricSnitch : requestedNodeMetricSnitchTags) {
      final Map<Node, Object> metricMap = new HashMap<>();
      metricSnitchToNodeToValue.put(metricSnitch, metricMap);
      allSnitchTagsToInsertion.put(metricSnitch, (node, value) -> metricMap.put(node, value));
    }
    requestedCollectionMetrics.forEach((collection, tags) -> {
      Set<String> collectionTags = tags.stream().map(tag -> tag.getInternalName()).collect(Collectors.toSet());
      collection.shards().forEach(shard ->
          shard.replicas().forEach(replica -> {
            Set<String> perNodeInternalTags = nodeToReplicaInternalTags
                .computeIfAbsent(replica.getNode(), n -> new HashSet<>());
            if (perNodeInternalTags.isEmpty()) {
              perNodeInternalTags.add(ReplicaMetric.INDEX_SIZE_GB.getInternalName());
            }
            perNodeInternalTags.addAll(collectionTags);
          }));
    });

    // Now that we know everything we need to fetch (and where to put it), just do it.
    for (Node node : nodes) {
      Map<String, Object> tagValues = cloudManager.getNodeStateProvider().getNodeValues(node.getName(), allSnitchTagsToInsertion.keySet());
      for (Map.Entry<String, Object> e : tagValues.entrySet()) {
        String tag = e.getKey();
        Object value = e.getValue(); // returned value from the node

        BiConsumer<Node, Object> inserter = allSnitchTagsToInsertion.get(tag);
        // If inserter is null it's a return of a tag that we didn't request
        if (inserter != null) {
          inserter.accept(node, value);
        } else {
          log.error("Received unsolicited snitch tag {} from node {}", tag, node);
        }
      }
    }

    for (Node node : nodeToReplicaInternalTags.keySet()) {
      Set<String> tags = nodeToReplicaInternalTags.get(node);
      Map<String, Map<String, List<Replica>>> infos = cloudManager.getNodeStateProvider().getReplicaInfo(node.getName(), tags);
      infos.entrySet().stream()
          .filter(entry -> requestedCollectionNamesMetrics.containsKey(entry.getKey()))
          .forEach(entry -> {
            CollectionMetricsBuilder collectionMetricsBuilder = collectionMetricsBuilders
                .computeIfAbsent(entry.getKey(), c -> new CollectionMetricsBuilder());
            entry.getValue().forEach((shardName, replicas) -> {
              CollectionMetricsBuilder.ShardMetricsBuilder shardMetricsBuilder =
                  collectionMetricsBuilder.getShardMetricsBuilders()
                  .computeIfAbsent(shardName, s -> new CollectionMetricsBuilder.ShardMetricsBuilder());
              replicas.forEach(replica -> {
                CollectionMetricsBuilder.ReplicaMetricsBuilder replicaMetricsBuilder =
                    shardMetricsBuilder.getReplicaMetricsBuilders()
                    .computeIfAbsent(replica.getName(), n -> new CollectionMetricsBuilder.ReplicaMetricsBuilder());
                replicaMetricsBuilder.setLeader(replica.isLeader());
                if (replica.isLeader()) {
                  shardMetricsBuilder.setLeaderMetrics(replicaMetricsBuilder);
                }
                Set<ReplicaMetric> requestedMetrics = requestedCollectionNamesMetrics.get(replica.getCollection());
                if (requestedMetrics == null) {
                  throw new RuntimeException("impossible error");
                }
                Double sizeGB = ReplicaMetric.INDEX_SIZE_GB.convert(replica.get(ReplicaMetric.INDEX_SIZE_GB.getInternalName()));
                if (sizeGB != null) {
                  replicaMetricsBuilder.setSizeGB(sizeGB);
                }
                requestedMetrics.forEach(metric -> {
                  Object value = metric.convert(replica.get(metric.getInternalName()));
                  if (value != null) {
                    replicaMetricsBuilder.addMetric(metric.getName(), value);
                  }
                });
              });
            });
          });
    }


    Map<String, CollectionMetrics> collectionMetrics = new HashMap<>();
    collectionMetricsBuilders.forEach((name, builder) -> collectionMetrics.put(name, builder.build()));

    return new AttributeValuesImpl(nodeToCoreCount,
        nodeToDiskType,
        nodeToFreeDisk,
        nodeToTotalDisk,
        nodeToHeapUsage,
        nodeToSystemLoadAverage,
        systemSnitchToNodeToValue,
        metricSnitchToNodeToValue, collectionMetrics);
  }

  private static SolrInfoBean.Group getGroupFromMetricRegistry(NodeMetricRegistry registry) {
    switch (registry) {
      case SOLR_JVM:
        return SolrInfoBean.Group.jvm;
      case SOLR_NODE:
        return SolrInfoBean.Group.node;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unsupported registry value " + registry);
    }
  }

  public static String getMetricSnitchTag(String metricName, NodeMetricRegistry registry) {
    return SolrClientNodeStateProvider.METRICS_PREFIX + SolrMetricManager.getRegistryName(getGroupFromMetricRegistry(registry), metricName);
  }

  public static String getMetricKeySnitchTag(String metricKey) {
    return SolrClientNodeStateProvider.METRICS_PREFIX + metricKey;
  }

  public static String getSystemPropertySnitchTag(String name) {
    return ImplicitSnitch.SYSPROP + name;
  }

  public static String getSystemEnvSnitchTag(String name) {
    return ImplicitSnitch.SYSENV + name;
  }
}
