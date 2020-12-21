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

import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.placement.CollectionMetrics;
import org.apache.solr.cluster.placement.NodeMetric;

import java.util.Map;
import java.util.Optional;

public class AttributeValuesImpl implements AttributeValues {
  final Map<Node, Integer> nodeToCoreCount;
  final Map<Node, Double> nodeToFreeDisk;
  final Map<Node, Double> nodeToTotalDisk;
  final Map<Node, Double> nodeToHeapUsage;
  final Map<Node, Double> nodeToSystemLoadAverage;
  // sysprop (or sysenv) name / node -> value
  final Map<String, Map<Node, String>> systemSnitchToNodeToValue;
  // metricName / node -> value
  final Map<NodeMetric<?>, Map<Node, Object>> metricSnitchToNodeToValue;
  // collection / shard / replica / metricName -> value
  final Map<String, CollectionMetrics> collectionMetrics;

  public AttributeValuesImpl(Map<Node, Integer> nodeToCoreCount,
                             Map<Node, Double> nodeToFreeDisk,
                             Map<Node, Double> nodeToTotalDisk,
                             Map<Node, Double> nodeToHeapUsage,
                             Map<Node, Double> nodeToSystemLoadAverage,
                             Map<String, Map<Node, String>> systemSnitchToNodeToValue,
                             Map<NodeMetric<?>, Map<Node, Object>> metricSnitchToNodeToValue,
                             Map<String, CollectionMetrics> collectionMetrics) {
    this.nodeToCoreCount = nodeToCoreCount;
    this.nodeToFreeDisk = nodeToFreeDisk;
    this.nodeToTotalDisk = nodeToTotalDisk;
    this.nodeToHeapUsage = nodeToHeapUsage;
    this.nodeToSystemLoadAverage = nodeToSystemLoadAverage;
    this.systemSnitchToNodeToValue = systemSnitchToNodeToValue;
    this.metricSnitchToNodeToValue = metricSnitchToNodeToValue;
    this.collectionMetrics = collectionMetrics;
  }

  @Override
  public Optional<Integer> getCoresCount(Node node) {
    return Optional.ofNullable(nodeToCoreCount.get(node));
  }

  @Override
  public Optional<Double> getFreeDisk(Node node) {
    return Optional.ofNullable(nodeToFreeDisk.get(node));
  }

  @Override
  public Optional<Double> getTotalDisk(Node node) {
    return Optional.ofNullable(nodeToTotalDisk.get(node));
  }

  @Override
  public Optional<Double> getHeapUsage(Node node) {
    return Optional.ofNullable(nodeToHeapUsage.get(node));
  }

  @Override
  public Optional<Double> getSystemLoadAverage(Node node) {
    return Optional.ofNullable(nodeToSystemLoadAverage.get(node));
  }

  @Override
  public Optional<String> getSystemProperty(Node node, String name) {
    Map<Node, String> nodeToValue = systemSnitchToNodeToValue.get(AttributeFetcherImpl.getSystemPropertySnitchTag(name));
    if (nodeToValue == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(nodeToValue.get(node));
  }

  @Override
  public Optional<String> getEnvironmentVariable(Node node, String name) {
    Map<Node, String> nodeToValue = systemSnitchToNodeToValue.get(AttributeFetcherImpl.getSystemEnvSnitchTag(name));
    if (nodeToValue == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(nodeToValue.get(node));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Optional<T> getNodeMetric(Node node, NodeMetric<T> metric) {
    Map<Node, Object> nodeToValue = metricSnitchToNodeToValue.get(metric);
    if (nodeToValue == null) {
      return Optional.empty();
    }
    return Optional.ofNullable((T) nodeToValue.get(node));
  }

  @Override
  public Optional<Object> getNodeMetric(Node node, String metricKey) {
    Map<Node, Object> nodeToValue = metricSnitchToNodeToValue.get(new NodeMetric<>(metricKey));
    if (nodeToValue == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(nodeToValue.get(node));
  }

  @Override
  public Optional<CollectionMetrics> getCollectionMetrics(String collectionName) {
    return Optional.ofNullable(collectionMetrics.get(collectionName));
  }
}
