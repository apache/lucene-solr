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

import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.Node;

import java.util.Map;
import java.util.Optional;

public class AttributeValuesImpl implements AttributeValues {
  final Map<Node, Integer> nodeToCoreCount;
  final Map<Node, AttributeFetcher.DiskHardwareType> nodeToDiskType;
  final Map<Node, Long> nodeToFreeDisk;
  final Map<Node, Long> nodeToTotalDisk;
  final Map<Node, Double> nodeToHeapUsage;
  final Map<Node, Double> nodeToSystemLoadAverage;
  final Map<String, Map<Node, String>> syspropSnitchToNodeToValue;
  final Map<String, Map<Node, Double>> metricSnitchToNodeToValue;

  public AttributeValuesImpl(Map<Node, Integer> nodeToCoreCount,
                             Map<Node, AttributeFetcher.DiskHardwareType> nodeToDiskType,
                             Map<Node, Long> nodeToFreeDisk,
                             Map<Node, Long> nodeToTotalDisk,
                             Map<Node, Double> nodeToHeapUsage,
                             Map<Node, Double> nodeToSystemLoadAverage,
                             Map<String, Map<Node, String>> syspropSnitchToNodeToValue,
                             Map<String, Map<Node, Double>> metricSnitchToNodeToValue) {
    this.nodeToCoreCount = nodeToCoreCount;
    this.nodeToDiskType = nodeToDiskType;
    this.nodeToFreeDisk = nodeToFreeDisk;
    this.nodeToTotalDisk = nodeToTotalDisk;
    this.nodeToHeapUsage = nodeToHeapUsage;
    this.nodeToSystemLoadAverage = nodeToSystemLoadAverage;
    this.syspropSnitchToNodeToValue = syspropSnitchToNodeToValue;
    this.metricSnitchToNodeToValue = metricSnitchToNodeToValue;
  }

  @Override
  public Optional<Integer> getCoresCount(Node node) {
    return Optional.ofNullable(nodeToCoreCount.get(node));
  }

  @Override
  public Optional<AttributeFetcher.DiskHardwareType> getDiskType(Node node) {
    return Optional.ofNullable(nodeToDiskType.get(node));
  }

  @Override
  public Optional<Long> getFreeDisk(Node node) {
    return Optional.ofNullable(nodeToFreeDisk.get(node));
  }

  @Override
  public Optional<Long> getTotalDisk(Node node) {
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
    Map<Node, String> nodeToValue = syspropSnitchToNodeToValue.get(AttributeFetcherImpl.getSystemPropertySnitchTag(name));
    if (nodeToValue == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(nodeToValue.get(node));
  }

  @Override
  public Optional<String> getEnvironmentVariable(Node node, String name) {
    // TODO implement
    return Optional.empty();
  }

  @Override
  public Optional<Double> getMetric(Node node, String metricName, AttributeFetcher.NodeMetricRegistry registry) {
    Map<Node, Double> nodeToValue = metricSnitchToNodeToValue.get(AttributeFetcherImpl.getMetricSnitchTag(metricName, registry));
    if (nodeToValue == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(nodeToValue.get(node));
  }

  @Override
  public Optional<Double> getMetric(String scope, String metricName) {
    // TODO implement
    return Optional.empty();
  }
}
