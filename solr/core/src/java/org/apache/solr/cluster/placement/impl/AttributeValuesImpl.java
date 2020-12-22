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

/**
 * Implementation of {@link AttributeValues} used by {@link AttributeFetcherImpl}.
 */
public class AttributeValuesImpl implements AttributeValues {
  // sysprop (or sysenv) name / node -> value
  final Map<String, Map<Node, String>> systemSnitchToNodeToValue;
  // metricName / node -> value
  final Map<NodeMetric<?>, Map<Node, Object>> metricSnitchToNodeToValue;
  // collection / shard / replica / metricName -> value
  final Map<String, CollectionMetrics> collectionMetrics;

  public AttributeValuesImpl(Map<String, Map<Node, String>> systemSnitchToNodeToValue,
                             Map<NodeMetric<?>, Map<Node, Object>> metricSnitchToNodeToValue,
                             Map<String, CollectionMetrics> collectionMetrics) {
    this.systemSnitchToNodeToValue = systemSnitchToNodeToValue;
    this.metricSnitchToNodeToValue = metricSnitchToNodeToValue;
    this.collectionMetrics = collectionMetrics;
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
  public Optional<CollectionMetrics> getCollectionMetrics(String collectionName) {
    return Optional.ofNullable(collectionMetrics.get(collectionName));
  }
}
