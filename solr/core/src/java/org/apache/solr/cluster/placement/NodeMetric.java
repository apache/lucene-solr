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

package org.apache.solr.cluster.placement;

import java.util.Objects;
import java.util.function.Function;

/**
 * Node metric wrapper that defines a short symbolic name of the metric, the corresponding
 * metric registry and the internal metric name, as well as the desired format/unit conversion.
 */
public class NodeMetric<T> extends MetricAttribute<T> {

  /** System load average metric. */
  public static final NodeMetric<Double> SYSLOAD_AVG =
      new NodeMetric<>("sysLoadAvg", AttributeFetcher.NodeMetricRegistry.SOLR_JVM, "os.systemLoadAverage");

  /** Available processors metric. */
  public static final NodeMetric<Integer> AVAILABLE_PROCESSORS =
      new NodeMetric<>("availableProcessors", AttributeFetcher.NodeMetricRegistry.SOLR_JVM, "os.availableProcessors");

  private final AttributeFetcher.NodeMetricRegistry registry;

  public NodeMetric(String name, AttributeFetcher.NodeMetricRegistry registry, String internalName) {
    this(name, registry, internalName, null);
  }

  public NodeMetric(String name, AttributeFetcher.NodeMetricRegistry registry, String internalName, Function<Object, T> converter) {
    super(name, internalName, converter);
    Objects.requireNonNull(registry);
    this.registry = registry;
  }

  public NodeMetric(String key) {
    this(key, null);
  }

  public NodeMetric(String key, Function<Object, T> converter) {
    super(key, key, converter);
    this.registry = null;
  }

  public AttributeFetcher.NodeMetricRegistry getRegistry() {
    return registry;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    NodeMetric<?> that = (NodeMetric<?>) o;
    return registry == that.registry;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), registry);
  }

  @Override
  public String toString() {
    if (registry != null) {
      return "NodeMetric{" +
          "name='" + name + '\'' +
          ", internalName='" + internalName + '\'' +
          ", converter=" + converter +
          ", registry=" + registry +
          '}';
    } else {
      return "NodeMetric{key=" + internalName + "}";
    }
  }
}
