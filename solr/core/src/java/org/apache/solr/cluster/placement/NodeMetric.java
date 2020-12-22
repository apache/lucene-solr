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

import org.apache.solr.common.cloud.rule.ImplicitSnitch;

import java.util.Objects;
import java.util.function.Function;

/**
 * Node metric identifier, corresponding
 * to a node-level metric registry and the internal metric name.
 */
public class NodeMetric<T> extends MetricAttribute<T> {

  /** Total disk space in GB. */
  public static final NodeMetric<Double> TOTAL_DISK_GB = new NodeMetric<>("totalDisk",
      AttributeFetcher.NodeMetricRegistry.SOLR_NODE, "CONTAINER.fs.totalSpace", BYTES_TO_GB_CONVERTER);

  /** Free (usable) disk space in GB. */
  public static final NodeMetric<Double> FREE_DISK_GB = new NodeMetric<>("freeDisk",
      AttributeFetcher.NodeMetricRegistry.SOLR_NODE, "CONTAINER.fs.usableSpace", BYTES_TO_GB_CONVERTER);

  /** Number of all cores. */
  public static final NodeMetric<Integer> NUM_CORES = new NodeMetric<>(ImplicitSnitch.CORES);
  public static final NodeMetric<Double> HEAP_USAGE = new NodeMetric<>(ImplicitSnitch.HEAPUSAGE);

  /** System load average. */
  public static final NodeMetric<Double> SYSLOAD_AVG =
      new NodeMetric<>("sysLoadAvg", AttributeFetcher.NodeMetricRegistry.SOLR_JVM, "os.systemLoadAverage");

  /** Number of available processors. */
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
