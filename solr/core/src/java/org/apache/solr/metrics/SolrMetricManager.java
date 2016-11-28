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
package org.apache.solr.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.solr.common.util.NamedList;

/**
 *
 */
public class SolrMetricManager {

  public static final String REGISTRY_NAME_PREFIX = "solr";
  public static final String DEFAULT_REGISTRY = MetricRegistry.name(REGISTRY_NAME_PREFIX, "default");

  // don't create instances of this class
  private SolrMetricManager() { }


  public static MetricRegistry registryFor(String registry) {
    return SharedMetricRegistries.getOrCreate(overridableRegistryName(registry));
  }

  public static void clearRegistryFor(String registry) {
    SharedMetricRegistries.getOrCreate(overridableRegistryName(registry)).removeMatching(MetricFilter.ALL);
  }

  public static void clearMetric(String registry, String metricName, String... metricPath) {
    SharedMetricRegistries.getOrCreate(overridableRegistryName(registry)).
        remove(mkName(metricName, metricPath));
  }

  public static Meter getOrCreateMeter(String registry, String metricName, String... metricPath) {
    return SharedMetricRegistries.getOrCreate(overridableRegistryName(registry)).
        meter(mkName(metricName, metricPath));
  }

  public static Timer getOrCreateTimer(String registry, String metricName, String... metricPath) {
    return SharedMetricRegistries.getOrCreate(overridableRegistryName(registry)).
        timer(mkName(metricName, metricPath));
  }

  public static Counter getOrCreateCounter(String registry, String metricName, String... metricPath) {
    return SharedMetricRegistries.getOrCreate(overridableRegistryName(registry)).
        counter(mkName(metricName, metricPath));
  }

  public static Histogram getOrCreateHistogram(String registry, String metricName, String... metricPath) {
    return SharedMetricRegistries.getOrCreate(overridableRegistryName(registry)).
        histogram(mkName(metricName, metricPath));
  }

  /**
   * This method creates a hierarchical name with arbitrary levels of hierarchy
   * @param name the final segment of the name, must not be null or empty.
   * @param path optional path segments, starting from the top level. Empty or null
   *             segments will be skipped.
   * @return fully-qualified name with all valid hierarchy segments prepended to the name.
   */
  public static String mkName(String name, String... path) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("name must not be empty");
    }
    if (path == null || path.length == 0) {
      return name;
    } else {
      StringBuilder sb = new StringBuilder();
      for (String s : path) {
        if (s == null || s.isEmpty()) {
          continue;
        }
        if (sb.length() > 0) {
          sb.append('.');
        }
        sb.append(s);
      }
      if (sb.length() > 0) {
        sb.append('.');
      }
      sb.append(name);
      return sb.toString();
    }
  }

  /**
   * Allows named registries to be renamed using System properties.
   * This would be mostly be useful if you want to combine the metrics from a few registries for a single
   * reporter.
   * @param registry The name of the registry
   * @return A potentially overridden (via System properties) registry name
   */
  public static String overridableRegistryName(String registry) {
    String fqRegistry = enforcePrefix(registry);
    return enforcePrefix(System.getProperty(fqRegistry,fqRegistry));
  }

  private static String enforcePrefix(String name) {
    if (name.startsWith(REGISTRY_NAME_PREFIX))
      return name;
    else
      return MetricRegistry.name(REGISTRY_NAME_PREFIX, name);
  }

  /**
   * Adds metrics from a Timer to a NamedList, using well-known names.
   * @param lst The NamedList to add the metrics data to
   * @param timer The Timer to extract the metrics from
   */
  public static void addTimerMetrics(NamedList<Object> lst, Timer timer) {
    Snapshot snapshot = timer.getSnapshot();
    lst.add("avgRequestsPerMinute", timer.getMeanRate());
    lst.add("5minRateRequestsPerMinute", timer.getFiveMinuteRate());
    lst.add("15minRateRequestsPerMinute", timer.getFifteenMinuteRate());
    lst.add("avgTimePerRequest", nsToMs(snapshot.getMean()));
    lst.add("medianRequestTime", nsToMs(snapshot.getMedian()));
    lst.add("75thPctlRequestTime", nsToMs(snapshot.get75thPercentile()));
    lst.add("95thPctlRequestTime", nsToMs(snapshot.get95thPercentile()));
    lst.add("99thPctlRequestTime", nsToMs(snapshot.get99thPercentile()));
    lst.add("999thPctlRequestTime", nsToMs(snapshot.get999thPercentile()));
  }

  /**
   * Timers return measurements as a double representing nanos.
   * This converts that to a double representing millis.
   * @param ns Nanoseconds
   * @return Milliseconds
   */
  public static double nsToMs(double ns) {
    return ns / 1000000;  // Using TimeUnit involves casting to Long, so don't bother.
  }
}
