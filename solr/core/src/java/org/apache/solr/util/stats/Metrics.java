package org.apache.solr.util.stats;

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


import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.solr.common.util.NamedList;

public enum Metrics {;   // empty enum, this is just a container for static methods

  public static final String REGISTRY_NAME_PREFIX = "solr.registry";
  public static final String DEFAULT_REGISTRY = MetricRegistry.name(REGISTRY_NAME_PREFIX, "default");

  public static String mkName(String name, String... names) { return MetricRegistry.name(name, names); }
  public static String mkName(Class<?> klass, String... names) { return MetricRegistry.name(klass, names); }
  /**
   * Use to get a Timer that you intend to use internally, and will never need to report metrics for.
   * @return
   */
  public static Timer anonymousTimer() {
    return new Timer();
  }

  /**
   * Use to get a named (shared, persistent) Timer from the default shared registry.
   * @param name A name that can be used to get this same Timer instance again
   * @return
   */
  public static Timer namedTimer(String name) {
    return namedTimer(name, DEFAULT_REGISTRY);
  }

  /**
   * Use to get a named (shared, persistent) Timer from a specified shared registry.
   * @param name A name that can be used to get this same Timer instance again
   * @param registry The name of the registry to get this timer from
   * @return
   */
  public static Timer namedTimer(String name, String registry) {
    return SharedMetricRegistries.getOrCreate(overridableRegistryName(registry)).timer(name);
  }

  /**
   * Gets a shared MetricRegistry by name, respecting overrides
   * @param name
   * @return
   */
  public static MetricRegistry registryFor(String name) {
    return SharedMetricRegistries.getOrCreate(overridableRegistryName(name));
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
