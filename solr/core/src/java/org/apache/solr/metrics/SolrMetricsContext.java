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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.solr.util.stats.MetricUtils;

/**
 * This class represents a metrics context that ties together components with the same life-cycle
 * and provides convenient access to the metric registry.
 * <p>Additionally it's used for registering and reporting metrics specific to the components that
 * use the same instance of context.</p>
 */
public class SolrMetricsContext {
  private final String registryName;
  private final SolrMetricManager metricManager;
  private final String tag;
  private final Set<String> metricNames = ConcurrentHashMap.newKeySet();

  public SolrMetricsContext(SolrMetricManager metricManager, String registryName, String tag) {
    this.registryName = registryName;
    this.metricManager = metricManager;
    this.tag = tag;
  }

  /**
   * Metrics tag that represents objects with the same life-cycle.
   */
  public String getTag() {
    return tag;
  }

  /**
   * Return metric registry name used in this context.
   */
  public String getRegistryName() {
    return registryName;
  }

  /**
   * Return the instance of {@link SolrMetricManager} used in this context.
   */
  public SolrMetricManager getMetricManager() {
    return metricManager;
  }

  /**
   * Return a modifiable set of metric names that this component registers.
   */
  public Set<String> getMetricNames() {
    return metricNames;
  }

  /**
   * Unregister all {@link Gauge} metrics that use this context's tag.
   *
   * <p><b>NOTE: This method MUST be called at the end of a life-cycle (typically in <code>close()</code>)
   * of components that register gauge metrics with references to the current object's instance. Failure to
   * do so may result in hard-to-debug memory leaks.</b></p>
   */
  public void unregister() {
    metricManager.unregisterGauges(registryName, tag);
  }

  /**
   * Get a context with the same registry name but a tag that represents a parent-child relationship.
   * Since it's a different tag than the parent's context it is assumed that the life-cycle of the parent
   * and child are different.
   * @param child child object that produces metrics with a different life-cycle than the parent.
   */
  public SolrMetricsContext getChildContext(Object child) {
    SolrMetricsContext childContext = new SolrMetricsContext(metricManager, registryName, SolrMetricProducer.getUniqueMetricTag(child, tag));
    return childContext;
  }

  /** Register a metric name that this component reports. This method is called by various
   * metric registration methods in {@link org.apache.solr.metrics.SolrMetricManager} in order
   * to capture what metric names are reported from this component (which in turn is called
   * from {@link org.apache.solr.metrics.SolrMetricProducer#initializeMetrics(SolrMetricsContext, String)}).
   */
  public void registerMetricName(String name) {
    metricNames.add(name);
  }

  /**
   * Return a snapshot of metric values that this component reports.
   */
  public Map<String, Object> getMetricsSnapshot() {
    return MetricUtils.convertMetrics(getMetricRegistry(), metricNames);
  }

  /**
   * Convenience method for {@link SolrMetricManager#meter(SolrMetricsContext, String, String, String...)}.
   */
  public Meter meter(String metricName, String... metricPath) {
    return metricManager.meter(this, registryName, metricName, metricPath);
  }

  /**
   * Convenience method for {@link SolrMetricManager#counter(SolrMetricsContext, String, String, String...)}.
   */
  public Counter counter(String metricName, String... metricPath) {
    return metricManager.counter(this, registryName, metricName, metricPath);

  }

  /**
   * Convenience method for {@link SolrMetricManager#registerGauge(SolrMetricsContext, String, Gauge, String, boolean, String, String...)}.
   */
  public void gauge(Gauge<?> gauge, boolean force, String metricName, String... metricPath) {
    metricManager.registerGauge(this, registryName, gauge, tag, force, metricName, metricPath);
  }

  /**
   * Convenience method for {@link SolrMetricManager#meter(SolrMetricsContext, String, String, String...)}.
   */
  public Timer timer(String metricName, String... metricPath) {
    return metricManager.timer(this, registryName, metricName, metricPath);
  }

  /**
   * Convenience method for {@link SolrMetricManager#histogram(SolrMetricsContext, String, String, String...)}.
   */
  public Histogram histogram(String metricName, String... metricPath) {
    return metricManager.histogram(this, registryName, metricName, metricPath);
  }

  /**
   * Get the {@link MetricRegistry} instance that is used for registering metrics in this context.
   */
  public MetricRegistry getMetricRegistry() {
    return metricManager.registry(registryName);
  }
}
