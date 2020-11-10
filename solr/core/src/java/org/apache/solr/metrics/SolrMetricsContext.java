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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.solr.core.SolrInfoBean;

/**
 * This class represents a metrics context that ties together components with the same life-cycle
 * and provides convenient access to the metric registry.
 */
public class SolrMetricsContext {
  public final String registry;
  public final SolrMetricManager metricManager;
  public final String tag;

  public SolrMetricsContext(SolrMetricManager metricManager, String registry, String tag) {
    this.registry = registry;
    this.metricManager = metricManager;
    this.tag = tag;
  }

  /**
   * See {@link SolrMetricManager#nullNumber()}.
   */
  public Object nullNumber() {
    return metricManager.nullNumber();
  }

  /**
   * See {@link SolrMetricManager#notANumber()}.
   */
  public Object notANumber() {
    return metricManager.notANumber();
  }

  /**
   * See {@link SolrMetricManager#nullString()}.
   */
  public Object nullString() {
    return metricManager.nullString();
  }

  /**
   * See {@link SolrMetricManager#nullObject()}.
   */
  public Object nullObject() {
    return metricManager.nullObject();
  }

  /**
   * Metrics tag that represents objects with the same life-cycle.
   */
  public String getTag() {
    return tag;
  }

  /**
   * Unregister all {@link Gauge} metrics that use this context's tag.
   *
   * <p><b>NOTE: This method MUST be called at the end of a life-cycle (typically in <code>close()</code>)
   * of components that register gauge metrics with references to the current object's instance. Failure to
   * do so may result in hard-to-debug memory leaks.</b></p>
   */
  public void unregister() {
    metricManager.unregisterGauges(registry, tag);
  }

  /**
   * Get a context with the same registry name but a tag that represents a parent-child relationship.
   * Since it's a different tag than the parent's context it is assumed that the life-cycle of the parent
   * and child are different.
   * @param child child object that produces metrics with a different life-cycle than the parent.
   */
  public SolrMetricsContext getChildContext(Object child) {
    SolrMetricsContext childContext = new SolrMetricsContext(metricManager, registry, SolrMetricProducer.getUniqueMetricTag(child, tag));
    return childContext;
  }

  /**
   * Convenience method for {@link SolrMetricManager#meter(SolrInfoBean, String, String, String...)}.
   */
  public Meter meter(SolrInfoBean info, String metricName, String... metricPath) {
    return metricManager.meter(info, registry, metricName, metricPath);
  }

  /**
   * Convenience method for {@link SolrMetricManager#counter(SolrInfoBean, String, String, String...)}.
   */
  public Counter counter(SolrInfoBean info, String metricName, String... metricPath) {
    return metricManager.counter(info, registry, metricName, metricPath);

  }

  /**
   * Convenience method for {@link SolrMetricManager#registerGauge(SolrInfoBean, String, Gauge, String, boolean, String, String...)}.
   */
  public void gauge(SolrInfoBean info, Gauge<?> gauge, boolean force, String metricName, String... metricPath) {
    metricManager.registerGauge(info, registry, gauge, tag, force, metricName, metricPath);
  }

  /**
   * Convenience method for {@link SolrMetricManager#meter(SolrInfoBean, String, String, String...)}.
   */
  public Timer timer(SolrInfoBean info, String metricName, String... metricPath) {
    return metricManager.timer(info, registry, metricName, metricPath);
  }

  /**
   * Convenience method for {@link SolrMetricManager#histogram(SolrInfoBean, String, String, String...)}.
   */
  public Histogram histogram(SolrInfoBean info, String metricName, String... metricPath) {
    return metricManager.histogram(info, registry, metricName, metricPath);
  }

  /**
   * Get the MetricRegistry instance that is used for registering metrics in this context.
   */
  public MetricRegistry getMetricRegistry() {
    return metricManager.registry(registry);
  }
}
