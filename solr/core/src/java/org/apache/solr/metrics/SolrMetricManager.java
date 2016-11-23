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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for collecting metrics from {@link SolrMetricProducer}'s
 * and exposing metrics to {@link SolrMetricReporter}'s.
 */
public class SolrMetricManager implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrCore core;
  private final MetricRegistry registry;
  private final ConcurrentHashMap<String, SolrMetricInfo> metricInfos;
  private final ConcurrentHashMap<String, SolrMetricReporter> reporters;

  /**
   * Constructs a metric manager.
   *
   * @param core the metric manager's core
   */
  public SolrMetricManager(SolrCore core) {
    this.core = core;
    this.registry = new MetricRegistry();
    this.metricInfos = new ConcurrentHashMap<>();
    this.reporters = new ConcurrentHashMap<>();
  }

  /**
   * Registers a mapping of name/metric's with the manager's metric registry.
   * If a metric with the same metric name has already been registered, this method
   * replaces the original metric with the new metric.
   *
   * @param scope     the scope of the metrics to be registered (e.g. `/admin/ping`)
   * @param category  the category of the metrics to be registered (e.g. `QUERYHANDLERS`)
   * @param metrics   the mapping of name/metric's to be registered (e.g. `requestTimes`)
   */
  public void registerMetrics(String scope, SolrInfoMBean.Category category, Map<String, Metric> metrics) {
    if (scope == null || category == null || metrics == null) {
      throw new IllegalArgumentException("registerMetrics() called with illegal arguments: " +
          "scope = " + scope + ", category = " + category + ", metrics = " + metrics);
    }

    for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
      String name = entry.getKey();
      Metric metric = entry.getValue();

      SolrMetricInfo metricInfo = new SolrMetricInfo(name, scope, category);
      String metricName = metricInfo.getMetricName();
      metricInfos.put(metricName, metricInfo);

      try {
        registry.register(metricName, metric);
      } catch (IllegalArgumentException e) {
        log.warn("{} has already been registered; replacing existing metric.", metricName);
        synchronized (registry) {
          registry.remove(metricName);
          registry.register(metricName, metric);
        }
      } catch (Exception e) {
        log.error("registerMetrics() encountered error registering {}: {}.", metricName, e);
        registry.remove(metricName);
        metricInfos.remove(metricName);
      }
    }
  }

  /**
   * Loads a reporter and registers it to listen to the manager's metric registry.
   * If a reporter with the same name has already been registered, this method
   * closes the original reporter and registers the new reporter.
   *
   * @param pluginInfo the configuration of the reporter
   * @throws IOException if the reporter could not be loaded
   */
  public void loadReporter(PluginInfo pluginInfo) throws IOException {
    if (pluginInfo == null) {
      throw new IllegalArgumentException("loadReporter called with null plugin info.");
    }

    SolrResourceLoader resourceLoader = core.getResourceLoader();
    SolrMetricReporter reporter = resourceLoader.newInstance(
      pluginInfo.className,
      SolrMetricReporter.class,
      new String[0],
      new Class[] { SolrMetricManager.class },
      new Object[] { this }
    );

    try {
      reporter.init(pluginInfo);
    } catch (IllegalStateException e) {
      throw new IllegalArgumentException("loadReporter called with invalid plugin info = " + pluginInfo);
    }

    SolrMetricReporter existing = reporters.putIfAbsent(pluginInfo.name, reporter);
    if (existing != null) {
      log.warn("{} has already been register; replacing existing reporter = {}.", pluginInfo.name, existing);
      synchronized (reporters) {
        reporters.get(pluginInfo.name).close(); // Get the existing reporter again in case it was replaced
        reporters.put(pluginInfo.name, reporter); // Replace the existing reporter with the new one
      }
    }

    log.info("{} is successfully registered.", pluginInfo.name);
  }

  /**
   * Closes registered reporters and clears registered metrics.
   */
  @Override
  public void close() throws IOException {
    // Close reporters first to ensure no reporter is listening to the registry.
    Iterator<Map.Entry<String, SolrMetricReporter>> it = reporters.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, SolrMetricReporter> entry = it.next();
      entry.getValue().close();
      it.remove();
    }

    // Clear `registry` before `metricInfos` to prevent the scenario where a metric's
    // meta-data is removed but the metric itself is still contained in the registry.
    registry.removeMatching(MetricFilter.ALL);
    metricInfos.clear();
  }

  /**
   * Retrieves meta-data for a metric. If the metric does not exist, null is returned.
   *
   * @param metricName the metric name with which the metric was registered
   * @return the meta-data of the metric
   */
  public SolrMetricInfo getMetricInfo(String metricName) {
    return metricInfos.get(metricName);
  }

  /**
   * Retrieves the solr core of the manager.
   *
   * @return the solr core of the manager.
   */
  public SolrCore getCore() {
    return core;
  }

  /**
   * Retrieves the metric registry of the manager.
   *
   * @return the metric registry of the manager.
   */
  public MetricRegistry getRegistry() {
    return registry;
  }

  /**
   * Retrieves all registered reporters and their names.
   *
   * @return all registered reporters and their names
   */
  public Map<String, SolrMetricReporter> getReporters() {
    return Collections.unmodifiableMap(reporters);
  }

  /**
   * Retrieves all metric meta-data.
   *
   * @return all metric meta-data
   */
  public Map<String, SolrMetricInfo> getMetricInfos() {
    return Collections.unmodifiableMap(metricInfos);
  }
}
