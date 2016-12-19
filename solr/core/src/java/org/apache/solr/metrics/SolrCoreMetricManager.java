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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for collecting metrics from {@link SolrMetricProducer}'s
 * and exposing metrics to {@link SolrMetricReporter}'s.
 */
public class SolrCoreMetricManager implements Closeable {

  public static final String REGISTRY_PREFIX = "core";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrCore core;
  private final String registryName;
  private final ConcurrentHashMap<String, SolrMetricReporter> reporters;

  /**
   * Constructs a metric manager.
   *
   * @param core the metric manager's core
   */
  public SolrCoreMetricManager(SolrCore core) {
    this.core = core;
    this.registryName = MetricRegistry.name(REGISTRY_PREFIX, core.getName());
    this.reporters = new ConcurrentHashMap<>();
  }

  /**
   * Registers a mapping of name/metric's with the manager's metric registry.
   * If a metric with the same metric name has already been registered, this method
   * replaces the original metric with the new metric.
   *
   * @param scope     the scope of the metrics to be registered (e.g. `/admin/ping`)
   * @param producer  producer of metrics to be registered
   */
  public void registerMetricProducer(String scope, SolrMetricProducer producer) {
    if (scope == null || producer == null || producer.getCategory() == null) {
      throw new IllegalArgumentException("registerMetricProducer() called with illegal arguments: " +
          "scope = " + scope + ", producer = " + producer);
    }
    Collection<String> registered = producer.initializeMetrics(registryName, scope);
    if (registered == null || registered.isEmpty()) {
      throw new IllegalArgumentException("registerMetricProducer() did not register any metrics " +
      "for scope = " + scope + ", producer = " + producer);
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
      new Class[] { String.class },
      new Object[] { registryName }
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
   * Closes registered reporters.
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
   * Retrieves the metric registry name of the manager.
   *
   * @return the metric registry name of the manager.
   */
  public String getRegistryName() {
    return registryName;
  }

  /**
   * Retrieves all registered reporters and their names.
   *
   * @return all registered reporters and their names
   */
  public Map<String, SolrMetricReporter> getReporters() {
    return Collections.unmodifiableMap(reporters);
  }

}
