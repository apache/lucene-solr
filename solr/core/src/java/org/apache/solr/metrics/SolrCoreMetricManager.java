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
import java.lang.invoke.MethodHandles;

import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for collecting metrics from {@link SolrMetricProducer}'s
 * and exposing metrics to {@link SolrMetricReporter}'s.
 */
public class SolrCoreMetricManager implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrCore core;
  private final SolrMetricManager metricManager;
  private String registryName;

  /**
   * Constructs a metric manager.
   *
   * @param core the metric manager's core
   */
  public SolrCoreMetricManager(SolrCore core) {
    this.core = core;
    this.metricManager = core.getCoreDescriptor().getCoreContainer().getMetricManager();
    registryName = createRegistryName(core.getCoreDescriptor().getCollectionName(), core.getName());
  }

  /**
   * Load reporters configured globally and specific to {@link org.apache.solr.core.SolrInfoMBean.Group#core}
   * group or with a registry name specific to this core.
   */
  public void loadReporters() {
    NodeConfig nodeConfig = core.getCoreDescriptor().getCoreContainer().getConfig();
    PluginInfo[] pluginInfos = nodeConfig.getMetricReporterPlugins();
    metricManager.loadReporters(pluginInfos, core.getResourceLoader(), SolrInfoMBean.Group.core, registryName);
  }

  /**
   * Make sure that metrics already collected that correspond to the old core name
   * are carried over and will be used under the new core name.
   * This method also reloads reporters so that they use the new core name.
   */
  public void afterCoreSetName() {
    String oldRegistryName = registryName;
    registryName = createRegistryName(core.getCoreDescriptor().getCollectionName(), core.getName());
    if (oldRegistryName.equals(registryName)) {
      return;
    }
    // close old reporters
    metricManager.closeReporters(oldRegistryName);
    metricManager.moveMetrics(oldRegistryName, registryName, null);
    // old registry is no longer used - we have moved the metrics
    metricManager.removeRegistry(oldRegistryName);
    // load reporters again, using the new core name
    loadReporters();
  }

  /**
   * Registers a mapping of name/metric's with the manager's metric registry.
   *
   * @param scope     the scope of the metrics to be registered (e.g. `/admin/ping`)
   * @param producer  producer of metrics to be registered
   */
  public void registerMetricProducer(String scope, SolrMetricProducer producer) {
    if (scope == null || producer == null || producer.getCategory() == null) {
      throw new IllegalArgumentException("registerMetricProducer() called with illegal arguments: " +
          "scope = " + scope + ", producer = " + producer);
    }
    Collection<String> registered = producer.initializeMetrics(metricManager, getRegistryName(), scope);
    if (registered == null || registered.isEmpty()) {
      throw new IllegalArgumentException("registerMetricProducer() did not register any metrics " +
      "for scope = " + scope + ", producer = " + producer);
    }
  }

  /**
   * Closes reporters specific to this core.
   */
  @Override
  public void close() throws IOException {
    metricManager.closeReporters(getRegistryName());
  }

  public SolrCore getCore() {
    return core;
  }

  /**
   * Retrieves the metric registry name of the manager.
   *
   * In order to make it easier for reporting tools to aggregate metrics from
   * different cores that logically belong to a single collection we convert the
   * core name into a dot-separated hierarchy of: collection name, shard name (with optional split)
   * and replica name.
   *
   * <p>For example, when the core name looks like this but it's NOT a SolrCloud collection:
   * <code>my_collection_shard1_1_replica1</code> then this will be used as the registry name (plus
   * the required <code>solr.core</code> prefix). However,
   * if this is a SolrCloud collection <code>my_collection</code> then the registry name will become
   * <code>solr.core.my_collection.shard1_1.replica1</code>.</p>
   *
   *
   * @return the metric registry name of the manager.
   */
  public String getRegistryName() {
    return registryName;
  }

  /* package visibility for tests. */
  String createRegistryName(String collectionName, String coreName) {
    if (collectionName == null || (collectionName != null && !coreName.startsWith(collectionName + "_"))) {
      // single core, or unknown naming scheme
      return SolrMetricManager.getRegistryName(SolrInfoMBean.Group.core, coreName);
    }
    // split "collection1_shard1_1_replica1" into parts
    String str = coreName.substring(collectionName.length() + 1);
    String shard;
    String replica = null;
    int pos = str.lastIndexOf("_replica");
    if (pos == -1) { // ?? no _replicaN part ??
      shard = str;
    } else {
      shard = str.substring(0, pos);
      replica = str.substring(pos + 1);
    }
    return SolrMetricManager.getRegistryName(SolrInfoMBean.Group.core, collectionName, shard, replica);
  }
}
