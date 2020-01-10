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

import com.codahale.metrics.MetricRegistry;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;

/**
 * Helper class for managing registration of {@link SolrMetricProducer}'s
 * and {@link SolrMetricReporter}'s specific to a {@link SolrCore} instance.
 */
public class SolrCoreMetricManager implements Closeable {

  private final SolrCore core;
  private SolrMetricsContext solrMetricsContext;
  private SolrMetricManager metricManager;
  private String collectionName;
  private String shardName;
  private String replicaName;
  private String leaderRegistryName;
  private boolean cloudMode;

  /**
   * Constructs a metric manager.
   *
   * @param core the metric manager's core
   */
  public SolrCoreMetricManager(SolrCore core) {
    this.core = core;
    initCloudMode();
    metricManager = core.getCoreContainer().getMetricManager();
    String registryName = createRegistryName(cloudMode, collectionName, shardName, replicaName, core.getName());
    solrMetricsContext = new SolrMetricsContext(metricManager, registryName, core.getMetricTag());
    leaderRegistryName = createLeaderRegistryName(cloudMode, collectionName, shardName);
  }

  private void initCloudMode() {
    CloudDescriptor cd = core.getCoreDescriptor().getCloudDescriptor();
    if (cd != null) {
      cloudMode = true;
      collectionName = core.getCoreDescriptor().getCollectionName();
      shardName = cd.getShardId();
      //replicaName = cd.getCoreNodeName();
      String coreName = core.getName();
      replicaName = Utils.parseMetricsReplicaName(collectionName, coreName);
      if (replicaName == null) {
        replicaName = cd.getCoreNodeName();
      }
    }
  }

  /**
   * Load reporters configured globally and specific to {@link org.apache.solr.core.SolrInfoBean.Group#core}
   * group or with a registry name specific to this core.
   */
  public void loadReporters() {
    CoreContainer coreContainer = core.getCoreContainer();
    NodeConfig nodeConfig = coreContainer.getConfig();
    PluginInfo[] pluginInfos = nodeConfig.getMetricsConfig().getMetricReporters();
    metricManager.loadReporters(pluginInfos, core.getResourceLoader(), coreContainer, core, solrMetricsContext.getTag(),
        SolrInfoBean.Group.core, solrMetricsContext.getRegistryName());
    if (cloudMode) {
      metricManager.loadShardReporters(pluginInfos, core);
    }
  }

  /**
   * Make sure that metrics already collected that correspond to the old core name
   * are carried over and will be used under the new core name.
   * This method also reloads reporters so that they use the new core name.
   */
  public void afterCoreSetName() {
    String oldRegistryName = solrMetricsContext.getRegistryName();
    String oldLeaderRegistryName = leaderRegistryName;
    initCloudMode();
    String newRegistryName = createRegistryName(cloudMode, collectionName, shardName, replicaName, core.getName());
    leaderRegistryName = createLeaderRegistryName(cloudMode, collectionName, shardName);
    if (oldRegistryName.equals(newRegistryName)) {
      return;
    }
    // close old reporters
    metricManager.closeReporters(oldRegistryName, solrMetricsContext.getTag());
    if (oldLeaderRegistryName != null) {
      metricManager.closeReporters(oldLeaderRegistryName, solrMetricsContext.getTag());
    }
    solrMetricsContext = new SolrMetricsContext(metricManager, newRegistryName, solrMetricsContext.getTag());
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
    if (scope == null || producer == null) {
      throw new IllegalArgumentException("registerMetricProducer() called with illegal arguments: " +
          "scope = " + scope + ", producer = " + producer);
    }
    // use deprecated method for back-compat, remove in 9.0
    producer.initializeMetrics(solrMetricsContext, scope);
  }

  /**
   * Return the registry used by this SolrCore.
   */
  public MetricRegistry getRegistry() {
    if (solrMetricsContext != null) {
      return solrMetricsContext.getMetricRegistry();
    } else {
      return null;
    }
  }

  /**
   * Closes reporters specific to this core and unregisters gauges with this core's instance tag.
   */
  @Override
  public void close() throws IOException {
    metricManager.closeReporters(solrMetricsContext.getRegistryName(), solrMetricsContext.getTag());
    if (getLeaderRegistryName() != null) {
      metricManager.closeReporters(getLeaderRegistryName(), solrMetricsContext.getTag());
    }
    metricManager.unregisterGauges(solrMetricsContext.getRegistryName(), solrMetricsContext.getTag());
  }

  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  public SolrCore getCore() {
    return core;
  }

  /**
   * Metric registry name of the manager.
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
    return solrMetricsContext != null ? solrMetricsContext.getRegistryName() : null;
  }

  /**
   * Metric registry name for leader metrics. This is null if not in cloud mode.
   * @return metric registry name for leader metrics
   */
  public String getLeaderRegistryName() {
    return leaderRegistryName;
  }

  /**
   * Return a tag specific to this instance.
   */
  public String getTag() {
    return solrMetricsContext.getTag();
  }

  public static String createRegistryName(boolean cloud, String collectionName, String shardName, String replicaName, String coreName) {
    if (cloud) { // build registry name from logical names
      return SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, collectionName, shardName, replicaName);
    } else {
      return SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, coreName);
    }
  }

  /**
   * This method is used by {@link org.apache.solr.core.CoreContainer#rename(String, String)}.
   * @param aCore existing core with old name
   * @param coreName new name
   * @return new registry name
   */
  public static String createRegistryName(SolrCore aCore, String coreName) {
    CloudDescriptor cd = aCore.getCoreDescriptor().getCloudDescriptor();
    String replicaName = null;
    if (cd != null) {
      replicaName = Utils.parseMetricsReplicaName(cd.getCollectionName(), coreName);
    }
    return createRegistryName(
        cd != null,
        cd != null ? cd.getCollectionName() : null,
        cd != null ? cd.getShardId() : null,
        replicaName,
        coreName
        );
  }

  public static String createLeaderRegistryName(boolean cloud, String collectionName, String shardName) {
    if (cloud) {
      return SolrMetricManager.getRegistryName(SolrInfoBean.Group.collection, collectionName, shardName, "leader");
    } else {
      return null;
    }
  }
}
