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
package org.apache.solr.metrics.reporters.solr;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.MetricsCollectorHandler;
import org.apache.solr.metrics.SolrCoreReporter;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricFilter;

/**
 * This class reports selected metrics from replicas to shard leader.
 * <p>The following configuration properties are supported:</p>
 * <ul>
 *   <li>handler - (optional str) handler path where reports are sent. Default is
 *   {@link MetricsCollectorHandler#HANDLER_PATH}.</li>
 *   <li>period - (optional int) how often reports are sent, in seconds. Default is 60. Setting this
 *   to 0 disables the reporter.</li>
 *   <li>filter - (optional multiple str) regex expression(s) matching selected metrics to be reported.</li>
 * </ul>
 * NOTE: this reporter uses predefined "shard" group, and it's always created even if explicit configuration
 * is missing. Default configuration uses filters defined in {@link #DEFAULT_FILTERS}.
 * <p>Example configuration:</p>
 * <pre>
 *    &lt;reporter name="test" group="shard" class="solr.SolrShardReporter"&gt;
 *      &lt;int name="period"&gt;11&lt;/int&gt;
 *      &lt;str name="filter"&gt;UPDATE\./update/.*requests&lt;/str&gt;
 *      &lt;str name="filter"&gt;QUERY\./select.*requests&lt;/str&gt;
 *    &lt;/reporter&gt;
 * </pre>
 *
 * @deprecated this functionality will be removed in Solr 9.0
 */
@Deprecated
public class SolrShardReporter extends SolrCoreReporter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final List<String> DEFAULT_FILTERS = new ArrayList(){{
    add("TLOG.*");
    add("CORE\\.fs.*");
    add("REPLICATION.*");
    add("INDEX\\.flush.*");
    add("INDEX\\.merge\\.major.*");
    add("UPDATE\\./update.*requests");
    add("QUERY\\./select.*requests");
  }};

  private String handler = MetricsCollectorHandler.HANDLER_PATH;

  private SolrReporter reporter;

  /**
   * Create a reporter for metrics managed in a named registry.
   *
   * @param metricManager metric manager
   * @param registryName  registry to use, one of registries managed by
   *                      {@link SolrMetricManager}
   */
  public SolrShardReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
  }

  public void setHandler(String handler) {
    this.handler = handler;
  }

  @Override
  protected void doInit() {
    if (filters.isEmpty()) {
      filters = DEFAULT_FILTERS;
    }
    // start in setCore(SolrCore) when core is available
  }

  @Override
  protected MetricFilter newMetricFilter() {
    // unsupported here since setCore(SolrCore) directly uses the this.filters
    throw new UnsupportedOperationException(getClass().getCanonicalName()+".newMetricFilter() is not supported");
  }

  @Override
  protected void validate() throws IllegalStateException {
    // (period < 1) means "don't start reporter" and so no (period > 0) validation needed
  }

  @Override
  public void close() throws IOException {
    if (reporter != null) {
      reporter.close();
    }
  }

  @Override
  public void init(PluginInfo pluginInfo, SolrCore core) {
    super.init(pluginInfo, core);
    if (reporter != null) {
      reporter.close();
    }
    if (!enabled) {
      log.info("Reporter disabled for registry {}", registryName);
      return;
    }
    if (core.getCoreDescriptor().getCloudDescriptor() == null) {
      // not a cloud core
      log.warn("Not initializing shard reporter for non-cloud core {}", core.getName());
      return;
    }
    if (period < 1) { // don't start it
      log.warn("period={}, not starting shard reporter ", period);
      return;
    }
    // our id is coreNodeName
    String id = core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
    // target registry is the leaderRegistryName
    String groupId = core.getCoreMetricManager().getLeaderRegistryName();
    if (groupId == null) {
      log.warn("No leaderRegistryName for core {}, not starting the reporter...", core);
      return;
    }
    SolrReporter.Report spec = new SolrReporter.Report(groupId, null, registryName, filters);
    reporter = SolrReporter.Builder.forReports(metricManager, Collections.singletonList(spec))
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .withHandler(handler)
        .withReporterId(id)
        .setCompact(true)
        .cloudClient(false) // we want to send reports specifically to a selected leader instance
        .skipAggregateValues(true) // we don't want to transport details of aggregates
        .skipHistograms(true) // we don't want to transport histograms
        .build(core.getCoreContainer().getSolrClientCache(), new LeaderUrlSupplier(core));

    reporter.start(period, TimeUnit.SECONDS);
  }

  private static class LeaderUrlSupplier implements Supplier<String> {
    private SolrCore core;

    LeaderUrlSupplier(SolrCore core) {
      this.core = core;
    }

    @Override
    public String get() {
      CloudDescriptor cd = core.getCoreDescriptor().getCloudDescriptor();
      if (cd == null) {
        return null;
      }
      ClusterState state = core.getCoreContainer().getZkController().getClusterState();
      DocCollection collection = state.getCollection(core.getCoreDescriptor().getCollectionName());
      Replica replica = collection.getLeader(core.getCoreDescriptor().getCloudDescriptor().getShardId());
      if (replica == null) {
        log.warn("No leader for {}/{}", collection.getName(), core.getCoreDescriptor().getCloudDescriptor().getShardId());
        return null;
      }
      String baseUrl = replica.getStr("base_url");
      if (baseUrl == null) {
        log.warn("No base_url for replica {}", replica);
      }
      return baseUrl;
    }
  }
}
