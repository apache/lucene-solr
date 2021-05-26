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
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.http.client.HttpClient;
import org.apache.solr.cloud.LeaderElector;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.handler.admin.MetricsCollectorHandler;
import org.apache.solr.metrics.SolrCoreContainerReporter;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.ID;

/**
 * This reporter sends selected metrics from local registries to {@link Overseer}.
 * <p>The following configuration properties are supported:</p>
 * <ul>
 *   <li>handler - (optional str) handler path where reports are sent. Default is
 *   {@link MetricsCollectorHandler#HANDLER_PATH}.</li>
 *   <li>period - (optional int) how often reports are sent, in seconds. Default is 60. Setting this
 *   to 0 disables the reporter.</li>
 *   <li>report - (optional multiple lst) report configuration(s), see below.</li>
 * </ul>
 * Each report configuration consist of the following properties:
 * <ul>
 *   <li>registry - (required str) regex pattern matching source registries (see {@link SolrMetricManager#registryNames(String...)}),
 *   may contain capture groups.</li>
 *   <li>group - (required str) target registry name where metrics will be grouped. This can be a regex pattern that
 *   contains back-references to capture groups collected by <code>registry</code> pattern</li>
 *   <li>label - (optional str) optional prefix to prepend to metric names, may contain back-references to
 *   capture groups collected by <code>registry</code> pattern</li>
 *   <li>filter - (optional multiple str) regex expression(s) matching selected metrics to be reported.</li>
 * </ul>
 * NOTE: this reporter uses predefined "cluster" group, and it's always created even if explicit configuration
 * is missing. Default configuration uses report specifications from {@link #DEFAULT_REPORTS}.
 * <p>Example configuration:</p>
 * <pre>
 *       &lt;reporter name="test" group="cluster" class="solr.SolrClusterReporter"&gt;
 *         &lt;str name="handler"&gt;/admin/metrics/collector&lt;/str&gt;
 *         &lt;int name="period"&gt;11&lt;/int&gt;
 *         &lt;lst name="report"&gt;
 *           &lt;str name="group"&gt;overseer&lt;/str&gt;
 *           &lt;str name="label"&gt;jvm&lt;/str&gt;
 *           &lt;str name="registry"&gt;solr\.jvm&lt;/str&gt;
 *           &lt;str name="filter"&gt;memory\.total\..*&lt;/str&gt;
 *           &lt;str name="filter"&gt;memory\.heap\..*&lt;/str&gt;
 *           &lt;str name="filter"&gt;os\.SystemLoadAverage&lt;/str&gt;
 *           &lt;str name="filter"&gt;threads\.count&lt;/str&gt;
 *         &lt;/lst&gt;
 *         &lt;lst name="report"&gt;
 *           &lt;str name="group"&gt;overseer&lt;/str&gt;
 *           &lt;str name="label"&gt;leader.$1&lt;/str&gt;
 *           &lt;str name="registry"&gt;solr\.core\.(.*)\.leader&lt;/str&gt;
 *           &lt;str name="filter"&gt;UPDATE\./update/.*&lt;/str&gt;
 *         &lt;/lst&gt;
 *       &lt;/reporter&gt;
 * </pre>
 *
 * @deprecated this functionality will be removed in Solr 9.0
 */
@Deprecated
public class SolrClusterReporter extends SolrCoreContainerReporter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String CLUSTER_GROUP = SolrMetricManager.enforcePrefix(SolrInfoBean.Group.cluster.toString());

  public static final List<SolrReporter.Report> DEFAULT_REPORTS = new ArrayList<SolrReporter.Report>() {{
    add(new SolrReporter.Report(CLUSTER_GROUP, "jetty",
        SolrMetricManager.enforcePrefix(SolrInfoBean.Group.jetty.toString()),
        Collections.emptySet())); // all metrics
    add(new SolrReporter.Report(CLUSTER_GROUP, "jvm",
        SolrMetricManager.enforcePrefix(SolrInfoBean.Group.jvm.toString()),
        new HashSet<String>() {{
          add("memory\\.total\\..*");
          add("memory\\.heap\\..*");
          add("os\\.SystemLoadAverage");
          add("os\\.FreePhysicalMemorySize");
          add("os\\.FreeSwapSpaceSize");
          add("os\\.OpenFileDescriptorCount");
          add("threads\\.count");
        }}));
    add(new SolrReporter.Report(CLUSTER_GROUP, "node", SolrMetricManager.enforcePrefix(SolrInfoBean.Group.node.toString()),
        new HashSet<String>() {{
          add("CONTAINER\\.cores\\..*");
          add("CONTAINER\\.fs\\..*");
        }}));
    add(new SolrReporter.Report(CLUSTER_GROUP, "leader.$1", "solr\\.core\\.(.*)\\.leader",
        new HashSet<String>(){{
          add("UPDATE\\./update/.*");
          add("QUERY\\./select.*");
          add("INDEX\\..*");
          add("TLOG\\..*");
    }}));
  }};

  private String handler = MetricsCollectorHandler.HANDLER_PATH;
  private List<SolrReporter.Report> reports = new ArrayList<>();

  private SolrReporter reporter;

  /**
   * Create a reporter for metrics managed in a named registry.
   *
   * @param metricManager metric manager
   * @param registryName  this is ignored
   */
  public SolrClusterReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
  }

  public void setHandler(String handler) {
    this.handler = handler;
  }

  public void setReport(@SuppressWarnings({"rawtypes"})List<Map> reportConfig) {
    if (reportConfig == null || reportConfig.isEmpty()) {
      return;
    }
    reportConfig.forEach(map -> {
      SolrReporter.Report r = SolrReporter.Report.fromMap(map);
      if (r != null) {
        reports.add(r);
      }
    });
  }

  public void setReport(@SuppressWarnings({"rawtypes"})Map map) {
    if (map == null || map.isEmpty()) {
      return;
    }
    SolrReporter.Report r = SolrReporter.Report.fromMap(map);
    if (r != null) {
      reports.add(r);
    }
  }

  List<SolrReporter.Report> getReports() {
    return reports;
  }

  @Override
  protected void doInit() {
    if (reports.isEmpty()) { // set defaults
      reports = DEFAULT_REPORTS;
    }
  }

  @Override
  protected void validate() throws IllegalStateException {
    // (period < 1) means "don't start reporter" and so no (period > 0) validation needed
  }

  @Override
  public void close() throws IOException {
    if (reporter != null) {
      reporter.close();;
    }
  }

  @Override
  public void init(PluginInfo pluginInfo, CoreContainer cc) {
    super.init(pluginInfo, cc);
    if (reporter != null) {
      reporter.close();;
    }
    if (!enabled) {
      log.info("Reporter disabled for registry {}", registryName);
      return;
    }
    // start reporter only in cloud mode
    if (!cc.isZooKeeperAware()) {
      log.warn("Not ZK-aware, not starting...");
      return;
    }
    if (period < 1) { // don't start it
      log.info("Turning off node reporter, period={}", period);
      return;
    }
    HttpClient httpClient = cc.getUpdateShardHandler().getDefaultHttpClient();
    ZkController zk = cc.getZkController();
    String reporterId = zk.getNodeName();
    reporter = SolrReporter.Builder.forReports(metricManager, reports)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .withHandler(handler)
        .withReporterId(reporterId)
        .setCompact(true)
        .cloudClient(false) // we want to send reports specifically to a selected leader instance
        .skipAggregateValues(true) // we don't want to transport details of aggregates
        .skipHistograms(true) // we don't want to transport histograms
        .build(httpClient, new OverseerUrlSupplier(zk));

    reporter.start(period, TimeUnit.SECONDS);
  }

  // TODO: fix this when there is an elegant way to retrieve URL of a node that runs Overseer leader.
  // package visibility for unit tests
  static class OverseerUrlSupplier implements Supplier<String> {
    private static final long DEFAULT_INTERVAL = 30000000; // 30s
    private ZkController zk;
    private String lastKnownUrl = null;
    private long lastCheckTime = 0;
    private long interval = DEFAULT_INTERVAL;

    OverseerUrlSupplier(ZkController zk) {
      this.zk = zk;
    }

    @Override
    public String get() {
      if (zk == null) {
        return null;
      }
      // primitive caching for lastKnownUrl
      long now = System.nanoTime();
      if (lastKnownUrl != null && (now - lastCheckTime) < interval) {
        return lastKnownUrl;
      }
      if (!zk.isConnected()) {
        return lastKnownUrl;
      }
      lastCheckTime = now;
      SolrZkClient zkClient = zk.getZkClient();
      ZkNodeProps props;
      try {
        props = ZkNodeProps.load(zkClient.getData(
            Overseer.OVERSEER_ELECT + "/leader", null, null, true));
      } catch (KeeperException e) {
        log.warn("Could not obtain overseer's address, skipping.", e);
        return lastKnownUrl;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return lastKnownUrl;
      }
      if (props == null) {
        return lastKnownUrl;
      }
      String oid = props.getStr(ID);
      if (oid == null) {
        return lastKnownUrl;
      }
      String nodeName = null;
      try {
        nodeName = LeaderElector.getNodeName(oid);
      } catch (Exception e) {
        log.warn("Unknown format of leader id, skipping: {}", oid, e);
        return lastKnownUrl;
      }
      // convert nodeName back to URL
      String url = zk.getZkStateReader().getBaseUrlForNodeName(nodeName);
      // check that it's parseable
      try {
        new java.net.URL(url);
      } catch (MalformedURLException mue) {
        log.warn("Malformed Overseer's leader URL: url", mue);
        return lastKnownUrl;
      }
      lastKnownUrl = url;
      return url;
    }
  }

}
