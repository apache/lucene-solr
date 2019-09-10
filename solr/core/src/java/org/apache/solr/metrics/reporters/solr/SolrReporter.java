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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.admin.MetricsCollectorHandler;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link ScheduledReporter} that reports metrics from selected registries and sends
 * them periodically as update requests to a selected Solr collection and to a configured handler.
 */
public class SolrReporter extends ScheduledReporter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String REGISTRY_ID = "_registry_";
  public static final String REPORTER_ID = "_reporter_";
  public static final String GROUP_ID = "_group_";
  public static final String LABEL_ID = "_label_";


  /**
   * Specification of what registries and what metrics to send.
   */
  public static final class Report {
    public String groupPattern;
    public String labelPattern;
    public String registryPattern;
    public Set<String> metricFilters = new HashSet<>();

    /**
     * Create a report specification
     * @param groupPattern logical group for these metrics. This is used in {@link MetricsCollectorHandler}
     *                     to select the target registry for metrics to aggregate. Must not be null or empty.
     *                     It may contain back-references to capture groups from {@code registryPattern}
     * @param labelPattern name of this group of metrics. This is used in {@link MetricsCollectorHandler}
     *                     to prefix metric names. May be null or empty. It may contain back-references
     *                     to capture groups from {@code registryPattern}.
     * @param registryPattern pattern for selecting matching registries, see {@link SolrMetricManager#registryNames(String...)}
     * @param metricFilters patterns for selecting matching metrics, see {@link org.apache.solr.metrics.SolrMetricManager.RegexFilter}
     */
    public Report(String groupPattern, String labelPattern, String registryPattern, Collection<String> metricFilters) {
      this.groupPattern = groupPattern;
      this.labelPattern = labelPattern;
      this.registryPattern = registryPattern;
      if (metricFilters != null) {
        this.metricFilters.addAll(metricFilters);
      }
    }

    public static Report fromMap(Map<?, ?> map) {
      String groupPattern = (String)map.get("group");
      String labelPattern = (String)map.get("label");
      String registryPattern = (String)map.get("registry");
      Object oFilters = map.get("filter");
      Collection<String> metricFilters = Collections.emptyList();
      if (oFilters != null) {
        if (oFilters instanceof String) {
          metricFilters = Collections.singletonList((String)oFilters);
        } else if (oFilters instanceof Collection) {
          metricFilters = (Collection<String>)oFilters;
        } else {
          log.warn("Invalid report filters, ignoring: " + oFilters);
        }
      }
      if (groupPattern == null || registryPattern == null) {
        log.warn("Invalid report configuration, group and registry required!: " + map);
        return null;
      }
      return new Report(groupPattern, labelPattern, registryPattern, metricFilters);
    }
  }

  /**
   * Builder for the {@link SolrReporter} class.
   */
  public static class Builder {
    private final SolrMetricManager metricManager;
    private final List<Report> reports;
    private String reporterId;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private String handler;
    private boolean skipHistograms;
    private boolean skipAggregateValues;
    private boolean cloudClient;
    private boolean compact;
    private SolrParams params;

    /**
     * Create a builder for SolrReporter.
     * @param metricManager metric manager that is the source of metrics
     * @param reports report definitions
     * @return builder
     */
    public static Builder forReports(SolrMetricManager metricManager, List<Report> reports) {
      return new Builder(metricManager, reports);
    }

    private Builder(SolrMetricManager metricManager, List<Report> reports) {
      this.metricManager = metricManager;
      this.reports = reports;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.skipHistograms = false;
      this.skipAggregateValues = false;
      this.cloudClient = false;
      this.compact = true;
      this.params = null;
    }

    /**
     * Additional {@link SolrParams} to add to every request.
     * @param params additional params
     * @return {@code this}
     */
    public Builder withSolrParams(SolrParams params) {
      this.params = params;
      return this;
    }
    /**
     * If true then use {@link org.apache.solr.client.solrj.impl.CloudSolrClient} for communication.
     * Default is false.
     * @param cloudClient use CloudSolrClient when true, {@link org.apache.solr.client.solrj.impl.HttpSolrClient} otherwise.
     * @return {@code this}
     */
    public Builder cloudClient(boolean cloudClient) {
      this.cloudClient = cloudClient;
      return this;
    }

    /**
     * If true then use "compact" data representation.
     * @param compact compact representation.
     * @return {@code this}
     */
    public Builder setCompact(boolean compact) {
      this.compact = compact;
      return this;
    }

    /**
     * Histograms are difficult / impossible to aggregate, so it may not be
     * worth to report them.
     * @param skipHistograms when true then skip histograms from reports
     * @return {@code this}
     */
    public Builder skipHistograms(boolean skipHistograms) {
      this.skipHistograms = skipHistograms;
      return this;
    }

    /**
     * Individual values from {@link org.apache.solr.metrics.AggregateMetric} may not be worth to report.
     * @param skipAggregateValues when tru then skip reporting individual values from the metric
     * @return {@code this}
     */
    public Builder skipAggregateValues(boolean skipAggregateValues) {
      this.skipAggregateValues = skipAggregateValues;
      return this;
    }

    /**
     * Handler name to use at the remote end.
     *
     * @param handler handler name, eg. "/admin/metricsCollector"
     * @return {@code this}
     */
    public Builder withHandler(String handler) {
      this.handler = handler;
      return this;
    }

    /**
     * Use this id to identify metrics from this instance.
     *
     * @param reporterId reporter id
     * @return {@code this}
     */
    public Builder withReporterId(String reporterId) {
      this.reporterId = reporterId;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Build it.
     * @param client an instance of {@link HttpClient} to be used for making calls.
     * @param urlProvider function that returns the base URL of Solr instance to target. May return
     *                    null to indicate that reporting should be skipped. Note: this
     *                    function will be called every time just before report is sent.
     * @return configured instance of reporter
     */
    public SolrReporter build(HttpClient client, Supplier<String> urlProvider) {
      return new SolrReporter(client, urlProvider, metricManager, reports, handler, reporterId, rateUnit, durationUnit,
          params, skipHistograms, skipAggregateValues, cloudClient, compact);
    }

  }

  private String reporterId;
  private String handler;
  private Supplier<String> urlProvider;
  private SolrClientCache clientCache;
  private List<CompiledReport> compiledReports;
  private SolrMetricManager metricManager;
  private boolean skipHistograms;
  private boolean skipAggregateValues;
  private boolean cloudClient;
  private boolean compact;
  private ModifiableSolrParams params;
  private Map<String, Object> metadata;

  private static final class CompiledReport {
    String group;
    String label;
    Pattern registryPattern;
    MetricFilter filter;

    CompiledReport(Report report) throws PatternSyntaxException {
      this.group = report.groupPattern;
      this.label = report.labelPattern;
      this.registryPattern = Pattern.compile(report.registryPattern);
      this.filter = new SolrMetricManager.RegexFilter(report.metricFilters);
    }

    @Override
    public String toString() {
      return "CompiledReport{" +
          "group='" + group + '\'' +
          ", label='" + label + '\'' +
          ", registryPattern=" + registryPattern +
          ", filter=" + filter +
          '}';
    }
  }

  public SolrReporter(HttpClient httpClient, Supplier<String> urlProvider, SolrMetricManager metricManager,
                      List<Report> metrics, String handler,
                      String reporterId, TimeUnit rateUnit, TimeUnit durationUnit,
                      SolrParams params, boolean skipHistograms, boolean skipAggregateValues,
                      boolean cloudClient, boolean compact) {
    super(null, "solr-reporter", MetricFilter.ALL, rateUnit, durationUnit, null, true);
    this.metricManager = metricManager;
    this.urlProvider = urlProvider;
    this.reporterId = reporterId;
    if (handler == null) {
      handler = MetricsCollectorHandler.HANDLER_PATH;
    }
    this.handler = handler;
    this.clientCache = new SolrClientCache(httpClient);
    this.compiledReports = new ArrayList<>();
    metrics.forEach(report -> {
      MetricFilter filter = new SolrMetricManager.RegexFilter(report.metricFilters);
      try {
        CompiledReport cs = new CompiledReport(report);
        compiledReports.add(cs);
      } catch (PatternSyntaxException e) {
        log.warn("Skipping report with invalid registryPattern: " + report.registryPattern, e);
      }
    });
    this.skipHistograms = skipHistograms;
    this.skipAggregateValues = skipAggregateValues;
    this.cloudClient = cloudClient;
    this.compact = compact;
    this.params = new ModifiableSolrParams();
    this.params.set(REPORTER_ID, reporterId);
    // allow overrides to take precedence
    if (params != null) {
      this.params.add(params);
    }
    metadata = new HashMap<>();
    metadata.put(REPORTER_ID, reporterId);
  }

  @Override
  public void close() {
    clientCache.close();
    super.close();
  }

  @Override
  public void report() {
    String url = urlProvider.get();
    // if null then suppress reporting
    if (url == null) {
      return;
    }

    SolrClient solr;
    if (cloudClient) {
      solr = clientCache.getCloudSolrClient(url);
    } else {
      solr = clientCache.getHttpSolrClient(url);
    }
    UpdateRequest req = new UpdateRequest(handler);
    req.setParams(params);
    compiledReports.forEach(report -> {
      Set<String> registryNames = metricManager.registryNames(report.registryPattern);
      registryNames.forEach(registryName -> {
        String label = report.label;
        if (label != null && label.indexOf('$') != -1) {
          // label with back-references
          Matcher m = report.registryPattern.matcher(registryName);
          label = m.replaceFirst(label);
        }
        final String effectiveLabel = label;
        String group = report.group;
        if (group.indexOf('$') != -1) {
          // group with back-references
          Matcher m = report.registryPattern.matcher(registryName);
          group = m.replaceFirst(group);
        }
        final String effectiveGroup = group;
        MetricUtils.toSolrInputDocuments(metricManager.registry(registryName), Collections.singletonList(report.filter), MetricFilter.ALL,
            MetricUtils.PropertyFilter.ALL, skipHistograms, skipAggregateValues, compact, metadata, doc -> {
              doc.setField(REGISTRY_ID, registryName);
              doc.setField(GROUP_ID, effectiveGroup);
              if (effectiveLabel != null) {
                doc.setField(LABEL_ID, effectiveLabel);
              }
              req.add(doc);
            });
      });
    });

    // if no docs added then don't send a report
    if (req.getDocuments() == null || req.getDocuments().isEmpty()) {
      return;
    }
    try {
      //log.info("%%% sending to " + url + ": " + req.getParams());
      solr.request(req);
    } catch (Exception e) {
      log.debug("Error sending metric report", e.toString());
    }

  }

  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
    // no-op - we do all the work in report()
  }
}