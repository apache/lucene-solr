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
package org.apache.solr.metrics.reporters;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import org.apache.solr.metrics.FilteringSolrMetricReporter;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Metrics reporter that wraps {@link com.codahale.metrics.Slf4jReporter}.
 * The following init arguments are supported:
 * <ul>
 *   <li><code>period</code>: (optional, int) number of seconds between reports, default is 60,</li>
 *   <li><code>prefix</code>: (optional, str) prefix for metric names, in addition to
 *   registry name. Default is none, ie. just registry name.</li>
 *   <li><code>filter</code>: (optional, str) if not empty only metric names that start
 *   with this value will be reported, default is all metrics from a registry,</li>
 *   <li><code>logger</code>: (optional, str) logger name to use. Default is the
 *   metrics group, eg. <code>solr.jvm</code>, <code>solr.core</code>, etc</li>
 * </ul>
 */
public class SolrSlf4jReporter extends FilteringSolrMetricReporter {

  @SuppressWarnings("unused") // we need this to pass validate-source-patterns
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String instancePrefix = null;
  private String logger = null;
  private Map<String, String> mdcContext;
  private Slf4jReporterWrapper reporter;
  private boolean active;

  // this wrapper allows us to set MDC context - unfortunately it's not possible to
  // simply override {@link Slf4jReporter#report()} because its constructor is private
  private class Slf4jReporterWrapper extends ScheduledReporter {
    final Slf4jReporter delegate;
    final Map<String, String> mdcContext;

    Slf4jReporterWrapper(String logger, Map<String, String> mdcContext, Slf4jReporter delegate, TimeUnit rateUnit, TimeUnit durationUnit) {
      super(metricManager.registry(registryName), logger, null, rateUnit, durationUnit);
      this.delegate = delegate;
      this.mdcContext = mdcContext;
    }

    @Override
    public void report() {
      // set up MDC
      MDC.setContextMap(mdcContext);
      try {
        delegate.report();
      } finally {
        // clear MDC
        MDC.clear();
      }
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
      throw new UnsupportedOperationException("this method should never be called here!");
    }

    @Override
    public void close() {
      super.close();
      delegate.close();
    }
  }
  /**
   * Create a SLF4J reporter for metrics managed in a named registry.
   *
   * @param metricManager metric manager instance that manages the selected registry
   * @param registryName  registry to use, one of registries managed by
   *                      {@link SolrMetricManager}
   */
  public SolrSlf4jReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
  }

  public void setPrefix(String prefix) {
    this.instancePrefix = prefix;
  }

  public void setLogger(String logger) {
    this.logger = logger;
  }

  @Override
  protected void doInit() {
    mdcContext = MDC.getCopyOfContextMap();
    mdcContext.put("registry", "m:" + registryName);
    Slf4jReporter.Builder builder = Slf4jReporter
        .forRegistry(metricManager.registry(registryName))
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS);

    final MetricFilter filter = newMetricFilter();
    builder = builder.filter(filter);
    if (instancePrefix != null) {
      builder = builder.prefixedWith(instancePrefix);
    }
    if (logger == null || logger.isEmpty()) {
      // construct logger name from Group
      if (pluginInfo.attributes.containsKey("group")) {
        logger = SolrMetricManager.enforcePrefix(pluginInfo.attributes.get("group"));
      } else if (pluginInfo.attributes.containsKey("registry")) {
        String reg = SolrMetricManager.enforcePrefix(pluginInfo.attributes.get("registry"));
        String[] names = reg.split("\\.");
        if (names.length < 2) {
          logger = reg;
        } else {
          logger = names[0] + "." + names[1];
        }
      }
    }
    builder = builder.outputTo(LoggerFactory.getLogger(logger));
    // build BUT don't start - scheduled execution is handled by the wrapper
    Slf4jReporter delegate = builder.build();
    reporter = new Slf4jReporterWrapper(logger, mdcContext, delegate, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    reporter.start(period, TimeUnit.SECONDS);
    active = true;
  }

  @Override
  protected void validate() throws IllegalStateException {
    if (period < 1) {
      throw new IllegalStateException("Init argument 'period' is in time unit 'seconds' and must be at least 1.");
    }
  }

  @Override
  public void close() throws IOException {
    if (reporter != null) {
      reporter.close();
    }
    active = false;
  }

  // for unit tests
  boolean isActive() {
    return active;
  }
}
