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
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Slf4jReporter;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *   metrics group, eg. <code>solr.jvm</code></li>
 * </ul>
 */
public class SolrSlf4jReporter extends SolrMetricReporter {
  // we need this to pass validate-source-patterns
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private int period = 60;
  private String instancePrefix = null;
  private String logger = null;
  private String filterPrefix = null;
  private Slf4jReporter reporter;

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

  public void setFilter(String filter) {
    this.filterPrefix = filter;
  }

  public void setLogger(String logger) {
    this.logger = logger;
  }

  public void setPeriod(int period) {
    this.period = period;
  }

  @Override
  protected void validate() throws IllegalStateException {
    if (period < 1) {
      throw new IllegalStateException("Init argument 'period' is in time unit 'seconds' and must be at least 1.");
    }
    if (instancePrefix == null) {
      instancePrefix = registryName;
    } else {
      instancePrefix = instancePrefix + "." + registryName;
    }
    Slf4jReporter.Builder builder = Slf4jReporter
        .forRegistry(metricManager.registry(registryName))
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS);

    MetricFilter filter;
    if (filterPrefix != null) {
      filter = new SolrMetricManager.PrefixFilter(filterPrefix);
    } else {
      filter = MetricFilter.ALL;
    }
    builder = builder.filter(filter);
    if (logger == null || logger.isEmpty()) {
      // construct logger name from Group
      if (pluginInfo.attributes.containsKey("group")) {
        logger = SolrMetricManager.overridableRegistryName(pluginInfo.attributes.get("group"));
      } else if (pluginInfo.attributes.containsKey("registry")) {
        String reg = SolrMetricManager.overridableRegistryName(pluginInfo.attributes.get("registry"));
        String[] names = reg.split("\\.");
        if (names.length < 2) {
          logger = reg;
        } else {
          logger = names[0] + "." + names[1];
        }
      }
    }
    builder = builder.outputTo(LoggerFactory.getLogger(logger));
    reporter = builder.build();
    reporter.start(period, TimeUnit.SECONDS);
  }

  @Override
  public void close() throws IOException {
    if (reporter != null) {
      reporter.close();
    }
  }
}
