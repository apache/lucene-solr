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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.GraphiteSender;
import com.codahale.metrics.graphite.PickledGraphite;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics reporter that wraps {@link com.codahale.metrics.graphite.GraphiteReporter}.
 */
public class SolrGraphiteReporter extends SolrMetricReporter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String host = null;
  private int port = -1;
  private int period = 60;
  private boolean pickled = false;
  private String instancePrefix = null;
  private List<String> filters = new ArrayList<>();
  private GraphiteReporter reporter = null;

  private static final ReporterClientCache<GraphiteSender> serviceRegistry = new ReporterClientCache<>();

  /**
   * Create a Graphite reporter for metrics managed in a named registry.
   *
   * @param metricManager metric manager instance that manages the selected registry
   * @param registryName  registry to use, one of registries managed by
   *                      {@link SolrMetricManager}
   */
  public SolrGraphiteReporter(SolrMetricManager metricManager, String registryName) {
    super(metricManager, registryName);
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setPrefix(String prefix) {
    this.instancePrefix = prefix;
  }

  /**
   * Report only metrics with names matching any of the prefix filters.
   * @param filters list of 0 or more prefixes. If the list is empty then
   *                all names will match.
   */
  public void setFilter(List<String> filters) {
    if (filters == null || filters.isEmpty()) {
      return;
    }
    this.filters.addAll(filters);
  }

  public void setFilter(String filter) {
    if (filter != null && !filter.isEmpty()) {
      this.filters.add(filter);
    }
  }


  public void setPickled(boolean pickled) {
    this.pickled = pickled;
  }

  public void setPeriod(int period) {
    this.period = period;
  }

  @Override
  protected void validate() throws IllegalStateException {
    if (!enabled) {
      log.info("Reporter disabled for registry " + registryName);
      return;
    }
    if (host == null) {
      throw new IllegalStateException("Init argument 'host' must be set to a valid Graphite server name.");
    }
    if (port == -1) {
      throw new IllegalStateException("Init argument 'port' must be set to a valid Graphite server port.");
    }
    if (reporter != null) {
      throw new IllegalStateException("Already started once?");
    }
    if (period < 1) {
      throw new IllegalStateException("Init argument 'period' is in time unit 'seconds' and must be at least 1.");
    }
    GraphiteSender graphite;
    String id = host + ":" + port + ":" + pickled;
    graphite = serviceRegistry.getOrCreate(id, () -> {
      if (pickled) {
        return new PickledGraphite(host, port);
      } else {
        return new Graphite(host, port);
      }
    });
    if (instancePrefix == null) {
      instancePrefix = registryName;
    } else {
      instancePrefix = instancePrefix + "." + registryName;
    }
    GraphiteReporter.Builder builder = GraphiteReporter
        .forRegistry(metricManager.registry(registryName))
        .prefixedWith(instancePrefix)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS);
    MetricFilter filter;
    if (!filters.isEmpty()) {
      filter = new SolrMetricManager.PrefixFilter(filters);
    } else {
      filter = MetricFilter.ALL;
    }
    builder = builder.filter(filter);
    reporter = builder.build(graphite);
    reporter.start(period, TimeUnit.SECONDS);
  }

  @Override
  public void close() throws IOException {
    if (reporter != null) {
      reporter.close();
    }
  }
}
