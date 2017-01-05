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
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ganglia.GangliaReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricReporter;

/**
 *
 */
public class SolrGangliaReporter extends SolrMetricReporter {

  private String host = null;
  private int port = -1;
  private boolean multicast;
  private int period = 60;
  private String instancePrefix = null;
  private String filterPrefix = null;
  private boolean testing;
  private GangliaReporter reporter;

  // for unit tests
  GMetric ganglia = null;

  /**
   * Create a Ganglia reporter for metrics managed in a named registry.
   *
   * @param metricManager metric manager instance that manages the selected registry
   * @param registryName  registry to use, one of registries managed by
   *                      {@link SolrMetricManager}
   */
  public SolrGangliaReporter(SolrMetricManager metricManager, String registryName) {
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

  public void setFilter(String filter) {
    this.filterPrefix = filter;
  }


  public void setPeriod(int period) {
    this.period = period;
  }

  public void setMulticast(boolean multicast) {
    this.multicast = multicast;
  }

  // only for unit tests!
  public void setTesting(boolean testing) {
    this.testing = testing;
  }

  void setGMetric(GMetric ganglia) {
    this.ganglia = ganglia;
  }

  @Override
  protected void validate() throws IllegalStateException {
    if (host == null) {
      throw new IllegalStateException("Init argument 'host' must be set to a valid Ganglia server name.");
    }
    if (port == -1) {
      throw new IllegalStateException("Init argument 'port' must be set to a valid Ganglia server port.");
    }
    if (period < 1) {
      throw new IllegalStateException("Init argument 'period' is in time unit 'seconds' and must be at least 1.");
    }
    if (!testing) {
      start();
    }
  }

  //this is a separate method for unit tests
  void start() {
    if (!testing) {
      try {
        ganglia = new GMetric(host, port,
            multicast ? GMetric.UDPAddressingMode.MULTICAST : GMetric.UDPAddressingMode.UNICAST,
            1);
      } catch (IOException ioe) {
        throw new IllegalStateException("Exception connecting to Ganglia", ioe);
      }
    }
    if (instancePrefix == null) {
      instancePrefix = registryName;
    } else {
      instancePrefix = instancePrefix + "." + registryName;
    }
    GangliaReporter.Builder builder = GangliaReporter
        .forRegistry(metricManager.registry(registryName))
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .prefixedWith(instancePrefix);
    MetricFilter filter;
    if (filterPrefix != null) {
      filter = new SolrMetricManager.PrefixFilter(filterPrefix);
    } else {
      filter = MetricFilter.ALL;
    }
    builder = builder.filter(filter);
    reporter = builder.build(ganglia);
    reporter.start(period, TimeUnit.SECONDS);
  }

  @Override
  public void close() throws IOException {
    if (reporter != null) {
      reporter.close();
    }
  }
}
