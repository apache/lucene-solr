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

package org.apache.solr.util.stats;

import java.net.URL;
import java.util.Arrays;
import java.util.Collection;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;

/**
 * Sub-class of PoolingHttpClientConnectionManager which tracks metrics interesting to Solr.
 * Inspired by dropwizard metrics-httpclient library implementation.
 */
public class InstrumentedPoolingHttpClientConnectionManager extends PoolingHttpClientConnectionManager implements SolrMetricProducer {

  protected MetricRegistry metricsRegistry;

  public InstrumentedPoolingHttpClientConnectionManager(Registry<ConnectionSocketFactory> socketFactoryRegistry) {
    super(socketFactoryRegistry);
  }

  public MetricRegistry getMetricsRegistry() {
    return metricsRegistry;
  }

  public void setMetricsRegistry(MetricRegistry metricRegistry) {
    this.metricsRegistry = metricRegistry;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public String getVersion() {
    return getClass().getPackage().getSpecificationVersion();
  }

  @Override
  public Collection<String> initializeMetrics(SolrMetricManager manager, String registry, String scope) {
    this.metricsRegistry = manager.registry(registry);
    metricsRegistry.register(SolrMetricManager.mkName("availableConnections", scope),
        (Gauge<Integer>) () -> {
          // this acquires a lock on the connection pool; remove if contention sucks
          return getTotalStats().getAvailable();
        });
    metricsRegistry.register(SolrMetricManager.mkName("leasedConnections", scope),
        (Gauge<Integer>) () -> {
          // this acquires a lock on the connection pool; remove if contention sucks
          return getTotalStats().getLeased();
        });
    metricsRegistry.register(SolrMetricManager.mkName("maxConnections", scope),
        (Gauge<Integer>) () -> {
          // this acquires a lock on the connection pool; remove if contention sucks
          return getTotalStats().getMax();
        });
    metricsRegistry.register(SolrMetricManager.mkName("pendingConnections", scope),
        (Gauge<Integer>) () -> {
          // this acquires a lock on the connection pool; remove if contention sucks
          return getTotalStats().getPending();
        });
    return Arrays.asList("availableConnections", "leasedConnections", "maxConnections", "pendingConnections");
  }

  @Override
  public String getDescription() {
    return "";
  }

  @Override
  public Category getCategory() {
    return Category.OTHER;
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  @Override
  public NamedList getStatistics() {
    return null;
  }
}
