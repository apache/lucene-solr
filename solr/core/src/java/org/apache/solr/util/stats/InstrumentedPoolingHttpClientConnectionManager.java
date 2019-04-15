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

import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;

/**
 * Sub-class of PoolingHttpClientConnectionManager which tracks metrics interesting to Solr.
 * Inspired by dropwizard metrics-httpclient library implementation.
 */
public class InstrumentedPoolingHttpClientConnectionManager extends PoolingHttpClientConnectionManager implements SolrMetricProducer {

  private SolrMetricManager metricManager;
  private String registryName;

  public InstrumentedPoolingHttpClientConnectionManager(Registry<ConnectionSocketFactory> socketFactoryRegistry) {
    super(socketFactoryRegistry);
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    this.metricManager = manager;
    this.registryName = registry;
    manager.registerGauge(null, registry, () -> getTotalStats().getAvailable(),
        tag, true, SolrMetricManager.mkName("availableConnections", scope));
    // this acquires a lock on the connection pool; remove if contention sucks
    manager.registerGauge(null, registry, () -> getTotalStats().getLeased(),
        tag, true, SolrMetricManager.mkName("leasedConnections", scope));
    manager.registerGauge(null, registry, () -> getTotalStats().getMax(),
        tag, true, SolrMetricManager.mkName("maxConnections", scope));
    manager.registerGauge(null, registry, () -> getTotalStats().getPending(),
        tag, true, SolrMetricManager.mkName("pendingConnections", scope));
  }
}
