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

import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;

/**
 * Sub-class of PoolingHttpClientConnectionManager which tracks metrics interesting to Solr.
 * Inspired by dropwizard metrics-httpclient library implementation.
 */
public class InstrumentedPoolingClientConnectionManager extends PoolingClientConnectionManager implements SolrMetricProducer {

  public InstrumentedPoolingClientConnectionManager(SchemeRegistry schreg) {
    super(schreg);
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String scope) {
    // these acquire a lock on the connection pool; remove if contention sucks
    manager.registerGauge(registry, () -> getTotalStats().getAvailable(), true, SolrMetricManager.mkName("availableConnections", scope));
    manager.registerGauge(registry, () -> getTotalStats().getLeased(), true, SolrMetricManager.mkName("leasedConnections", scope));
    manager.registerGauge(registry, () -> getTotalStats().getMax(), true, SolrMetricManager.mkName("maxConnections", scope));
    manager.registerGauge(registry, () -> getTotalStats().getPending(), true, SolrMetricManager.mkName("pendingConnections", scope));
  }
}
