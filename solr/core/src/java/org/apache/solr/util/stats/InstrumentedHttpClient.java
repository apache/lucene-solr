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
import java.util.Collection;

import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;

public class InstrumentedHttpClient extends DefaultHttpClient implements SolrMetricProducer {

  protected final InstrumentedHttpRequestExecutor requestExecutor;

  public InstrumentedHttpClient(ClientConnectionManager conman) {
    super(conman);
    this.requestExecutor = new InstrumentedHttpRequestExecutor();
  }

  @Override
  protected HttpRequestExecutor createRequestExecutor() {
    return requestExecutor;
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
    return requestExecutor.initializeMetrics(manager, registry, scope);
  }

  @Override
  public String getDescription() {
    return "Metrics tracked by http client";
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
    return new URL[0];
  }

  @Override
  public NamedList getStatistics() {
    return null;
  }
}
