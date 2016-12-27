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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;

/**
 * Sub-class of HttpRequestExecutor which tracks metrics interesting to solr
 * Inspired and partially copied from dropwizard httpclient library
 */
public class InstrumentedHttpRequestExecutor extends HttpRequestExecutor implements SolrMetricProducer {
  protected MetricRegistry metricsRegistry;
  protected String scope;

  private static String methodNameString(HttpRequest request) {
    return request.getRequestLine().getMethod().toLowerCase(Locale.ROOT) + "-requests";
  }

  @Override
  public HttpResponse execute(HttpRequest request, HttpClientConnection conn, HttpContext context) throws IOException, HttpException {
    assert metricsRegistry != null;
    final Timer.Context timerContext = timer(request).time();
    try {
      return super.execute(request, conn, context);
    } finally {
      timerContext.stop();
    }
  }

  private Timer timer(HttpRequest request) {
    return metricsRegistry.timer(getNameFor(request));
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
    this.scope = scope;
    return Collections.emptyList(); // we do not know the names of the metrics yet
  }

  @Override
  public String getDescription() {
    return null;
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

  private String getNameFor(HttpRequest request) {
    try {
      final RequestLine requestLine = request.getRequestLine();
      String schemeHostPort = null;
      if (request instanceof HttpRequestWrapper) {
        HttpRequestWrapper wrapper = (HttpRequestWrapper) request;
        schemeHostPort = wrapper.getTarget().getSchemeName() + "://" + wrapper.getTarget().getHostName() + ":" +  wrapper.getTarget().getPort();
      }
      final URIBuilder url = new URIBuilder(requestLine.getUri());
      return SolrMetricManager.mkName((schemeHostPort != null ? schemeHostPort : "") + url.removeQuery().build().toString() + "." + methodNameString(request), scope);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
