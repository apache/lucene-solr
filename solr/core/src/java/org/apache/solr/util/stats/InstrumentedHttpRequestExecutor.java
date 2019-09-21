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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;

import static org.apache.solr.metrics.SolrMetricManager.mkName;

/**
 * Sub-class of HttpRequestExecutor which tracks metrics interesting to solr
 * Inspired and partially copied from dropwizard httpclient library
 */
public class InstrumentedHttpRequestExecutor extends HttpRequestExecutor implements SolrMetricProducer {
  public static final HttpClientMetricNameStrategy QUERYLESS_URL_AND_METHOD =
      (scope, request) -> {
        try {
          final RequestLine requestLine = request.getRequestLine();
          String schemeHostPort = null;
          if (request instanceof HttpRequestWrapper) {
            HttpRequestWrapper wrapper = (HttpRequestWrapper) request;
            if (wrapper.getTarget() != null) {
              schemeHostPort = wrapper.getTarget().getSchemeName() + "://" + wrapper.getTarget().getHostName() + ":" + wrapper.getTarget().getPort();
            }
          }
          final URIBuilder url = new URIBuilder(requestLine.getUri());
          return mkName((schemeHostPort != null ? schemeHostPort : "") + url.removeQuery().build().toString() + "." + methodNameString(request), scope);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
      };

  public static final HttpClientMetricNameStrategy METHOD_ONLY =
      (scope, request) -> mkName(methodNameString(request), scope);

  public static final HttpClientMetricNameStrategy HOST_AND_METHOD =
      (scope, request) -> {
        try {
          final RequestLine requestLine = request.getRequestLine();
          String schemeHostPort = null;
          if (request instanceof HttpRequestWrapper) {
            HttpRequestWrapper wrapper = (HttpRequestWrapper) request;
            if (wrapper.getTarget() != null) {
              schemeHostPort = wrapper.getTarget().getSchemeName() + "://" + wrapper.getTarget().getHostName() + ":" + wrapper.getTarget().getPort();
            }
          }
          final URIBuilder url = new URIBuilder(requestLine.getUri());
          return mkName((schemeHostPort != null ? schemeHostPort : "") + "." + methodNameString(request), scope);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
      };

  public static final Map<String, HttpClientMetricNameStrategy> KNOWN_METRIC_NAME_STRATEGIES = new HashMap<>(3);

  static  {
    KNOWN_METRIC_NAME_STRATEGIES.put("queryLessURLAndMethod", QUERYLESS_URL_AND_METHOD);
    KNOWN_METRIC_NAME_STRATEGIES.put("hostAndMethod", HOST_AND_METHOD);
    KNOWN_METRIC_NAME_STRATEGIES.put("methodOnly", METHOD_ONLY);
  }

  protected MetricRegistry metricsRegistry;
  protected SolrMetricManager metricManager;
  protected String registryName;
  protected String scope;
  protected HttpClientMetricNameStrategy nameStrategy;

  public InstrumentedHttpRequestExecutor(int waitForContinue, HttpClientMetricNameStrategy nameStrategy) {
    super(waitForContinue);
    this.nameStrategy = nameStrategy;
  }

  public InstrumentedHttpRequestExecutor(HttpClientMetricNameStrategy nameStrategy) {
    this.nameStrategy = nameStrategy;
  }

  private static String methodNameString(HttpRequest request) {
    return request.getRequestLine().getMethod().toLowerCase(Locale.ROOT) + ".requests";
  }

  @Override
  public HttpResponse execute(HttpRequest request, HttpClientConnection conn, HttpContext context) throws IOException, HttpException {
    Timer.Context timerContext = null;
    if (metricsRegistry != null) {
      timerContext = timer(request).time();
    }
    try {
      return super.execute(request, conn, context);
    } finally {
      if (timerContext != null) {
        timerContext.stop();
      }
    }
  }

  private Timer timer(HttpRequest request) {
    return metricsRegistry.timer(nameStrategy.getNameFor(scope, request));
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    this.metricManager = manager;
    this.registryName = registry;
    this.metricsRegistry = manager.registry(registry);
    this.scope = scope;
  }
}
