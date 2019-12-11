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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.codahale.metrics.Timer;
import org.apache.solr.client.solrj.impl.HttpListenerFactory;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Result;

import static org.apache.solr.metrics.SolrMetricManager.mkName;

/**
 * A HttpListenerFactory tracks metrics interesting to solr
 * Inspired and partially copied from dropwizard httpclient library
 */
public class InstrumentedHttpListenerFactory implements SolrMetricProducer, HttpListenerFactory {

  public interface NameStrategy {
    String getNameFor(String scope, Request request);
  }

  private static final NameStrategy QUERYLESS_URL_AND_METHOD =
      (scope, request) -> {
        String schemeHostPort = request.getScheme() + "://" + request.getHost() + ":" + request.getPort() + request.getPath();
        return mkName(schemeHostPort + "." + methodNameString(request), scope);
      };

  private static final NameStrategy METHOD_ONLY =
      (scope, request) -> mkName(methodNameString(request), scope);

  private static final NameStrategy HOST_AND_METHOD =
      (scope, request) -> {
        String schemeHostPort = request.getScheme() + "://" + request.getHost() + ":" + request.getPort();
        return mkName(schemeHostPort + "." + methodNameString(request), scope);
      };

  public static final Map<String, NameStrategy> KNOWN_METRIC_NAME_STRATEGIES = new HashMap<>(3);

  static  {
    KNOWN_METRIC_NAME_STRATEGIES.put("queryLessURLAndMethod", QUERYLESS_URL_AND_METHOD);
    KNOWN_METRIC_NAME_STRATEGIES.put("hostAndMethod", HOST_AND_METHOD);
    KNOWN_METRIC_NAME_STRATEGIES.put("methodOnly", METHOD_ONLY);
  }

  protected SolrMetricsContext solrMetricsContext;
  protected String scope;
  protected NameStrategy nameStrategy;

  public InstrumentedHttpListenerFactory(NameStrategy nameStrategy) {
    this.nameStrategy = nameStrategy;
  }

  private static String methodNameString(Request request) {
    return request.getMethod().toLowerCase(Locale.ROOT) + ".requests";
  }

  @Override
  public RequestResponseListener get() {
    return new RequestResponseListener() {
      Timer.Context timerContext;

      @Override
      public void onBegin(Request request) {
        if (solrMetricsContext != null) {
          timerContext = timer(request).time();
        }
      }

      @Override
      public void onComplete(Result result) {
        if (timerContext != null) {
          timerContext.stop();
        }
      }
    };
  }

  private Timer timer(Request request) {
    return solrMetricsContext.timer(nameStrategy.getNameFor(scope, request));
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    this.solrMetricsContext = parentContext;
    this.scope = scope;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }
}

