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
package org.apache.solr.security;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.http.HttpRequest;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.eclipse.jetty.client.api.Request;

/**
 * 
 * @lucene.experimental
 */
public abstract class AuthenticationPlugin implements SolrInfoBean {

  final public static String AUTHENTICATION_PLUGIN_PROP = "authenticationPlugin";
  final public static String HTTP_HEADER_X_SOLR_AUTHDATA = "X-Solr-AuthData";

  // Metrics
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  protected SolrMetricsContext solrMetricsContext;

  protected Meter numErrors = new Meter();
  protected Counter requests = new Counter();
  protected Timer requestTimes = new Timer();
  protected Counter totalTime = new Counter();
  protected Counter numAuthenticated = new Counter();
  protected Counter numPassThrough = new Counter();
  protected Counter numWrongCredentials = new Counter();
  protected Counter numMissingCredentials = new Counter();

  /**
   * This is called upon loading up of a plugin, used for setting it up.
   * @param pluginConfig Config parameters, possibly from a ZK source
   */
  public abstract void init(Map<String, Object> pluginConfig);

  /**
   * This method attempts to authenticate the request. Upon a successful authentication, this
   * must call the next filter in the filter chain and set the user principal of the request,
   * or else, upon an error or an authentication failure, throw an exception.
   *
   * @param request the http request
   * @param response the http response
   * @param filterChain the servlet filter chain
   * @return false if the request not be processed by Solr (not continue), i.e.
   * the response and status code have already been sent.
   * @throws Exception any exception thrown during the authentication, e.g. PrivilegedActionException
   */
  //TODO redeclare params as HttpServletRequest & HttpServletResponse
  public abstract boolean doAuthenticate(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws Exception;

  /**
   * This method is called by SolrDispatchFilter in order to initiate authentication.
   * It does some standard metrics counting.
   */
  public final boolean authenticate(ServletRequest request, ServletResponse response, FilterChain filterChain) throws Exception {
    Timer.Context timer = requestTimes.time();
    requests.inc();
    try {
      return doAuthenticate(request, response, filterChain);
    } catch(Exception e) {
      numErrors.mark();
      throw e;
    } finally {
      long elapsed = timer.stop();
      totalTime.inc(elapsed);
    }
  }

  /**
   * Override this method to intercept internode requests. This allows your authentication
   * plugin to decide on per-request basis whether it should handle inter-node requests or
   * delegate to {@link PKIAuthenticationPlugin}. Return true to indicate that your plugin
   * did handle the request, or false to signal that PKI plugin should handle it. This method
   * will be called by {@link PKIAuthenticationPlugin}'s interceptor.
   *
   * <p>
   *   If not overridden, this method will return true for plugins implementing {@link HttpClientBuilderPlugin}.
   *   This method can be overridden by subclasses e.g. to set HTTP headers, even if you don't use a clientBuilder.
   * </p>
   * @param httpRequest the httpRequest that is about to be sent to another internal Solr node
   * @param httpContext the context of that request.
   * @return true if this plugin handled authentication for the request, else false
   */
  protected boolean interceptInternodeRequest(HttpRequest httpRequest, HttpContext httpContext) {
    return this instanceof HttpClientBuilderPlugin;
  }

  /**
   * Override this method to intercept internode requests. This allows your authentication
   * plugin to decide on per-request basis whether it should handle inter-node requests or
   * delegate to {@link PKIAuthenticationPlugin}. Return true to indicate that your plugin
   * did handle the request, or false to signal that PKI plugin should handle it. This method
   * will be called by {@link PKIAuthenticationPlugin}'s interceptor.
   *
   * <p>
   *   If not overridden, this method will return true for plugins implementing {@link HttpClientBuilderPlugin}.
   *   This method can be overridden by subclasses e.g. to set HTTP headers, even if you don't use a clientBuilder.
   * </p>
   * @param request the httpRequest that is about to be sent to another internal Solr node
   * @return true if this plugin handled authentication for the request, else false
   */
  protected boolean interceptInternodeRequest(Request request) {
    return this instanceof HttpClientBuilderPlugin;
  }

  /**
   * Cleanup any per request  data
   */
  public void closeRequest() {
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    this.solrMetricsContext = parentContext.getChildContext(this);
    // Metrics
    numErrors = this.solrMetricsContext.meter("errors", getCategory().toString(), scope);
    requests = this.solrMetricsContext.counter("requests", getCategory().toString(), scope);
    numAuthenticated = this.solrMetricsContext.counter("authenticated",getCategory().toString(), scope);
    numPassThrough = this.solrMetricsContext.counter("passThrough",  getCategory().toString(), scope);
    numWrongCredentials = this.solrMetricsContext.counter("failWrongCredentials",getCategory().toString(), scope);
    numMissingCredentials = this.solrMetricsContext.counter("failMissingCredentials",getCategory().toString(), scope);
    requestTimes = this.solrMetricsContext.timer("requestTimes", getCategory().toString(), scope);
    totalTime = this.solrMetricsContext.counter("totalTime", getCategory().toString(), scope);
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return "Authentication Plugin " + this.getClass().getName();
  }

  @Override
  public Category getCategory() {
    return Category.SECURITY;
  }
}
