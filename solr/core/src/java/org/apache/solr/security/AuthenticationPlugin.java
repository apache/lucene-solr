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
import java.io.Closeable;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;

/**
 * 
 * @lucene.experimental
 */
public abstract class AuthenticationPlugin implements Closeable, SolrInfoBean, SolrMetricProducer {

  final public static String AUTHENTICATION_PLUGIN_PROP = "authenticationPlugin";

  // Metrics
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private MetricRegistry registry;

  protected String registryName;
  protected SolrMetricManager metricManager;
  protected Meter numErrors = new Meter();
  protected Meter numTimeouts = new Meter();
  protected Counter requests = new Counter();
  protected Timer requestTimes = new Timer();
  protected Counter totalTime = new Counter();
  protected Counter numAuthenticated = new Counter();
  protected Counter numPassThrough = new Counter();
  protected Counter numWrongCredentials = new Counter();
  protected Counter numMissingCredentials = new Counter();
  protected Counter numInvalidCredentials = new Counter();

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
   * Cleanup any per request  data
   */
  public void closeRequest() {
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registryName, String tag, final String scope) {
    this.metricManager = manager;
    this.registryName = registryName;
    // Metrics
    registry = manager.registry(registryName);
    numErrors = manager.meter(this, registryName, "errors", getCategory().toString(), scope);
    numTimeouts = manager.meter(this, registryName, "timeouts", getCategory().toString(), scope);
    requests = manager.counter(this, registryName, "requests", getCategory().toString(), scope);
    numAuthenticated = manager.counter(this, registryName, "authenticated", getCategory().toString(), scope);
    numPassThrough = manager.counter(this, registryName, "passThrough", getCategory().toString(), scope);
    numWrongCredentials = manager.counter(this, registryName, "failWrongCredentials", getCategory().toString(), scope);
    numInvalidCredentials = manager.counter(this, registryName, "failInvalidCredentials", getCategory().toString(), scope);
    numMissingCredentials = manager.counter(this, registryName, "failMissingCredentials", getCategory().toString(), scope);
    requestTimes = manager.timer(this, registryName, "requestTimes", getCategory().toString(), scope);
    totalTime = manager.counter(this, registryName, "totalTime", getCategory().toString(), scope);
    metricNames.addAll(Arrays.asList("errors", "timeouts", "requests", "authenticated", "passThrough",
        "failWrongCredentials", "failMissingCredentials", "failInvalidCredentials", "requestTimes", "totalTime"));
  }
  
  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public String getDescription() {
    return this.getClass().getName();
  }

  @Override
  public Category getCategory() {
    return Category.SECURITY;
  }
  
  @Override
  public Set<String> getMetricNames() {
    return metricNames;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return registry;
  }
  
}
