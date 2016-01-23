package org.apache.solr.handler.component;
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

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class HttpShardHandlerFactory extends ShardHandlerFactory implements org.apache.solr.util.plugin.PluginInfoInitialized {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String DEFAULT_SCHEME = "http";
  
  // We want an executor that doesn't take up any resources if
  // it's not used, so it could be created statically for
  // the distributed search component if desired.
  //
  // Consider CallerRuns policy and a lower max threads to throttle
  // requests at some point (or should we simply return failure?)
  private ThreadPoolExecutor commExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
      0,
      Integer.MAX_VALUE,
      5, TimeUnit.SECONDS, // terminate idle threads after 5 sec
      new SynchronousQueue<Runnable>(),  // directly hand off tasks
      new DefaultSolrThreadFactory("httpShardExecutor")
  );

  protected HttpClient defaultClient;
  private LBHttpSolrClient loadbalancer;
  //default values:
  int soTimeout = UpdateShardHandlerConfig.DEFAULT_DISTRIBUPDATESOTIMEOUT;
  int connectionTimeout = UpdateShardHandlerConfig.DEFAULT_DISTRIBUPDATECONNTIMEOUT;
  int maxConnectionsPerHost = 20;
  int maxConnections = 10000;
  int corePoolSize = 0;
  int maximumPoolSize = Integer.MAX_VALUE;
  int keepAliveTime = 5;
  int queueSize = -1;
  boolean accessPolicy = false;
  boolean useRetries = false;

  private String scheme = null;

  private final Random r = new Random();

  // URL scheme to be used in distributed search.
  static final String INIT_URL_SCHEME = "urlScheme";

  // The core size of the threadpool servicing requests
  static final String INIT_CORE_POOL_SIZE = "corePoolSize";

  // The maximum size of the threadpool servicing requests
  static final String INIT_MAX_POOL_SIZE = "maximumPoolSize";

  // The amount of time idle threads persist for in the queue, before being killed
  static final String MAX_THREAD_IDLE_TIME = "maxThreadIdleTime";

  // If the threadpool uses a backing queue, what is its maximum size (-1) to use direct handoff
  static final String INIT_SIZE_OF_QUEUE = "sizeOfQueue";

  // Configure if the threadpool favours fairness over throughput
  static final String INIT_FAIRNESS_POLICY = "fairnessPolicy";
  
  // Turn on retries for certain IOExceptions, many of which can happen
  // due to connection pooling limitations / races
  static final String USE_RETRIES = "useRetries";

  /**
   * Get {@link ShardHandler} that uses the default http client.
   */
  @Override
  public ShardHandler getShardHandler() {
    return getShardHandler(defaultClient);
  }

  /**
   * Get {@link ShardHandler} that uses custom http client.
   */
  public ShardHandler getShardHandler(final HttpClient httpClient){
    return new HttpShardHandler(this, httpClient);
  }

  @Override
  public void init(PluginInfo info) {
    StringBuilder sb = new StringBuilder();
    NamedList args = info.initArgs;
    this.soTimeout = getParameter(args, HttpClientUtil.PROP_SO_TIMEOUT, soTimeout,sb);
    this.scheme = getParameter(args, INIT_URL_SCHEME, null,sb);
    if(StringUtils.endsWith(this.scheme, "://")) {
      this.scheme = StringUtils.removeEnd(this.scheme, "://");
    }

    this.connectionTimeout = getParameter(args, HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout, sb);
    this.maxConnectionsPerHost = getParameter(args, HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost,sb);
    this.maxConnections = getParameter(args, HttpClientUtil.PROP_MAX_CONNECTIONS, maxConnections,sb);
    this.corePoolSize = getParameter(args, INIT_CORE_POOL_SIZE, corePoolSize,sb);
    this.maximumPoolSize = getParameter(args, INIT_MAX_POOL_SIZE, maximumPoolSize,sb);
    this.keepAliveTime = getParameter(args, MAX_THREAD_IDLE_TIME, keepAliveTime,sb);
    this.queueSize = getParameter(args, INIT_SIZE_OF_QUEUE, queueSize,sb);
    this.accessPolicy = getParameter(args, INIT_FAIRNESS_POLICY, accessPolicy,sb);
    this.useRetries = getParameter(args, USE_RETRIES, useRetries,sb);
    log.info("created with {}",sb);
    
    // magic sysprop to make tests reproducible: set by SolrTestCaseJ4.
    String v = System.getProperty("tests.shardhandler.randomSeed");
    if (v != null) {
      r.setSeed(Long.parseLong(v));
    }

    BlockingQueue<Runnable> blockingQueue = (this.queueSize == -1) ?
        new SynchronousQueue<Runnable>(this.accessPolicy) :
        new ArrayBlockingQueue<Runnable>(this.queueSize, this.accessPolicy);

    this.commExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
        this.corePoolSize,
        this.maximumPoolSize,
        this.keepAliveTime, TimeUnit.SECONDS,
        blockingQueue,
        new DefaultSolrThreadFactory("httpShardExecutor")
    );

    ModifiableSolrParams clientParams = getClientParams();

    this.defaultClient = HttpClientUtil.createClient(clientParams);
    
    // must come after createClient
    if (useRetries) {
      // our default retry handler will never retry on IOException if the request has been sent already,
      // but for these read only requests we can use the standard DefaultHttpRequestRetryHandler rules
      ((DefaultHttpClient) this.defaultClient).setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler());
    }
    
    this.loadbalancer = createLoadbalancer(defaultClient);
  }
  
  protected ModifiableSolrParams getClientParams() {
    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);
    clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS, maxConnections);
    clientParams.set(HttpClientUtil.PROP_SO_TIMEOUT, soTimeout);
    clientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout);
    if (!useRetries) {
      clientParams.set(HttpClientUtil.PROP_USE_RETRY, false);
    }
    return clientParams;
  }

  /**
   * For an already created internal httpclient, this can be used to configure it 
   * again. Useful for authentication plugins.
   * @param configurer an HttpClientConfigurer instance
   */
  public void reconfigureHttpClient(HttpClientConfigurer configurer) {
    log.info("Reconfiguring the default client with: " + configurer);
    configurer.configure((DefaultHttpClient)this.defaultClient, getClientParams());
  }

  protected ThreadPoolExecutor getThreadPoolExecutor(){
    return this.commExecutor;
  }

  protected LBHttpSolrClient createLoadbalancer(HttpClient httpClient){
    return new LBHttpSolrClient(httpClient);
  }

  protected <T> T getParameter(NamedList initArgs, String configKey, T defaultValue, StringBuilder sb) {
    T toReturn = defaultValue;
    if (initArgs != null) {
      T temp = (T) initArgs.get(configKey);
      toReturn = (temp != null) ? temp : defaultValue;
    }
    if(sb!=null && toReturn != null) sb.append(configKey).append(" : ").append(toReturn).append(",");
    return toReturn;
  }


  @Override
  public void close() {
    try {
      ExecutorUtil.shutdownAndAwaitTermination(commExecutor);
    } finally {
      try {
        if (defaultClient != null) {
          defaultClient.getConnectionManager().shutdown();
        }
      } finally {
        
        if (loadbalancer != null) {
          loadbalancer.close();
        }
      }
    }
  }

  /**
   * Makes a request to one or more of the given urls, using the configured load balancer.
   *
   * @param req The solr search request that should be sent through the load balancer
   * @param urls The list of solr server urls to load balance across
   * @return The response from the request
   */
  public LBHttpSolrClient.Rsp makeLoadBalancedRequest(final QueryRequest req, List<String> urls)
    throws SolrServerException, IOException {
    return loadbalancer.request(new LBHttpSolrClient.Req(req, urls));
  }

  /**
   * Creates a randomized list of urls for the given shard.
   *
   * @param shard the urls for the shard, separated by '|'
   * @return A list of valid urls (including protocol) that are replicas for the shard
   */
  public List<String> makeURLList(String shard) {
    List<String> urls = StrUtils.splitSmart(shard, "|", true);

    // convert shard to URL
    for (int i=0; i<urls.size(); i++) {
      urls.set(i, buildUrl(urls.get(i)));
    }

    //
    // Shuffle the list instead of use round-robin by default.
    // This prevents accidental synchronization where multiple shards could get in sync
    // and query the same replica at the same time.
    //
    if (urls.size() > 1)
      Collections.shuffle(urls, r);

    return urls;
  }

  /**
   * Creates a new completion service for use by a single set of distributed requests.
   */
  public CompletionService newCompletionService() {
    return new ExecutorCompletionService<ShardResponse>(commExecutor);
  }
  
  /**
   * Rebuilds the URL replacing the URL scheme of the passed URL with the
   * configured scheme replacement.If no scheme was configured, the passed URL's
   * scheme is left alone.
   */
  private String buildUrl(String url) {
    if(!URLUtil.hasScheme(url)) {
      return StringUtils.defaultIfEmpty(scheme, DEFAULT_SCHEME) + "://" + url;
    } else if(StringUtils.isNotEmpty(scheme)) {
      return scheme + "://" + URLUtil.removeScheme(url);
    }
    
    return url;
  }
}
