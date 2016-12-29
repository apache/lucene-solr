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
package org.apache.solr.handler.component;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient.Builder;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.stats.InstrumentedHttpRequestExecutor;
import org.apache.solr.util.stats.InstrumentedPoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class HttpShardHandlerFactory extends ShardHandlerFactory implements org.apache.solr.util.plugin.PluginInfoInitialized, SolrMetricProducer {
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

  protected InstrumentedPoolingHttpClientConnectionManager clientConnectionManager;
  protected CloseableHttpClient defaultClient;
  protected InstrumentedHttpRequestExecutor httpRequestExecutor;
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

  private String scheme = null;

  private final Random r = new Random();

  private final ReplicaListTransformer shufflingReplicaListTransformer = new ShufflingReplicaListTransformer(r);

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
    log.debug("created with {}",sb);
    
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
    httpRequestExecutor = new InstrumentedHttpRequestExecutor();
    clientConnectionManager = new InstrumentedPoolingHttpClientConnectionManager(HttpClientUtil.getSchemaRegisteryProvider().getSchemaRegistry());
    this.defaultClient = HttpClientUtil.createClient(clientParams, clientConnectionManager, false, httpRequestExecutor);
    this.loadbalancer = createLoadbalancer(defaultClient);
  }

  protected ModifiableSolrParams getClientParams() {
    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);
    clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS, maxConnections);
    return clientParams;
  }

  protected ThreadPoolExecutor getThreadPoolExecutor(){
    return this.commExecutor;
  }

  protected LBHttpSolrClient createLoadbalancer(HttpClient httpClient){
    LBHttpSolrClient client = new Builder()
        .withHttpClient(httpClient)
        .build();
    client.setConnectionTimeout(connectionTimeout);
    client.setSoTimeout(soTimeout);
    return client;
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
        if (loadbalancer != null) {
          loadbalancer.close();
        }
      } finally { 
        if (defaultClient != null) {
          HttpClientUtil.close(defaultClient);
        }
        if (clientConnectionManager != null)  {
          clientConnectionManager.close();
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
   * Creates a list of urls for the given shard.
   *
   * @param shard the urls for the shard, separated by '|'
   * @return A list of valid urls (including protocol) that are replicas for the shard
   */
  public List<String> buildURLList(String shard) {
    List<String> urls = StrUtils.splitSmart(shard, "|", true);

    // convert shard to URL
    for (int i=0; i<urls.size(); i++) {
      urls.set(i, buildUrl(urls.get(i)));
    }

    return urls;
  }

  /**
   * A distributed request is made via {@link LBHttpSolrClient} to the first live server in the URL list.
   * This means it is just as likely to choose current host as any of the other hosts.
   * This function makes sure that the cores of current host are always put first in the URL list.
   * If all nodes prefer local-cores then a bad/heavily-loaded node will receive less requests from healthy nodes.
   * This will help prevent a distributed deadlock or timeouts in all the healthy nodes due to one bad node.
   */
  private class IsOnPreferredHostComparator implements Comparator<Object> {
    final private String preferredHostAddress;
    public IsOnPreferredHostComparator(String preferredHostAddress) {
      this.preferredHostAddress = preferredHostAddress;
    }
    @Override
    public int compare(Object left, Object right) {
      final boolean lhs = hasPrefix(objectToString(left));
      final boolean rhs = hasPrefix(objectToString(right));
      if (lhs != rhs) {
        if (lhs) {
          return -1;
        } else {
          return +1;
        }
      } else {
        return 0;
      }
    }
    private String objectToString(Object o) {
      final String s;
      if (o instanceof String) {
        s = (String)o;
      }
      else if (o instanceof Replica) {
        s = ((Replica)o).getCoreUrl();
      } else {
        s = null;
      }
      return s;
    }
    private boolean hasPrefix(String s) {
      return s != null && s.startsWith(preferredHostAddress);
    }
  }
  ReplicaListTransformer getReplicaListTransformer(final SolrQueryRequest req)
  {
    final SolrParams params = req.getParams();

    if (params.getBool(CommonParams.PREFER_LOCAL_SHARDS, false)) {
      final CoreDescriptor coreDescriptor = req.getCore().getCoreDescriptor();
      final ZkController zkController = coreDescriptor.getCoreContainer().getZkController();
      final String preferredHostAddress = (zkController != null) ? zkController.getBaseUrl() : null;
      if (preferredHostAddress == null) {
        log.warn("Couldn't determine current host address to prefer local shards");
      } else {
        return new ShufflingReplicaListTransformer(r) {
          @Override
          public void transform(List<?> choices)
          {
            if (choices.size() > 1) {
              super.transform(choices);
              if (log.isDebugEnabled()) {
                log.debug("Trying to prefer local shard on {} among the choices: {}",
                    preferredHostAddress, Arrays.toString(choices.toArray()));
              }
              choices.sort(new IsOnPreferredHostComparator(preferredHostAddress));
              if (log.isDebugEnabled()) {
                log.debug("Applied local shard preference for choices: {}",
                    Arrays.toString(choices.toArray()));
              }
            }
          }
        };
      }
    }

    return shufflingReplicaListTransformer;
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
    List<String> metricNames = new ArrayList<>(4);
    metricNames.addAll(clientConnectionManager.initializeMetrics(manager, registry, scope));
    metricNames.addAll(httpRequestExecutor.initializeMetrics(manager, registry, scope));
    return metricNames;
  }

  @Override
  public String getDescription() {
    return "Metrics tracked by HttpShardHandlerFactory for distributed query requests";
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
