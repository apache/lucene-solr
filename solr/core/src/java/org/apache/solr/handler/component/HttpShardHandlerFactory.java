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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.stats.HttpClientMetricNameStrategy;
import org.apache.solr.util.stats.InstrumentedHttpRequestExecutor;
import org.apache.solr.util.stats.InstrumentedPoolingHttpClientConnectionManager;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.util.stats.InstrumentedHttpRequestExecutor.KNOWN_METRIC_NAME_STRATEGIES;


public class HttpShardHandlerFactory extends ShardHandlerFactory implements org.apache.solr.util.plugin.PluginInfoInitialized, SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String DEFAULT_SCHEME = "http";
  
  // We want an executor that doesn't take up any resources if
  // it's not used, so it could be created statically for
  // the distributed search component if desired.
  //
  // Consider CallerRuns policy and a lower max threads to throttle
  // requests at some point (or should we simply return failure?)
  private ExecutorService commExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
      0,
      Integer.MAX_VALUE,
      5, TimeUnit.SECONDS, // terminate idle threads after 5 sec
      new SynchronousQueue<>(),  // directly hand off tasks
      new DefaultSolrThreadFactory("httpShardExecutor"),
      // the Runnable added to this executor handles all exceptions so we disable stack trace collection as an optimization
      // see SOLR-11880 for more details
      false
  );

  protected InstrumentedPoolingHttpClientConnectionManager clientConnectionManager;
  protected CloseableHttpClient defaultClient;
  protected InstrumentedHttpRequestExecutor httpRequestExecutor;
  private LBHttpSolrClient loadbalancer;
  //default values:
  int soTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;
  int connectionTimeout = HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;
  int maxConnectionsPerHost = HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST;
  int maxConnections = HttpClientUtil.DEFAULT_MAXCONNECTIONS;
  int corePoolSize = 0;
  int maximumPoolSize = Integer.MAX_VALUE;
  int keepAliveTime = 5;
  int queueSize = -1;
  int   permittedLoadBalancerRequestsMinimumAbsolute = 0;
  float permittedLoadBalancerRequestsMaximumFraction = 1.0f;
  boolean accessPolicy = false;

  private String scheme = null;

  private HttpClientMetricNameStrategy metricNameStrategy;

  private String metricTag;

  protected final Random r = new Random();

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

  // The minimum number of replicas that may be used
  static final String LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE = "loadBalancerRequestsMinimumAbsolute";

  // The maximum proportion of replicas to be used
  static final String LOAD_BALANCER_REQUESTS_MAX_FRACTION = "loadBalancerRequestsMaximumFraction";

  // Configure if the threadpool favours fairness over throughput
  static final String INIT_FAIRNESS_POLICY = "fairnessPolicy";

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

    String strategy = getParameter(args, "metricNameStrategy", UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY, sb);
    this.metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(strategy);
    if (this.metricNameStrategy == null)  {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unknown metricNameStrategy: " + strategy + " found. Must be one of: " + KNOWN_METRIC_NAME_STRATEGIES.keySet());
    }

    this.connectionTimeout = getParameter(args, HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout, sb);
    this.maxConnectionsPerHost = getParameter(args, HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost,sb);
    this.maxConnections = getParameter(args, HttpClientUtil.PROP_MAX_CONNECTIONS, maxConnections,sb);
    this.corePoolSize = getParameter(args, INIT_CORE_POOL_SIZE, corePoolSize,sb);
    this.maximumPoolSize = getParameter(args, INIT_MAX_POOL_SIZE, maximumPoolSize,sb);
    this.keepAliveTime = getParameter(args, MAX_THREAD_IDLE_TIME, keepAliveTime,sb);
    this.queueSize = getParameter(args, INIT_SIZE_OF_QUEUE, queueSize,sb);
    this.permittedLoadBalancerRequestsMinimumAbsolute = getParameter(
        args,
        LOAD_BALANCER_REQUESTS_MIN_ABSOLUTE,
        permittedLoadBalancerRequestsMinimumAbsolute,
        sb);
    this.permittedLoadBalancerRequestsMaximumFraction = getParameter(
        args,
        LOAD_BALANCER_REQUESTS_MAX_FRACTION,
        permittedLoadBalancerRequestsMaximumFraction,
        sb);
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
    httpRequestExecutor = new InstrumentedHttpRequestExecutor(this.metricNameStrategy);
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

  protected ExecutorService getThreadPoolExecutor(){
    return this.commExecutor;
  }

  protected LBHttpSolrClient createLoadbalancer(HttpClient httpClient){
    LBHttpSolrClient client = new Builder()
        .withHttpClient(httpClient)
        .withConnectionTimeout(connectionTimeout)
        .withSocketTimeout(soTimeout)
        .build();
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
    return loadbalancer.request(newLBHttpSolrClientReq(req, urls));
  }

  protected LBHttpSolrClient.Req newLBHttpSolrClientReq(final QueryRequest req, List<String> urls) {
    int numServersToTry = (int)Math.floor(urls.size() * this.permittedLoadBalancerRequestsMaximumFraction);
    if (numServersToTry < this.permittedLoadBalancerRequestsMinimumAbsolute) {
      numServersToTry = this.permittedLoadBalancerRequestsMinimumAbsolute;
    }
    return new LBHttpSolrClient.Req(req, urls, numServersToTry);
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
   * This function makes sure that the cores are sorted according to the given list of preferences.
   * E.g. If all nodes prefer local cores then a bad/heavily-loaded node will receive less requests from 
   * healthy nodes. This will help prevent a distributed deadlock or timeouts in all the healthy nodes due 
   * to one bad node.
   */
  static class NodePreferenceRulesComparator implements Comparator<Object> {
    private static class PreferenceRule {
      public final String name;
      public final String value;

      public PreferenceRule(String name, String value) {
        this.name = name;
        this.value = value;
      }
    }

    private final SolrQueryRequest request;
    private List<PreferenceRule> preferenceRules;
    private String localHostAddress = null;

    public NodePreferenceRulesComparator(final List<String> sortRules, final SolrQueryRequest request) {
      this.request = request;
      this.preferenceRules = new ArrayList<PreferenceRule>(sortRules.size());
      sortRules.forEach(rule -> {
        String[] parts = rule.split(":", 2);
        if (parts.length != 2) {
          throw new IllegalArgumentException("Invalid " + ShardParams.SHARDS_PREFERENCE + " rule: " + rule);
        }
        this.preferenceRules.add(new PreferenceRule(parts[0], parts[1])); 
      });
    }
    @Override
    public int compare(Object left, Object right) {
      for (PreferenceRule preferenceRule: this.preferenceRules) {
        final boolean lhs;
        final boolean rhs;
        switch (preferenceRule.name) {
          case ShardParams.SHARDS_PREFERENCE_REPLICA_TYPE:
            lhs = hasReplicaType(left, preferenceRule.value);
            rhs = hasReplicaType(right, preferenceRule.value);
            break;
          case ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION:
            lhs = hasCoreUrlPrefix(left, preferenceRule.value);
            rhs = hasCoreUrlPrefix(right, preferenceRule.value);
            break;
          default:
            throw new IllegalArgumentException("Invalid " + ShardParams.SHARDS_PREFERENCE + " type: " + preferenceRule.name);
        }
        if (lhs != rhs) {
          return lhs ? -1 : +1;
        }
      }
      return 0;
    }
    private boolean hasCoreUrlPrefix(Object o, String prefix) {
      final String s;
      if (o instanceof String) {
        s = (String)o;
      }
      else if (o instanceof Replica) {
        s = ((Replica)o).getCoreUrl();
      } else {
        return false;
      }
      if (prefix.equals(ShardParams.REPLICA_LOCAL)) {
        if (null == localHostAddress) {
          final ZkController zkController = this.request.getCore().getCoreContainer().getZkController();
          localHostAddress = zkController != null ? zkController.getBaseUrl() : "";
          if (localHostAddress.isEmpty()) {
            log.warn("Couldn't determine current host address for sorting of local replicas");
          }
        }
        if (!localHostAddress.isEmpty()) {
          if (s.startsWith(localHostAddress)) {
            return true;
          }
        }
      } else {
        if (s.startsWith(prefix)) {
          return true;
        }
      }
      return false;
    }
    private static boolean hasReplicaType(Object o, String preferred) {
      if (!(o instanceof Replica)) {
        return false;
      }
      final String s = ((Replica)o).getType().toString();
      return s.equals(preferred);
    }
  }

  protected ReplicaListTransformer getReplicaListTransformer(final SolrQueryRequest req) {
    final SolrParams params = req.getParams();
    @SuppressWarnings("deprecation")
    final boolean preferLocalShards = params.getBool(CommonParams.PREFER_LOCAL_SHARDS, false);
    final String shardsPreferenceSpec = params.get(ShardParams.SHARDS_PREFERENCE, "");

    if (preferLocalShards || !shardsPreferenceSpec.isEmpty()) {
      if (preferLocalShards && !shardsPreferenceSpec.isEmpty()) {
        throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "preferLocalShards is deprecated and must not be used with shards.preference" 
        );
      }
      List<String> preferenceRules = StrUtils.splitSmart(shardsPreferenceSpec, ',');
      if (preferLocalShards) {
        preferenceRules.add(ShardParams.SHARDS_PREFERENCE_REPLICA_LOCATION + ":" + ShardParams.REPLICA_LOCAL);
      }

      return new ShufflingReplicaListTransformer(r) {
        @Override
        public void transform(List<?> choices)
        {
          if (choices.size() > 1) {
            super.transform(choices);
            if (log.isDebugEnabled()) {
              log.debug("Applying the following sorting preferences to replicas: {}",
                  Arrays.toString(preferenceRules.toArray()));
            }
            try {
              choices.sort(new NodePreferenceRulesComparator(preferenceRules, req));
            } catch (IllegalArgumentException iae) {
              throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                iae.getMessage()
              );
            }
            if (log.isDebugEnabled()) {
              log.debug("Applied sorting preferences to replica list: {}",
                  Arrays.toString(choices.toArray()));
            }
          }
        }
      };
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
  public void initializeMetrics(SolrMetricManager manager, String registry, String tag, String scope) {
    this.metricTag = tag;
    String expandedScope = SolrMetricManager.mkName(scope, SolrInfoBean.Category.QUERY.name());
    clientConnectionManager.initializeMetrics(manager, registry, tag, expandedScope);
    httpRequestExecutor.initializeMetrics(manager, registry, tag, expandedScope);
    commExecutor = MetricUtils.instrumentedExecutorService(commExecutor, null,
        manager.registry(registry),
        SolrMetricManager.mkName("httpShardExecutor", expandedScope, "threadPool"));
  }

}
