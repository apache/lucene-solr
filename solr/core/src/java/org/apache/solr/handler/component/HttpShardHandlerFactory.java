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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBSolrClient;
import org.apache.solr.client.solrj.routing.AffinityReplicaListTransformerFactory;
import org.apache.solr.client.solrj.routing.ReplicaListTransformer;
import org.apache.solr.client.solrj.routing.ReplicaListTransformerFactory;
import org.apache.solr.client.solrj.routing.RequestReplicaListTransformerGenerator;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.stats.InstrumentedHttpListenerFactory;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.util.stats.InstrumentedHttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES;

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

  protected volatile Http2SolrClient defaultClient;
  protected InstrumentedHttpListenerFactory httpListenerFactory;
  private LBHttp2SolrClient loadbalancer;

  int corePoolSize = 0;
  int maximumPoolSize = Integer.MAX_VALUE;
  int keepAliveTime = 5;
  int queueSize = -1;
  int   permittedLoadBalancerRequestsMinimumAbsolute = 0;
  float permittedLoadBalancerRequestsMaximumFraction = 1.0f;
  boolean accessPolicy = false;
  private WhitelistHostChecker whitelistHostChecker = null;
  private SolrMetricsContext solrMetricsContext;

  private String scheme = null;

  private InstrumentedHttpListenerFactory.NameStrategy metricNameStrategy;

  protected final Random r = new Random();

  private RequestReplicaListTransformerGenerator requestReplicaListTransformerGenerator = new RequestReplicaListTransformerGenerator();

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

  public static final String INIT_SHARDS_WHITELIST = "shardsWhitelist";

  static final String INIT_SOLR_DISABLE_SHARDS_WHITELIST = "solr.disable." + INIT_SHARDS_WHITELIST;

  static final String SET_SOLR_DISABLE_SHARDS_WHITELIST_CLUE = " set -D"+INIT_SOLR_DISABLE_SHARDS_WHITELIST+"=true to disable shards whitelist checks";

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
  public ShardHandler getShardHandler(final Http2SolrClient httpClient){
    return new HttpShardHandler(this, httpClient);
  }

  @Deprecated
  public ShardHandler getShardHandler(final HttpClient httpClient) {
    // a little hack for backward-compatibility when we are moving from apache http client to jetty client
    return new HttpShardHandler(this, null) {
      @Override
      protected NamedList<Object> request(String url, SolrRequest req) throws IOException, SolrServerException {
        try (SolrClient client = new HttpSolrClient.Builder(url).withHttpClient(httpClient).build()) {
          return client.request(req);
        }
      }
    };
  }

  /**
   * Returns this Factory's {@link WhitelistHostChecker}.
   * This method can be overridden to change the checker implementation.
   */
  public WhitelistHostChecker getWhitelistHostChecker() {
    return this.whitelistHostChecker;
  }

  @Deprecated // For temporary use by the TermsComponent only.
  static boolean doGetDisableShardsWhitelist() {
    return getDisableShardsWhitelist();
  }


  private static boolean getDisableShardsWhitelist() {
    return Boolean.getBoolean(INIT_SOLR_DISABLE_SHARDS_WHITELIST);
  }

  private static NamedList<?> getNamedList(Object val) {
    if (val instanceof NamedList) {
      return (NamedList<?>)val;
    } else {
      throw new IllegalArgumentException("Invalid config for replicaRouting; expected NamedList, but got " + val);
    }
  }

  private static String checkDefaultReplicaListTransformer(NamedList<?> c, String setTo, String extantDefaultRouting) {
    if (!Boolean.TRUE.equals(c.getBooleanArg("default"))) {
      return null;
    } else {
      if (extantDefaultRouting == null) {
        return setTo;
      } else {
        throw new IllegalArgumentException("more than one routing scheme marked as default");
      }
    }
  }

  private void initReplicaListTransformers(NamedList routingConfig) {
    String defaultRouting = null;
    ReplicaListTransformerFactory stableRltFactory = null;
    ReplicaListTransformerFactory defaultRltFactory;
    if (routingConfig != null && routingConfig.size() > 0) {
      Iterator<Entry<String,?>> iter = routingConfig.iterator();
      do {
        Entry<String, ?> e = iter.next();
        String key = e.getKey();
        switch (key) {
          case ShardParams.REPLICA_RANDOM:
            // Only positive assertion of default status (i.e., default=true) is supported.
            // "random" is currently the implicit default, so explicitly configuring
            // "random" as default would not currently be useful, but if the implicit default
            // changes in the future, checkDefault could be relevant here.
            defaultRouting = checkDefaultReplicaListTransformer(getNamedList(e.getValue()), key, defaultRouting);
            break;
          case ShardParams.REPLICA_STABLE:
            NamedList<?> c = getNamedList(e.getValue());
            defaultRouting = checkDefaultReplicaListTransformer(c, key, defaultRouting);
            stableRltFactory = new AffinityReplicaListTransformerFactory(c);
            break;
          default:
            throw new IllegalArgumentException("invalid replica routing spec name: " + key);
        }
      } while (iter.hasNext());
    }
    if (stableRltFactory == null) {
      stableRltFactory = new AffinityReplicaListTransformerFactory();
    }
    if (ShardParams.REPLICA_STABLE.equals(defaultRouting)) {
      defaultRltFactory = stableRltFactory;
    } else {
      defaultRltFactory = RequestReplicaListTransformerGenerator.RANDOM_RLTF;
    }
    this.requestReplicaListTransformerGenerator = new RequestReplicaListTransformerGenerator(defaultRltFactory, stableRltFactory);
  }

  @Override
  public void init(PluginInfo info) {
    StringBuilder sb = new StringBuilder();
    NamedList args = info.initArgs;
    this.scheme = getParameter(args, INIT_URL_SCHEME, null,sb);
    if(StringUtils.endsWith(this.scheme, "://")) {
      this.scheme = StringUtils.removeEnd(this.scheme, "://");
    }

    String strategy = getParameter(args, "metricNameStrategy", UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY, sb);
    this.metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(strategy);
    if (this.metricNameStrategy == null)  {
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "Unknown metricNameStrategy: " + strategy + " found. Must be one of: " + KNOWN_METRIC_NAME_STRATEGIES.keySet());
    }

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
    this.whitelistHostChecker = new WhitelistHostChecker(args == null? null: (String) args.get(INIT_SHARDS_WHITELIST), !getDisableShardsWhitelist());
    log.info("Host whitelist initialized: {}", this.whitelistHostChecker);

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

    this.httpListenerFactory = new InstrumentedHttpListenerFactory(this.metricNameStrategy);
    int connectionTimeout = getParameter(args, HttpClientUtil.PROP_CONNECTION_TIMEOUT,
        HttpClientUtil.DEFAULT_CONNECT_TIMEOUT, sb);
    int maxConnectionsPerHost = getParameter(args, HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST,
        HttpClientUtil.DEFAULT_MAXCONNECTIONSPERHOST, sb);
    int soTimeout = getParameter(args, HttpClientUtil.PROP_SO_TIMEOUT,
        HttpClientUtil.DEFAULT_SO_TIMEOUT, sb);

    this.defaultClient = new Http2SolrClient.Builder()
        .connectionTimeout(connectionTimeout)
        .idleTimeout(soTimeout)
        .maxConnectionsPerHost(maxConnectionsPerHost).build();
    this.defaultClient.addListenerFactory(this.httpListenerFactory);
    this.loadbalancer = new LBHttp2SolrClient(defaultClient);

    initReplicaListTransformers(getParameter(args, "replicaRouting", null, sb));

    log.debug("created with {}",sb);
  }

  @Override
  public void setSecurityBuilder(HttpClientBuilderPlugin clientBuilderPlugin) {
    clientBuilderPlugin.setup(defaultClient);
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
          IOUtils.closeQuietly(defaultClient);
        }
      }
    }
    try {
      SolrMetricProducer.super.close();
    } catch (Exception e) {
      log.warn("Exception closing.", e);
    }
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  /**
   * Makes a request to one or more of the given urls, using the configured load balancer.
   *
   * @param req The solr search request that should be sent through the load balancer
   * @param urls The list of solr server urls to load balance across
   * @return The response from the request
   */
  public LBSolrClient.Rsp makeLoadBalancedRequest(final QueryRequest req, List<String> urls)
    throws SolrServerException, IOException {
    return loadbalancer.request(newLBHttpSolrClientReq(req, urls));
  }

  protected LBSolrClient.Req newLBHttpSolrClientReq(final QueryRequest req, List<String> urls) {
    int numServersToTry = (int)Math.floor(urls.size() * this.permittedLoadBalancerRequestsMaximumFraction);
    if (numServersToTry < this.permittedLoadBalancerRequestsMinimumAbsolute) {
      numServersToTry = this.permittedLoadBalancerRequestsMinimumAbsolute;
    }
    return new LBSolrClient.Req(req, urls, numServersToTry);
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

  protected ReplicaListTransformer getReplicaListTransformer(final SolrQueryRequest req) {
    final SolrParams params = req.getParams();
    final SolrCore core = req.getCore(); // explicit check for null core (temporary?, for tests)
    ZkController zkController = core == null ? null : core.getCoreContainer().getZkController();
    if (zkController != null) {
      return requestReplicaListTransformerGenerator.getReplicaListTransformer(
          params,
          zkController.getZkStateReader().getClusterProperties()
              .getOrDefault(ZkStateReader.DEFAULT_SHARD_PREFERENCES, "")
              .toString(),
          zkController.getNodeName(),
          zkController.getBaseUrl(),
          zkController.getSysPropsCacher()
      );
    } else {
      return requestReplicaListTransformerGenerator.getReplicaListTransformer(params);
    }
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
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    String expandedScope = SolrMetricManager.mkName(scope, SolrInfoBean.Category.QUERY.name());
    httpListenerFactory.initializeMetrics(solrMetricsContext, expandedScope);
    commExecutor = MetricUtils.instrumentedExecutorService(commExecutor, null,
        solrMetricsContext.getMetricRegistry(),
        SolrMetricManager.mkName("httpShardExecutor", expandedScope, "threadPool"));
  }

  /**
   * Class used to validate the hosts in the "shards" parameter when doing a distributed
   * request
   */
  public static class WhitelistHostChecker {

    /**
     * List of the whitelisted hosts. Elements in the list will be host:port (no protocol or context)
     */
    private final Set<String> whitelistHosts;

    /**
     * Indicates whether host checking is enabled
     */
    private final boolean whitelistHostCheckingEnabled;

    public WhitelistHostChecker(String whitelistStr, boolean enabled) {
      this.whitelistHosts = implGetShardsWhitelist(whitelistStr);
      this.whitelistHostCheckingEnabled = enabled;
    }

    final static Set<String> implGetShardsWhitelist(final String shardsWhitelist) {
      if (shardsWhitelist != null && !shardsWhitelist.isEmpty()) {
        return StrUtils.splitSmart(shardsWhitelist, ',')
            .stream()
            .map(String::trim)
            .map((hostUrl) -> {
              URL url;
              try {
                if (!hostUrl.startsWith("http://") && !hostUrl.startsWith("https://")) {
                  // It doesn't really matter which protocol we set here because we are not going to use it. We just need a full URL.
                  url = new URL("http://" + hostUrl);
                } else {
                  url = new URL(hostUrl);
                }
              } catch (MalformedURLException e) {
                throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid URL syntax in \"" + INIT_SHARDS_WHITELIST + "\": " + shardsWhitelist, e);
              }
              if (url.getHost() == null || url.getPort() < 0) {
                throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid URL syntax in \"" + INIT_SHARDS_WHITELIST + "\": " + shardsWhitelist);
              }
              return url.getHost() + ":" + url.getPort();
            }).collect(Collectors.toSet());
      }
      return null;
    }


    /**
     * @see #checkWhitelist(ClusterState, String, List)
     */
    protected void checkWhitelist(String shardsParamValue, List<String> shardUrls) {
      checkWhitelist(null, shardsParamValue, shardUrls);
    }

    /**
     * Checks that all the hosts for all the shards requested in shards parameter exist in the configured whitelist
     * or in the ClusterState (in case of cloud mode)
     *
     * @param clusterState The up to date ClusterState, can be null in case of non-cloud mode
     * @param shardsParamValue The original shards parameter
     * @param shardUrls The list of cores generated from the shards parameter.
     */
    protected void checkWhitelist(ClusterState clusterState, String shardsParamValue, List<String> shardUrls) {
      if (!whitelistHostCheckingEnabled) {
        return;
      }
      Set<String> localWhitelistHosts;
      if (whitelistHosts == null && clusterState != null) {
        // TODO: We could implement caching, based on the version of the live_nodes znode
        localWhitelistHosts = generateWhitelistFromLiveNodes(clusterState);
      } else if (whitelistHosts != null) {
        localWhitelistHosts = whitelistHosts;
      } else {
        localWhitelistHosts = Collections.emptySet();
      }

      shardUrls.stream().map(String::trim).forEach((shardUrl) -> {
        URL url;
        try {
          if (!shardUrl.startsWith("http://") && !shardUrl.startsWith("https://")) {
            // It doesn't really matter which protocol we set here because we are not going to use it. We just need a full URL.
            url = new URL("http://" + shardUrl);
          } else {
            url = new URL(shardUrl);
          }
        } catch (MalformedURLException e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid URL syntax in \"shards\" parameter: " + shardsParamValue, e);
        }
        if (url.getHost() == null || url.getPort() < 0) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid URL syntax in \"shards\" parameter: " + shardsParamValue);
        }
        if (!localWhitelistHosts.contains(url.getHost() + ":" + url.getPort())) {
          log.warn("The '"+ShardParams.SHARDS+"' parameter value '"+shardsParamValue+"' contained value(s) not on the shards whitelist ("+localWhitelistHosts+"), shardUrl:" + shardUrl);
          throw new SolrException(ErrorCode.FORBIDDEN,
              "The '"+ShardParams.SHARDS+"' parameter value '"+shardsParamValue+"' contained value(s) not on the shards whitelist. shardUrl:" + shardUrl + "." +
                  HttpShardHandlerFactory.SET_SOLR_DISABLE_SHARDS_WHITELIST_CLUE);
        }
      });
    }
    
    Set<String> generateWhitelistFromLiveNodes(ClusterState clusterState) {
      return clusterState
          .getLiveNodes()
          .stream()
          .map((liveNode) -> liveNode.substring(0, liveNode.indexOf('_')))
          .collect(Collectors.toSet());
    }
    
    public boolean hasExplicitWhitelist() {
      return this.whitelistHosts != null;
    }
    
    public boolean isWhitelistHostCheckingEnabled() {
      return whitelistHostCheckingEnabled;
    }
    
    /**
     * Only to be used by tests
     */
    @VisibleForTesting
    Set<String> getWhitelistHosts() {
      return this.whitelistHosts;
    }

    @Override
    public String toString() {
      return "WhitelistHostChecker [whitelistHosts=" + whitelistHosts + ", whitelistHostCheckingEnabled="
          + whitelistHostCheckingEnabled + "]";
    }
    
  }
  
}
