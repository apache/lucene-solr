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
package org.apache.solr.update;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.util.stats.HttpClientMetricNameStrategy;
import org.apache.solr.util.stats.InstrumentedHttpListenerFactory;
import org.apache.solr.util.stats.InstrumentedHttpRequestExecutor;
import org.apache.solr.util.stats.InstrumentedPoolingHttpClientConnectionManager;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.util.stats.InstrumentedHttpRequestExecutor.KNOWN_METRIC_NAME_STRATEGIES;

public class UpdateShardHandler implements SolrMetricProducer, SolrInfoBean {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /*
   * A downside to configuring an upper bound will be big update reorders (when that upper bound is hit)
   * and then undetected shard inconsistency as a result.
   * Therefore this thread pool is left unbounded. See SOLR-8205
   */
  private ExecutorService updateExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(0, Integer.MAX_VALUE,
      60L, TimeUnit.SECONDS,
      new SynchronousQueue<>(),
      new SolrNamedThreadFactory("updateExecutor"),
      // the Runnable added to this executor handles all exceptions so we disable stack trace collection as an optimization
      // see SOLR-11880 for more details
      false);
  
  private ExecutorService recoveryExecutor;
  
  private final Http2SolrClient updateOnlyClient;
  
  private final CloseableHttpClient recoveryOnlyClient;
  
  private final CloseableHttpClient defaultClient;

  private final InstrumentedPoolingHttpClientConnectionManager updateOnlyConnectionManager;
  
  private final InstrumentedPoolingHttpClientConnectionManager recoveryOnlyConnectionManager;
  
  private final InstrumentedPoolingHttpClientConnectionManager defaultConnectionManager;

  private final InstrumentedHttpRequestExecutor httpRequestExecutor;

  private final InstrumentedHttpListenerFactory updateHttpListenerFactory;


  private final Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private SolrMetricsContext solrMetricsContext;

  private int socketTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;
  private int connectionTimeout = HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;

  public UpdateShardHandler(UpdateShardHandlerConfig cfg) {
    updateOnlyConnectionManager = new InstrumentedPoolingHttpClientConnectionManager(HttpClientUtil.getSocketFactoryRegistryProvider().getSocketFactoryRegistry());
    recoveryOnlyConnectionManager = new InstrumentedPoolingHttpClientConnectionManager(HttpClientUtil.getSocketFactoryRegistryProvider().getSocketFactoryRegistry());
    defaultConnectionManager = new InstrumentedPoolingHttpClientConnectionManager(HttpClientUtil.getSocketFactoryRegistryProvider().getSocketFactoryRegistry());
    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    if (cfg != null ) {
      updateOnlyConnectionManager.setMaxTotal(cfg.getMaxUpdateConnections());
      updateOnlyConnectionManager.setDefaultMaxPerRoute(cfg.getMaxUpdateConnectionsPerHost());
      recoveryOnlyConnectionManager.setMaxTotal(cfg.getMaxUpdateConnections());
      recoveryOnlyConnectionManager.setDefaultMaxPerRoute(cfg.getMaxUpdateConnectionsPerHost());
      defaultConnectionManager.setMaxTotal(cfg.getMaxUpdateConnections());
      defaultConnectionManager.setDefaultMaxPerRoute(cfg.getMaxUpdateConnectionsPerHost());
      clientParams.set(HttpClientUtil.PROP_SO_TIMEOUT, cfg.getDistributedSocketTimeout());
      clientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, cfg.getDistributedConnectionTimeout());
      // following is done only for logging complete configuration.
      // The maxConnections and maxConnectionsPerHost have already been specified on the connection manager
      clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS, cfg.getMaxUpdateConnections());
      clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, cfg.getMaxUpdateConnectionsPerHost());
      socketTimeout = cfg.getDistributedSocketTimeout();
      connectionTimeout = cfg.getDistributedConnectionTimeout();
    }
    log.debug("Created default UpdateShardHandler HTTP client with params: {}", clientParams);

    httpRequestExecutor = new InstrumentedHttpRequestExecutor(getMetricNameStrategy(cfg));
    updateHttpListenerFactory = new InstrumentedHttpListenerFactory(getNameStrategy(cfg));
    recoveryOnlyClient = HttpClientUtil.createClient(clientParams, recoveryOnlyConnectionManager, false, httpRequestExecutor);
    defaultClient = HttpClientUtil.createClient(clientParams, defaultConnectionManager, false, httpRequestExecutor);

    Http2SolrClient.Builder updateOnlyClientBuilder = new Http2SolrClient.Builder();
    if (cfg != null) {
      updateOnlyClientBuilder
          .connectionTimeout(cfg.getDistributedConnectionTimeout())
          .idleTimeout(cfg.getDistributedSocketTimeout())
          .maxConnectionsPerHost(cfg.getMaxUpdateConnectionsPerHost());
    }
    updateOnlyClient = updateOnlyClientBuilder.build();
    updateOnlyClient.addListenerFactory(updateHttpListenerFactory);
    Set<String> queryParams = new HashSet<>(2);
    queryParams.add(DistributedUpdateProcessor.DISTRIB_FROM);
    queryParams.add(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM);
    updateOnlyClient.setQueryParams(queryParams);

    ThreadFactory recoveryThreadFactory = new SolrNamedThreadFactory("recoveryExecutor");
    if (cfg != null && cfg.getMaxRecoveryThreads() > 0) {
      if (log.isDebugEnabled()) {
        log.debug("Creating recoveryExecutor with pool size {}", cfg.getMaxRecoveryThreads());
      }
      recoveryExecutor = ExecutorUtil.newMDCAwareFixedThreadPool(cfg.getMaxRecoveryThreads(), recoveryThreadFactory);
    } else {
      log.debug("Creating recoveryExecutor with unbounded pool");
      recoveryExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(recoveryThreadFactory);
    }
  }

  private HttpClientMetricNameStrategy getMetricNameStrategy(UpdateShardHandlerConfig cfg) {
    HttpClientMetricNameStrategy metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY);
    if (cfg != null)  {
      metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(cfg.getMetricNameStrategy());
      if (metricNameStrategy == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unknown metricNameStrategy: " + cfg.getMetricNameStrategy() + " found. Must be one of: " + KNOWN_METRIC_NAME_STRATEGIES.keySet());
      }
    }
    return metricNameStrategy;
  }

  private InstrumentedHttpListenerFactory.NameStrategy getNameStrategy(UpdateShardHandlerConfig cfg) {
    InstrumentedHttpListenerFactory.NameStrategy nameStrategy =
        InstrumentedHttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES.get(UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY);

    if (cfg != null)  {
      nameStrategy = InstrumentedHttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES.get(cfg.getMetricNameStrategy());
      if (nameStrategy == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unknown metricNameStrategy: " + cfg.getMetricNameStrategy() + " found. Must be one of: " + KNOWN_METRIC_NAME_STRATEGIES.keySet());
      }
    }
    return nameStrategy;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    String expandedScope = SolrMetricManager.mkName(scope, getCategory().name());
    updateHttpListenerFactory.initializeMetrics(solrMetricsContext, expandedScope);
    defaultConnectionManager.initializeMetrics(solrMetricsContext, expandedScope);
    updateExecutor = MetricUtils.instrumentedExecutorService(updateExecutor, this, solrMetricsContext.getMetricRegistry(),
        SolrMetricManager.mkName("updateOnlyExecutor", expandedScope, "threadPool"));
    recoveryExecutor = MetricUtils.instrumentedExecutorService(recoveryExecutor, this, solrMetricsContext.getMetricRegistry(),
        SolrMetricManager.mkName("recoveryExecutor", expandedScope, "threadPool"));
  }

  @Override
  public String getDescription() {
    return "Metrics tracked by UpdateShardHandler related to distributed updates and recovery";
  }

  @Override
  public Category getCategory() {
    return Category.UPDATE;
  }

  @Override
  public Set<String> getMetricNames() {
    return metricNames;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  // if you are looking for a client to use, it's probably this one.
  public HttpClient getDefaultHttpClient() {
    return defaultClient;
  }
  
  // don't introduce a bug, this client is for sending updates only!
  public Http2SolrClient getUpdateOnlyHttpClient() {
    return updateOnlyClient;
  }
  
  // don't introduce a bug, this client is for recovery ops only!
  public HttpClient getRecoveryOnlyHttpClient() {
    return recoveryOnlyClient;
  }
  

   /**
   * This method returns an executor that is meant for non search related tasks.
   * 
   * @return an executor for update side related activities.
   */
  public ExecutorService getUpdateExecutor() {
    return updateExecutor;
  }

  public PoolingHttpClientConnectionManager getDefaultConnectionManager() {
    return defaultConnectionManager;
  }
  
  public PoolingHttpClientConnectionManager getRecoveryOnlyConnectionManager() {
    return recoveryOnlyConnectionManager;
  }

  /**
   * 
   * @return executor for recovery operations
   */
  public ExecutorService getRecoveryExecutor() {
    return recoveryExecutor;
  }

  public void close() {
    try {
      // do not interrupt, do not interrupt
      ExecutorUtil.shutdownAndAwaitTermination(updateExecutor);
      ExecutorUtil.shutdownAndAwaitTermination(recoveryExecutor);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        SolrMetricProducer.super.close();
      } catch (Exception e) {
        // do nothing
      }
      IOUtils.closeQuietly(updateOnlyClient);
      HttpClientUtil.close(recoveryOnlyClient);
      HttpClientUtil.close(defaultClient);
      defaultConnectionManager.close();
      recoveryOnlyConnectionManager.close();
    }
  }

  @VisibleForTesting
  public int getSocketTimeout() {
    return socketTimeout;
  }

  @VisibleForTesting
  public int getConnectionTimeout() {
    return connectionTimeout;
  }

  public void setSecurityBuilder(HttpClientBuilderPlugin builder) {
    builder.setup(updateOnlyClient);
  }
}
