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
import java.util.concurrent.ThreadFactory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.util.stats.HttpClientMetricNameStrategy;
import org.apache.solr.util.stats.InstrumentedHttpRequestExecutor;
import org.apache.solr.util.stats.InstrumentedPoolingHttpClientConnectionManager;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.util.stats.InstrumentedHttpRequestExecutor.KNOWN_METRIC_NAME_STRATEGIES;

public class UpdateShardHandler implements SolrInfoBean {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CloseTracker closeTracker;

  private final Http2SolrClient updateOnlyClient;

  private final Http2SolrClient searchOnlyClient;

  private final Http2SolrClient recoveryOnlyClient;

  private final CloseableHttpClient defaultClient;

  private ExecutorService recoveryExecutor;

  private final InstrumentedPoolingHttpClientConnectionManager defaultConnectionManager;

  private final InstrumentedHttpRequestExecutor httpRequestExecutor;

  //private final InstrumentedHttpListenerFactory updateHttpListenerFactory;


  private final Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private SolrMetricsContext solrMetricsContext;

  private int socketTimeout = HttpClientUtil.DEFAULT_SO_TIMEOUT;
  private int connectionTimeout = HttpClientUtil.DEFAULT_CONNECT_TIMEOUT;

  public UpdateShardHandler(UpdateShardHandlerConfig cfg) {
    assert ObjectReleaseTracker.track(this);
    assert (closeTracker = new CloseTracker()) != null;
    defaultConnectionManager = new InstrumentedPoolingHttpClientConnectionManager(HttpClientUtil.getSocketFactoryRegistryProvider().getSocketFactoryRegistry());
    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    if (cfg != null ) {
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
    //updateHttpListenerFactory = new InstrumentedHttpListenerFactory(getNameStrategy(cfg));

    defaultClient = HttpClientUtil.createClient(clientParams, defaultConnectionManager, true, httpRequestExecutor);

    Http2SolrClient.Builder updateOnlyClientBuilder = new Http2SolrClient.Builder();
    if (cfg != null) {
      updateOnlyClientBuilder
          .connectionTimeout(cfg.getDistributedConnectionTimeout())
          .idleTimeout(cfg.getDistributedSocketTimeout());
    }
    updateOnlyClient = updateOnlyClientBuilder.markInternalRequest()
        .maxRequestsQueuedPerDestination(12000).strictEventOrdering(false).build();
    updateOnlyClient.enableCloseLock();
   // updateOnlyClient.addListenerFactory(updateHttpListenerFactory);
    Set<String> queryParams = new HashSet<>(2);
    queryParams.add(DistributedUpdateProcessor.DISTRIB_FROM);
    queryParams.add(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM);
    updateOnlyClient.setQueryParams(queryParams);


    Http2SolrClient.Builder recoveryOnlyClientBuilder = new Http2SolrClient.Builder();
    recoveryOnlyClientBuilder = recoveryOnlyClientBuilder.connectionTimeout(5000).idleTimeout(30000);


    recoveryOnlyClient = recoveryOnlyClientBuilder.markInternalRequest().build();
    recoveryOnlyClient.enableCloseLock();

    Http2SolrClient.Builder searchOnlyClientBuilder = new Http2SolrClient.Builder();
    searchOnlyClientBuilder.connectionTimeout(5000).idleTimeout(60000);


    searchOnlyClient = recoveryOnlyClientBuilder.markInternalRequest().build();
    searchOnlyClient.enableCloseLock();


//    ThreadFactory recoveryThreadFactory = new SolrNamedThreadFactory("recoveryExecutor");
//    if (cfg != null && cfg.getMaxRecoveryThreads() > 0) {
//      if (log.isDebugEnabled()) {
//        log.debug("Creating recoveryExecutor with pool size {}", cfg.getMaxRecoveryThreads());
//      }
//      recoveryExecutor = ExecutorUtil.newMDCAwareFixedThreadPool(cfg.getMaxRecoveryThreads(), recoveryThreadFactory);
//    } else {

      recoveryExecutor = ParWork.getRootSharedExecutor();
 //   }
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

//  private InstrumentedHttpListenerFactory.NameStrategy getNameStrategy(UpdateShardHandlerConfig cfg) {
//    InstrumentedHttpListenerFactory.NameStrategy nameStrategy =
//        InstrumentedHttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES.get(UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY);
//
//    if (cfg != null)  {
//      nameStrategy = InstrumentedHttpListenerFactory.KNOWN_METRIC_NAME_STRATEGIES.get(cfg.getMetricNameStrategy());
//      if (nameStrategy == null) {
//        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
//            "Unknown metricNameStrategy: " + cfg.getMetricNameStrategy() + " found. Must be one of: " + KNOWN_METRIC_NAME_STRATEGIES.keySet());
//      }
//    }
//    return nameStrategy;
//  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    String expandedScope = SolrMetricManager.mkName(scope, getCategory().name());
    //.initializeMetrics(solrMetricsContext, expandedScope);
    defaultConnectionManager.initializeMetrics(solrMetricsContext, expandedScope);
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
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  // if you are looking for a client to use, it's probably this one.
  public HttpClient getDefaultHttpClient() {
    return defaultClient;
  }
  
  // don't introduce a bug, this client is for sending updates only!
  public Http2SolrClient getTheSharedHttpClient() {
    return updateOnlyClient;
  }

  public Http2SolrClient getRecoveryOnlyClient() {
    return recoveryOnlyClient;
  }

  public Http2SolrClient getSearchOnlyClient() {
    return searchOnlyClient;
  }


  public PoolingHttpClientConnectionManager getDefaultConnectionManager() {
    return defaultConnectionManager;
  }

  /**
   * 
   * @return executor for recovery operations
   */
  public ExecutorService getRecoveryExecutor() {
    return recoveryExecutor;
  }

  public void close() {
    assert closeTracker.close();
    if (updateOnlyClient != null) updateOnlyClient.disableCloseLock();
    if (recoveryOnlyClient != null) recoveryOnlyClient.disableCloseLock();
    if (searchOnlyClient != null) searchOnlyClient.disableCloseLock();


    try (ParWork closer = new ParWork(this, true, true)) {
      closer.collect("", () -> {
        HttpClientUtil.close(defaultClient);
        return defaultClient;
      });
      closer.collect(recoveryOnlyClient);
      closer.collect(searchOnlyClient);
      closer.collect(updateOnlyClient);
      closer.collect(defaultConnectionManager);
      closer.collect("SolrInfoBean", () -> {
        SolrInfoBean.super.close();
        return this;
      });
    }
    assert ObjectReleaseTracker.release(this);
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
