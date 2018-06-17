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

import static org.apache.solr.util.stats.InstrumentedHttpRequestExecutor.KNOWN_METRIC_NAME_STRATEGIES;

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.util.SolrInternalHttpClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ExecutorUtil.MDCAwareThreadPoolExecutor;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.apache.solr.util.stats.HttpClientMetricNameStrategy;
import org.apache.solr.util.stats.InstrumentedHttpRequestExecutor;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

public class UpdateShardHandler implements SolrMetricProducer, SolrInfoBean {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
  

  private SolrInternalHttpClient httpClient;
  
  private Http2SolrClient updateSolrClient = null;
  
  private MDCAwareThreadPoolExecutor recoveryExecutor;

//  private final InstrumentedPoolingHttpClientConnectionManager updateOnlyConnectionManager;
  
//  private final InstrumentedPoolingHttpClientConnectionManager defaultConnectionManager;

  private final InstrumentedHttpRequestExecutor httpRequestExecutor;


  private final Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private MetricRegistry registry;

  private int origRecoveryMaxPoolSize;

  private ExecutorService updateExecutor;
  private boolean shutdownExecutor = false;

  public UpdateShardHandler(UpdateShardHandlerConfig cfg, SolrInternalHttpClient httpClient, ExecutorService updateExecutor) {
    //updateOnlyConnectionManager = new InstrumentedPoolingHttpClientConnectionManager(HttpClientUtil.getSchemaRegisteryProvider().getSchemaRegistry());
    //defaultConnectionManager = new InstrumentedPoolingHttpClientConnectionManager(HttpClientUtil.getSchemaRegisteryProvider().getSchemaRegistry());
    if (cfg != null ) {
//      updateOnlyConnectionManager.setMaxTotal(cfg.getMaxUpdateConnections());
//      updateOnlyConnectionManager.setDefaultMaxPerRoute(cfg.getMaxUpdateConnectionsPerHost());
//      defaultConnectionManager.setMaxTotal(cfg.getMaxUpdateConnections());
//      defaultConnectionManager.setDefaultMaxPerRoute(cfg.getMaxUpdateConnectionsPerHost());
    }
    this.updateExecutor = updateExecutor;
    
    if (this.updateExecutor == null) {
      shutdownExecutor = true;
      updateExecutor = new ExecutorUtil.MDCAwareThreadPoolExecutor(0, Integer.MAX_VALUE,
          5L, TimeUnit.SECONDS,
          new SynchronousQueue<>(),
          new SolrjNamedThreadFactory(CoreContainer.class.getSimpleName() + "Executor"),
          // the Runnable added to this executor handles all exceptions so we disable stack trace collection as an optimization
          // see SOLR-11880 for more details
          false);
    }
    
    assert httpClient != null;
    this.httpClient = httpClient;
    ModifiableSolrParams clientParams = new ModifiableSolrParams();
//    if (cfg != null)  {
//      clientParams.set(HttpClientUtil.PROP_SO_TIMEOUT, cfg.getDistributedSocketTimeout());
//      clientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, cfg.getDistributedConnectionTimeout());
//      socketTimeout = cfg.getDistributedSocketTimeout();
//      connectionTimeout = cfg.getDistributedConnectionTimeout();
//    }
    HttpClientMetricNameStrategy metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(UpdateShardHandlerConfig.DEFAULT_METRICNAMESTRATEGY);
    if (cfg != null)  {
      metricNameStrategy = KNOWN_METRIC_NAME_STRATEGIES.get(cfg.getMetricNameStrategy());
      if (metricNameStrategy == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Unknown metricNameStrategy: " + cfg.getMetricNameStrategy() + " found. Must be one of: " + KNOWN_METRIC_NAME_STRATEGIES.keySet());
      }
    }


    httpRequestExecutor = new InstrumentedHttpRequestExecutor(metricNameStrategy);
    //updateOnlyClient = HttpClientUtil.createClient(clientParams, updateOnlyConnectionManager, false, httpRequestExecutor);
    //defaultClient = HttpClientUtil.createClient(clientParams, defaultConnectionManager, false, httpRequestExecutor);

    updateSolrClient = new Http2SolrClient.Builder("")
        .withHttpClient(httpClient).solrInternal()
        .build();
    // client.setParser(new BinaryResponseParser());
    // client.setRequestWriter(new BinaryRequestWriter());
    Set<String> queryParams = new HashSet<>(2);
    queryParams.add(DistributedUpdateProcessor.DISTRIB_FROM);
    queryParams.add(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM);
    updateSolrClient.setQueryParams(queryParams);

    // following is done only for logging complete configuration.
    // The maxConnections and maxConnectionsPerHost have already been specified on the connection manager
    if (cfg != null)  {
      clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS, cfg.getMaxUpdateConnections());
      clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, cfg.getMaxUpdateConnectionsPerHost());
    }
    log.debug("Created default UpdateShardHandler HTTP client with params: {}", clientParams);
    log.debug("Created update only UpdateShardHandler HTTP client with params: {}", clientParams);

    ThreadFactory recoveryThreadFactory = new SolrjNamedThreadFactory("recoveryExecutor");
    if (cfg != null && cfg.getMaxRecoveryThreads() > 0) {
      log.debug("Creating recoveryExecutor with pool size {}", cfg.getMaxRecoveryThreads());
      recoveryExecutor = new MDCAwareThreadPoolExecutor(0, cfg.getMaxRecoveryThreads(),
          5L, TimeUnit.SECONDS,
          new SynchronousQueue<>(),
          recoveryThreadFactory);
      origRecoveryMaxPoolSize = cfg.getMaxRecoveryThreads();
    } else {
      log.debug("Creating recoveryExecutor with unbounded pool");
      recoveryExecutor = new MDCAwareThreadPoolExecutor(0, 100,
          5L, TimeUnit.SECONDS,
          new ArrayBlockingQueue<Runnable>(1000),
          recoveryThreadFactory);
      origRecoveryMaxPoolSize = 100;
    }
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  @Override
  public void initializeMetrics(SolrMetricManager manager, String registryName, String tag, String scope) {
    registry = manager.registry(registryName);
    String expandedScope = SolrMetricManager.mkName(scope, getCategory().name());
    //updateOnlyConnectionManager.initializeMetrics(manager, registryName, tag, expandedScope);
    //defaultConnectionManager.initializeMetrics(manager, registryName, tag, expandedScope);
    updateExecutor = MetricUtils.instrumentedExecutorService(updateExecutor, this, registry,
        SolrMetricManager.mkName("updateOnlyExecutor", expandedScope, "threadPool"));
    // nocommit - no metrics 
    //recoveryExecutor = MetricUtils.instrumentedExecutorService(recoveryExecutor, this, registry,
    //    SolrMetricManager.mkName("recoveryExecutor", expandedScope, "threadPool"));
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
  public MetricRegistry getMetricRegistry() {
    return registry;
  }

  // if you are looking for a client to use, it's probably this one.
  public SolrInternalHttpClient getDefaultHttpClient() {
    return httpClient;
  }
//  
//  // don't introduce a bug, this client is for sending updates only!
//  public HttpClient getUpdateOnlyHttpClient() {
//    return defaultClient;
//  }
  
  public Http2SolrClient getUpdateSolrClient() {
    return updateSolrClient;
  }
  
   /**
   * This method returns an executor that is meant for non search related tasks.
   * 
   * @return an executor for update side related activities.
   */
  public ExecutorService getUpdateExecutor() {
    return updateExecutor;
  }
  
  /**
   * 
   * @return executor for recovery operations
   */
  public ExecutorService getRecoveryExecutor() {
    return recoveryExecutor;
  }
  
  public void sizeRecoveryExecutorForLoad() {
    double load =  ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
    double sLoad = load / (double)PROC_COUNT;
    if (sLoad > 0.95D) {
      int cMax = recoveryExecutor.getMaximumPoolSize();
      if (cMax > 2) {
        recoveryExecutor.setMaximumPoolSize(Math.max(2, (int) ((double)cMax * 0.60D)));
      }
    } else if (sLoad < 0.7D && origRecoveryMaxPoolSize != recoveryExecutor.getMaximumPoolSize()) {
      recoveryExecutor.setMaximumPoolSize(origRecoveryMaxPoolSize);
    }
    System.out.println("recovery request, load:" + load);
  }

  public void close() {
    try {
      // do not interrupt, do not interrupt
      ExecutorUtil.shutdownAndAwaitTermination(recoveryExecutor);
      if (shutdownExecutor) {
        ExecutorUtil.shutdownAndAwaitTermination(updateExecutor);
      }
    } catch (Exception e) {
      SolrException.log(log, e);
    } finally {
      IOUtils.closeQuietly(updateSolrClient);
    }
  }

}
