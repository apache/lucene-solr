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
import java.util.concurrent.ExecutorService;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.cloud.RecoveryStrategy;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateShardHandler {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /*
   * A downside to configuring an upper bound will be big update reorders (when that upper bound is hit)
   * and then undetected shard inconsistency as a result.
   * This update executor is used for different things too... both update streams (which may be very long lived)
   * and control messages (peersync? LIR?) and could lead to starvation if limited.
   * Therefore this thread pool is left unbounded. See SOLR-8205
   */
  private ExecutorService updateExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(
      new SolrjNamedThreadFactory("updateExecutor"));
  
  private ExecutorService recoveryExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(
      new SolrjNamedThreadFactory("recoveryExecutor"));
  
  private final CloseableHttpClient client;

  private final PoolingHttpClientConnectionManager clientConnectionManager;

  public UpdateShardHandler(UpdateShardHandlerConfig cfg) {
    clientConnectionManager = new PoolingHttpClientConnectionManager(HttpClientUtil.getSchemaRegisteryProvider().getSchemaRegistry());
    if (cfg != null ) {
      clientConnectionManager.setMaxTotal(cfg.getMaxUpdateConnections());
      clientConnectionManager.setDefaultMaxPerRoute(cfg.getMaxUpdateConnectionsPerHost());
    }

    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    if (cfg != null)  {
      clientParams.set(HttpClientUtil.PROP_SO_TIMEOUT, cfg.getDistributedSocketTimeout());
      clientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, cfg.getDistributedConnectionTimeout());
    }
    client = HttpClientUtil.createClient(clientParams, clientConnectionManager);

    // following is done only for logging complete configuration.
    // The maxConnections and maxConnectionsPerHost have already been specified on the connection manager
    if (cfg != null)  {
      clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS, cfg.getMaxUpdateConnections());
      clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, cfg.getMaxUpdateConnectionsPerHost());
    }
    log.info("Created UpdateShardHandler HTTP client with params: {}", clientParams);
  }
  
  public HttpClient getHttpClient() {
    return client;
  }
  
  /**
   * This method returns an executor that is not meant for disk IO and that will
   * be interrupted on shutdown.
   * 
   * @return an executor for update related activities that do not do disk IO.
   */
  public ExecutorService getUpdateExecutor() {
    return updateExecutor;
  }
  

  public PoolingHttpClientConnectionManager getConnectionManager() {
    return clientConnectionManager;
  }

  /**
   * In general, RecoveryStrategy threads do not do disk IO, but they open and close SolrCores
   * in async threads, among other things, and can trigger disk IO, so we use this alternate 
   * executor rather than the 'updateExecutor', which is interrupted on shutdown.
   * 
   * @return executor for {@link RecoveryStrategy} thread which will not be interrupted on close.
   */
  public ExecutorService getRecoveryExecutor() {
    return recoveryExecutor;
  }

  public void close() {
    try {
      // we interrupt on purpose here, but this executor should not run threads that do disk IO!
      ExecutorUtil.shutdownWithInterruptAndAwaitTermination(updateExecutor);
      ExecutorUtil.shutdownAndAwaitTermination(recoveryExecutor);
    } catch (Exception e) {
      SolrException.log(log, e);
    } finally {
      HttpClientUtil.close(client);
      clientConnectionManager.close();
    }
  }

}
