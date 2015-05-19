package org.apache.solr.update;

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

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.impl.conn.SchemeRegistryFactory;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.core.NodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UpdateShardHandler {
  
  private static Logger log = LoggerFactory.getLogger(UpdateShardHandler.class);
  
  private ExecutorService updateExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(
      new SolrjNamedThreadFactory("updateExecutor"));
  
  private PoolingClientConnectionManager clientConnectionManager;
  
  private final CloseableHttpClient client;

  private final UpdateShardHandlerConfig cfg;

  @Deprecated
  public UpdateShardHandler(NodeConfig cfg) {
    this(cfg.getUpdateShardHandlerConfig());
  }

  public UpdateShardHandler(UpdateShardHandlerConfig cfg) {
    this.cfg = cfg;
    clientConnectionManager = new PoolingClientConnectionManager(SchemeRegistryFactory.createSystemDefault());
    if (cfg != null ) {
      clientConnectionManager.setMaxTotal(cfg.getMaxUpdateConnections());
      clientConnectionManager.setDefaultMaxPerRoute(cfg.getMaxUpdateConnectionsPerHost());
    }

    ModifiableSolrParams clientParams = getClientParams();
    log.info("Creating UpdateShardHandler HTTP client with params: {}", clientParams);
    client = HttpClientUtil.createClient(clientParams, clientConnectionManager);
  }

  protected ModifiableSolrParams getClientParams() {
    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    if (cfg != null) {
      clientParams.set(HttpClientUtil.PROP_SO_TIMEOUT,
          cfg.getDistributedSocketTimeout());
      clientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT,
          cfg.getDistributedConnectionTimeout());
    }
    // in the update case, we want to do retries, and to use
    // the default Solr retry handler that createClient will 
    // give us
    clientParams.set(HttpClientUtil.PROP_USE_RETRY, true);
    return clientParams;
  }
  
  
  public HttpClient getHttpClient() {
    return client;
  }

  public void reconfigureHttpClient(HttpClientConfigurer configurer) {
    log.info("Reconfiguring the default client with: " + configurer);
    synchronized (client) {
      configurer.configure((DefaultHttpClient)client, getClientParams());
    }
  }

  public ClientConnectionManager getConnectionManager() {
    return clientConnectionManager;
  }
  
  public ExecutorService getUpdateExecutor() {
    return updateExecutor;
  }

  public void close() {
    try {
      ExecutorUtil.shutdownAndAwaitTermination(updateExecutor);
    } catch (Exception e) {
      SolrException.log(log, e);
    } finally {
      IOUtils.closeQuietly(client);
      clientConnectionManager.shutdown();
    }
  }

}
