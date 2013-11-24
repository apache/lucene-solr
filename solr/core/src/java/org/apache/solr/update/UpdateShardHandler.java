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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.core.ConfigSolr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateShardHandler {
  
  private static Logger log = LoggerFactory.getLogger(UpdateShardHandler.class);
  
  private ExecutorService updateExecutor = Executors.newCachedThreadPool(
      new SolrjNamedThreadFactory("updateExecutor"));
  
  private PoolingClientConnectionManager clientConnectionManager;
  
  private final HttpClient client;

  public UpdateShardHandler(ConfigSolr cfg) {
    
    clientConnectionManager = new PoolingClientConnectionManager();
    clientConnectionManager.setDefaultMaxPerRoute(cfg.getMaxUpdateConnections());
    clientConnectionManager.setDefaultMaxPerRoute(cfg.getMaxUpdateConnectionsPerHost());
    
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_SO_TIMEOUT, cfg.getDistributedSocketTimeout());
    params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, cfg.getDistributedConnectionTimeout());
    params.set(HttpClientUtil.PROP_USE_RETRY, false);
    client = HttpClientUtil.createClient(params, clientConnectionManager);
  }
  
  
  public HttpClient getHttpClient() {
    return client;
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
    } catch (Throwable e) {
      SolrException.log(log, e);
    } finally {
      clientConnectionManager.shutdown();
    }
  }

}
