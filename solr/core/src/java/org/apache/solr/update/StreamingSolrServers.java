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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.update.SolrCmdDistributor.Error;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingSolrServers {
  public static Logger log = LoggerFactory.getLogger(StreamingSolrServers.class);
  
  private HttpClient httpClient;

  
  private Map<String,ConcurrentUpdateSolrServer> solrServers = new HashMap<String,ConcurrentUpdateSolrServer>();
  private List<Error> errors = Collections.synchronizedList(new ArrayList<Error>());

  private ExecutorService updateExecutor;

  public StreamingSolrServers(UpdateShardHandler updateShardHandler) {
    this.updateExecutor = updateShardHandler.getUpdateExecutor();
    
    httpClient = updateShardHandler.getHttpClient();
  }

  public List<Error> getErrors() {
    return errors;
  }
  
  public void clearErrors() {
    errors.clear();
  }

  public synchronized SolrServer getSolrServer(final SolrCmdDistributor.Req req) {
    String url = getFullUrl(req.node.getUrl());
    ConcurrentUpdateSolrServer server = solrServers.get(url);
    if (server == null) {
      server = new ConcurrentUpdateSolrServer(url, httpClient, 100, 1, updateExecutor, true) {
        @Override
        public void handleError(Throwable ex) {
          log.error("error", ex);
          Error error = new Error();
          error.e = (Exception) ex;
          if (ex instanceof SolrException) {
            error.statusCode = ((SolrException) ex).code();
          }
          error.req = req;
          errors.add(error);
        }
      };
      server.setParser(new BinaryResponseParser());
      server.setRequestWriter(new BinaryRequestWriter());
      server.setPollQueueTime(0);
      Set<String> queryParams = new HashSet<String>(2);
      queryParams.add(DistributedUpdateProcessor.DISTRIB_FROM);
      queryParams.add(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM);
      server.setQueryParams(queryParams);
      solrServers.put(url, server);
    }

    return server;
  }

  public synchronized void blockUntilFinished() {
    for (ConcurrentUpdateSolrServer server : solrServers.values()) {
      server.blockUntilFinished();
    }
  }
  
  public synchronized void shutdown() {
    for (ConcurrentUpdateSolrServer server : solrServers.values()) {
      server.shutdown();
    }
  }
  
  private String getFullUrl(String url) {
    String fullUrl;
    if (!url.startsWith("http://") && !url.startsWith("https://")) {
      fullUrl = "http://" + url;
    } else {
      fullUrl = url;
    }
    return fullUrl;
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }
}
