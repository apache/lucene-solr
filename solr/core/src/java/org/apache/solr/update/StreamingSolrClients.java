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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.update.SolrCmdDistributor.Error;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.DistributingUpdateProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingSolrClients {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final int runnerCount = Integer.getInteger("solr.cloud.replication.runners", 1);
  
  private HttpClient httpClient;
  
  private Map<String, ConcurrentUpdateSolrClient> solrClients = new HashMap<>();
  private List<Error> errors = Collections.synchronizedList(new ArrayList<Error>());

  private ExecutorService updateExecutor;

  public StreamingSolrClients(UpdateShardHandler updateShardHandler) {
    this.updateExecutor = updateShardHandler.getUpdateExecutor();
    
    httpClient = updateShardHandler.getHttpClient();
  }

  public List<Error> getErrors() {
    return errors;
  }
  
  public void clearErrors() {
    errors.clear();
  }

  public synchronized SolrClient getSolrClient(final SolrCmdDistributor.Req req) {
    String url = getFullUrl(req.node.getUrl());
    ConcurrentUpdateSolrClient client = solrClients.get(url);
    if (client == null) {
      // NOTE: increasing to more than 1 threadCount for the client could cause updates to be reordered
      // on a greater scale since the current behavior is to only increase the number of connections/Runners when
      // the queue is more than half full.
      client = new ErrorReportingConcurrentUpdateSolrClient(url, httpClient, 100, runnerCount, updateExecutor, true, req);
      client.setPollQueueTime(Integer.MAX_VALUE); // minimize connections created
      client.setParser(new BinaryResponseParser());
      client.setRequestWriter(new BinaryRequestWriter());
      Set<String> queryParams = new HashSet<>(2);
      queryParams.add(DistributedUpdateProcessor.DISTRIB_FROM);
      queryParams.add(DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM);
      client.setQueryParams(queryParams);
      solrClients.put(url, client);
    }

    return client;
  }

  public synchronized void blockUntilFinished() {
    for (ConcurrentUpdateSolrClient client : solrClients.values()) {
      client.blockUntilFinished();
    }
  }
  
  public synchronized void shutdown() {
    for (ConcurrentUpdateSolrClient client : solrClients.values()) {
      client.close();
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
  
  public ExecutorService getUpdateExecutor() {
    return updateExecutor;
  }
  
  class ErrorReportingConcurrentUpdateSolrClient extends ConcurrentUpdateSolrClient {
    private final SolrCmdDistributor.Req req;
    
    public ErrorReportingConcurrentUpdateSolrClient(String solrServerUrl, HttpClient client, int queueSize,
        int threadCount, ExecutorService es, boolean streamDeletes, SolrCmdDistributor.Req req) {
      super(solrServerUrl, client, queueSize, threadCount, es, streamDeletes);
      this.req = req;
    }
    
    @Override
    public void handleError(Throwable ex) {
      req.trackRequestResult(null, false);
      log.error("error", ex);
      Error error = new Error();
      error.e = (Exception) ex;
      if (ex instanceof SolrException) {
        error.statusCode = ((SolrException) ex).code();
      }
      error.req = req;
      errors.add(error);
    }
    @Override
    public void onSuccess(HttpResponse resp) {
      req.trackRequestResult(resp, true);
    }
  }
}
