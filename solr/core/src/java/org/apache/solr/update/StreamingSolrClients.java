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

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.update.SolrCmdDistributor.Error;
import org.eclipse.jetty.client.api.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingSolrClients {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final int runnerCount = Integer.getInteger("solr.cloud.replication.runners", 1);
  // should be less than solr.jetty.http.idleTimeout
  private final int pollQueueTime = Integer.getInteger("solr.cloud.client.pollQueueTime", 10000);

  private Http2SolrClient httpClient;

  private Map<String, ConcurrentUpdateHttp2SolrClient> solrClients = new HashMap<>();
  private List<Error> errors = Collections.synchronizedList(new ArrayList<Error>());

  private ExecutorService updateExecutor;

  public StreamingSolrClients(UpdateShardHandler updateShardHandler) {
    this.updateExecutor = updateShardHandler.getUpdateExecutor();
    this.httpClient = updateShardHandler.getUpdateOnlyHttpClient();
  }

  public List<Error> getErrors() {
    return errors;
  }

  public void clearErrors() {
    errors.clear();
  }

  public synchronized SolrClient getSolrClient(final SolrCmdDistributor.Req req) {
    String url = getFullUrl(req.node.getUrl());
    ConcurrentUpdateHttp2SolrClient client = solrClients.get(url);
    if (client == null) {
      // NOTE: increasing to more than 1 threadCount for the client could cause updates to be reordered
      // on a greater scale since the current behavior is to only increase the number of connections/Runners when
      // the queue is more than half full.
      client = new ErrorReportingConcurrentUpdateSolrClient.Builder(url, httpClient, req, errors)
          .withQueueSize(100)
          .withThreadCount(runnerCount)
          .withExecutorService(updateExecutor)
          .alwaysStreamDeletes()
          .build();
      client.setPollQueueTime(pollQueueTime); // minimize connections created
      solrClients.put(url, client);
    }

    return client;
  }

  public synchronized void blockUntilFinished() throws IOException {
    for (ConcurrentUpdateHttp2SolrClient client : solrClients.values()) {
      client.blockUntilFinished();
    }
  }

  public synchronized void shutdown() {
    for (ConcurrentUpdateHttp2SolrClient client : solrClients.values()) {
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

  public Http2SolrClient getHttpClient() {
    return httpClient;
  }

  public ExecutorService getUpdateExecutor() {
    return updateExecutor;
  }
}

class ErrorReportingConcurrentUpdateSolrClient extends ConcurrentUpdateHttp2SolrClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrCmdDistributor.Req req;
  private final List<Error> errors;

  public ErrorReportingConcurrentUpdateSolrClient(Builder builder) {
    super(builder);
    this.req = builder.req;
    this.errors = builder.errors;
  }

  @Override
  public void handleError(Throwable ex) {
    log.error("Error when calling {} to {}", req, req.node.getUrl(), ex);
    Error error = new Error();
    error.e = (Exception) ex;
    if (ex instanceof SolrException) {
      error.statusCode = ((SolrException) ex).code();
    }
    error.req = req;
    errors.add(error);
    if (!req.shouldRetry(error)) {
      // only track the error if we are not retrying the request
      req.trackRequestResult(null, null, false);
    }
  }
  @Override
  public void onSuccess(Response resp, InputStream respBody) {
    req.trackRequestResult(resp, respBody, true);
  }

  static class Builder extends ConcurrentUpdateHttp2SolrClient.Builder {
    protected SolrCmdDistributor.Req req;
    protected List<Error> errors;

    public Builder(String baseSolrUrl, Http2SolrClient client, SolrCmdDistributor.Req req, List<Error> errors) {
      super(baseSolrUrl, client);
      this.req = req;
      this.errors = errors;
    }

    public ErrorReportingConcurrentUpdateSolrClient build() {
      return new ErrorReportingConcurrentUpdateSolrClient(this);
    }
  }
}
