/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.solr.handler.component;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.util.NamedThreadFactory;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IterativeMergeStrategy implements MergeStrategy  {

  protected ExecutorService executorService;
  protected static HttpClient httpClient;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public void merge(ResponseBuilder rb, ShardRequest sreq) {
    rb._responseDocs = new SolrDocumentList(); // Null pointers will occur otherwise.
    rb.onePassDistributedQuery = true;   // Turn off the second pass distributed.
    executorService =     ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("IterativeMergeStrategy"));
    try {
      process(rb, sreq);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      executorService.shutdownNow();
    }
  }

  public boolean mergesIds() {
    return true;
  }

  public int getCost() {
    return 0;
  }

  public boolean handlesMergeFields() {
    return false;
  }

  public void handleMergeFields(ResponseBuilder rb, SolrIndexSearcher searcher) {

  }

  public static class CallBack implements Callable<CallBack> {
    private HttpSolrClient solrClient;
    private QueryRequest req;
    private QueryResponse response;
    private ShardResponse originalShardResponse;

    public CallBack(ShardResponse originalShardResponse, QueryRequest req) {

      this.solrClient = new HttpSolrClient(originalShardResponse.getShardAddress(), getHttpClient());
      this.req = req;
      this.originalShardResponse = originalShardResponse;
      req.setMethod(SolrRequest.METHOD.POST);
      ModifiableSolrParams params = (ModifiableSolrParams)req.getParams();
      params.add("distrib", "false");
    }

    public QueryResponse getResponse() {
      return this.response;
    }

    public ShardResponse getOriginalShardResponse() {
      return this.originalShardResponse;
    }

    public CallBack call() throws Exception{
      this.response = req.process(solrClient);
      return this;
    }
  }

  public List<Future<CallBack>> callBack(List<ShardResponse> responses, QueryRequest req) {
    List<Future<CallBack>> futures = new ArrayList();
    for(ShardResponse response : responses) {
      futures.add(this.executorService.submit(new CallBack(response, req)));
    }
    return futures;
  }

  public Future<CallBack> callBack(ShardResponse response, QueryRequest req) {
    return this.executorService.submit(new CallBack(response, req));
  }

  protected abstract void process(ResponseBuilder rb, ShardRequest sreq) throws Exception;

  static synchronized HttpClient getHttpClient() {

      if(httpClient == null) {
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 128);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 32);
        httpClient = HttpClientUtil.createClient(params);
        return httpClient;
      } else {
        return httpClient;
      }
  }
}