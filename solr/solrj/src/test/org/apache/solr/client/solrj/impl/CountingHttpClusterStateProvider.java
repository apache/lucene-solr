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
package org.apache.solr.client.solrj.impl;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"unchecked"})
public class CountingHttpClusterStateProvider extends BaseHttpClusterStateProvider {

  private final HttpClient httpClient;
  private final boolean clientIsInternal;

  private final AtomicInteger counter = new AtomicInteger(0);

  public CountingHttpClusterStateProvider(List<String> solrUrls, HttpClient httpClient) throws Exception {
    this.httpClient = httpClient == null ? HttpClientUtil.createClient(null) : httpClient;
    this.clientIsInternal = httpClient == null;
    init(solrUrls);
  }

  @Override
  protected SolrClient getSolrClient(String baseUrl) {
    return new AssertingHttpSolrClient(new HttpSolrClient.Builder().withBaseSolrUrl(baseUrl).withHttpClient(httpClient));
  }

  @Override
  public void close() throws IOException {
    if (this.clientIsInternal && this.httpClient != null) {
      HttpClientUtil.close(httpClient);
    }
  }

  public int getRequestCount() {
    return counter.get();
  }

  class AssertingHttpSolrClient extends HttpSolrClient {
    public AssertingHttpSolrClient(Builder builder) {
      super(builder);
    }

    @Override
    public NamedList<Object> request(@SuppressWarnings({"rawtypes"}) SolrRequest request, ResponseParser processor, String collection) throws SolrServerException, IOException {
      new Exception().printStackTrace();
      counter.incrementAndGet();
      return super.request(request, processor, collection);
    }
  }
}
