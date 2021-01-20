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

import java.io.IOException;
import java.util.List;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;

@SuppressWarnings({"unchecked"})
public class HttpClusterStateProvider extends BaseHttpClusterStateProvider {

  private final HttpClient httpClient;
  private final boolean clientIsInternal;

  public HttpClusterStateProvider(List<String> solrUrls, HttpClient httpClient) throws Exception {
    this.httpClient = httpClient == null? HttpClientUtil.createClient(null): httpClient;
    this.clientIsInternal = httpClient == null;
    init(solrUrls);
  }

  @Override
  protected SolrClient getSolrClient(String baseUrl) {
    return new HttpSolrClient.Builder().withBaseSolrUrl(baseUrl).withHttpClient(httpClient).build();
  }

  @Override
  public void close() throws IOException {
    if (this.clientIsInternal && this.httpClient != null) {
      HttpClientUtil.close(httpClient);
    }
  }
}
