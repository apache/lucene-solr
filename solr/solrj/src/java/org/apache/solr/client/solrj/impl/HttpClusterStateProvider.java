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
import org.apache.solr.common.util.ObjectReleaseTracker;

public class HttpClusterStateProvider extends BaseHttpClusterStateProvider {

  private final HttpClient httpClient;
  private final boolean clientIsInternal;

  public HttpClusterStateProvider(List<String> solrUrls, HttpClient httpClient) throws Exception {
    assert ObjectReleaseTracker.track(this);
    if (httpClient == null) {
      this.clientIsInternal = true;
      this.httpClient = null;
    } else {
      this.clientIsInternal = false;
      this.httpClient = null;
    }
    init(solrUrls);
  }

  @Override
  protected SolrClient getSolrClient(String baseUrl) {
    return new HttpSolrClient.Builder().withBaseSolrUrl(baseUrl).withHttpClient(httpClient).markInternalRequest().build();
  }

  @Override
  public void close() throws IOException {
    assert ObjectReleaseTracker.release(this);
  }
}
