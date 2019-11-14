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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;

public class DelegationTokenHttpSolrClient extends HttpSolrClient {
  public final static String DELEGATION_TOKEN_PARAM = "delegation";

  /**
   * Package protected constructor for use by 
   * {@linkplain org.apache.solr.client.solrj.impl.HttpSolrClient.Builder}.
   * @lucene.internal
   * 
   * @deprecated use {@link DelegationTokenHttpSolrClient#DelegationTokenHttpSolrClient(HttpSolrClient.Builder)} instead, as it is a more
   * extension/subclassing-friendly alternative
   */
  @Deprecated
  DelegationTokenHttpSolrClient(String baseURL,
                                HttpClient client,
                                ResponseParser parser,
                                boolean allowCompression,
                                String delegationToken) {
    super(baseURL, client, parser, allowCompression);
    if (delegationToken == null) {
      throw new IllegalArgumentException("Delegation token cannot be null");
    }
    setQueryParams(new TreeSet<>(Arrays.asList(DELEGATION_TOKEN_PARAM)));
    invariantParams = new ModifiableSolrParams();
    invariantParams.set(DELEGATION_TOKEN_PARAM, delegationToken);
  }
  
  protected DelegationTokenHttpSolrClient(Builder builder) {
    super(builder);
    setQueryParams(new TreeSet<>(Arrays.asList(DELEGATION_TOKEN_PARAM)));
  }

  /**
   * This constructor is defined at "protected" scope. Ideally applications should
   * use {@linkplain org.apache.solr.client.solrj.impl.HttpSolrClient.Builder} instance
   * to configure this Solr client instance.
   *
   * @param baseURL The base url to communicate with the Solr server
   * @param client Http client instance to use for communication
   * @param parser Response parser instance to use to decode response from Solr server
   * @param allowCompression Should compression be allowed ?
   * @param invariantParams The parameters which should be passed with every request.
   * 
   * @deprecated use {@link DelegationTokenHttpSolrClient#DelegationTokenHttpSolrClient(HttpSolrClient.Builder)} instead, as it is a more
   * extension/subclassing-friendly alternative
   */
  @Deprecated
  protected DelegationTokenHttpSolrClient(String baseURL,
      HttpClient client,
      ResponseParser parser,
      boolean allowCompression,
      ModifiableSolrParams invariantParams) {
    super(baseURL, client, parser, allowCompression, invariantParams);

    setQueryParams(new TreeSet<>(Arrays.asList(DELEGATION_TOKEN_PARAM)));
  }

  @Override
  protected HttpRequestBase createMethod(final SolrRequest request, String collection) throws IOException, SolrServerException {
    SolrParams params = request.getParams();
    if (params != null && params.getParams(DELEGATION_TOKEN_PARAM) != null) {
      throw new IllegalArgumentException(DELEGATION_TOKEN_PARAM + " parameter not supported");
    }
    return super.createMethod(request, collection);
  }

  @Override
  public void setQueryParams(Set<String> queryParams) {
    queryParams = queryParams == null ?
        Set.of(DELEGATION_TOKEN_PARAM): queryParams;
    if (!queryParams.contains(DELEGATION_TOKEN_PARAM)) {
      queryParams = new HashSet<String>(queryParams);
      queryParams.add(DELEGATION_TOKEN_PARAM);
      queryParams = Collections.unmodifiableSet(queryParams);
    }
    super.setQueryParams(queryParams);
  }
}
