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
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;

public abstract class SolrClientBuilder<B extends SolrClientBuilder<B>> {

  protected HttpClient httpClient;
  protected ResponseParser responseParser;
  protected Integer connectionTimeoutMillis = 15000;
  protected Integer socketTimeoutMillis = 120000;

  /** The solution for the unchecked cast warning. */
  public abstract B getThis();
  
  /**
   * Provides a {@link HttpClient} for the builder to use when creating clients.
   */
  public B withHttpClient(HttpClient httpClient) {
    this.httpClient = httpClient;
    return getThis();
  }
  
  /**
   * Provides a {@link ResponseParser} for created clients to use when handling requests.
   */
  public B withResponseParser(ResponseParser responseParser) {
    this.responseParser = responseParser;
    return getThis();
  }
  
  /**
   * Tells {@link Builder} that created clients should obey the following timeout when connecting to Solr servers.
   * <p>
   * For valid values see {@link org.apache.http.client.config.RequestConfig#getConnectTimeout()}
   * </p>
   */
  public B withConnectionTimeout(int connectionTimeoutMillis) {
    if (connectionTimeoutMillis < 0) {
      throw new IllegalArgumentException("connectionTimeoutMillis must be a non-negative integer.");
    }
    
    this.connectionTimeoutMillis = connectionTimeoutMillis;
    return getThis();
  }
  
  /**
   * Tells {@link Builder} that created clients should set the following read timeout on all sockets.
   * <p>
   * For valid values see {@link org.apache.http.client.config.RequestConfig#getSocketTimeout()}
   * </p>
   */
  public B withSocketTimeout(int socketTimeoutMillis) {
    if (socketTimeoutMillis < 0) {
      throw new IllegalArgumentException("socketTimeoutMillis must be a non-negative integer.");
    }
    
    this.socketTimeoutMillis = socketTimeoutMillis;
    return getThis();
  }
}
