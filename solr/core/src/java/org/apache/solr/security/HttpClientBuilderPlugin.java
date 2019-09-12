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
package org.apache.solr.security;

import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;

/**
 * Plugin interface for configuring internal HttpClients. This
 * relies on the internal HttpClient implementation and is subject to
 * change.
 * 
 * @lucene.experimental
 */
public interface HttpClientBuilderPlugin {
  /**
   *
   * @return Returns an instance of a SolrHttpClientBuilder to be used for configuring the
   * HttpClients for use with SolrJ clients.
   *
   * @lucene.experimental
   */
  public SolrHttpClientBuilder getHttpClientBuilder(SolrHttpClientBuilder builder);

  public default void setup(Http2SolrClient client) {

  }
}
