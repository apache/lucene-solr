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

import java.util.concurrent.ExecutorService;

/**
 * @deprecated Use {@link org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient}
 */
@Deprecated
public class ConcurrentUpdateSolrServer extends ConcurrentUpdateSolrClient {

  public ConcurrentUpdateSolrServer(String solrServerUrl, int queueSize, int threadCount) {
    super(solrServerUrl, queueSize, threadCount);
  }

  public ConcurrentUpdateSolrServer(String solrServerUrl, HttpClient client, int queueSize, int threadCount) {
    super(solrServerUrl, client, queueSize, threadCount);
  }

  public ConcurrentUpdateSolrServer(String solrServerUrl, HttpClient client, int queueSize, int threadCount, ExecutorService es) {
    super(solrServerUrl, client, queueSize, threadCount, es);
  }

  public ConcurrentUpdateSolrServer(String solrServerUrl, HttpClient client, int queueSize, int threadCount, ExecutorService es, boolean streamDeletes) {
    super(solrServerUrl, client, queueSize, threadCount, es, streamDeletes);
  }

}
