package org.apache.solr.client.solrj.impl;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.SolrRequest;

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

/**
 * A HttpContext derivative to encapsulate a client-side SolrRequest
 * object.
 */
public class SolrHttpContext extends HttpClientContext {
  final protected static String SOLR_CONTEXT_KEY = "solr.context";
  
  private SolrRequest solrRequest;
  
  public static HttpContext EMPTY_CONTEXT = new SolrHttpContext();
  
  protected SolrHttpContext() {
    setAttribute(SOLR_CONTEXT_KEY, this);
  }
  
  public SolrHttpContext(SolrRequest request) {
    this.solrRequest = request;
    setAttribute(SOLR_CONTEXT_KEY, this);
  }
  
  public SolrRequest getSolrRequest() {
    return solrRequest;
  }
  
  @Override
  public String toString() {
    return "[SolrHttpContext contains: "+solrRequest+"]";
  }
}
