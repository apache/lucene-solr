package org.apache.solr.util;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.SystemDefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.SolrHttpContext;
import org.apache.solr.request.SolrQueryRequestContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;

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

public class SolrHttpClient {
  
  private static HttpContext getHttpContext() {
    SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
    SolrQueryRequest request = requestInfo == null ? null : requestInfo.getReq();
    return request == null ? SolrHttpContext.EMPTY_CONTEXT: new SolrQueryRequestContext(request);
  }

  public static class SolrSystemDefaultHttpClient extends SystemDefaultHttpClient {
    
    public SolrSystemDefaultHttpClient() {
      super();
    }
    
    @Override
    public CloseableHttpResponse execute(HttpUriRequest request)
        throws IOException {
      return super.execute(request, getHttpContext());
    }
    
    @Override
    public CloseableHttpResponse execute(HttpHost target, HttpRequest request)
        throws IOException, ClientProtocolException {
      return super.execute(target, request, getHttpContext());
    }
    
    @Override
    public <T> T execute(HttpUriRequest request,
        ResponseHandler<? extends T> responseHandler) throws IOException,
        ClientProtocolException {
      return super.execute(request, responseHandler, getHttpContext());
    }
  }
  
  public static class SolrDefaultHttpClient extends DefaultHttpClient {
    
    public SolrDefaultHttpClient(ClientConnectionManager cm) {
      super(cm);
    }
    
    @Override
    public CloseableHttpResponse execute(HttpUriRequest request)
        throws IOException {
      return super.execute(request, getHttpContext());
    }
    
    @Override
    public CloseableHttpResponse execute(HttpHost target, HttpRequest request)
        throws IOException, ClientProtocolException {
      return super.execute(target, request, getHttpContext());
    }
    
    @Override
    public <T> T execute(HttpUriRequest request,
        ResponseHandler<? extends T> responseHandler) throws IOException,
        ClientProtocolException {
      return super.execute(request, responseHandler, getHttpContext());
    }
  }
}

