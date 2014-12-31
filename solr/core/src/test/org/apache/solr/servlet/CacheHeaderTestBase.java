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
package org.apache.solr.servlet;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public abstract class CacheHeaderTestBase extends SolrJettyTestBase {

  protected HttpRequestBase getSelectMethod(String method, String... params) throws URISyntaxException {
    HttpSolrClient client = (HttpSolrClient) getSolrClient();
    HttpRequestBase m = null;
    
    ArrayList<BasicNameValuePair> qparams = new ArrayList<>();
    if(params.length==0) {
      qparams.add(new BasicNameValuePair("q", "solr"));
      qparams.add(new BasicNameValuePair("qt", "standard"));
    }
    for (int i = 0; i < params.length / 2; i++) {
      qparams.add(new BasicNameValuePair(params[i * 2], params[i * 2 + 1]));
    }

    URI uri = URI.create(client.getBaseURL() + "/select?" +
                         URLEncodedUtils.format(qparams, StandardCharsets.UTF_8));
   
    if ("GET".equals(method)) {
      m = new HttpGet(uri);
    } else if ("HEAD".equals(method)) {
      m = new HttpHead(uri);
    } else if ("POST".equals(method)) {
      m = new HttpPost(uri);
    }
    
    return m;
  }

  protected HttpRequestBase getUpdateMethod(String method, String... params) throws URISyntaxException {
    HttpSolrClient client = (HttpSolrClient) getSolrClient();
    HttpRequestBase m = null;
    
    ArrayList<BasicNameValuePair> qparams = new ArrayList<>();
    for(int i=0;i<params.length/2;i++) {
      qparams.add(new BasicNameValuePair(params[i*2], params[i*2+1]));
    }

    URI uri = URI.create(client.getBaseURL() + "/update?" +
                         URLEncodedUtils.format(qparams, StandardCharsets.UTF_8));
    
    if ("GET".equals(method)) {
      m=new HttpGet(uri);
    } else if ("POST".equals(method)) {
      m=new HttpPost(uri);
    } else if ("HEAD".equals(method)) {
      m=new HttpHead(uri);
    }

    return m;
  }
  
  protected HttpClient getClient() {
    HttpSolrClient client = (HttpSolrClient) getSolrClient();
    return client.getHttpClient();
  }

  protected void checkResponseBody(String method, HttpResponse resp)
      throws Exception {
    String responseBody ="";
    
    if (resp.getEntity() != null) {
      responseBody = EntityUtils.toString(resp.getEntity());
    }

    if ("GET".equals(method)) {
      switch (resp.getStatusLine().getStatusCode()) {
        case 200:
          assertTrue("Response body was empty for method " + method,
              responseBody != null && responseBody.length() > 0);
          break;
        case 304:
          assertTrue("Response body was not empty for method " + method,
              responseBody == null || responseBody.length() == 0);
          break;
        case 412:
          assertTrue("Response body was not empty for method " + method,
              responseBody == null || responseBody.length() == 0);
          break;
        default:
          System.err.println(responseBody);
          assertEquals("Unknown request response", 0, resp.getStatusLine().getStatusCode());
      }
    }
    if ("HEAD".equals(method)) {
      assertTrue("Response body was not empty for method " + method,
          responseBody == null || responseBody.length() == 0);
    }
  }

  // The tests
  @Test
  public void testLastModified() throws Exception {
    doLastModified("GET");
    doLastModified("HEAD");
  }

  @Test
  public void testEtag() throws Exception {
    doETag("GET");
    doETag("HEAD");
  }

  @Test
  public void testCacheControl() throws Exception {
    doCacheControl("GET");
    doCacheControl("HEAD");
    doCacheControl("POST");
  }

  protected abstract void doCacheControl(String method) throws Exception;
  protected abstract void doETag(String method) throws Exception;
  protected abstract void doLastModified(String method) throws Exception;
  
}
