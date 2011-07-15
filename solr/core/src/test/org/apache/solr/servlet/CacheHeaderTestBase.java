/**
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

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.junit.Test;

public abstract class CacheHeaderTestBase extends SolrJettyTestBase {

  protected HttpMethodBase getSelectMethod(String method) {
    CommonsHttpSolrServer httpserver = (CommonsHttpSolrServer)getSolrServer();
    HttpMethodBase m = null;
    if ("GET".equals(method)) {
      m = new GetMethod(httpserver.getBaseURL() + "/select");
    } else if ("HEAD".equals(method)) {
      m = new HeadMethod(httpserver.getBaseURL() + "/select");
    } else if ("POST".equals(method)) {
      m = new PostMethod(httpserver.getBaseURL() + "/select");
    }
    m.setQueryString(new NameValuePair[] { new NameValuePair("q", "solr"),
          new NameValuePair("qt", "standard") });
    return m;
  }

  protected HttpMethodBase getUpdateMethod(String method) {
    CommonsHttpSolrServer httpserver = (CommonsHttpSolrServer)getSolrServer();
    HttpMethodBase m = null;
    
    if ("GET".equals(method)) {
      m=new GetMethod(httpserver.getBaseURL()+"/update/csv");
    } else if ("POST".equals(method)) {
      m=new PostMethod(httpserver.getBaseURL()+"/update/csv");
    } else if ("HEAD".equals(method)) {
      m=new HeadMethod(httpserver.getBaseURL()+"/update/csv");
    }
    
    return m;
  }
  
  protected HttpClient getClient() {
    CommonsHttpSolrServer httpserver = (CommonsHttpSolrServer)getSolrServer();
    return httpserver.getHttpClient();
  }

  protected void checkResponseBody(String method, HttpMethodBase resp)
      throws Exception {
    String responseBody = resp.getResponseBodyAsString();
    if ("GET".equals(method)) {
      switch (resp.getStatusCode()) {
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
          assertEquals("Unknown request response", 0, resp.getStatusCode());
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
