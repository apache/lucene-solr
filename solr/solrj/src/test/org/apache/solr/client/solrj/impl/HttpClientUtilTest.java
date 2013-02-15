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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.auth.AuthScope;
import org.apache.http.client.HttpClient;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpConnectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.Test;

public class HttpClientUtilTest {

  @Test
  public void testNoParamsSucceeds() {
    HttpClient clien = HttpClientUtil.createClient(null);
    clien.getConnectionManager().shutdown();
  }

  @Test
  public void testSetParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_ALLOW_COMPRESSION, true);
    params.set(HttpClientUtil.PROP_BASIC_AUTH_PASS, "pass");
    params.set(HttpClientUtil.PROP_BASIC_AUTH_USER, "user");
    params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, 12345);
    params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, true);
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 22345);
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 32345);
    params.set(HttpClientUtil.PROP_SO_TIMEOUT, 42345);
    params.set(HttpClientUtil.PROP_USE_RETRY, false);
    DefaultHttpClient client = (DefaultHttpClient) HttpClientUtil.createClient(params);
    assertEquals(12345, HttpConnectionParams.getConnectionTimeout(client.getParams()));
    assertEquals(PoolingClientConnectionManager.class, client.getConnectionManager().getClass());
    assertEquals(22345, ((PoolingClientConnectionManager)client.getConnectionManager()).getMaxTotal());
    assertEquals(32345, ((PoolingClientConnectionManager)client.getConnectionManager()).getDefaultMaxPerRoute());
    assertEquals(42345, HttpConnectionParams.getSoTimeout(client.getParams()));
    assertEquals(HttpClientUtil.NO_RETRY, client.getHttpRequestRetryHandler());
    assertEquals("pass", client.getCredentialsProvider().getCredentials(new AuthScope("127.0.0.1", 1234)).getPassword());
    assertEquals("user", client.getCredentialsProvider().getCredentials(new AuthScope("127.0.0.1", 1234)).getUserPrincipal().getName());
    assertEquals(true, client.getParams().getParameter(ClientPNames.HANDLE_REDIRECTS));
    client.getConnectionManager().shutdown();
  }
  
  @Test
  public void testReplaceConfigurer(){
    
    try {
    final AtomicInteger counter = new AtomicInteger();
    HttpClientConfigurer custom = new HttpClientConfigurer(){
      @Override
      protected void configure(DefaultHttpClient httpClient, SolrParams config) {
        super.configure(httpClient, config);
        counter.set(config.getInt("custom-param", -1));
      }
      
    };
    
    HttpClientUtil.setConfigurer(custom);
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("custom-param", 5);
    HttpClientUtil.createClient(params).getConnectionManager().shutdown();
    assertEquals(5, counter.get());
    } finally {
      //restore default configurer
      HttpClientUtil.setConfigurer(new HttpClientConfigurer());
    }

  }
  
}
