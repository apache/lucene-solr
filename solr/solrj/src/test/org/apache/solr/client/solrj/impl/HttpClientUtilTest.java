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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.auth.AuthScope;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.HttpClient;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.BrowserCompatHostnameVerifier;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.SSLTestConfig;
import org.junit.Test;

public class HttpClientUtilTest {

  @Test
  public void testNoParamsSucceeds() throws IOException {
    CloseableHttpClient client = HttpClientUtil.createClient(null);
    client.close();
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
    try {
      assertEquals(12345, HttpConnectionParams.getConnectionTimeout(client.getParams()));
      assertEquals(PoolingClientConnectionManager.class, client.getConnectionManager().getClass());
      assertEquals(22345, ((PoolingClientConnectionManager)client.getConnectionManager()).getMaxTotal());
      assertEquals(32345, ((PoolingClientConnectionManager)client.getConnectionManager()).getDefaultMaxPerRoute());
      assertEquals(42345, HttpConnectionParams.getSoTimeout(client.getParams()));
      assertEquals(HttpClientUtil.NO_RETRY, client.getHttpRequestRetryHandler());
      assertEquals("pass", client.getCredentialsProvider().getCredentials(new AuthScope("127.0.0.1", 1234)).getPassword());
      assertEquals("user", client.getCredentialsProvider().getCredentials(new AuthScope("127.0.0.1", 1234)).getUserPrincipal().getName());
      assertEquals(true, client.getParams().getParameter(ClientPNames.HANDLE_REDIRECTS));
    } finally {
      client.close();
    }
  }

  @Test
  public void testAuthSchemeConfiguration() {
    System.setProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP, "test");
    try {
      HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
      AbstractHttpClient client = (AbstractHttpClient)HttpClientUtil.createClient(null);
      assertEquals(1, client.getAuthSchemes().getSchemeNames().size());
      assertTrue(AuthSchemes.SPNEGO.equalsIgnoreCase(client.getAuthSchemes().getSchemeNames().get(0)));
    } finally {
      //Cleanup the system property.
      System.clearProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
    }
  }

  @Test
  public void testReplaceConfigurer() throws IOException{
    
    try {
    final AtomicInteger counter = new AtomicInteger();
    HttpClientConfigurer custom = new HttpClientConfigurer(){
      @Override
      public void configure(DefaultHttpClient httpClient, SolrParams config) {
        super.configure(httpClient, config);
        counter.set(config.getInt("custom-param", -1));
      }
      
    };
    
    HttpClientUtil.setConfigurer(custom);
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("custom-param", 5);
    HttpClientUtil.createClient(params).close();
    assertEquals(5, counter.get());
    } finally {
      //restore default configurer
      HttpClientUtil.setConfigurer(new HttpClientConfigurer());
    }

  }
  
  @Test
  @SuppressWarnings("deprecation")
  public void testSSLSystemProperties() throws IOException {
    CloseableHttpClient client = HttpClientUtil.createClient(null);
    try {
      SSLTestConfig.setSSLSystemProperties();
      assertNotNull("HTTPS scheme could not be created using the javax.net.ssl.* system properties.", 
          client.getConnectionManager().getSchemeRegistry().get("https"));
      
      System.clearProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME);
      client.close();
      client = HttpClientUtil.createClient(null);
      assertEquals(BrowserCompatHostnameVerifier.class, getHostnameVerifier(client).getClass());
      
      System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "true");
      client.close();
      client = HttpClientUtil.createClient(null);
      assertEquals(BrowserCompatHostnameVerifier.class, getHostnameVerifier(client).getClass());
      
      System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "");
      client.close();
      client = HttpClientUtil.createClient(null);
      assertEquals(BrowserCompatHostnameVerifier.class, getHostnameVerifier(client).getClass());
      
      System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "false");
      client.close();
      client = HttpClientUtil.createClient(null);
      assertEquals(AllowAllHostnameVerifier.class, getHostnameVerifier(client).getClass());
    } finally {
      SSLTestConfig.clearSSLSystemProperties();
      System.clearProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME);
      client.close();
    }
  }
  
  @SuppressWarnings("deprecation")
  private X509HostnameVerifier getHostnameVerifier(HttpClient client) {
    return ((SSLSocketFactory) client.getConnectionManager().getSchemeRegistry()
        .get("https").getSchemeSocketFactory()).getHostnameVerifier();
  }
  
}
