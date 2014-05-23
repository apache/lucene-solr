package org.apache.solr.client.solrj.impl;

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


import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.common.params.SolrParams;

/**
 * The default http client configurer. If the behaviour needs to be customized a
 * new HttpCilentConfigurer can be set by calling
 * {@link HttpClientUtil#setConfigurer(HttpClientConfigurer)}
 */
public class HttpClientConfigurer {
  
  protected void configure(DefaultHttpClient httpClient, SolrParams config) {
    
    if (config.get(HttpClientUtil.PROP_MAX_CONNECTIONS) != null) {
      HttpClientUtil.setMaxConnections(httpClient,
          config.getInt(HttpClientUtil.PROP_MAX_CONNECTIONS));
    }
    
    if (config.get(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST) != null) {
      HttpClientUtil.setMaxConnectionsPerHost(httpClient,
          config.getInt(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST));
    }
    
    if (config.get(HttpClientUtil.PROP_CONNECTION_TIMEOUT) != null) {
      HttpClientUtil.setConnectionTimeout(httpClient,
          config.getInt(HttpClientUtil.PROP_CONNECTION_TIMEOUT));
    }
    
    if (config.get(HttpClientUtil.PROP_SO_TIMEOUT) != null) {
      HttpClientUtil.setSoTimeout(httpClient,
          config.getInt(HttpClientUtil.PROP_SO_TIMEOUT));
    }
    
    if (config.get(HttpClientUtil.PROP_USE_RETRY) != null) {
      HttpClientUtil.setUseRetry(httpClient,
          config.getBool(HttpClientUtil.PROP_USE_RETRY));
    }
    
    if (config.get(HttpClientUtil.PROP_FOLLOW_REDIRECTS) != null) {
      HttpClientUtil.setFollowRedirects(httpClient,
          config.getBool(HttpClientUtil.PROP_FOLLOW_REDIRECTS));
    }
    
    final String basicAuthUser = config
        .get(HttpClientUtil.PROP_BASIC_AUTH_USER);
    final String basicAuthPass = config
        .get(HttpClientUtil.PROP_BASIC_AUTH_PASS);
    HttpClientUtil.setBasicAuth(httpClient, basicAuthUser, basicAuthPass);
    
    if (config.get(HttpClientUtil.PROP_ALLOW_COMPRESSION) != null) {
      HttpClientUtil.setAllowCompression(httpClient,
          config.getBool(HttpClientUtil.PROP_ALLOW_COMPRESSION));
    }
    
    boolean sslCheckPeerName = toBooleanDefaultIfNull(
        toBooleanObject(System.getProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME)), true);
    if(sslCheckPeerName == false) {
      HttpClientUtil.setHostNameVerifier(httpClient, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
    }
  }
  
  public static boolean toBooleanDefaultIfNull(Boolean bool, boolean valueIfNull) {
    if (bool == null) {
      return valueIfNull;
    }
    return bool.booleanValue() ? true : false;
  }
  
  public static Boolean toBooleanObject(String str) {
    if ("true".equalsIgnoreCase(str)) {
      return Boolean.TRUE;
    } else if ("false".equalsIgnoreCase(str)) {
      return Boolean.FALSE;
    }
    // no match
    return null;
  }
}
