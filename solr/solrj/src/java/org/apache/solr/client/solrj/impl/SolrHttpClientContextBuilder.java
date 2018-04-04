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

import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Lookup;
import org.apache.http.cookie.CookieSpecProvider;

public class SolrHttpClientContextBuilder {
  public static SolrHttpClientContextBuilder create() {
    return new SolrHttpClientContextBuilder();
  }
  
  public static abstract class CredentialsProviderProvider {
    public abstract CredentialsProvider getCredentialsProvider();
  }
  
  public static abstract class AuthSchemeRegistryProvider {
    public abstract Lookup<AuthSchemeProvider> getAuthSchemeRegistry();
  }
  
  public static abstract class CookieSpecRegistryProvider {
    public abstract Lookup<CookieSpecProvider> getCookieSpecRegistry();
  }
  
  private CookieSpecRegistryProvider cookieSpecRegistryProvider;
  private AuthSchemeRegistryProvider authSchemeRegistryProvider;
  private CredentialsProviderProvider credentialsProviderProvider;

  public SolrHttpClientContextBuilder() {
    super();
  }

  public final SolrHttpClientContextBuilder setCookieSpecRegistryProvider(
      final CookieSpecRegistryProvider cookieSpecRegistryProvider) {
    this.cookieSpecRegistryProvider = cookieSpecRegistryProvider;
    return this;
  }
  
  public final SolrHttpClientContextBuilder setDefaultCredentialsProvider(
      final CredentialsProviderProvider credentialsProviderProvider) {
    this.credentialsProviderProvider = credentialsProviderProvider;
    return this;
  }
  
  public final SolrHttpClientContextBuilder setAuthSchemeRegistryProvider(
      final AuthSchemeRegistryProvider authSchemeRegistryProvider) {
    this.authSchemeRegistryProvider = authSchemeRegistryProvider;
    return this;
  }

  public AuthSchemeRegistryProvider getAuthSchemeRegistryProvider() {
    return authSchemeRegistryProvider;
  }

  public CookieSpecRegistryProvider getCookieSpecRegistryProvider() {
    return cookieSpecRegistryProvider;
  }

  public CredentialsProviderProvider getCredentialsProviderProvider() {
    return credentialsProviderProvider;
  }

  public HttpClientContext createContext(Object userToken) {
    HttpClientContext context = new HttpClientContext();
    if (getCredentialsProviderProvider() != null) {
      context.setCredentialsProvider(getCredentialsProviderProvider().getCredentialsProvider());
    }
    if (getAuthSchemeRegistryProvider() != null) {
      context.setAuthSchemeRegistry( getAuthSchemeRegistryProvider().getAuthSchemeRegistry());
    }
    
    if (getCookieSpecRegistryProvider() != null) {
      context.setCookieSpecRegistry(getCookieSpecRegistryProvider().getCookieSpecRegistry());
    }

    context.setUserToken(userToken);
    
    return context;
  }

}
