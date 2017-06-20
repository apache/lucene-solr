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

import java.io.IOException;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.protocol.HttpContext;

/**
 * This HTTP request interceptor adds HTTP authentication credentials to every outgoing
 * request. This implementation is required since Solr client is not capable of performing
 * non preemptive authentication. By adding the Http authentication credentials to every request,
 * this interceptor enables "preemptive" authentication.
 */
public class PreemptiveAuth implements HttpRequestInterceptor {
  private AuthScheme authScheme = null;

  public PreemptiveAuth(AuthScheme authScheme) {
    this.authScheme = authScheme;
  }

  @Override
  public void process(final HttpRequest request, final HttpContext context) throws HttpException,
      IOException {

    AuthState authState = (AuthState) context.getAttribute(ClientContext.TARGET_AUTH_STATE);
    // If no auth scheme available yet, try to initialize it preemptively
    if (authState.getAuthScheme() == null) {
      CredentialsProvider credsProvider = (CredentialsProvider) context
          .getAttribute(ClientContext.CREDS_PROVIDER);
      Credentials creds = credsProvider.getCredentials(AuthScope.ANY);
      authState.update(authScheme, creds);
    }
  }
}