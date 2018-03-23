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
package org.apache.solr.security;

import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.auth.AUTH;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.ContextAwareAuthScheme;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.MalformedChallengeException;
import org.apache.http.message.BufferedHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.CharArrayBuffer;

public class BearerAuthSchemeProvider implements AuthSchemeProvider {

  /**
   * Creates an instance of {@link AuthScheme}.
   *
   * @param context the http context
   * @return auth scheme.
   */
  @Override
  public AuthScheme create(HttpContext context) {
    return new BearerAuthScheme();
  }

  private static class BearerAuthScheme implements ContextAwareAuthScheme {
        private boolean complete = false;

        @Override
        public void processChallenge(Header header) throws MalformedChallengeException {
            this.complete = true;
        }

        @Override
        public Header authenticate(Credentials credentials, HttpRequest request) throws AuthenticationException {
            return authenticate(credentials, request, null);
        }

        @Override
        public Header authenticate(Credentials credentials, HttpRequest request, HttpContext httpContext)
                throws AuthenticationException {
            CharArrayBuffer buffer = new CharArrayBuffer(128);
            buffer.append(AUTH.WWW_AUTH_RESP);
            buffer.append(": Bearer ");
            buffer.append(credentials.getUserPrincipal().getName());
            return new BufferedHeader(buffer);
        }

        @Override
        public String getSchemeName() {
            return "Bearer";
        }

        @Override
        public String getParameter(String name) {
            return null;
        }

        @Override
        public String getRealm() {
            return null;
        }

        @Override
        public boolean isConnectionBased() {
            return false;
        }

        @Override
        public boolean isComplete() {
            return this.complete;
        }
    }
}