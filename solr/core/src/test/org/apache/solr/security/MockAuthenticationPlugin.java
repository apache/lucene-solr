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

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.security.Principal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import org.apache.http.auth.BasicUserPrincipal;

public class MockAuthenticationPlugin extends AuthenticationPlugin {
  static Predicate<ServletRequest> predicate;

  @Override
  public void init(Map<String, Object> pluginConfig) {
  }

  @Override
  public boolean doAuthenticate(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
    String user = null;
    if (predicate != null) {
      if (predicate.test(request)) {
        user = (String) request.getAttribute(Principal.class.getName());
        request.removeAttribute(Principal.class.getName());
      }
    }

    final FilterChain ffc = filterChain;
    final AtomicBoolean requestContinues = new AtomicBoolean(false);
    forward(user, request, response, new FilterChain() {
      @Override
      public void doFilter(ServletRequest req, ServletResponse res) throws IOException, ServletException {
        ffc.doFilter(req, res);
        requestContinues.set(true);
      }
    });
    return requestContinues.get();
  }

  protected void forward(String user, ServletRequest  req, ServletResponse rsp,
                                    FilterChain chain) throws IOException, ServletException {
    if(user != null) {
      final Principal p = new BasicUserPrincipal(user);
      req = new HttpServletRequestWrapper((HttpServletRequest) req) {
        @Override
        public Principal getUserPrincipal() {
          return p;
        }
      };
    }
    chain.doFilter(req,rsp);
  }

  @Override
  public void close() throws IOException {

  }
}
