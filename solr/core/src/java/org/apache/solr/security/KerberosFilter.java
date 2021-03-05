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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Locale;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosFilter extends AuthenticationFilter {

  private final Locale defaultLocale = Locale.getDefault();
  
  private final CoreContainer coreContainer;

  public KerberosFilter(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  public void init(FilterConfig conf) throws ServletException {
    super.init(conf);
  }

  @Override
  protected void initializeAuthHandler(String authHandlerClassName,
                                       FilterConfig filterConfig) throws ServletException {
    // set the internal authentication handler in order to record whether the request should continue
    super.initializeAuthHandler(authHandlerClassName, filterConfig);
    AuthenticationHandler authHandler = getAuthenticationHandler();
    super.initializeAuthHandler(
        RequestContinuesRecorderAuthenticationHandler.class.getName(), filterConfig);
    RequestContinuesRecorderAuthenticationHandler newAuthHandler =
        (RequestContinuesRecorderAuthenticationHandler)getAuthenticationHandler();
    newAuthHandler.setAuthHandler(authHandler);
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected void doFilter(FilterChain filterChain, HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    Locale.setDefault(defaultLocale);

    request = substituteOriginalUserRequest(request);

    super.doFilter(filterChain, request, response);
  }

  /**
   * If principal is an admin user, i.e. has ALL permissions (e.g. request coming from Solr
   * node), and "originalUserPrincipal" is specified, then set originalUserPrincipal
   * as the principal. This is the case in forwarded/remote requests
   * through KerberosPlugin. This is needed because the original node that received
   * this request did not perform any authorization, and hence we are the first ones
   * to authorize the request (and we need the original user principal to do so).
   * @return Substituted request, if applicable, or the original request
   */
  private HttpServletRequest substituteOriginalUserRequest(HttpServletRequest request) {
    final HttpServletRequest originalRequest = request;
    AuthorizationPlugin authzPlugin = coreContainer.getAuthorizationPlugin();
    if (authzPlugin instanceof RuleBasedAuthorizationPlugin) {
      RuleBasedAuthorizationPlugin ruleBased = (RuleBasedAuthorizationPlugin) authzPlugin;
      if (request.getHeader(KerberosPlugin.ORIGINAL_USER_PRINCIPAL_HEADER) != null &&
          ruleBased.doesUserHavePermission(request.getUserPrincipal(), PermissionNameProvider.Name.ALL)) {
        request = new HttpServletRequestWrapper(request) {
          @Override
          public Principal getUserPrincipal() {
            String originalUserPrincipal = originalRequest.getHeader(KerberosPlugin.ORIGINAL_USER_PRINCIPAL_HEADER);
            if (log.isInfoEnabled()) {
              log.info("Substituting user principal from {} to {}.", originalRequest.getUserPrincipal(), originalUserPrincipal);
            }
            return new Principal() {
              @Override
              public String getName() {
                return originalUserPrincipal;
              }
              @Override
              public String toString() {
                return originalUserPrincipal;
              }
            };
          }
        };
      }
    }
    return request;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws IOException, ServletException {
    // A hack until HADOOP-15681 get committed
    Locale.setDefault(Locale.US);
    super.doFilter(request, response, filterChain);
  }
}
