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
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;

/*
 * {@link AuthenticationHandler} that delegates to another {@link AuthenticationHandler}
 * and records the response of managementOperation (which indicates whether the request
 * should continue or not).
 */
public class RequestContinuesRecorderAuthenticationHandler implements AuthenticationHandler {
  // filled in by Plugin/Filter
  static final String REQUEST_CONTINUES_ATTR =
      "org.apache.solr.security.authentication.requestcontinues";

  private AuthenticationHandler authHandler;

  public void setAuthHandler(AuthenticationHandler authHandler) {
    this.authHandler = authHandler;
  }

  public String getType() {
    return authHandler.getType();
  }

  public void init(Properties config) throws ServletException {
    // authHandler has already been init'ed, nothing to do here
  }

  public void destroy() {
    authHandler.destroy();
  }

  public boolean managementOperation(AuthenticationToken token,
                                     HttpServletRequest request,
                                     HttpServletResponse response)
      throws IOException, AuthenticationException {
    boolean result = authHandler.managementOperation(token, request, response);
    request.setAttribute(RequestContinuesRecorderAuthenticationHandler.REQUEST_CONTINUES_ATTR, Boolean.toString(result));
    return result;
  }

  public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    return authHandler.authenticate(request, response);
  }
}