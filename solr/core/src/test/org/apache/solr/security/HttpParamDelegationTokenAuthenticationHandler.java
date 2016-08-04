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
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

/**
 * AuthenticationHandler that supports delegation tokens and simple
 * authentication via the "user" http parameter
 */
public class HttpParamDelegationTokenAuthenticationHandler extends
    DelegationTokenAuthenticationHandler {

  public static final String USER_PARAM = "user";

  public HttpParamDelegationTokenAuthenticationHandler() {
    super(new HttpParamAuthenticationHandler());
  }

  @Override
  public void init(Properties config) throws ServletException {
    Properties conf = new Properties();
    for (Map.Entry entry : config.entrySet()) {
      conf.setProperty((String) entry.getKey(), (String) entry.getValue());
    }
    conf.setProperty(TOKEN_KIND, KerberosPlugin.DELEGATION_TOKEN_TYPE_DEFAULT);
    super.init(conf);
  }
 
  private static String getHttpParam(HttpServletRequest request, String param) {
    List<NameValuePair> pairs =
      URLEncodedUtils.parse(request.getQueryString(), Charset.forName("UTF-8"));
    for (NameValuePair nvp : pairs) {
      if(param.equals(nvp.getName())) {
        return nvp.getValue();
      }
    }
    return null;
  }

  private static class HttpParamAuthenticationHandler
      implements AuthenticationHandler {

    @Override
    public String getType() {
      return "dummy";
    }

    @Override
    public void init(Properties config) throws ServletException {
    }

    @Override
    public void destroy() {
    }

    @Override
    public boolean managementOperation(AuthenticationToken token,
        HttpServletRequest request, HttpServletResponse response)
        throws IOException, AuthenticationException {
      return false;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request,
        HttpServletResponse response)
        throws IOException, AuthenticationException {
      AuthenticationToken token = null;
      String userName = getHttpParam(request, USER_PARAM);
      if (userName != null) {
        return new AuthenticationToken(userName, userName, "test");
      } else {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setHeader("WWW-Authenticate", "dummy");
      }
      return token;
    }
  }
}
