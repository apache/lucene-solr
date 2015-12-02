package org.apache.solr.security;

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


import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.CommandOperation;

public class BasicAuthPlugin extends AuthenticationPlugin implements ConfigEditablePlugin {
  private AuthenticationProvider zkAuthentication;
  private final static ThreadLocal<Header> authHeader = new ThreadLocal<>();

  public boolean authenticate(String username, String pwd) {
    return zkAuthentication.authenticate(username, pwd);
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {
    zkAuthentication = getAuthenticationProvider(pluginConfig);
  }

  @Override
  public Map<String, Object> edit(Map<String, Object> latestConf, List<CommandOperation> commands) {
    if (zkAuthentication instanceof ConfigEditablePlugin) {
      ConfigEditablePlugin editablePlugin = (ConfigEditablePlugin) zkAuthentication;
      return editablePlugin.edit(latestConf, commands);
    }
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "This cannot be edited");
  }

  protected AuthenticationProvider getAuthenticationProvider(Map<String, Object> pluginConfig) {
    Sha256AuthenticationProvider provider = new Sha256AuthenticationProvider();
    provider.init(pluginConfig);
    return provider;
  }

  private void authenticationFailure(HttpServletResponse response, String message) throws IOException {
    for (Map.Entry<String, String> entry : zkAuthentication.getPromptHeaders().entrySet()) {
      response.setHeader(entry.getKey(), entry.getValue());
    }
    response.sendError(401, message);
  }

  @Override
  public void doAuthenticate(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws Exception {

    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;

    String authHeader = request.getHeader("Authorization");
    if (authHeader != null) {
      BasicAuthPlugin.authHeader.set(new BasicHeader("Authorization", authHeader));
      StringTokenizer st = new StringTokenizer(authHeader);
      if (st.hasMoreTokens()) {
        String basic = st.nextToken();
        if (basic.equalsIgnoreCase("Basic")) {
          try {
            String credentials = new String(Base64.decodeBase64(st.nextToken()), "UTF-8");
            int p = credentials.indexOf(":");
            if (p != -1) {
              final String username = credentials.substring(0, p).trim();
              String pwd = credentials.substring(p + 1).trim();
              if (!authenticate(username, pwd)) {
                authenticationFailure(response, "Bad credentials");
              } else {
                HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(request) {
                  @Override
                  public Principal getUserPrincipal() {
                    return new BasicUserPrincipal(username);
                  }
                };
                filterChain.doFilter(wrapper, response);
              }

            } else {
              authenticationFailure(response, "Invalid authentication token");
            }
          } catch (UnsupportedEncodingException e) {
            throw new Error("Couldn't retrieve authentication", e);
          }
        }
      }
    } else {
      request.setAttribute(AuthenticationPlugin.class.getName(), zkAuthentication.getPromptHeaders());
      filterChain.doFilter(request, response);
    }
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void closeRequest() {
    authHeader.remove();
  }

  public interface AuthenticationProvider {
    void init(Map<String, Object> pluginConfig);

    boolean authenticate(String user, String pwd);

    Map<String, String> getPromptHeaders();
  }


}
