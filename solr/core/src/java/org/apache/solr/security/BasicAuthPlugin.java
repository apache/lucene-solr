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
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.message.BasicHeader;
import org.apache.solr.common.SolrException;
import org.apache.solr.util.CommandOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicAuthPlugin extends AuthenticationPlugin implements ConfigEditablePlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private AuthenticationProvider authenticationProvider;
  private final static ThreadLocal<Header> authHeader = new ThreadLocal<>();
  private boolean blockUnknown = false;

  public boolean authenticate(String username, String pwd) {
    return authenticationProvider.authenticate(username, pwd);
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {
    Object o = pluginConfig.get(BLOCK_UNKNOWN);
    if (o != null) {
      try {
        blockUnknown = Boolean.parseBoolean(o.toString());
      } catch (Exception e) {
        log.error(e.getMessage());
      }
    }
    authenticationProvider = getAuthenticationProvider(pluginConfig);
  }

  @Override
  public Map<String, Object> edit(Map<String, Object> latestConf, List<CommandOperation> commands) {
    for (CommandOperation command : commands) {
      if (command.name.equals("set-property")) {
        for (Map.Entry<String, Object> e : command.getDataMap().entrySet()) {
          if (PROPS.contains(e.getKey())) {
            latestConf.put(e.getKey(), e.getValue());
            return latestConf;
          } else {
            command.addError("Unknown property " + e.getKey());
          }
        }
      }
    }
    if (!CommandOperation.captureErrors(commands).isEmpty()) return null;
    if (authenticationProvider instanceof ConfigEditablePlugin) {
      ConfigEditablePlugin editablePlugin = (ConfigEditablePlugin) authenticationProvider;
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
    for (Map.Entry<String, String> entry : authenticationProvider.getPromptHeaders().entrySet()) {
      response.setHeader(entry.getKey(), entry.getValue());
    }
    response.sendError(401, message);
  }

  @Override
  public boolean doAuthenticate(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws Exception {

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
                log.debug("Bad auth credentials supplied in Authorization header");
                authenticationFailure(response, "Bad credentials");
              } else {
                HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(request) {
                  @Override
                  public Principal getUserPrincipal() {
                    return new BasicUserPrincipal(username);
                  }
                };
                filterChain.doFilter(wrapper, response);
                return true;
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
      if (blockUnknown) {
        authenticationFailure(response, "require authentication");
      } else {
        request.setAttribute(AuthenticationPlugin.class.getName(), authenticationProvider.getPromptHeaders());
        filterChain.doFilter(request, response);
        return true;
      }
    }
    return false;
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

  public boolean getBlockUnknown(){
    return blockUnknown;
  }

  public static final String BLOCK_UNKNOWN = "blockUnknown";
  private static final Set<String> PROPS = ImmutableSet.of(BLOCK_UNKNOWN);


}
