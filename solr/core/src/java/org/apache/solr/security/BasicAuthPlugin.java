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

import javax.security.auth.Subject;
import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.annotation.Contract;
import org.apache.http.annotation.ThreadingBehavior;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SpecProvider;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.eclipse.jetty.client.api.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicAuthPlugin extends AuthenticationPlugin implements ConfigEditablePlugin , SpecProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private AuthenticationProvider authenticationProvider;
  private final static ThreadLocal<Header> authHeader = new ThreadLocal<>();
  private static final String X_REQUESTED_WITH_HEADER = "X-Requested-With";
  private boolean blockUnknown = true;
  private boolean forwardCredentials = false;

  public boolean authenticate(String username, String pwd) {
    return authenticationProvider.authenticate(username, pwd);
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {
    Object o = pluginConfig.get(PROPERTY_BLOCK_UNKNOWN);
    if (o != null) {
      try {
        blockUnknown = Boolean.parseBoolean(o.toString());
      } catch (Exception e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid value for parameter " + PROPERTY_BLOCK_UNKNOWN);
      }
    }
    o = pluginConfig.get(FORWARD_CREDENTIALS);
    if (o != null) {
      try {
        forwardCredentials = Boolean.parseBoolean(o.toString());
      } catch (Exception e) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid value for parameter " + FORWARD_CREDENTIALS);
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
    throw new SolrException(ErrorCode.BAD_REQUEST, "This cannot be edited");
  }

  protected AuthenticationProvider getAuthenticationProvider(Map<String, Object> pluginConfig) {
    Sha256AuthenticationProvider provider = new Sha256AuthenticationProvider();
    provider.init(pluginConfig);
    return provider;
  }

  private void authenticationFailure(HttpServletResponse response, boolean isAjaxRequest, String message) throws IOException {
    getPromptHeaders(isAjaxRequest).forEach(response::setHeader);
    response.sendError(401, message);
  }

  @Override
  public boolean doAuthenticate(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws Exception {

    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;

    String authHeader = request.getHeader("Authorization");
    boolean isAjaxRequest = isAjaxRequest(request);
    
    if (authHeader != null) {
      BasicAuthPlugin.authHeader.set(new BasicHeader("Authorization", authHeader));
      StringTokenizer st = new StringTokenizer(authHeader);
      if (st.hasMoreTokens()) {
        String basic = st.nextToken();
        if (basic.equalsIgnoreCase("Basic")) {
          if (st.hasMoreTokens()) {
            try {
              String credentials = new String(Base64.decodeBase64(st.nextToken()), StandardCharsets.UTF_8);
              int p = credentials.indexOf(":");
              if (p != -1) {
                final String username = credentials.substring(0, p).trim();
                String pwd = credentials.substring(p + 1).trim();
                if (!authenticate(username, pwd)) {
                  numWrongCredentials.inc();
                  log.debug("Bad auth credentials supplied in Authorization header");
                  authenticationFailure(response, isAjaxRequest, "Bad credentials");
                  return false;
                } else {
                  HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(request) {
                    @Override
                    public Principal getUserPrincipal() {
                      return new BasicAuthUserPrincipal(username, pwd);
                    }
                  };
                  numAuthenticated.inc();
                  filterChain.doFilter(wrapper, response);
                  return true;
                }
              } else {
                numErrors.mark();
                authenticationFailure(response, isAjaxRequest, "Invalid authentication token");
                return false;
              }
            } catch (UnsupportedEncodingException e) {
              throw new Error("Couldn't retrieve authentication", e);
            }
          } else {
            numErrors.mark();
            authenticationFailure(response, isAjaxRequest, "Malformed Basic Auth header");
            return false;
          }
        }
      }
    }
    
    // No auth header OR header empty OR Authorization header not of type Basic, i.e. "unknown" user
    if (blockUnknown) {
      numMissingCredentials.inc();
      authenticationFailure(response, isAjaxRequest, "require authentication");
      return false;
    } else {
      numPassThrough.inc();
      request.setAttribute(AuthenticationPlugin.class.getName(), getPromptHeaders(isAjaxRequest));
      filterChain.doFilter(request, response);
      return true;
    }
  }

  /**
   * Get the prompt headers, and replace Basic with xBasic if ajax request to avoid
   * browser intercepting the authentication
   * @param isAjaxRequest set to true if the request is an ajax request
   * @return map of headers
   */
  private Map<String, String> getPromptHeaders(boolean isAjaxRequest) {
    Map<String,String> headers = new HashMap(authenticationProvider.getPromptHeaders());
    if (isAjaxRequest && headers.containsKey(HttpHeaders.WWW_AUTHENTICATE) 
        && headers.get(HttpHeaders.WWW_AUTHENTICATE).startsWith("Basic ")) {
      headers.put(HttpHeaders.WWW_AUTHENTICATE, "x" + headers.get(HttpHeaders.WWW_AUTHENTICATE));
      log.debug("Prefixing {} header for Basic Auth with 'x' to prevent browser basic auth popup",
          HttpHeaders.WWW_AUTHENTICATE);
    }
    return headers;
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void closeRequest() {
    authHeader.remove();
  }

  public interface AuthenticationProvider extends SpecProvider {
    void init(Map<String, Object> pluginConfig);

    boolean authenticate(String user, String pwd);

    Map<String, String> getPromptHeaders();
  }

  @Override
  protected boolean interceptInternodeRequest(HttpRequest httpRequest, HttpContext httpContext) {
    if (forwardCredentials) {
      if (httpContext instanceof HttpClientContext) {
        HttpClientContext httpClientContext = (HttpClientContext) httpContext;
        if (httpClientContext.getUserToken() instanceof BasicAuthUserPrincipal) {
          BasicAuthUserPrincipal principal = (BasicAuthUserPrincipal) httpClientContext.getUserToken();
          String userPassBase64 = Base64.encodeBase64String((principal.getName() + ":" + principal.getPassword()).getBytes(StandardCharsets.UTF_8));
          httpRequest.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + userPassBase64);
          return true;
        }
      }
    }
    return false;
  }

  @Override
  protected boolean interceptInternodeRequest(Request request) {
    if (forwardCredentials) {
      Object userToken = request.getAttributes().get(Http2SolrClient.REQ_PRINCIPAL_KEY);
      if (userToken instanceof BasicAuthUserPrincipal) {
        BasicAuthUserPrincipal principal = (BasicAuthUserPrincipal) userToken;
        String userPassBase64 = Base64.encodeBase64String((principal.getName() + ":" + principal.getPassword()).getBytes(StandardCharsets.UTF_8));
        request.header(HttpHeaders.AUTHORIZATION, "Basic " + userPassBase64);
        return true;
      }
    }
    return false;
  }

  @Override
  public ValidatingJsonMap getSpec() {
    return authenticationProvider.getSpec();
  }
  public boolean getBlockUnknown(){
    return blockUnknown;
  }

  public static final String PROPERTY_BLOCK_UNKNOWN = "blockUnknown";
  public static final String PROPERTY_REALM = "realm";
  public static final String FORWARD_CREDENTIALS = "forwardCredentials";
  private static final Set<String> PROPS = ImmutableSet.of(PROPERTY_BLOCK_UNKNOWN, PROPERTY_REALM, FORWARD_CREDENTIALS);

  /**
   * Check if the request is an AJAX request, i.e. from the Admin UI or other SPA front 
   * @param request the servlet request
   * @return true if the request is AJAX request
   */
  private boolean isAjaxRequest(HttpServletRequest request) {
    return "XMLHttpRequest".equalsIgnoreCase(request.getHeader(X_REQUESTED_WITH_HEADER));
  }
  
  @Contract(threading = ThreadingBehavior.IMMUTABLE)
  private class BasicAuthUserPrincipal implements Principal, Serializable {
    private String username;
    private final String password;

    public BasicAuthUserPrincipal(String username, String pwd) {
      this.username = username;
      this.password = pwd;
    }

    @Override
    public String getName() {
        return this.username;
    }

    public String getPassword() {
      return password;
    }

    @Override
    public boolean implies(Subject subject) {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      BasicAuthUserPrincipal that = (BasicAuthUserPrincipal) o;
      return Objects.equals(username, that.username) &&
          Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
      return Objects.hash(username, password);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("username", username)
          .append("pwd", "*****")
          .toString();
    }
  }
}
