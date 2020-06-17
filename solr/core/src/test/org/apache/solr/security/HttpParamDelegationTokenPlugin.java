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
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpListenerFactory;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrRequestInfo;
import org.eclipse.jetty.client.api.Request;

/**
 * AuthenticationHandler that supports delegation tokens and simple
 * authentication via the "user" http parameter
 */
public class HttpParamDelegationTokenPlugin extends KerberosPlugin {
  public static final String USER_PARAM = "user"; // http parameter for user authentication
  public static final String REMOTE_HOST_PARAM = "remoteHost"; // http parameter for indicating remote host
  public static final String REMOTE_ADDRESS_PARAM = "remoteAddress"; // http parameter for indicating remote address
  public static final String INTERNAL_REQUEST_HEADER = "internalRequest"; // http header for indicating internal request

  boolean isSolrThread() {
    return ExecutorUtil.isSolrServerThread();
  }

  private final HttpRequestInterceptor interceptor = new HttpRequestInterceptor() {
    @Override
    public void process(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
      getPrincipal().ifPresent(usr -> httpRequest.setHeader(INTERNAL_REQUEST_HEADER, usr));
    }
  };

  public HttpParamDelegationTokenPlugin(CoreContainer coreContainer) {
    super(coreContainer);
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {
    try {
      final FilterConfig initConf = getInitFilterConfig(pluginConfig, true);

      FilterConfig conf = new FilterConfig() {
        @Override
        public ServletContext getServletContext() {
          return initConf.getServletContext();
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
          return initConf.getInitParameterNames();
        }

        @Override
        public String getInitParameter(String param) {
          if (AuthenticationFilter.AUTH_TYPE.equals(param)) {
            return HttpParamDelegationTokenAuthenticationHandler.class.getName();
          }
          return initConf.getInitParameter(param);
        }

        @Override
        public String getFilterName() {
         return "HttpParamFilter";
        }
      };
      Filter kerberosFilter = new HttpParamToRequestFilter();
      kerberosFilter.init(conf);
      setKerberosFilter(kerberosFilter);
    } catch (ServletException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error initializing kerberos authentication plugin: "+e);
    }
  }

  private Optional<String> getPrincipal() {
    SolrRequestInfo reqInfo = SolrRequestInfo.getRequestInfo();
    String usr;
    if (reqInfo != null) {
      Principal principal = reqInfo.getUserPrincipal();
      if (principal == null) {
        //this had a request but not authenticated
        //so we don't not need to set a principal
        return Optional.empty();
      } else {
        usr = principal.getName();
      }
    } else {
      if (!isSolrThread()) {
        //if this is not running inside a Solr threadpool (as in testcases)
        // then no need to add any header
        return Optional.empty();
      }
      //this request seems to be originated from Solr itself
      usr = "$"; //special name to denote the user is the node itself
    }
    return Optional.of(usr);
  }

  @Override
  public void setup(Http2SolrClient client) {
    final HttpListenerFactory.RequestResponseListener listener = new HttpListenerFactory.RequestResponseListener() {
      @Override
      public void onQueued(Request request) {
        getPrincipal().ifPresent(usr -> request.header(INTERNAL_REQUEST_HEADER, usr));
      }
    };
    client.addListenerFactory(() -> listener);
  }

  @Override
  public SolrHttpClientBuilder getHttpClientBuilder(SolrHttpClientBuilder builder) {
    HttpClientUtil.addRequestInterceptor(interceptor);
    builder = super.getHttpClientBuilder(builder);
    return builder;
  }

  @Override
  public void close() {
    HttpClientUtil.removeRequestInterceptor(interceptor);
    super.close();
  }

  private static String getHttpParam(HttpServletRequest request, String param) {
    List<NameValuePair> pairs = URLEncodedUtils.parse(request.getQueryString(), Charset.forName("UTF-8"));
    for (NameValuePair nvp : pairs) {
      if (param.equals(nvp.getName())) {
        return nvp.getValue();
      }
    }
    return null;
  }

  public static class HttpParamDelegationTokenAuthenticationHandler extends
      DelegationTokenAuthenticationHandler {

    public HttpParamDelegationTokenAuthenticationHandler() {
      super(new HttpParamAuthenticationHandler());
    }

    @Override
    public void init(Properties config) throws ServletException {
      Properties conf = new Properties();
      for (@SuppressWarnings({"rawtypes"})Map.Entry entry : config.entrySet()) {
        conf.setProperty((String) entry.getKey(), (String) entry.getValue());
      }
      conf.setProperty(TOKEN_KIND, KerberosPlugin.DELEGATION_TOKEN_TYPE_DEFAULT);
      super.init(conf);
    }

    private static class HttpParamAuthenticationHandler implements AuthenticationHandler {
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
        if (userName == null) {
          //check if this is an internal request
          userName = request.getHeader(INTERNAL_REQUEST_HEADER);
        }
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

  /**
   * Filter that converts http params to HttpServletRequest params
   */
  private static class HttpParamToRequestFilter extends DelegationTokenKerberosFilter {
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
      final HttpServletRequest httpRequest = (HttpServletRequest) request;
      final HttpServletRequestWrapper requestWrapper = new HttpServletRequestWrapper(httpRequest) {
        @Override
        public String getRemoteHost() {
          String param = getHttpParam(httpRequest, REMOTE_HOST_PARAM);
          return param != null ? param : httpRequest.getRemoteHost();
        }

        @Override
        public String getRemoteAddr() {
          String param = getHttpParam(httpRequest, REMOTE_ADDRESS_PARAM);
          return param != null ? param : httpRequest.getRemoteAddr();
        }
      };

      super.doFilter(requestWrapper, response, chain);
    }

    @Override
    protected void doFilter(FilterChain filterChain, HttpServletRequest request,
                            HttpServletResponse response) throws IOException, ServletException {
      // remove the filter-specific authentication information, so it doesn't get accidentally forwarded.
      List<NameValuePair> newPairs = new LinkedList<NameValuePair>();
      List<NameValuePair> pairs = URLEncodedUtils.parse(request.getQueryString(), Charset.forName("UTF-8"));
      for (NameValuePair nvp : pairs) {
        if (!USER_PARAM.equals(nvp.getName())) {
          newPairs.add(nvp);
        }
        else {
          request.setAttribute(USER_PARAM, nvp.getValue());
        }
      }
      final String queryStringNoUser = URLEncodedUtils.format(newPairs, StandardCharsets.UTF_8);
      HttpServletRequest requestWrapper = new HttpServletRequestWrapper(request) {
        @Override
        public String getQueryString() {
          return queryStringNoUser;
        }
      };
      super.doFilter(filterChain, requestWrapper, response);
    }
  }
}
