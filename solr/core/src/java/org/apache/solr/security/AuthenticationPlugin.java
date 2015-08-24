package org.apache.solr.security;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.attribute.UserPrincipal;
import java.security.Principal;
import java.util.Map;

import org.apache.http.auth.BasicUserPrincipal;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;

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

/**
 * 
 * @lucene.experimental
 */
public abstract class AuthenticationPlugin implements Closeable {

  final public static String AUTHENTICATION_PLUGIN_PROP = "authenticationPlugin";

  /**
   * This is called upon loading up of a plugin, used for setting it up.
   * @param pluginConfig Config parameters, possibly from a ZK source
   */
  public abstract void init(Map<String, Object> pluginConfig);

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
 
  /**
   * This method must authenticate the request. Upon a successful authentication, this 
   * must call the next filter in the filter chain and set the user principal of the request,
   * or else, upon an error or an authentication failure, throw an exception.
   * 
   * @param request the http request
   * @param response the http response
   * @param filterChain the servlet filter chain
   * @throws Exception any exception thrown during the authentication, e.g. 
   * PriviledgedAccessException
   */
  public abstract void doAuthenticate(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws Exception;


  /**
   * Cleanup any per request  data
   */
  public void closeRequest() {
  }

}
