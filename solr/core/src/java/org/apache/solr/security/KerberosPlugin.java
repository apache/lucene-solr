package org.apache.solr.security;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.FilterRegistration;
import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.SessionCookieConfig;
import javax.servlet.SessionTrackingMode;
import javax.servlet.FilterRegistration.Dynamic;
import javax.servlet.descriptor.JspConfigDescriptor;

import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class KerberosPlugin extends AuthenticationPlugin {
  static final Logger log = LoggerFactory.getLogger(KerberosPlugin.class);

  HttpClientConfigurer kerberosConfigurer = new Krb5HttpClientConfigurer();
  Filter kerberosFilter = new KerberosFilter();
  
  final String NAME_RULES_PARAM = "solr.kerberos.name.rules";
  final String COOKIE_DOMAIN_PARAM = "solr.kerberos.cookie.domain";
  final String COOKIE_PATH_PARAM = "solr.kerberos.cookie.path";
  final String PRINCIPAL_PARAM = "solr.kerberos.principal";
  final String KEYTAB_PARAM = "solr.kerberos.keytab";
  final String TOKEN_VALID_PARAM = "solr.kerberos.token.valid";

  @Override
  public void init(Map<String, Object> pluginConfig) {
    try {
      Map<String, String> params = new HashMap();
      params.put("type", "kerberos");
      putParam(params, "kerberos.name.rules", NAME_RULES_PARAM, "DEFAULT");
      putParam(params, "token.valid", TOKEN_VALID_PARAM, "30");
      putParam(params, "cookie.domain", COOKIE_DOMAIN_PARAM, null);
      putParam(params, "cookie.path", COOKIE_PATH_PARAM, "/");
      putParam(params, "kerberos.principal", PRINCIPAL_PARAM, null);
      putParam(params, "kerberos.keytab", KEYTAB_PARAM, null);

      log.info("Params: "+params);

      FilterConfig conf = new FilterConfig() {
        @Override
        public ServletContext getServletContext() {
          return noContext;
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
          return new IteratorEnumeration(params.keySet().iterator());
        }

        @Override
        public String getInitParameter(String param) {
          return params.get(param);
        }

        @Override
        public String getFilterName() {
          return "KerberosFilter";
        }
      };

      kerberosFilter.init(conf);
    } catch (ServletException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error initializing kerberos authentication plugin: "+e);
    }
  }

  private void putParam(Map<String, String> params, String internalParamName, String externalParamName, String defaultValue) {
    String value = System.getProperty(externalParamName, defaultValue);
    if (value==null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Missing required parameter '"+externalParamName+"'.");
    }
    params.put(internalParamName, value);
  }

  @Override
  public void doAuthenticate(ServletRequest req, ServletResponse rsp,
      FilterChain chain) throws Exception {
    log.debug("Request to authenticate using kerberos: "+req);
    kerberosFilter.doFilter(req, rsp, chain);    
  }

  @Override
  public HttpClientConfigurer getDefaultConfigurer() {
    return kerberosConfigurer;
  }

  public void close() {
    kerberosFilter.destroy();
  }

  protected static ServletContext noContext = new ServletContext() {
    
    @Override
    public void setSessionTrackingModes(Set<SessionTrackingMode> sessionTrackingModes) {}
    
    @Override
    public boolean setInitParameter(String name, String value) {
      return false;
    }
    
    @Override
    public void setAttribute(String name, Object object) {}
    
    @Override
    public void removeAttribute(String name) {}
    
    @Override
    public void log(String message, Throwable throwable) {}
    
    @Override
    public void log(Exception exception, String msg) {}
    
    @Override
    public void log(String msg) {}
    
    @Override
    public String getVirtualServerName() {
      return null;
    }
    
    @Override
    public SessionCookieConfig getSessionCookieConfig() {
      return null;
    }
    
    @Override
    public Enumeration<Servlet> getServlets() {
      return null;
    }
    
    @Override
    public Map<String,? extends ServletRegistration> getServletRegistrations() {
      return null;
    }
    
    @Override
    public ServletRegistration getServletRegistration(String servletName) {
      return null;
    }
    
    @Override
    public Enumeration<String> getServletNames() {
      return null;
    }
    
    @Override
    public String getServletContextName() {
      return null;
    }
    
    @Override
    public Servlet getServlet(String name) throws ServletException {
      return null;
    }
    
    @Override
    public String getServerInfo() {
      return null;
    }
    
    @Override
    public Set<String> getResourcePaths(String path) {
      return null;
    }
    
    @Override
    public InputStream getResourceAsStream(String path) {
      return null;
    }
    
    @Override
    public URL getResource(String path) throws MalformedURLException {
      return null;
    }
    
    @Override
    public RequestDispatcher getRequestDispatcher(String path) {
      return null;
    }
    
    @Override
    public String getRealPath(String path) {
      return null;
    }
    
    @Override
    public RequestDispatcher getNamedDispatcher(String name) {
      return null;
    }
    
    @Override
    public int getMinorVersion() {
      return 0;
    }
    
    @Override
    public String getMimeType(String file) {
      return null;
    }
    
    @Override
    public int getMajorVersion() {
      return 0;
    }
    
    @Override
    public JspConfigDescriptor getJspConfigDescriptor() {
      return null;
    }
    
    @Override
    public Enumeration<String> getInitParameterNames() {
      return null;
    }
    
    @Override
    public String getInitParameter(String name) {
      return null;
    }
    
    @Override
    public Map<String,? extends FilterRegistration> getFilterRegistrations() {
      return null;
    }
    
    @Override
    public FilterRegistration getFilterRegistration(String filterName) {
      return null;
    }
    
    @Override
    public Set<SessionTrackingMode> getEffectiveSessionTrackingModes() {
      return null;
    }
    
    @Override
    public int getEffectiveMinorVersion() {
      return 0;
    }
    
    @Override
    public int getEffectiveMajorVersion() {
      return 0;
    }
    
    @Override
    public Set<SessionTrackingMode> getDefaultSessionTrackingModes() {
      return null;
    }
    
    @Override
    public String getContextPath() {
      return null;
    }
    
    @Override
    public ServletContext getContext(String uripath) {
      return null;
    }
    
    @Override
    public ClassLoader getClassLoader() {
      return null;
    }
    
    @Override
    public Enumeration<String> getAttributeNames() {
      return null;
    }
    
    @Override
    public Object getAttribute(String name) {
      return null;
    }
    
    @Override
    public void declareRoles(String... roleNames) {}
    
    @Override
    public <T extends Servlet> T createServlet(Class<T> clazz) throws ServletException {
      return null;
    }
    
    @Override
    public <T extends EventListener> T createListener(Class<T> clazz) throws ServletException {
      return null;
    }
    
    @Override
    public <T extends Filter> T createFilter(Class<T> clazz) throws ServletException {
      return null;
    }
    
    @Override
    public javax.servlet.ServletRegistration.Dynamic addServlet(String servletName, Class<? extends Servlet> servletClass) {
      return null;
    }
    
    @Override
    public javax.servlet.ServletRegistration.Dynamic addServlet(String servletName, Servlet servlet) {
      return null;
    }
    
    @Override
    public javax.servlet.ServletRegistration.Dynamic addServlet(String servletName, String className) {
      return null;
    }
    
    @Override
    public void addListener(Class<? extends EventListener> listenerClass) {}
    
    @Override
    public <T extends EventListener> void addListener(T t) {}
    
    @Override
    public void addListener(String className) {}
    
    @Override
    public Dynamic addFilter(String filterName, Class<? extends Filter> filterClass) {
      return null;
    }
    
    @Override
    public Dynamic addFilter(String filterName, Filter filter) {
      return null;
    }
    
    @Override
    public Dynamic addFilter(String filterName, String className) {
      return null;
    }
  };
}
