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
import java.io.InputStream;
import java.io.PrintWriter;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.FilterRegistration;
import javax.servlet.FilterRegistration.Dynamic;
import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.SessionCookieConfig;
import javax.servlet.SessionTrackingMode;
import javax.servlet.descriptor.JspConfigDescriptor;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosPlugin extends AuthenticationPlugin implements HttpClientBuilderPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  Krb5HttpClientBuilder kerberosBuilder = new Krb5HttpClientBuilder();
  private Filter kerberosFilter;
  
  public static final String NAME_RULES_PARAM = "solr.kerberos.name.rules";
  public static final String COOKIE_DOMAIN_PARAM = "solr.kerberos.cookie.domain";
  public static final String COOKIE_PATH_PARAM = "solr.kerberos.cookie.path";
  public static final String PRINCIPAL_PARAM = "solr.kerberos.principal";
  public static final String KEYTAB_PARAM = "solr.kerberos.keytab";
  public static final String TOKEN_VALID_PARAM = "solr.kerberos.token.valid";
  public static final String COOKIE_PORT_AWARE_PARAM = "solr.kerberos.cookie.portaware";
  public static final String IMPERSONATOR_PREFIX = "solr.kerberos.impersonator.user.";
  public static final String DELEGATION_TOKEN_ENABLED = "solr.kerberos.delegation.token.enabled";
  public static final String DELEGATION_TOKEN_KIND = "solr.kerberos.delegation.token.kind";
  public static final String DELEGATION_TOKEN_VALIDITY = "solr.kerberos.delegation.token.validity";
  public static final String DELEGATION_TOKEN_SECRET_PROVIDER = "solr.kerberos.delegation.token.signer.secret.provider";
  public static final String DELEGATION_TOKEN_SECRET_PROVIDER_ZK_PATH =
      "solr.kerberos.delegation.token.signer.secret.provider.zookeper.path";
  public static final String DELEGATION_TOKEN_SECRET_MANAGER_ZNODE_WORKING_PATH =
      "solr.kerberos.delegation.token.secret.manager.znode.working.path";

  public static final String DELEGATION_TOKEN_TYPE_DEFAULT = "solr-dt";
  public static final String IMPERSONATOR_DO_AS_HTTP_PARAM = "doAs";
  public static final String IMPERSONATOR_USER_NAME = "solr.impersonator.user.name";

  // filled in by Plugin/Filter
  static final String REQUEST_CONTINUES_ATTR =
      "org.apache.solr.security.kerberosplugin.requestcontinues";
  static final String DELEGATION_TOKEN_ZK_CLIENT =
      "solr.kerberos.delegation.token.zk.client";

  private final CoreContainer coreContainer;

  public KerberosPlugin(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  public void init(Map<String, Object> pluginConfig) {
    try {
      FilterConfig conf = getInitFilterConfig(pluginConfig, false);
      kerberosFilter.init(conf);
    } catch (ServletException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error initializing kerberos authentication plugin: "+e);
    }
  }

  @VisibleForTesting
  protected FilterConfig getInitFilterConfig(Map<String, Object> pluginConfig, boolean skipKerberosChecking) {
    Map<String, String> params = new HashMap();
    params.put("type", "kerberos");
    putParam(params, "kerberos.name.rules", NAME_RULES_PARAM, "DEFAULT");
    putParam(params, "token.valid", TOKEN_VALID_PARAM, "30");
    putParam(params, "cookie.path", COOKIE_PATH_PARAM, "/");
    if (!skipKerberosChecking) {
      putParam(params, "kerberos.principal", PRINCIPAL_PARAM, null);
      putParam(params, "kerberos.keytab", KEYTAB_PARAM, null);
    } else {
      putParamOptional(params, "kerberos.principal", PRINCIPAL_PARAM);
      putParamOptional(params, "kerberos.keytab", KEYTAB_PARAM);
    }

    String delegationTokenStr = System.getProperty(DELEGATION_TOKEN_ENABLED, null);
    boolean delegationTokenEnabled =
        (delegationTokenStr == null) ? false : Boolean.parseBoolean(delegationTokenStr);
    ZkController controller = coreContainer.getZkController();

    if (delegationTokenEnabled) {
      putParam(params, "delegation-token.token-kind", DELEGATION_TOKEN_KIND, DELEGATION_TOKEN_TYPE_DEFAULT);
      if (coreContainer.isZooKeeperAware()) {
        putParam(params, "signer.secret.provider", DELEGATION_TOKEN_SECRET_PROVIDER, "zookeeper");
        if ("zookeeper".equals(params.get("signer.secret.provider"))) {
          String zkHost = controller.getZkServerAddress();
          putParam(params, "token.validity", DELEGATION_TOKEN_VALIDITY, "36000");
          params.put("zk-dt-secret-manager.enable", "true");
          // Note - Curator complains if the znodeWorkingPath starts with /
          String chrootPath = zkHost.substring(zkHost.indexOf("/"));
          String relativePath = chrootPath.startsWith("/") ? chrootPath.substring(1) : chrootPath;
          putParam(params, "zk-dt-secret-manager.znodeWorkingPath",
              DELEGATION_TOKEN_SECRET_MANAGER_ZNODE_WORKING_PATH,
              relativePath + SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH + "/zkdtsm");
          putParam(params, "signer.secret.provider.zookeeper.path",
              DELEGATION_TOKEN_SECRET_PROVIDER_ZK_PATH, "/token");
          // ensure krb5 is setup properly before running curator
          getHttpClientBuilder(SolrHttpClientBuilder.create());
        }
      } else {
        log.info("CoreContainer is not ZooKeeperAware, not setting ZK-related delegation token properties");
      }
    }

    // Special handling for the "cookie.domain" based on whether port should be
    // appended to the domain. Useful for situations where multiple solr nodes are
    // on the same host.
    String usePortStr = System.getProperty(COOKIE_PORT_AWARE_PARAM, null);
    boolean needPortAwareCookies = (usePortStr == null) ? false: Boolean.parseBoolean(usePortStr);

    if (!needPortAwareCookies || !coreContainer.isZooKeeperAware()) {
      putParam(params, "cookie.domain", COOKIE_DOMAIN_PARAM, null);
    } else { // we need port aware cookies and we are in SolrCloud mode.
      String host = System.getProperty(COOKIE_DOMAIN_PARAM, null);
      if (host==null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Missing required parameter '"+COOKIE_DOMAIN_PARAM+"'.");
      }
      int port = controller.getHostPort();
      params.put("cookie.domain", host + ":" + port);
    }

    // check impersonator config
    for (Enumeration e = System.getProperties().propertyNames(); e.hasMoreElements();) {
      String key = e.nextElement().toString();
      if (key.startsWith(IMPERSONATOR_PREFIX)) {
        if (!delegationTokenEnabled) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "Impersonator configuration requires delegation tokens to be enabled: " + key);
        }
        params.put(key, System.getProperty(key));
      }
    }
    final ServletContext servletContext = new AttributeOnlyServletContext();
    if (controller != null) {
      servletContext.setAttribute(DELEGATION_TOKEN_ZK_CLIENT, controller.getZkClient());
    }
    if (delegationTokenEnabled) {
      kerberosFilter = new DelegationTokenKerberosFilter();
      // pass an attribute-enabled context in order to pass the zkClient
      // and because the filter may pass a curator instance.
    } else {
      kerberosFilter = new KerberosFilter();
    }
    log.info("Params: "+params);

    FilterConfig conf = new FilterConfig() {
      @Override
      public ServletContext getServletContext() {
        return servletContext;
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

    return conf;
  }

  private void putParam(Map<String, String> params, String internalParamName, String externalParamName, String defaultValue) {
    String value = System.getProperty(externalParamName, defaultValue);
    if (value==null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Missing required parameter '"+externalParamName+"'.");
    }
    params.put(internalParamName, value);
  }

  private void putParamOptional(Map<String, String> params, String internalParamName, String externalParamName) {
    String value = System.getProperty(externalParamName);
    if (value!=null) {
      params.put(internalParamName, value);
    }
  }

  @Override
  public boolean doAuthenticate(ServletRequest req, ServletResponse rsp,
      FilterChain chain) throws Exception {
    log.debug("Request to authenticate using kerberos: "+req);

    final HttpServletResponse frsp = (HttpServletResponse)rsp;

    // kerberosFilter may close the stream and write to closed streams,
    // see HADOOP-13346.  To work around, pass a PrintWriter that ignores
    // closes
    HttpServletResponse rspCloseShield = new HttpServletResponseWrapper(frsp) {
      @SuppressForbidden(reason = "Hadoop DelegationTokenAuthenticationFilter uses response writer, this" +
          "is providing a CloseShield on top of that")
      @Override
      public PrintWriter getWriter() throws IOException {
        final PrintWriter pw = new PrintWriterWrapper(frsp.getWriter()) {
          @Override
          public void close() {};
        };
        return pw;
      }
    };
    kerberosFilter.doFilter(req, rspCloseShield, chain);
    String requestContinuesAttr = (String)req.getAttribute(REQUEST_CONTINUES_ATTR);
    if (requestContinuesAttr == null) {
      log.warn("Could not find " + REQUEST_CONTINUES_ATTR);
      return false;
    } else {
      return Boolean.parseBoolean(requestContinuesAttr);
    }
  }

  @Override
  public SolrHttpClientBuilder getHttpClientBuilder(SolrHttpClientBuilder builder) {
    return kerberosBuilder.getBuilder(builder);
  }

  @Override
  public void close() {
    kerberosFilter.destroy();
    kerberosBuilder.close();
  }

  protected Filter getKerberosFilter() { return kerberosFilter; }

  protected void setKerberosFilter(Filter kerberosFilter) { this.kerberosFilter = kerberosFilter; }

  protected static class AttributeOnlyServletContext implements ServletContext {
    private Map<String, Object> attributes = new HashMap<String, Object>();

    @Override
    public void setSessionTrackingModes(Set<SessionTrackingMode> sessionTrackingModes) {}
    
    @Override
    public boolean setInitParameter(String name, String value) {
      return false;
    }

    @Override
    public void setAttribute(String name, Object object) {
      attributes.put(name, object);
    }

    @Override
    public void removeAttribute(String name) {
      attributes.remove(name);
    }
    
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
      return Collections.enumeration(attributes.keySet());
    }

    @Override
    public Object getAttribute(String name) {
      return attributes.get(name);
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

  /*
   * {@link AuthenticationHandler} that delegates to another {@link AuthenticationHandler}
   * and records the response of managementOperation (which indicates whether the request
   * should continue or not).
   */
  public static class RequestContinuesRecorderAuthenticationHandler implements AuthenticationHandler {
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
      request.setAttribute(KerberosPlugin.REQUEST_CONTINUES_ATTR, new Boolean(result).toString());
      return result;
    }


    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
        throws IOException, AuthenticationException {
      return authHandler.authenticate(request, response);
    }
  }
}
