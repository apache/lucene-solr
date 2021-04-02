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

import static org.apache.solr.security.RequestContinuesRecorderAuthenticationHandler.REQUEST_CONTINUES_ATTR;
import static org.apache.solr.security.HadoopAuthFilter.DELEGATION_TOKEN_ZK_CLIENT;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a generic plugin which can use authentication schemes exposed by the
 * Hadoop framework. This plugin supports following features
 * - integration with authentication mehcanisms (e.g. kerberos)
 * - Delegation token support
 * - Proxy users (or secure impersonation) support
 *
 * This plugin enables defining configuration parameters required by the undelying Hadoop authentication
 * mechanism. These configuration parameters can either be specified as a Java system property or the default
 * value can be specified as part of the plugin configuration.
 *
 * The proxy users are configured by specifying relevant Hadoop configuration parameters. Please note that
 * the delegation token support must be enabled for using the proxy users support.
 *
 * Note - this class does not support configuring authentication mechanism for Solr internal communication.
 * For this purpose {@linkplain ConfigurableInternodeAuthHadoopPlugin} should be used. If this plugin is used in the
 * SolrCloud mode, it will use PKI based authentication mechanism for Solr internal communication.
 **/
public class HadoopAuthPlugin extends AuthenticationPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * A property specifying the type of authentication scheme to be configured.
   */
  private static final String HADOOP_AUTH_TYPE = "type";

  /**
   * A property specifies the value of the prefix to be used to define Java system property
   * for configuring the authentication mechanism. The name of the Java system property is
   * defined by appending the configuration parmeter namne to this prefix value e.g. if prefix
   * is 'solr' then the Java system property 'solr.kerberos.principal' defines the value of
   * configuration parameter 'kerberos.principal'.
   */
  private static final String SYSPROP_PREFIX_PROPERTY = "sysPropPrefix";

  /**
   * A property specifying the configuration parameters required by the authentication scheme
   * defined by {@linkplain #HADOOP_AUTH_TYPE} property.
   */
  private static final String AUTH_CONFIG_NAMES_PROPERTY = "authConfigs";

  /**
   * A property specifying the default values for the configuration parameters specified by the
   * {@linkplain #AUTH_CONFIG_NAMES_PROPERTY} property. The default values are specified as a
   * collection of key-value pairs (i.e. property-name : default_value).
   */
  private static final String DEFAULT_AUTH_CONFIGS_PROPERTY = "defaultConfigs";

  /**
   * A property which enable (or disable) the delegation tokens functionality.
   */
  private static final String DELEGATION_TOKEN_ENABLED_PROPERTY = "enableDelegationToken";

  /**
   * A property which enables initialization of kerberos before connecting to Zookeeper.
   */
  private static final String INIT_KERBEROS_ZK = "initKerberosZk";

  /**
   * A property which configures proxy users for the underlying Hadoop authentication mechanism.
   * This configuration is expressed as a collection of key-value pairs  (i.e. property-name : value).
   */
  public static final String PROXY_USER_CONFIGS = "proxyUserConfigs";

  /**
   * This parameter is used to debug the authentication related issues during development.
   * This should not be used in production.
   */
  private static final boolean TRACE_HTTP = Boolean.getBoolean("hadoopauth.tracehttp");

  private AuthenticationFilter authFilter;
  private final Locale defaultLocale = Locale.getDefault();
  protected final CoreContainer coreContainer;
  private boolean delegationTokenEnabled;

  public HadoopAuthPlugin(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  public void init(Map<String,Object> pluginConfig) {
    try {
      delegationTokenEnabled = Boolean.parseBoolean((String)pluginConfig.get(DELEGATION_TOKEN_ENABLED_PROPERTY));
      authFilter = delegationTokenEnabled ? new HadoopAuthFilter() : new AuthenticationFilter() {
        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
          // A hack until HADOOP-15681 get committed
          Locale.setDefault(Locale.US);
          super.doFilter(request, response, filterChain);
        }

        @Override
        protected void doFilter(FilterChain filterChain, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
          Locale.setDefault(defaultLocale);
          super.doFilter(filterChain, request, response);
        }
      };

      // Initialize kerberos before initializing curator instance.
      boolean initKerberosZk = Boolean.parseBoolean((String)pluginConfig.getOrDefault(INIT_KERBEROS_ZK, "false"));
      if (initKerberosZk) {
        (new Krb5HttpClientBuilder()).getBuilder();
      }

      FilterConfig conf = getInitFilterConfig(pluginConfig);
      authFilter.init(conf);

    } catch (ServletException e) {
      log.error("Error initializing {}", getClass().getSimpleName(), e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error initializing " + getClass().getName() + ": "+e);
    }
  }

  @SuppressWarnings("unchecked")
  protected FilterConfig getInitFilterConfig(Map<String, Object> pluginConfig) {
    Map<String, String> params = new HashMap<>();

    String type = (String) Objects.requireNonNull(pluginConfig.get(HADOOP_AUTH_TYPE));
    params.put(HADOOP_AUTH_TYPE, type);

    String sysPropPrefix = (String) pluginConfig.getOrDefault(SYSPROP_PREFIX_PROPERTY, "solr.");
    Collection<String> authConfigNames = (Collection<String>) pluginConfig.
        getOrDefault(AUTH_CONFIG_NAMES_PROPERTY, Collections.emptyList());
    Map<String,String> authConfigDefaults = (Map<String,String>) pluginConfig
        .getOrDefault(DEFAULT_AUTH_CONFIGS_PROPERTY, Collections.emptyMap());
    Map<String,String> proxyUserConfigs = (Map<String,String>) pluginConfig
        .getOrDefault(PROXY_USER_CONFIGS, Collections.emptyMap());

    for ( String configName : authConfigNames) {
      String systemProperty = sysPropPrefix + configName;
      String defaultConfigVal = authConfigDefaults.get(configName);
      String configVal = System.getProperty(systemProperty, defaultConfigVal);
      if (configVal != null) {
        params.put(configName, configVal);
      }
    }
    if (delegationTokenEnabled) {
      // This is the only kind we support right now anyway
      params.putIfAbsent("delegation-token.token-kind", KerberosPlugin.DELEGATION_TOKEN_TYPE_DEFAULT);
    }

    // Configure proxy user settings.
    params.putAll(proxyUserConfigs);

    // Needed to work around HADOOP-13346
    params.put(DelegationTokenAuthenticationHandler.JSON_MAPPER_PREFIX + JsonGenerator.Feature.AUTO_CLOSE_TARGET,
        "false");

    final ServletContext servletContext = new AttributeOnlyServletContext();
    if (log.isInfoEnabled()) {
      log.info("Params: {}", params);
    }

    ZkController controller = coreContainer.getZkController();
    if (controller != null) {
      servletContext.setAttribute(DELEGATION_TOKEN_ZK_CLIENT, controller.getZkClient());
    }

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
        return "HadoopAuthFilter";
      }
    };

    return conf;
  }

  @Override
  public boolean doAuthenticate(ServletRequest request, ServletResponse response, FilterChain filterChain)
      throws Exception {
    final HttpServletResponse frsp = (HttpServletResponse)response;

    if (TRACE_HTTP) {
      HttpServletRequest req = (HttpServletRequest) request;
      log.info("----------HTTP Request---------");
      if (log.isInfoEnabled()) {
        log.info("{} : {}", req.getMethod(), req.getRequestURI());
        log.info("Query : {}", req.getQueryString()); // logOk
      }
      log.info("Headers :");
      Enumeration<String> headers = req.getHeaderNames();
      while (headers.hasMoreElements()) {
        String name = headers.nextElement();
        Enumeration<String> hvals = req.getHeaders(name);
        while (hvals.hasMoreElements()) {
          if (log.isInfoEnabled()) {
            log.info("{} : {}", name, hvals.nextElement());
          }
        }
      }
      log.info("-------------------------------");
    }

    authFilter.doFilter(request, frsp, filterChain);

    switch (frsp.getStatus()) {
      case HttpServletResponse.SC_UNAUTHORIZED:
        // Cannot tell whether the 401 is due to wrong or missing credentials
        numWrongCredentials.inc();
        break;

      case HttpServletResponse.SC_FORBIDDEN:
        // Are there other status codes which should also translate to error?
        numErrors.mark();
        break;
      default:
        if (frsp.getStatus() >= 200 && frsp.getStatus() <= 299) {
          numAuthenticated.inc();
        } else {
          numErrors.mark();
        }
    }
     
    if (TRACE_HTTP) {
      log.info("----------HTTP Response---------");
      if (log.isInfoEnabled()) {
        log.info("Status : {}", frsp.getStatus());
      }
      log.info("Headers :");
      for (String name : frsp.getHeaderNames()) {
        for (String value : frsp.getHeaders(name)) {
          log.info("{} : {}", name, value);
        }
      }
      log.info("-------------------------------");
    }


    if (authFilter instanceof HadoopAuthFilter) { // delegation token mgmt.
      String requestContinuesAttr = (String)request.getAttribute(REQUEST_CONTINUES_ATTR);
      if (requestContinuesAttr == null) {
        log.warn("Could not find {}", REQUEST_CONTINUES_ATTR);
        return false;
      } else {
        return Boolean.parseBoolean(requestContinuesAttr);
      }
    }

    return true;
  }

  @Override
  public void close() throws IOException {
    if (authFilter != null) {
      authFilter.destroy();
    }
  }
}
