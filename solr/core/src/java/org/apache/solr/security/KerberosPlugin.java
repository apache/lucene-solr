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

import java.lang.invoke.MethodHandles;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationHandler;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
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

          String chrootPath = zkHost.contains("/")? zkHost.substring(zkHost.indexOf("/")): "";
          String znodeWorkingPath = chrootPath + SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH + "/zkdtsm";
          // Note - Curator complains if the znodeWorkingPath starts with /
          znodeWorkingPath = znodeWorkingPath.startsWith("/")? znodeWorkingPath.substring(1): znodeWorkingPath;
          putParam(params, "zk-dt-secret-manager.znodeWorkingPath",
              DELEGATION_TOKEN_SECRET_MANAGER_ZNODE_WORKING_PATH, znodeWorkingPath);
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

    // Needed to work around HADOOP-13346
    params.put(DelegationTokenAuthenticationHandler.JSON_MAPPER_PREFIX + JsonGenerator.Feature.AUTO_CLOSE_TARGET,
        "false");

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
    kerberosFilter.doFilter(req, rsp, chain);
    String requestContinuesAttr = (String)req.getAttribute(RequestContinuesRecorderAuthenticationHandler.REQUEST_CONTINUES_ATTR);
    if (requestContinuesAttr == null) {
      log.warn("Could not find " + RequestContinuesRecorderAuthenticationHandler.REQUEST_CONTINUES_ATTR);
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
  public void setup(Http2SolrClient client) {
    kerberosBuilder.setup(client);
  }

  @Override
  public void close() {
    kerberosFilter.destroy();
    kerberosBuilder.close();
  }

  protected Filter getKerberosFilter() { return kerberosFilter; }

  protected void setKerberosFilter(Filter kerberosFilter) { this.kerberosFilter = kerberosFilter; }
}
