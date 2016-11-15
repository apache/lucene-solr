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
import java.lang.invoke.MethodHandles;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkACLProvider;
import org.apache.solr.common.cloud.ZkCredentialsProvider;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an authentication filter based on Hadoop's {@link DelegationTokenAuthenticationFilter}.
 * The Kerberos plugin can be configured to use delegation tokens, which allow an
 * application to reuse the authentication of an end-user or another application.
 */
public class DelegationTokenKerberosFilter extends DelegationTokenAuthenticationFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CuratorFramework curatorFramework;

  @Override
  public void init(FilterConfig conf) throws ServletException {
    if (conf != null && "zookeeper".equals(conf.getInitParameter("signer.secret.provider"))) {
      SolrZkClient zkClient =
          (SolrZkClient)conf.getServletContext().getAttribute(KerberosPlugin.DELEGATION_TOKEN_ZK_CLIENT);
      conf.getServletContext().setAttribute("signer.secret.provider.zookeeper.curator.client",
          getCuratorClient(zkClient));
    }
    super.init(conf);
  }

  /**
   * Return the ProxyUser Configuration.  FilterConfig properties beginning with
   * "solr.impersonator.user.name" will be added to the configuration.
   */
  @Override
  protected Configuration getProxyuserConfiguration(FilterConfig filterConf)
      throws ServletException {
    Configuration conf = new Configuration(false);

    Enumeration<?> names = filterConf.getInitParameterNames();
    while (names.hasMoreElements()) {
      String name = (String) names.nextElement();
      if (name.startsWith(KerberosPlugin.IMPERSONATOR_PREFIX)) {
        String value = filterConf.getInitParameter(name);
        conf.set(PROXYUSER_PREFIX + "." + name.substring(KerberosPlugin.IMPERSONATOR_PREFIX.length()), value);
        conf.set(name, value);
      }
    }
    return conf;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain filterChain) throws IOException, ServletException {
    // HttpClient 4.4.x throws NPE if query string is null and parsed through URLEncodedUtils.
    // See HTTPCLIENT-1746 and HADOOP-12767
    HttpServletRequest httpRequest = (HttpServletRequest)request;
    String queryString = httpRequest.getQueryString();
    final String nonNullQueryString = queryString == null ? "" : queryString;
    HttpServletRequest requestNonNullQueryString = new HttpServletRequestWrapper(httpRequest){
      @Override
      public String getQueryString() {
        return nonNullQueryString;
      }
    };

    // include Impersonator User Name in case someone (e.g. logger) wants it
    FilterChain filterChainWrapper = new FilterChain() {
      @Override
      public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
          throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;

        UserGroupInformation ugi = HttpUserGroupInformation.get();
        if (ugi != null && ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.PROXY) {
          UserGroupInformation realUserUgi = ugi.getRealUser();
          if (realUserUgi != null) {
            httpRequest.setAttribute(KerberosPlugin.IMPERSONATOR_USER_NAME, realUserUgi.getShortUserName());
          }
        }
        filterChain.doFilter(servletRequest, servletResponse);
      }
    };

    super.doFilter(requestNonNullQueryString, response, filterChainWrapper);
  }

  @Override
  public void destroy() {
    super.destroy();
    if (curatorFramework != null) curatorFramework.close();
    curatorFramework = null;
  }

  @Override
  protected void initializeAuthHandler(String authHandlerClassName,
                                       FilterConfig filterConfig) throws ServletException {
    // set the internal authentication handler in order to record whether the request should continue
    super.initializeAuthHandler(authHandlerClassName, filterConfig);
    AuthenticationHandler authHandler = getAuthenticationHandler();
    super.initializeAuthHandler(KerberosPlugin.RequestContinuesRecorderAuthenticationHandler.class.getName(), filterConfig);
    KerberosPlugin.RequestContinuesRecorderAuthenticationHandler newAuthHandler =
        (KerberosPlugin.RequestContinuesRecorderAuthenticationHandler)getAuthenticationHandler();
    newAuthHandler.setAuthHandler(authHandler);
  }

  protected CuratorFramework getCuratorClient(SolrZkClient zkClient) {
    // should we try to build a RetryPolicy off of the ZkController?
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    if (zkClient == null) {
      throw new IllegalArgumentException("zkClient required");
    }
    String zkHost = zkClient.getZkServerAddress();
    String zkChroot = zkHost.contains("/")? zkHost.substring(zkHost.indexOf("/")): "";
    String zkNamespace = zkChroot + SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH;
    zkNamespace = zkNamespace.startsWith("/") ? zkNamespace.substring(1) : zkNamespace;
    String zkConnectionString = zkHost.contains("/")? zkHost.substring(0, zkHost.indexOf("/")): zkHost;
    SolrZkToCuratorCredentialsACLs curatorToSolrZk = new SolrZkToCuratorCredentialsACLs(zkClient);
    final int connectionTimeoutMs = 30000; // this value is currently hard coded, see SOLR-7561.

    curatorFramework = CuratorFrameworkFactory.builder()
        .namespace(zkNamespace)
        .connectString(zkConnectionString)
        .retryPolicy(retryPolicy)
        .aclProvider(curatorToSolrZk.getACLProvider())
        .authorization(curatorToSolrZk.getAuthInfos())
        .sessionTimeoutMs(zkClient.getZkClientTimeout())
        .connectionTimeoutMs(connectionTimeoutMs)
        .build();
    curatorFramework.start();
    return curatorFramework;
  }

  /**
   * Convert Solr Zk Credentials/ACLs to Curator versions
   */
  protected static class SolrZkToCuratorCredentialsACLs {
    private final ACLProvider aclProvider;
    private final List<AuthInfo> authInfos;

    public SolrZkToCuratorCredentialsACLs(SolrZkClient zkClient) {
      this.aclProvider = createACLProvider(zkClient);
      this.authInfos = createAuthInfo(zkClient);
    }

    public ACLProvider getACLProvider() { return aclProvider; }
    public List<AuthInfo> getAuthInfos() { return authInfos; }

    private ACLProvider createACLProvider(SolrZkClient zkClient) {
      final ZkACLProvider zkACLProvider = zkClient.getZkACLProvider();
      return new ACLProvider() {
        @Override
        public List<ACL> getDefaultAcl() {
          return zkACLProvider.getACLsToAdd(null);
        }

        @Override
        public List<ACL> getAclForPath(String path) {
           List<ACL> acls = zkACLProvider.getACLsToAdd(path);
           return acls;
        }
      };
    }

    private List<AuthInfo> createAuthInfo(SolrZkClient zkClient) {
      List<AuthInfo> ret = new LinkedList<AuthInfo>();

      // In theory the credentials to add could change here if zookeeper hasn't been initialized
      ZkCredentialsProvider credentialsProvider =
        zkClient.getZkClientConnectionStrategy().getZkCredentialsToAddAutomatically();
      for (ZkCredentialsProvider.ZkCredentials zkCredentials : credentialsProvider.getCredentials()) {
        ret.add(new AuthInfo(zkCredentials.getScheme(), zkCredentials.getAuth()));
      }
      return ret;
    }
  }
}
