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
package org.apache.solr.client.solrj.impl;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.eclipse.jetty.client.HttpAuthenticationStore;
import org.eclipse.jetty.client.WWWAuthenticationProtocolHandler;
import org.eclipse.jetty.client.util.SPNEGOAuthentication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kerberos-enabled SolrHttpClientBuilder
 */
public class Krb5HttpClientBuilder implements HttpClientBuilderFactory {
  
  public static final String LOGIN_CONFIG_PROP = "java.security.auth.login.config";
  private static final String SPNEGO_OID = "1.3.6.1.5.5.2";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static Configuration jaasConfig = new SolrJaasConfiguration();

  public Krb5HttpClientBuilder() {

  }

  /**
   * The jaasConfig is static, which makes it problematic for testing in the same jvm.
   * Call this function to regenerate the static config (this is not thread safe).
   * Note: only used for tests
   */
  public static void regenerateJaasConfiguration() {
    jaasConfig = new SolrJaasConfiguration();
  }

  public SolrHttpClientBuilder getBuilder() {
    return getBuilder(HttpClientUtil.getHttpClientBuilder());
  }
  
  public void close() {
    HttpClientUtil.removeRequestInterceptor(bufferedEntityInterceptor);
  }

  @Override
  public SolrHttpClientBuilder getHttpClientBuilder(Optional<SolrHttpClientBuilder> builder) {
    return builder.isPresent() ? getBuilder(builder.get()) : getBuilder();
  }

  private SPNEGOAuthentication createSPNEGOAuthentication() {
    SPNEGOAuthentication authentication = new SPNEGOAuthentication(null) {

      public boolean matches(String type, URI uri, String realm) {
        return this.getType().equals(type);
      }
    };
    String clientAppName = System.getProperty("solr.kerberos.jaas.appname", "Client");
    AppConfigurationEntry[] entries = jaasConfig.getAppConfigurationEntry(clientAppName);
    if (entries == null) {
      log.warn("Could not find login configuration entry for {}. SPNego authentication may not be successful.", (Object) clientAppName);
      return authentication;
    }
    if (entries.length != 1) {
      log.warn("Multiple login modules are specified in the configuration file");
      return authentication;
    }
    Map<String, ?> options = entries[0].getOptions();
    setAuthenticationOptions(authentication, options, (String) options.get("principal"));
    return authentication;
  }

  static void setAuthenticationOptions(SPNEGOAuthentication authentication, Map<String, ?> options, String username) {
    String keyTab = (String)options.get("keyTab");
    if (keyTab != null) {
      authentication.setUserKeyTabPath(Paths.get(keyTab));
    }
    authentication.setServiceName("HTTP");
    authentication.setUserName(username);
    if ("true".equalsIgnoreCase((String)options.get("useTicketCache"))) {
      authentication.setUseTicketCache(true);
      String ticketCachePath = (String)options.get("ticketCache");
      if (ticketCachePath != null) {
        authentication.setTicketCachePath(Paths.get(ticketCachePath));
      }
      authentication.setRenewTGT("true".equalsIgnoreCase((String)options.get("renewTGT")));
    }
  }

  @Override
  public void setup(Http2SolrClient http2Client) {
    HttpAuthenticationStore authenticationStore = new HttpAuthenticationStore();
    authenticationStore.addAuthentication(createSPNEGOAuthentication());
    http2Client.getHttpClient().setAuthenticationStore(authenticationStore);
    http2Client.getProtocolHandlers().put(new WWWAuthenticationProtocolHandler(http2Client.getHttpClient()));
  }

  public SolrHttpClientBuilder getBuilder(SolrHttpClientBuilder builder) {
    if (System.getProperty(LOGIN_CONFIG_PROP) != null) {
      String configValue = System.getProperty(LOGIN_CONFIG_PROP);

      if (configValue != null) {
        log.info("Setting up SPNego auth with config: {}", configValue);
        final String useSubjectCredsProp = "javax.security.auth.useSubjectCredsOnly";
        String useSubjectCredsVal = System.getProperty(useSubjectCredsProp);

        // "javax.security.auth.useSubjectCredsOnly" should be false so that the underlying
        // authentication mechanism can load the credentials from the JAAS configuration.
        if (useSubjectCredsVal == null) {
          System.setProperty(useSubjectCredsProp, "false");
        } else if (!useSubjectCredsVal.toLowerCase(Locale.ROOT).equals("false")) {
          // Don't overwrite the prop value if it's already been written to something else,
          // but log because it is likely the Credentials won't be loaded correctly.
          log.warn("System Property: {} set to: {} not false.  SPNego authentication may not be successful."
              , useSubjectCredsProp, useSubjectCredsVal);
        }

        javax.security.auth.login.Configuration.setConfiguration(jaasConfig);
        //Enable only SPNEGO authentication scheme.

        builder.setAuthSchemeRegistryProvider(() -> {
          Lookup<AuthSchemeProvider> authProviders = RegistryBuilder.<AuthSchemeProvider>create()
              .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, false))
              .build();
          return authProviders;
        });
        // Get the credentials from the JAAS configuration rather than here
        Credentials useJaasCreds = new Credentials() {
          public String getPassword() {
            return null;
          }
          public Principal getUserPrincipal() {
            return null;
          }
        };

        HttpClientUtil.setCookiePolicy(SolrPortAwareCookieSpecFactory.POLICY_NAME);

        builder.setCookieSpecRegistryProvider(() -> {
          SolrPortAwareCookieSpecFactory cookieFactory = new SolrPortAwareCookieSpecFactory();

          Lookup<CookieSpecProvider> cookieRegistry = RegistryBuilder.<CookieSpecProvider> create()
              .register(SolrPortAwareCookieSpecFactory.POLICY_NAME, cookieFactory).build();

          return cookieRegistry;
        });
        
        builder.setDefaultCredentialsProvider(() -> {
          CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(AuthScope.ANY, useJaasCreds);
          return credentialsProvider;
        });
        HttpClientUtil.addRequestInterceptor(bufferedEntityInterceptor);
      }
    } else {
      log.warn("{} is configured without specifying system property '{}'",
          getClass().getName(), LOGIN_CONFIG_PROP);
    }

    return builder;
  }

  // Set a buffered entity based request interceptor
  private HttpRequestInterceptor bufferedEntityInterceptor = (request, context) -> {
    if(request instanceof HttpEntityEnclosingRequest) {
      HttpEntityEnclosingRequest enclosingRequest = ((HttpEntityEnclosingRequest) request);
      HttpEntity requestEntity = enclosingRequest.getEntity();
      enclosingRequest.setEntity(new BufferedHttpEntity(requestEntity));
    }
  };

  static class SolrJaasConfiguration extends javax.security.auth.login.Configuration {

    private javax.security.auth.login.Configuration baseConfig;

    // the com.sun.security.jgss appNames
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Set<String> initiateAppNames = new HashSet(
      Arrays.asList("com.sun.security.jgss.krb5.initiate", "com.sun.security.jgss.initiate"));

    public SolrJaasConfiguration() {
      try {
        
        this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
      } catch (SecurityException e) {
        this.baseConfig = null;
      }
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (baseConfig == null) return null;

      if (log.isDebugEnabled()) {
        log.debug("Login prop: {}", System.getProperty(LOGIN_CONFIG_PROP));
      }

      String clientAppName = System.getProperty("solr.kerberos.jaas.appname", "Client");
      if (initiateAppNames.contains(appName)) {
        log.debug("Using AppConfigurationEntry for appName '{}' instead of: '{}'", clientAppName, appName);
        return baseConfig.getAppConfigurationEntry(clientAppName);
      }
      return baseConfig.getAppConfigurationEntry(appName);
    }
  }
}
