package org.apache.solr.client.solrj.impl;

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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.AuthSchemeRegistry;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.params.ClientPNames;
import org.apache.solr.common.params.SolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.http.entity.BufferedHttpEntity;

/**
 * Kerberos-enabled HttpClientConfigurer
 */
public class Krb5HttpClientConfigurer extends HttpClientConfigurer {
  
  public static final String LOGIN_CONFIG_PROP = "java.security.auth.login.config";
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Configuration jaasConfig = new SolrJaasConfiguration();

  public void configure(DefaultHttpClient httpClient, SolrParams config) {
    super.configure(httpClient, config);

    if (System.getProperty(LOGIN_CONFIG_PROP) != null) {
      String configValue = System.getProperty(LOGIN_CONFIG_PROP);

      if (configValue != null) {
        logger.info("Setting up SPNego auth with config: " + configValue);
        final String useSubjectCredsProp = "javax.security.auth.useSubjectCredsOnly";
        String useSubjectCredsVal = System.getProperty(useSubjectCredsProp);

        // "javax.security.auth.useSubjectCredsOnly" should be false so that the underlying
        // authentication mechanism can load the credentials from the JAAS configuration.
        if (useSubjectCredsVal == null) {
          System.setProperty(useSubjectCredsProp, "false");
        }
        else if (!useSubjectCredsVal.toLowerCase(Locale.ROOT).equals("false")) {
          // Don't overwrite the prop value if it's already been written to something else,
          // but log because it is likely the Credentials won't be loaded correctly.
          logger.warn("System Property: " + useSubjectCredsProp + " set to: " + useSubjectCredsVal
              + " not false.  SPNego authentication may not be successful.");
        }

        javax.security.auth.login.Configuration.setConfiguration(jaasConfig);
        //Enable only SPNEGO authentication scheme.
        AuthSchemeRegistry registry = new AuthSchemeRegistry();
        registry.register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, false));
        httpClient.setAuthSchemes(registry);
        // Get the credentials from the JAAS configuration rather than here
        Credentials useJaasCreds = new Credentials() {
          public String getPassword() {
            return null;
          }
          public Principal getUserPrincipal() {
            return null;
          }
        };

        SolrPortAwareCookieSpecFactory cookieFactory = new SolrPortAwareCookieSpecFactory();
        httpClient.getCookieSpecs().register(cookieFactory.POLICY_NAME, cookieFactory);
        httpClient.getParams().setParameter(ClientPNames.COOKIE_POLICY, cookieFactory.POLICY_NAME);
        
        httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, useJaasCreds);

        httpClient.addRequestInterceptor(bufferedEntityInterceptor);
      } else {
        httpClient.getCredentialsProvider().clear();
      }
    }
  }

  // Set a buffered entity based request interceptor
  private HttpRequestInterceptor bufferedEntityInterceptor = new HttpRequestInterceptor() {
    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException,
        IOException {
      if(request instanceof HttpEntityEnclosingRequest) {
        HttpEntityEnclosingRequest enclosingRequest = ((HttpEntityEnclosingRequest) request);  
        HttpEntity requestEntity = enclosingRequest.getEntity();
        enclosingRequest.setEntity(new BufferedHttpEntity(requestEntity));
      }
    }
  };

  private static class SolrJaasConfiguration extends javax.security.auth.login.Configuration {

    private javax.security.auth.login.Configuration baseConfig;

    // the com.sun.security.jgss appNames
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

      logger.debug("Login prop: "+System.getProperty(LOGIN_CONFIG_PROP));

      String clientAppName = System.getProperty("solr.kerberos.jaas.appname", "Client");
      if (initiateAppNames.contains(appName)) {
        logger.debug("Using AppConfigurationEntry for appName '"+clientAppName+"' instead of: " + appName);
        return baseConfig.getAppConfigurationEntry(clientAppName);
      }
      return baseConfig.getAppConfigurationEntry(appName);
    }
  }
}
