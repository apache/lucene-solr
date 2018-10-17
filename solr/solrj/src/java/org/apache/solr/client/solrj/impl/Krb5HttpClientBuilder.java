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

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.security.Principal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

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
import org.eclipse.jetty.client.SolrWWWAuthenticationProtocolHandler;
import org.eclipse.jetty.client.api.Authentication;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.Attributes;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
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

  @Override
  public void setup(Http2SolrClient http2Client) {
    HttpAuthenticationStore authenticationStore = new HttpAuthenticationStore();
    authenticationStore.addAuthentication(new Authentication() {
      @Override
      public boolean matches(String type, URI uri, String realm) {
        return "Negotiate".equals(type);
      }

      @Override
      public Result authenticate(Request request, ContentResponse response, HeaderInfo headerInfo, Attributes context) {
        String challenge = headerInfo.getBase64();
        if (challenge == null) challenge = "";
        byte[] input = java.util.Base64.getDecoder().decode(challenge);
        byte[] token;
        String authServer = request.getHost();
        final GSSManager manager = GSSManager.getInstance();
        try {
          GSSName serverName = manager.createName("HTTP@" + authServer, GSSName.NT_HOSTBASED_SERVICE);
          final GSSContext gssContext = createGSSContext(manager, new Oid(SPNEGO_OID), serverName, null);
          if (input != null) {
            token = gssContext.initSecContext(input, 0, input.length);
          } else {
            token = gssContext.initSecContext(new byte[] {}, 0, 0);
          }
        } catch (GSSException e) {
          throw new IllegalArgumentException("Unable to init GSSContext", e);
        }
        return new Result() {
          AtomicBoolean sentToken = new AtomicBoolean(false);
          @Override
          public URI getURI() {
            // Since Kerberos is connection based authentication, sub-sequence requests won't need to resend the token in header
            // by return null, the ProtocolHandler won't try to apply this result on sequence requests
            return null;
          }

          @Override
          public void apply(Request request) {
            if (sentToken.get()) return;

            final String tokenstr = java.util.Base64.getEncoder().encodeToString(token);
            if (log.isDebugEnabled()) {
              log.info("Sending response '" + tokenstr + "' back to the auth server");
            }
            request.header(headerInfo.getHeader().asString(), "Negotiate "+tokenstr);
          }
        };
      }

      private GSSContext createGSSContext(GSSManager manager, Oid oid, GSSName serverName, final GSSCredential gssCredential) throws GSSException {
        // Get the credentials from the JAAS configuration rather than here
        final GSSContext gssContext = manager.createContext(serverName.canonicalize(oid), oid, gssCredential,
            GSSContext.DEFAULT_LIFETIME);
        gssContext.requestMutualAuth(true);
        return gssContext;
      }
    });
    http2Client.getHttpClient().setAuthenticationStore(authenticationStore);
    http2Client.getProtocolHandlers().put(new SolrWWWAuthenticationProtocolHandler(http2Client.getHttpClient()));
  }

  public SolrHttpClientBuilder getBuilder(SolrHttpClientBuilder builder) {
    if (System.getProperty(LOGIN_CONFIG_PROP) != null) {
      String configValue = System.getProperty(LOGIN_CONFIG_PROP);

      if (configValue != null) {
        log.info("Setting up SPNego auth with config: " + configValue);
        final String useSubjectCredsProp = "javax.security.auth.useSubjectCredsOnly";
        String useSubjectCredsVal = System.getProperty(useSubjectCredsProp);

        // "javax.security.auth.useSubjectCredsOnly" should be false so that the underlying
        // authentication mechanism can load the credentials from the JAAS configuration.
        if (useSubjectCredsVal == null) {
          System.setProperty(useSubjectCredsProp, "false");
        } else if (!useSubjectCredsVal.toLowerCase(Locale.ROOT).equals("false")) {
          // Don't overwrite the prop value if it's already been written to something else,
          // but log because it is likely the Credentials won't be loaded correctly.
          log.warn("System Property: " + useSubjectCredsProp + " set to: " + useSubjectCredsVal
              + " not false.  SPNego authentication may not be successful.");
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

      log.debug("Login prop: "+System.getProperty(LOGIN_CONFIG_PROP));

      String clientAppName = System.getProperty("solr.kerberos.jaas.appname", "Client");
      if (initiateAppNames.contains(appName)) {
        log.debug("Using AppConfigurationEntry for appName '"+clientAppName+"' instead of: " + appName);
        return baseConfig.getAppConfigurationEntry(clientAppName);
      }
      return baseConfig.getAppConfigurationEntry(appName);
    }
  }
}
