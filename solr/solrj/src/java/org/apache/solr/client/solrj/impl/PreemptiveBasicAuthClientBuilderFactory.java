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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.eclipse.jetty.client.HttpAuthenticationStore;
import org.eclipse.jetty.client.ProxyAuthenticationProtocolHandler;
import org.apache.solr.client.solrj.util.SolrBasicAuthentication;
import org.eclipse.jetty.client.WWWAuthenticationProtocolHandler;

/**
 * HttpClientConfigurer implementation providing support for preemptive Http Basic authentication
 * scheme.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PreemptiveBasicAuthClientBuilderFactory implements HttpClientBuilderFactory {
  /**
   * A system property used to specify a properties file containing default parameters used for
   * creating a HTTP client. This is specifically useful for configuring the HTTP basic auth
   * credentials (i.e. username/password). The name of the property must match the relevant
   * Solr config property name.
   */
  public static final String SYS_PROP_HTTP_CLIENT_CONFIG = "solr.httpclient.config";

  /**
   * A system property to configure the Basic auth credentials via a java system property.
   * Since this will expose the password on the command-line, it is not very secure. But
   * this mechanism is added for backwards compatibility.
   */
  public static final String SYS_PROP_BASIC_AUTH_CREDENTIALS = "basicauth";

  private static SolrParams defaultParams;
  private static PreemptiveAuth requestInterceptor = new PreemptiveAuth(new BasicScheme());

  static {
    String credentials = System.getProperty(SYS_PROP_BASIC_AUTH_CREDENTIALS);
    String configFile = System.getProperty(SYS_PROP_HTTP_CLIENT_CONFIG);

    if (credentials != null && configFile != null) {
      throw new RuntimeException("Basic authentication credentials passed via a configuration file"
          + " as well as java system property. Please choose one mechanism!");
    }

    if (credentials != null) {
      List<String> ss = StrUtils.splitSmart(credentials, ':');
      if (ss.size() != 2) {
        throw new RuntimeException("Please provide 'basicauth' in the 'user:password' format");
      }
      Properties defaultProps = new Properties();
      defaultProps.setProperty(HttpClientUtil.PROP_BASIC_AUTH_USER, ss.get(0));
      defaultProps.setProperty(HttpClientUtil.PROP_BASIC_AUTH_PASS, ss.get(1));
      defaultParams = new MapSolrParams(new HashMap(defaultProps));
    }

    if(configFile != null) {
      try {
        Properties defaultProps = new Properties();
        defaultProps.load(new InputStreamReader(new FileInputStream(configFile), StandardCharsets.UTF_8));
        defaultParams = new MapSolrParams(new HashMap(defaultProps));
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to read the Http client config file", e);
      }
    }
  }

  /**
   * This method enables configuring system wide defaults (apart from using a config file based approach).
   */
  public static void setDefaultSolrParams(SolrParams params) {
    defaultParams = params;
  }

  @Override
  public void close() throws IOException {
    HttpClientUtil.removeRequestInterceptor(requestInterceptor);
  }

  @Override
  public void setup(Http2SolrClient client) {
    final String basicAuthUser = defaultParams.get(HttpClientUtil.PROP_BASIC_AUTH_USER);
    final String basicAuthPass = defaultParams.get(HttpClientUtil.PROP_BASIC_AUTH_PASS);
    if(basicAuthUser == null || basicAuthPass == null) {
      throw new IllegalArgumentException("username & password must be specified with " + getClass().getName());
    }

    HttpAuthenticationStore authenticationStore = new HttpAuthenticationStore();
    authenticationStore.addAuthentication(new SolrBasicAuthentication(basicAuthUser, basicAuthPass));
    client.getHttpClient().setAuthenticationStore(authenticationStore);
    client.getProtocolHandlers().put(new WWWAuthenticationProtocolHandler(client.getHttpClient()));
    client.getProtocolHandlers().put(new ProxyAuthenticationProtocolHandler(client.getHttpClient()));
  }

  @Override
  public SolrHttpClientBuilder getHttpClientBuilder(Optional<SolrHttpClientBuilder> optionalBuilder) {
    final String basicAuthUser = defaultParams.get(HttpClientUtil.PROP_BASIC_AUTH_USER);
    final String basicAuthPass = defaultParams.get(HttpClientUtil.PROP_BASIC_AUTH_PASS);
    if(basicAuthUser == null || basicAuthPass == null) {
      throw new IllegalArgumentException("username & password must be specified with " + getClass().getName());
    }

    SolrHttpClientBuilder builder = optionalBuilder.isPresent() ?
        initHttpClientBuilder(optionalBuilder.get(), basicAuthUser, basicAuthPass)
        : initHttpClientBuilder(SolrHttpClientBuilder.create(), basicAuthUser, basicAuthPass);
    return builder;
  }

  private SolrHttpClientBuilder initHttpClientBuilder(SolrHttpClientBuilder builder, String basicAuthUser, String basicAuthPass) {
    builder.setDefaultCredentialsProvider(() -> {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(basicAuthUser, basicAuthPass));
      return credsProvider;
    });

    HttpClientUtil.addRequestInterceptor(requestInterceptor);
    return builder;
  }
}