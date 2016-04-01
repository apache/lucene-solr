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
package org.apache.solr.util;

import java.io.File;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;

import javax.net.ssl.SSLContext;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpClientUtil.SchemaRegistryProvider;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.CertificateUtils;

public class SSLTestConfig extends SSLConfig {
  public static File TEST_KEYSTORE = ExternalPaths.SERVER_HOME == null ? null
      : new File(ExternalPaths.SERVER_HOME, "../etc/test/solrtest.keystore");
  
  private static String TEST_KEYSTORE_PATH = TEST_KEYSTORE != null
      && TEST_KEYSTORE.exists() ? TEST_KEYSTORE.getAbsolutePath() : null;
  private static String TEST_KEYSTORE_PASSWORD = "secret";
  
  public SSLTestConfig() {
    this(false, false);
  }
  
  public SSLTestConfig(boolean useSSL, boolean clientAuth) {
    super(useSSL, clientAuth, TEST_KEYSTORE_PATH, TEST_KEYSTORE_PASSWORD, TEST_KEYSTORE_PATH, TEST_KEYSTORE_PASSWORD);
  }
 
  public SSLTestConfig(boolean useSSL, boolean clientAuth, String keyStore, String keyStorePassword, String trustStore, String trustStorePassword) {
    super(useSSL, clientAuth, keyStore, keyStorePassword, trustStore, trustStorePassword);
  }
  
  /**
   * Will provide an SolrHttpClientBuilder for SSL support (adds https and
   * removes http schemes) is SSL is enabled, otherwise return the default
   * SolrHttpClientBuilder
   */
  public SolrHttpClientBuilder getHttpClientBuilder() {
    SolrHttpClientBuilder builder = HttpClientUtil.getHttpClientBuilder();
    return isSSLMode() ? new SSLHttpClientBuilderProvider().getBuilder(builder) : builder;
  }

  /**
   * Builds a new SSLContext with the given configuration and allows the uses of
   * self-signed certificates during testing.
   */
  protected SSLContext buildSSLContext() throws KeyManagementException, 
    UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException {
    
    return SSLContexts.custom()
        .loadKeyMaterial(buildKeyStore(getKeyStore(), getKeyStorePassword()), getKeyStorePassword().toCharArray())
        .loadTrustMaterial(buildKeyStore(getTrustStore(), getTrustStorePassword()), new TrustSelfSignedStrategy()).build();
  }
  
  
  protected static KeyStore buildKeyStore(String keyStoreLocation, String password) {
    try {
      return CertificateUtils.getKeyStore(Resource.newResource(keyStoreLocation), "JKS", null, password);
    } catch (Exception ex) {
      throw new IllegalStateException("Unable to build KeyStore from file: " + keyStoreLocation, ex);
    }
  }
  
  private class SSLHttpClientBuilderProvider  {
    

    public SolrHttpClientBuilder getBuilder(SolrHttpClientBuilder builder) {

      HttpClientUtil.setSchemeRegistryProvider(new SchemaRegistryProvider() {
        
        @Override
        public Registry<ConnectionSocketFactory> getSchemaRegistry() {
          SSLConnectionSocketFactory sslConnectionFactory;
          try {
            boolean sslCheckPeerName = toBooleanDefaultIfNull(
                toBooleanObject(System.getProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME)), true);
            if (sslCheckPeerName == false) {
              sslConnectionFactory = new SSLConnectionSocketFactory(buildSSLContext(),
                  SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
            } else {
              sslConnectionFactory = new SSLConnectionSocketFactory(buildSSLContext());
            }
          } catch (KeyManagementException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
            throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", e);
          }
          return  RegistryBuilder.<ConnectionSocketFactory>create()
              .register("https", sslConnectionFactory).build();
        }
      });
      HttpClientUtil.setHttpClientBuilder(builder);
      return builder;
    }

  }
  
  public static boolean toBooleanDefaultIfNull(Boolean bool, boolean valueIfNull) {
    if (bool == null) {
      return valueIfNull;
    }
    return bool.booleanValue() ? true : false;
  }
  
  public static Boolean toBooleanObject(String str) {
    if ("true".equalsIgnoreCase(str)) {
      return Boolean.TRUE;
    } else if ("false".equalsIgnoreCase(str)) {
      return Boolean.FALSE;
    }
    // no match
    return null;
  }
  
  public static void setSSLSystemProperties() {
    System.setProperty("javax.net.ssl.keyStore", TEST_KEYSTORE_PATH);
    System.setProperty("javax.net.ssl.keyStorePassword", TEST_KEYSTORE_PASSWORD);
    System.setProperty("javax.net.ssl.trustStore", TEST_KEYSTORE_PATH);
    System.setProperty("javax.net.ssl.trustStorePassword", TEST_KEYSTORE_PASSWORD);
  }
  
  public static void clearSSLSystemProperties() {
    System.clearProperty("javax.net.ssl.keyStore");
    System.clearProperty("javax.net.ssl.keyStorePassword");
    System.clearProperty("javax.net.ssl.trustStore");
    System.clearProperty("javax.net.ssl.trustStorePassword");
  }
  
}