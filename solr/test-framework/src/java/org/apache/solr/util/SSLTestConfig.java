package org.apache.solr.util;

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

import java.io.File;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;

import javax.net.ssl.SSLContext;

import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.common.params.SolrParams;
import org.eclipse.jetty.util.security.CertificateUtils;

public class SSLTestConfig extends SSLConfig {
  public static File TEST_KEYSTORE = ExternalPaths.SOURCE_HOME == null ? null
      : new File(ExternalPaths.SOURCE_HOME, "example/etc/solrtest.keystore");
  
  private static String TEST_KEYSTORE_PATH = TEST_KEYSTORE != null
      && TEST_KEYSTORE.exists() ? TEST_KEYSTORE.getAbsolutePath() : null;
  private static String TEST_KEYSTORE_PASSWORD = "secret";
  private static HttpClientConfigurer DEFAULT_CONFIGURER = new HttpClientConfigurer();
  
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
   * Will provide an HttpClientConfigurer for SSL support (adds https and
   * removes http schemes) is SSL is enabled, otherwise return the default
   * configurer
   */
  public HttpClientConfigurer getHttpClientConfigurer() {
    return isSSLMode() ? new SSLHttpClientConfigurer() : DEFAULT_CONFIGURER;
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
      return CertificateUtils.getKeyStore(null, keyStoreLocation, "JKS", null, password);
    } catch (Exception ex) {
      throw new IllegalStateException("Unable to build KeyStore from file: " + keyStoreLocation, ex);
    }
  }
  
  private class SSLHttpClientConfigurer extends HttpClientConfigurer {
    @SuppressWarnings("deprecation")
    protected void configure(DefaultHttpClient httpClient, SolrParams config) {
      super.configure(httpClient, config);
      SchemeRegistry registry = httpClient.getConnectionManager().getSchemeRegistry();
      // Make sure no tests cheat by using HTTP
      registry.unregister("http");
      try {
        registry.register(new Scheme("https", 443, new SSLSocketFactory(buildSSLContext())));
      } catch (KeyManagementException ex) {
        throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", ex);
      } catch (UnrecoverableKeyException ex) {
        throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", ex);
      } catch (NoSuchAlgorithmException ex) {
        throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", ex);
      } catch (KeyStoreException ex) {
        throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", ex);
      }
    }
  }
  
  public static void cleanStatics() {
    DEFAULT_CONFIGURER = null;
    TEST_KEYSTORE = null;
    TEST_KEYSTORE_PASSWORD = null;
    TEST_KEYSTORE_PATH = null;
  }
}