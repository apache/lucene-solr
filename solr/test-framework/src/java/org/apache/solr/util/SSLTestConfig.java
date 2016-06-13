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
import java.util.Random;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.SecureRandomSpi;
import java.security.UnrecoverableKeyException;

import javax.net.ssl.SSLContext;
import java.net.MalformedURLException;

import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpClientConfigurer;
import org.apache.solr.common.params.SolrParams;

import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.CertificateUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * An {@link SSLConfig} that supports reading key/trust store information directly from resource 
 * files provided with the Solr test-framework classes
 */
public class SSLTestConfig extends SSLConfig {

  /** @deprecated No longer used except by {@link #setSSLSystemProperties} */
  public static File TEST_KEYSTORE = ExternalPaths.SERVER_HOME == null ? null
    : new File(ExternalPaths.SERVER_HOME, "../etc/test/solrtest.keystore");
  
  /** @deprecated No longer used except by {@link #setSSLSystemProperties} */
  private static String TEST_KEYSTORE_PATH = TEST_KEYSTORE != null
    && TEST_KEYSTORE.exists() ? TEST_KEYSTORE.getAbsolutePath() : null;

  private static final String TEST_KEYSTORE_RESOURCE = "SSLTestConfig.testing.keystore";
  private static final String TEST_KEYSTORE_PASSWORD = "secret";

  private final Resource keyStore;
  private final Resource trustStore;
  
  /** Creates an SSLTestConfig that does not use SSL or client authentication */
  public SSLTestConfig() {
    this(false, false);
  }

  /** 
   * Create an SSLTestConfig based on a few caller specified options.  As needed, 
   * keystore/truststore information will be pulled from a hardocded resource file provided 
   * by the solr test-framework.
   *
   * @param useSSL - wether SSL should be required.
   * @param clientAuth - whether client authentication should be required.
   */
  public SSLTestConfig(boolean useSSL, boolean clientAuth) {
    super(useSSL, clientAuth, null, TEST_KEYSTORE_PASSWORD, null, TEST_KEYSTORE_PASSWORD);
    trustStore = keyStore = Resource.newClassPathResource(TEST_KEYSTORE_RESOURCE);
    if (null == keyStore || ! keyStore.exists() ) {
      throw new IllegalStateException("Unable to locate keystore resource file in classpath: "
                                      + TEST_KEYSTORE_RESOURCE);
    }
  }

  /** 
   * Create an SSLTestConfig using explicit paths for files 
   * @deprecated - use {@link SSLConfig} directly
   */
  @Deprecated
  public SSLTestConfig(boolean useSSL, boolean clientAuth, String keyStore, String keyStorePassword, String trustStore, String trustStorePassword) {
    super(useSSL, clientAuth, keyStore, keyStorePassword, trustStore, trustStorePassword);
    this.keyStore = tryNewResource(keyStore, "KeyStore");
    this.trustStore = tryNewResource(trustStore, "TrustStore");
  }

  /**
   * Helper utility for building resources from arbitrary user input paths/urls
   * if input is null, returns null; otherwise attempts to build Resource and verifies that Resource exists.
   */
  private static final Resource tryNewResource(String userInput, String type) {
    if (null == userInput) {
      return null;
    }
    Resource result;
    try {
      result = Resource.newResource(userInput);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Can't build " + type + " Resource: " + e.getMessage(), e);
    }
    if (! result.exists()) {
      throw new IllegalArgumentException(type + " Resource does not exist " + result.getName());
    }
    return result;
  }

  /** NOTE: This method is meaningless unless you explicitly provide paths when constructing this instance 
   * @see #SSLTestConfig(boolean,boolean,String,String,String,String)
   */
  @Override
  public String getKeyStore() {
    return super.getKeyStore();
  }
  /** NOTE: This method is meaningless unless you explicitly provide paths when constructing this instance 
   * @see #SSLTestConfig(boolean,boolean,String,String,String,String)
   */
  @Override
  public String getTrustStore() {
    return super.getTrustStore();
  }
  
  /**
   * Creates a {@link HttpClientConfigurer} for HTTP <b>clients</b> to use when communicating with servers 
   * which have been configured based on the settings of this object.  When {@link #isSSLMode} is true, this 
   * <code>HttpClientConfigurer</code> will <i>only</i> support HTTPS (no HTTP scheme) using the 
   * appropriate certs.  When {@link #isSSLMode} is false, <i>only</i> HTTP (no HTTPS scheme) will be 
   * supported.
   */
  public HttpClientConfigurer getHttpClientConfigurer() {
    try {
      return isSSLMode() ? new SSLHttpClientConfigurer(buildClientSSLContext()) : HTTP_ONLY_NO_SSL_CONFIGURER;
    } catch (KeyManagementException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
      throw new IllegalStateException("Unable to setup HttpClientConfigurer test SSL", e);
    }
  }
  
  /**
   * Builds a new SSLContext for HTTP <b>clients</b> to use when communicating with servers which have 
   * been configured based on the settings of this object.  
   *
   * NOTE: Uses a completely insecure {@link SecureRandom} instance to prevent tests from blocking 
   * due to lack of entropy, also explicitly allows the use of self-signed 
   * certificates (since that's what is almost always used during testing).
   */
  public SSLContext buildClientSSLContext() throws KeyManagementException, 
    UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException {

    assert isSSLMode();
    
    SSLContextBuilder builder = SSLContexts.custom();
    builder.setSecureRandom(NotSecurePsuedoRandom.INSTANCE);
    
    // NOTE: KeyStore & TrustStore are swapped because they are from configured from server perspective...
    // we are a client - our keystore contains the keys the server trusts, and vice versa
    builder.loadTrustMaterial(buildKeyStore(keyStore, getKeyStorePassword()), new TrustSelfSignedStrategy()).build();

    if (isClientAuthMode()) {
      builder.loadKeyMaterial(buildKeyStore(trustStore, getTrustStorePassword()), getTrustStorePassword().toCharArray());
      
    }

    return builder.build();
  }
  
  /**
   * Builds a new SSLContext for jetty servers which have been configured based on the settings of 
   * this object.
   *
   * NOTE: Uses a completely insecure {@link SecureRandom} instance to prevent tests from blocking 
   * due to lack of entropy, also explicitly allows the use of self-signed 
   * certificates (since that's what is almost always used during testing).
   * almost always used during testing). 
   */
  public SSLContext buildServerSSLContext() throws KeyManagementException, 
    UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException {

    assert isSSLMode();
    
    SSLContextBuilder builder = SSLContexts.custom();
    builder.setSecureRandom(NotSecurePsuedoRandom.INSTANCE);
    
    builder.loadKeyMaterial(buildKeyStore(keyStore, getKeyStorePassword()), getKeyStorePassword().toCharArray());

    if (isClientAuthMode()) {
      builder.loadTrustMaterial(buildKeyStore(trustStore, getTrustStorePassword()), new TrustSelfSignedStrategy()).build();
      
    }

    return builder.build();
  }

  /**
   * Returns an SslContextFactory using {@link #buildServerSSLContext} if SSL should be used, else returns null.
   */
  @Override
  public SslContextFactory createContextFactory() {
    if (!isSSLMode()) {
      return null;
    }
    // else...

    
    SslContextFactory factory = new SslContextFactory(false);
    try {
      factory.setSslContext(buildServerSSLContext());
    } catch (Exception e) { 
      throw new RuntimeException("ssl context init failure: " + e.getMessage(), e); 
    }
    factory.setNeedClientAuth(isClientAuthMode());
    return factory;
  }
  
  /**
   * Constructs a KeyStore using the specified filename and password
   */
  protected static KeyStore buildKeyStore(Resource resource, String password) {
    try {
      return CertificateUtils.getKeyStore(resource, "JKS", null, password);
    } catch (Exception ex) {
      throw new IllegalStateException("Unable to build KeyStore from resource: " + resource.getName(), ex);
    }
  }
  
  private static class SSLHttpClientConfigurer extends HttpClientConfigurer {
    private final SSLContext sslContext;
    public SSLHttpClientConfigurer(SSLContext sslContext) {
       this.sslContext = sslContext;
     }
    @SuppressWarnings("deprecation")
    public void configure(DefaultHttpClient httpClient, SolrParams config) {
      super.configure(httpClient, config);
      SchemeRegistry registry = httpClient.getConnectionManager().getSchemeRegistry();
      // Make sure no tests cheat by using HTTP
      registry.unregister("http");
      registry.register(new Scheme("https", 443, new SSLSocketFactory(sslContext)));
    }
  }

  private static final HttpClientConfigurer HTTP_ONLY_NO_SSL_CONFIGURER =
    new HttpClientConfigurer() {
      @SuppressWarnings("deprecation")
      public void configure(DefaultHttpClient httpClient, SolrParams config) {
        super.configure(httpClient, config);
        SchemeRegistry registry = httpClient.getConnectionManager().getSchemeRegistry();
        registry.unregister("https");
      }
    };
  
  /** 
   * Constructs a new SSLConnectionSocketFactory for HTTP <b>clients</b> to use when communicating 
   * with servers which have been configured based on the settings of this object. Will return null
   * unless {@link #isSSLMode} is true.
   */
  public SSLConnectionSocketFactory buildClientSSLConnectionSocketFactory() {
    if (!isSSLMode()) {
      return null;
    }
    SSLConnectionSocketFactory sslConnectionFactory;
    try {
      boolean sslCheckPeerName = toBooleanDefaultIfNull(toBooleanObject(System.getProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME)), true);
      SSLContext sslContext = buildClientSSLContext();
      if (sslCheckPeerName == false) {
        sslConnectionFactory = new SSLConnectionSocketFactory
          (sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
      } else {
        sslConnectionFactory = new SSLConnectionSocketFactory(sslContext);
      }
    } catch (KeyManagementException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
      throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", e);
    }
    return sslConnectionFactory;
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
  
  /**
   * @deprecated this method has very little practical use, in most cases you'll want to use 
   * {@link SSLContext#setDefault} with {@link #buildClientSSLContext} instead.
   */
  @Deprecated
  public static void setSSLSystemProperties() {
    System.setProperty("javax.net.ssl.keyStore", TEST_KEYSTORE_PATH);
    System.setProperty("javax.net.ssl.keyStorePassword", TEST_KEYSTORE_PASSWORD);
    System.setProperty("javax.net.ssl.trustStore", TEST_KEYSTORE_PATH);
    System.setProperty("javax.net.ssl.trustStorePassword", TEST_KEYSTORE_PASSWORD);
  }
  
  /**
   * @deprecated this method has very little practical use, in most cases you'll want to use 
   * {@link SSLContext#setDefault} with {@link #buildClientSSLContext} instead.
   */
  @Deprecated
  public static void clearSSLSystemProperties() {
    System.clearProperty("javax.net.ssl.keyStore");
    System.clearProperty("javax.net.ssl.keyStorePassword");
    System.clearProperty("javax.net.ssl.trustStore");
    System.clearProperty("javax.net.ssl.trustStorePassword");
  }

  /**
   * A mocked up instance of SecureRandom that just uses {@link Random} under the covers.
   * This is to prevent blocking issues that arise in platform default 
   * SecureRandom instances due to too many instances / not enough random entropy.  
   * Tests do not need secure SSL.
   */
  private static class NotSecurePsuedoRandom extends SecureRandom {
    public static final SecureRandom INSTANCE = new NotSecurePsuedoRandom();
    private static final Random RAND = new Random(42);
    
    /** 
     * Helper method that can be used to fill an array with non-zero data.
     * (Attempted workarround of Solaris SSL Padding bug: SOLR-9068)
     */
    private static final byte[] fillData(byte[] data) {
      RAND.nextBytes(data);
      return data;
    }
    
    /** SPI Used to init all instances */
    private static final SecureRandomSpi NOT_SECURE_SPI = new SecureRandomSpi() {
      /** returns a new byte[] filled with static data */
      public byte[] engineGenerateSeed(int numBytes) {
        return fillData(new byte[numBytes]);
      }
      /** fills the byte[] with static data */
      public void engineNextBytes(byte[] bytes) {
        fillData(bytes);
      }
      /** NOOP */
      public void engineSetSeed(byte[] seed) { /* NOOP */ }
    };
    
    private NotSecurePsuedoRandom() {
      super(NOT_SECURE_SPI, null) ;
    }
    
    /** returns a new byte[] filled with static data */
    public byte[] generateSeed(int numBytes) {
      return fillData(new byte[numBytes]);
    }
    /** fills the byte[] with static data */
    synchronized public void nextBytes(byte[] bytes) {
      fillData(bytes);
    }
    /** NOOP */
    synchronized public void setSeed(byte[] seed) { /* NOOP */ }
    /** NOOP */
    synchronized public void setSeed(long seed) { /* NOOP */ }
    
  }
}
