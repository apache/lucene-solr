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

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpClientUtil.SchemaRegistryProvider;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;

import org.apache.lucene.util.Constants;

import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.CertificateUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;

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
    this(useSSL, clientAuth, TEST_KEYSTORE_PATH, TEST_KEYSTORE_PASSWORD, TEST_KEYSTORE_PATH, TEST_KEYSTORE_PASSWORD);
  }
 
  public SSLTestConfig(boolean useSSL, boolean clientAuth, String keyStore, String keyStorePassword, String trustStore, String trustStorePassword) {
    super(useSSL, clientAuth, keyStore, keyStorePassword, trustStore, trustStorePassword);
  }
  
  /**
   * Creates a {@link SchemaRegistryProvider} for HTTP <b>clients</b> to use when communicating with servers 
   * which have been configured based on the settings of this object.  When {@link #isSSLMode} is true, this 
   * <code>SchemaRegistryProvider</code> will <i>only</i> support HTTPS (no HTTP scheme) using the 
   * appropriate certs.  When {@link #isSSLMode} is false, <i>only</i> HTTP (no HTTPS scheme) will be 
   * supported.
   */
  public SchemaRegistryProvider buildClientSchemaRegistryProvider() {
    if (isSSLMode()) {
      SSLConnectionSocketFactory sslConnectionFactory = buildClientSSLConnectionSocketFactory();
      assert null != sslConnectionFactory;
      return new SSLSchemaRegistryProvider(sslConnectionFactory);
    } else {
      return HTTP_ONLY_SCHEMA_PROVIDER;
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
    builder.setSecureRandom(NullSecureRandom.INSTANCE);
    
    // NOTE: KeyStore & TrustStore are swapped because they are from configured from server perspective...
    // we are a client - our keystore contains the keys the server trusts, and vice versa
    builder.loadTrustMaterial(buildKeyStore(getKeyStore(), getKeyStorePassword()), new TrustSelfSignedStrategy()).build();

    if (isClientAuthMode()) {
      builder.loadKeyMaterial(buildKeyStore(getTrustStore(), getTrustStorePassword()), getTrustStorePassword().toCharArray());
      
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
    builder.setSecureRandom(NullSecureRandom.INSTANCE);
    
    builder.loadKeyMaterial(buildKeyStore(getKeyStore(), getKeyStorePassword()), getKeyStorePassword().toCharArray());

    if (isClientAuthMode()) {
      builder.loadTrustMaterial(buildKeyStore(getTrustStore(), getTrustStorePassword()), new TrustSelfSignedStrategy()).build();
      
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
  protected static KeyStore buildKeyStore(String keyStoreLocation, String password) {
    try {
      return CertificateUtils.getKeyStore(Resource.newResource(keyStoreLocation), "JKS", null, password);
    } catch (Exception ex) {
      throw new IllegalStateException("Unable to build KeyStore from file: " + keyStoreLocation, ex);
    }
  }

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

  /** A SchemaRegistryProvider that only knows about SSL using a specified SSLConnectionSocketFactory */
  private static class SSLSchemaRegistryProvider extends SchemaRegistryProvider {
    private final SSLConnectionSocketFactory sslConnectionFactory;
    public SSLSchemaRegistryProvider(SSLConnectionSocketFactory sslConnectionFactory) {
      this.sslConnectionFactory = sslConnectionFactory;
    }
    @Override
    public Registry<ConnectionSocketFactory> getSchemaRegistry() {
      return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("https", sslConnectionFactory).build();
    }
  }

  /** A SchemaRegistryProvider that only knows about HTTP */
  private static final SchemaRegistryProvider HTTP_ONLY_SCHEMA_PROVIDER = new SchemaRegistryProvider() {
    @Override
    public Registry<ConnectionSocketFactory> getSchemaRegistry() {
      return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("http", PlainConnectionSocketFactory.getSocketFactory()).build();
    }
  };
  
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
   * A mocked up instance of SecureRandom that always does the minimal amount of work to generate 
   * "random" numbers.  This is to prevent blocking issues that arise in platform default 
   * SecureRandom instances due to too many instances / not enough random entropy.  
   * Tests do not need secure SSL.
   */
  private static class NullSecureRandom extends SecureRandom {

    /** 
     * The one and only instance that should be used, specific impl may vary based on platform 
     * @see Constants#SUN_OS
     * @see <a href="https://issues.apache.org/jira/browse/SOLR-9068">SOLR-9068</a>
     */
    public static final SecureRandom INSTANCE = Constants.SUN_OS
      ? new NullSecureRandom(NullSecureRandomSpi.PSUEDO_RAND_INSTANCE)
      : new NullSecureRandom(NullSecureRandomSpi.NULL_INSTANCE);

    /** A source of psuedo random data if needed */
    private static final Random RAND = new Random(42);
    
    /** SPI base class for all NullSecureRandom instances */
    private static class NullSecureRandomSpi extends SecureRandomSpi {
      private NullSecureRandomSpi() {
        /* NOOP */
      }
      /** 
       * Helper method that can be used to fill an array with non-zero data.
       * Default impl is No-Op
       */
      public byte[] fillData(byte[] data) {
        return data; /* NOOP */
      }
      /** returns a new byte[] filled with static data */
      @Override
      public byte[] engineGenerateSeed(int numBytes) {
        return fillData(new byte[numBytes]);
      }
      /** fills the byte[] with static data */
      @Override
      public void engineNextBytes(byte[] bytes) {
        fillData(bytes);
      }
      /** NOOP */
      @Override
      public void engineSetSeed(byte[] seed) { /* NOOP */ }
      
      /** Instance to use on platforms w/SSLEngines that work fine when SecureRandom returns constant bytes */
      public static final NullSecureRandomSpi NULL_INSTANCE = new NullSecureRandomSpi();

      /** 
       * Instance to use on platforms that need at least psuedo-random data for the SSLEngine to not break
       * (Attempted workarround of Solaris SSL Padding bug: SOLR-9068)
       */
      public static final NullSecureRandomSpi PSUEDO_RAND_INSTANCE = new NullSecureRandomSpi() {
        /** 
         * Fill with Psuedo-Random data.
         * (Attempted workarround of Solaris SSL Padding bug: SOLR-9068)
         */
        @Override
        public byte[] fillData(byte[] data) {
          RAND.nextBytes(data);
          return data;
        }
      };
    }
    
    private NullSecureRandom(NullSecureRandomSpi spi) {
      super(spi, null);
      this.spi = spi;
    }
    
    private NullSecureRandomSpi spi;
    
    /** fills a new byte[] with data from SPI */
    @Override
    public byte[] generateSeed(int numBytes) {
      return spi.fillData(new byte[numBytes]);
    }
    /** fills the byte[] with data from SPI */
    @Override
    synchronized public void nextBytes(byte[] bytes) {
      spi.fillData(bytes);
    }
    /** NOOP */
    @Override
    synchronized public void setSeed(byte[] seed) { /* NOOP */ }
    /** NOOP */
    @Override
    synchronized public void setSeed(long seed) { /* NOOP */ }
    
  }
}
