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

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
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
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.CertificateUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * An {@link SSLConfig} that supports reading key/trust store information directly from resource 
 * files provided with the Solr test-framework classes
 */
public class SSLTestConfig extends SSLConfig {

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

  /** 
   * NOTE: This method is meaningless in SSLTestConfig.
   * @return null
   */
  @Override
  public String getKeyStore() {
    return null;
  }
  /** 
   * NOTE: This method is meaningless in SSLTestConfig.
   * @return null
   */
  @Override
  public String getTrustStore() {
    return null;
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
