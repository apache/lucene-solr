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

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.SecureRandomSpi;
import java.security.UnrecoverableKeyException;
import java.util.Random;
import java.util.regex.Pattern;
  
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpClientUtil.SocketFactoryRegistryProvider;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.CertificateUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import com.carrotsearch.randomizedtesting.RandomizedTest;

/**
 * An SSLConfig that provides {@link SSLConfig} and {@link SocketFactoryRegistryProvider} for both clients and servers
 * that supports reading key/trust store information directly from resource files provided with the
 * Solr test-framework classes
 */
public class SSLTestConfig {
  private static final String TEST_KEYSTORE_BOGUSHOST_RESOURCE = "SSLTestConfig.hostname-and-ip-missmatch.keystore";
  private static final String TEST_KEYSTORE_LOCALHOST_RESOURCE = "SSLTestConfig.testing.keystore";
  private static final String TEST_PASSWORD = "secret";

  private final boolean checkPeerName;
  private final Resource keyStore;
  private final Resource trustStore;
  private final boolean useSsl;
  private final boolean clientAuth;
  
  /** Creates an SSLTestConfig that does not use SSL or client authentication */
  public SSLTestConfig() {
    this(false, false);
  }
  
  /**
   * Create an SSLTestConfig based on a few caller specified options, 
   * implicitly assuming <code>checkPeerName=false</code>.  
   * <p>
   * As needed, keystore/truststore information will be pulled from a hardcoded resource 
   * file provided by the solr test-framework
   * </p>
   *
   * @param useSSL - whether SSL should be required.
   * @param clientAuth - whether client authentication should be required.
   */
  public SSLTestConfig(boolean useSSL, boolean clientAuth) {
    this(useSSL, clientAuth, false);
  }

  // NOTE: if any javadocs below change, update create-keystores.sh
  /**
   * Create an SSLTestConfig based on a few caller specified options.  As needed, 
   * keystore/truststore information will be pulled from a hardcoded resource files provided 
   * by the solr test-framework based on the value of <code>checkPeerName</code>:
   * <ul>
   * <li><code>true</code> - A keystore resource file will be used that specifies 
   *     a CN of <code>localhost</code> and a SAN IP of <code>127.0.0.1</code>, to 
   *     ensure that all connections should be valid regardless of what machine runs the tests.</li> 
   * <li><code>false</code> - A keystore resource file will be used that specifies 
   *     a bogus hostname in the CN and reserved IP as the SAN, since no (valid) tests using this 
   *     SSLTestConfig should care what CN/SAN are.</li> 
   * </ul>
   *
   * @param useSSL - whether SSL should be required.
   * @param clientAuth - whether client authentication should be required.
   * @param checkPeerName - whether the client should validate the 'peer name' of the SSL Certificate (and which testing Cert should be used)
   * @see HttpClientUtil#SYS_PROP_CHECK_PEER_NAME
   */
  public SSLTestConfig(boolean useSSL, boolean clientAuth, boolean checkPeerName) {
    this.useSsl = useSSL;
    this.clientAuth = clientAuth;
    this.checkPeerName = checkPeerName;

    if (useSsl) {
      assumeSslIsSafeToTest();
    }
    
    final String resourceName = checkPeerName
      ? TEST_KEYSTORE_LOCALHOST_RESOURCE : TEST_KEYSTORE_BOGUSHOST_RESOURCE;
    trustStore = keyStore = Resource.newClassPathResource(resourceName);
    if (null == keyStore || ! keyStore.exists() ) {
      throw new IllegalStateException("Unable to locate keystore resource file in classpath: "
                                      + resourceName);
    }
  }

  /** If true, then servers hostname/ip should be validated against the SSL Cert metadata */
  public boolean getCheckPeerName() {
    return checkPeerName;
  }

  /** All other settings on this object are ignored unless this is true */
  public boolean isSSLMode() {
    return useSsl;
  }

  public boolean isClientAuthMode() {
    return clientAuth;
  }
  
  /**
   * Creates a {@link SocketFactoryRegistryProvider} for HTTP <b>clients</b> to use when communicating with servers 
   * which have been configured based on the settings of this object.  When {@link #isSSLMode} is true, this 
   * <code>SocketFactoryRegistryProvider</code> will <i>only</i> support HTTPS (no HTTP scheme) using the 
   * appropriate certs.  When {@link #isSSLMode} is false, <i>only</i> HTTP (no HTTPS scheme) will be 
   * supported.
   */
  public SocketFactoryRegistryProvider buildClientSocketFactoryRegistryProvider() {
    if (isSSLMode()) {
      SSLConnectionSocketFactory sslConnectionFactory = buildClientSSLConnectionSocketFactory();
      assert null != sslConnectionFactory;
      return new SSLSocketFactoryRegistryProvider(sslConnectionFactory);
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
    builder.loadTrustMaterial(buildKeyStore(keyStore, TEST_PASSWORD), new TrustSelfSignedStrategy()).build();

    if (isClientAuthMode()) {
      builder.loadKeyMaterial(buildKeyStore(trustStore, TEST_PASSWORD), TEST_PASSWORD.toCharArray());
    }

    return builder.build();
  }

  public SSLConfig buildClientSSLConfig() {
    if (!isSSLMode()) {
      return null;
    }

    return new SSLConfig(isSSLMode(), isClientAuthMode(), null, null, null, null) {
      @Override
      public SslContextFactory.Client createClientContextFactory() {
        SslContextFactory.Client factory = new SslContextFactory.Client(!checkPeerName);
        try {
          factory.setSslContext(buildClientSSLContext());
        } catch (KeyManagementException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
          throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", e);
        }
        return factory;
      }
    };
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
  public SSLConfig buildServerSSLConfig() {
    if (!isSSLMode()) {
      return null;
    }

    return new SSLConfig(isSSLMode(), isClientAuthMode(), null, null, null, null) {
      @Override
      public SslContextFactory.Server createContextFactory() {
        SslContextFactory.Server factory = new SslContextFactory.Server();
        try {
          SSLContextBuilder builder = SSLContexts.custom();
          builder.setSecureRandom(NotSecurePsuedoRandom.INSTANCE);

          builder.loadKeyMaterial(buildKeyStore(keyStore, TEST_PASSWORD), TEST_PASSWORD.toCharArray());

          if (isClientAuthMode()) {
            builder.loadTrustMaterial(buildKeyStore(trustStore, TEST_PASSWORD), new TrustSelfSignedStrategy()).build();

          }
          factory.setSslContext(builder.build());
        } catch (Exception e) {
          throw new RuntimeException("ssl context init failure: " + e.getMessage(), e);
        }
        factory.setNeedClientAuth(isClientAuthMode());
        return factory;
      }
    };
  }

  /**
   * Constructs a KeyStore using the specified filename and password
   */
  private static KeyStore buildKeyStore(Resource resource, String password) {
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
      SSLContext sslContext = buildClientSSLContext();
      if (checkPeerName == false) {
        sslConnectionFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
      } else {
        sslConnectionFactory = new SSLConnectionSocketFactory(sslContext);
      }
    } catch (KeyManagementException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
      throw new IllegalStateException("Unable to setup https scheme for HTTPClient to test SSL.", e);
    }
    return sslConnectionFactory;
  }

  /** A SocketFactoryRegistryProvider that only knows about SSL using a specified SSLConnectionSocketFactory */
  private static class SSLSocketFactoryRegistryProvider extends SocketFactoryRegistryProvider {
    private final SSLConnectionSocketFactory sslConnectionFactory;
    public SSLSocketFactoryRegistryProvider(SSLConnectionSocketFactory sslConnectionFactory) {
      this.sslConnectionFactory = sslConnectionFactory;
    }
    @Override
    public Registry<ConnectionSocketFactory> getSocketFactoryRegistry() {
      return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("https", sslConnectionFactory).build();
    }
  }

  /** A SocketFactoryRegistryProvider that only knows about HTTP */
  private static final SocketFactoryRegistryProvider HTTP_ONLY_SCHEMA_PROVIDER = new SocketFactoryRegistryProvider() {
    @Override
    public Registry<ConnectionSocketFactory> getSocketFactoryRegistry() {
      return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("http", PlainConnectionSocketFactory.getSocketFactory()).build();
    }
  };

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

  /**
   * Helper method for sanity checking if it's safe to use SSL on this JVM
   *
   * @see <a href="https://issues.apache.org/jira/browse/SOLR-12988">SOLR-12988</a>
   * @throws org.junit.internal.AssumptionViolatedException if this JVM is known to have SSL problems
   */
  public static void assumeSslIsSafeToTest() {
    if (Constants.JVM_NAME.startsWith("OpenJDK") ||
        Constants.JVM_NAME.startsWith("Java HotSpot(TM)")) {
      RandomizedTest.assumeFalse("Test (or randomization for this seed) wants to use SSL, " +
                                 "but SSL is known to fail on your JVM: " +
                                 Constants.JVM_NAME + " / " + Constants.JVM_VERSION, 
                                 isOpenJdkJvmVersionKnownToHaveProblems(Constants.JVM_VERSION));
    }
  }
  
  /** 
   * package visibility for tests
   * @see Constants#JVM_VERSION
   * @lucene.internal
   */
  static boolean isOpenJdkJvmVersionKnownToHaveProblems(final String jvmVersion) {
    // TODO: would be nice to replace with Runtime.Version once we don't have to
    // worry about java8 support when backporting to branch_8x
    return KNOWN_BAD_OPENJDK_JVMS.matcher(jvmVersion).matches();

  }
  private static final Pattern KNOWN_BAD_OPENJDK_JVMS
    = Pattern.compile(// 11 to 11.0.2 were all definitely problematic
                      // - https://bugs.openjdk.java.net/browse/JDK-8212885
                      // - https://bugs.openjdk.java.net/browse/JDK-8213202
                      "(^11(\\.0(\\.0|\\.1|\\.2)?)?($|(\\_|\\+|\\-).*$))|" +
                      // early (pre-ea) "testing" builds of 11, 12, and 13 were also buggy
                      // - https://bugs.openjdk.java.net/browse/JDK-8224829
                      "(^(11|12|13).*-testing.*$)|" +
                      // So far, all 13-ea builds (up to 13-ea-26) have been buggy
                      // - https://bugs.openjdk.java.net/browse/JDK-8226338
                      "(^13-ea.*$)"
                      );
}
