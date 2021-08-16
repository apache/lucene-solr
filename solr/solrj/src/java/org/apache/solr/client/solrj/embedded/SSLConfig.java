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
package org.apache.solr.client.solrj.embedded;

import org.eclipse.jetty.util.ssl.SslContextFactory;

/** 
 * Encapsulates settings related to SSL Configuration.
 * NOTE: all other settings are ignored if {@link #isSSLMode} is false.
 * @see #setUseSSL
 */
public class SSLConfig {
  private boolean useSsl;
  private boolean clientAuth;
  private String keyStore;
  private String keyStorePassword;
  private String trustStore;
  private String trustStorePassword;

  /** NOTE: all other settings are ignored if useSsl is false; trustStore settings are ignored if clientAuth is false */
  public SSLConfig(boolean useSsl, boolean clientAuth, String keyStore, String keyStorePassword, String trustStore, String trustStorePassword) {
    this.useSsl = useSsl;
    this.clientAuth = clientAuth;
    this.keyStore = keyStore;
    this.keyStorePassword = keyStorePassword;
    this.trustStore = trustStore;
    this.trustStorePassword = trustStorePassword;
  }
  
  public void setUseSSL(boolean useSsl) {
    this.useSsl = useSsl;
  }
  
  public void setClientAuth(boolean clientAuth) {
    this.clientAuth = clientAuth;
  }
  
  /** All other settings on this object are ignored unless this is true */
  public boolean isSSLMode() {
    return useSsl;
  }
  
  public boolean isClientAuthMode() {
    return clientAuth;
  }

  public String getKeyStore() {
    return keyStore;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getTrustStore() {
    return trustStore;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  /**
   * Returns an SslContextFactory.Server that should be used by a jetty server based on the specified
   * SSLConfig param which may be null.
   *
   * if the SSLConfig param is non-null, then this method will return the results of 
   * {@link #createContextFactory()}.
   * 
   * If the SSLConfig param is null, then this method will return null unless the 
   * <code>tests.jettySsl</code> system property is true, in which case standard "javax.net.ssl.*" 
   * system properties will be used instead, along with "tests.jettySsl.clientAuth".
   * 
   * @see #createContextFactory()
   */
  public static SslContextFactory.Server createContextFactory(SSLConfig sslConfig) {
    if (sslConfig != null) {
      return sslConfig.createContextFactory();
    }
    // else...
    if (Boolean.getBoolean("tests.jettySsl")) {
      return configureSslFromSysProps();
    }
    // else...
    return null;
  }
  
  /**
   * Returns an SslContextFactory.Server that should be used by a jetty server based on this SSLConfig instance,
   * or null if SSL should not be used.
   *
   * The default implementation generates a simple factory according to the keystore, truststore, 
   * and clientAuth properties of this object.
   *
   * @see #getKeyStore
   * @see #getKeyStorePassword
   * @see #isClientAuthMode
   * @see #getTrustStore
   * @see #getTrustStorePassword
   */
  public SslContextFactory.Server createContextFactory() {
    if (! isSSLMode()) {
      return null;
    }
    // else...
    
    SslContextFactory.Server factory = new SslContextFactory.Server();
    if (getKeyStore() != null)
      factory.setKeyStorePath(getKeyStore());
    if (getKeyStorePassword() != null)
      factory.setKeyStorePassword(getKeyStorePassword());
    
    factory.setNeedClientAuth(isClientAuthMode());
    
    if (isClientAuthMode()) {
      if (getTrustStore() != null)
        factory.setTrustStorePath(getTrustStore());
      if (getTrustStorePassword() != null)
        factory.setTrustStorePassword(getTrustStorePassword());
    }
    return factory;
  }

  public SslContextFactory.Client createClientContextFactory() {
    if (! isSSLMode()) {
      return null;
    }
    // else...

    SslContextFactory.Client factory = new SslContextFactory.Client();
    if (getKeyStore() != null) {
      factory.setKeyStorePath(getKeyStore());
    }
    if (getKeyStorePassword() != null) {
      factory.setKeyStorePassword(getKeyStorePassword());
    }

    if (isClientAuthMode()) {
      if (getTrustStore() != null)
        factory.setTrustStorePath(getTrustStore());
      if (getTrustStorePassword() != null)
        factory.setTrustStorePassword(getTrustStorePassword());
    }

    return factory;
  }

  private static SslContextFactory.Server configureSslFromSysProps() {
    SslContextFactory.Server sslcontext = new SslContextFactory.Server();

    if (null != System.getProperty("javax.net.ssl.keyStore")) {
      sslcontext.setKeyStorePath
          (System.getProperty("javax.net.ssl.keyStore"));
    }
    if (null != System.getProperty("javax.net.ssl.keyStorePassword")) {
      sslcontext.setKeyStorePassword
          (System.getProperty("javax.net.ssl.keyStorePassword"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStore")) {
      sslcontext.setTrustStorePath
          (System.getProperty("javax.net.ssl.trustStore"));
    }
    if (null != System.getProperty("javax.net.ssl.trustStorePassword")) {
      sslcontext.setTrustStorePassword
          (System.getProperty("javax.net.ssl.trustStorePassword"));
    }
    sslcontext.setNeedClientAuth(Boolean.getBoolean("tests.jettySsl.clientAuth"));

    return sslcontext;
  }
}
