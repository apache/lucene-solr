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
 * Encapsulates settings related to SSL Configuration for an embedded Jetty Server.
 * NOTE: all other settings are ignogred if {@link #isSSLMode} is false.
 * @see #setUseSSL
 */
public class SSLConfig {
  
  private boolean useSsl;
  private boolean clientAuth;
  private String keyStore;
  private String keyStorePassword;
  private String trustStore;
  private String trustStorePassword;

  /** NOTE: all other settings are ignored if useSSL is false; trustStore settings are ignored if clientAuth is false */
  public SSLConfig(boolean useSSL, boolean clientAuth, String keyStore, String keyStorePassword, String trustStore, String trustStorePassword) {
    this.useSsl = useSSL;
    this.clientAuth = clientAuth;
    this.keyStore = keyStore;
    this.keyStorePassword = keyStorePassword;
    this.trustStore = trustStore;
    this.trustStorePassword = trustStorePassword;
  }
  
  public void setUseSSL(boolean useSSL) {
    this.useSsl = useSSL;
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
   * Returns an SslContextFactory that should be used by a jetty server based on the specified 
   * configuration, or null if no SSL should be used.
   *
   * The specified sslConfig will be completely ignored if the "tests.jettySsl" system property is 
   * true - in which case standard "javax.net.ssl.*" system properties will be used instead, along 
   * with "tests.jettySsl.clientAuth"
   * 
   * @see #isSSLMode
   */
  public static SslContextFactory createContextFactory(SSLConfig sslConfig) {

    if (sslConfig == null) {
      if (Boolean.getBoolean("tests.jettySsl")) {
        return configureSslFromSysProps();
      }
      return null;
    }

    if (!sslConfig.isSSLMode()) 
       return null;

    SslContextFactory factory = new SslContextFactory(false);
    if (sslConfig.getKeyStore() != null)
      factory.setKeyStorePath(sslConfig.getKeyStore());
    if (sslConfig.getKeyStorePassword() != null)
      factory.setKeyStorePassword(sslConfig.getKeyStorePassword());
    factory.setNeedClientAuth(sslConfig.isClientAuthMode());
    
    if (sslConfig.isClientAuthMode()) {
      if (sslConfig.getTrustStore() != null)
        factory.setTrustStorePath(sslConfig.getTrustStore());
      if (sslConfig.getTrustStorePassword() != null)
        factory.setTrustStorePassword(sslConfig.getTrustStorePassword());
    }
    return factory;

  }

  private static SslContextFactory configureSslFromSysProps() {

    SslContextFactory sslcontext = new SslContextFactory(false);

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
