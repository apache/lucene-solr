package org.apache.solr.client.solrj.embedded;

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

import org.eclipse.jetty.util.ssl.SslContextFactory;

public class SSLConfig {
  
  private boolean useSsl;
  private boolean clientAuth;
  private String keyStore;
  private String keyStorePassword;
  private String trustStore;
  private String trustStorePassword;
  
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

  public static SslContextFactory createContextFactory(SSLConfig sslConfig) {

    if (sslConfig == null) {
      if (Boolean.getBoolean(System.getProperty("tests.jettySsl"))) {
        return configureSslFromSysProps();
      }
      return null;
    }

    if (!sslConfig.useSsl)
      return null;

    SslContextFactory factory = new SslContextFactory(false);
    if (sslConfig.getKeyStore() != null)
      factory.setKeyStorePath(sslConfig.getKeyStore());
    if (sslConfig.getKeyStorePassword() != null)
      factory.setKeyStorePassword(sslConfig.getKeyStorePassword());
    if (sslConfig.getTrustStore() != null)
      factory.setTrustStorePath(System.getProperty(sslConfig.getTrustStore()));
    if (sslConfig.getTrustStorePassword() != null)
      factory.setTrustStorePassword(sslConfig.getTrustStorePassword());

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
