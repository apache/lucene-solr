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

package org.apache.solr.util.configuration;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.apache.solr.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dedicated object to handle Solr configurations
 */
public class SSLConfigurations {
  public static final String DEFAULT_STORE_PASSWORD = "secret";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final List<SSLCredentialProvider> credentialProviders;

  public static class SysProps {
    public static final String SSL_KEY_STORE_PASSWORD = "solr.jetty.keystore.password";
    public static final String SSL_TRUST_STORE_PASSWORD = "solr.jetty.truststore.password";
    public static final String SSL_CLIENT_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    public static final String SSL_CLIENT_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  }

  /**
   * @param sslCredentialProviderFactory Credential provider factory to use for providers
   */
  public SSLConfigurations(SSLCredentialProviderFactory sslCredentialProviderFactory) {
    credentialProviders = sslCredentialProviderFactory.getProviders();
  }

  /**
   * @param credentialProviders Explicit list of credential providers to use
   */
  public SSLConfigurations(List<SSLCredentialProvider> credentialProviders) {
    this.credentialProviders = credentialProviders;
  }

  /**
   * Initiates javax.net.ssl.* system properties from the proper sources.
   */
  public void init() {

    String clientKeystorePassword = getClientKeyStorePassword();
    String keystorePassword = getKeyStorePassword();

    String clientTruststorePassword = getClientTrustStorePassword();
    String truststorePassword = getTrustStorePassword();

    if (isEmpty(System.getProperty(SysProps.SSL_CLIENT_KEY_STORE_PASSWORD))
        && !(isEmpty(clientKeystorePassword) && isEmpty(keystorePassword))) {
      log.info("Setting {}", SysProps.SSL_CLIENT_KEY_STORE_PASSWORD);
      System.setProperty(SysProps.SSL_CLIENT_KEY_STORE_PASSWORD, clientKeystorePassword != null ? clientKeystorePassword : keystorePassword);
    }
    if (isEmpty(System.getProperty(SysProps.SSL_CLIENT_TRUST_STORE_PASSWORD))
        && !(isEmpty(clientTruststorePassword) && isEmpty(truststorePassword))) {
      log.info("Setting {}", SysProps.SSL_CLIENT_TRUST_STORE_PASSWORD);
      System.setProperty(SysProps.SSL_CLIENT_TRUST_STORE_PASSWORD, clientTruststorePassword != null ? clientTruststorePassword : truststorePassword);
    }

  }

  /**
   * @return password for keystore used for SSL connections
   */
  public String getKeyStorePassword() {
    String keyStorePassword = getPassword(SSLCredentialProvider.CredentialType.SSL_KEY_STORE_PASSWORD);
    return keyStorePassword;
  }

  /**
   * @return password for truststore used for SSL connections
   */
  public String getTrustStorePassword() {
    String trustStorePassword = getPassword(SSLCredentialProvider.CredentialType.SSL_TRUST_STORE_PASSWORD);
    return trustStorePassword;
  }

  /**
   * @return password for keystore used for SSL client connections
   */
  public String getClientKeyStorePassword() {
    String keyStorePassword = getPassword(SSLCredentialProvider.CredentialType.SSL_CLIENT_KEY_STORE_PASSWORD);
    return keyStorePassword;
  }

  /**
   * @return password for truststore used for SSL client connections
   */
  public String getClientTrustStorePassword() {
    String trustStorePassword = getPassword(SSLCredentialProvider.CredentialType.SSL_CLIENT_TRUST_STORE_PASSWORD);
    return trustStorePassword;
  }

  protected String getPassword(SSLCredentialProvider.CredentialType type) {
    for(SSLCredentialProvider provider: credentialProviders){
      String credential = provider.getCredential(type);
      if(credential != null) return credential;
    }
    return null;
  }

  private boolean isEmpty(String str) {
    return StringUtils.isEmpty(str);
  }
}
