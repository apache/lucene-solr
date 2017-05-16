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
import java.util.Map;

import org.apache.solr.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dedicated object to handle Solr configurations
 */
public class SSLConfigurations {
  private final Map<String, String> envVars;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static class SysProps {
    public static final String SSL_KEY_STORE_PASSWORD = "javax.net.ssl.keyStorePassword";
    public static final String SSL_TRUST_STORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  }

  public static class EnvVars {
    public static final String SOLR_SSL_CLIENT_KEY_STORE_PASSWORD = "SOLR_SSL_CLIENT_KEY_STORE_PASSWORD";
    public static final String SOLR_SSL_KEY_STORE_PASSWORD = "SOLR_SSL_KEY_STORE_PASSWORD";
    public static final String SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD = "SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD";
    public static final String SOLR_SSL_TRUST_STORE_PASSWORD = "SOLR_SSL_TRUST_STORE_PASSWORD";
  }

  /**
   * @param envVars Map of environment variables to use
   */
  public SSLConfigurations(Map<String, String> envVars) {
    this.envVars = envVars;
  }

  /** Initiates javax.net.ssl.* system properties from the proper sources. */
  public void init() {

    String clientKeystorePassword = envVars.get(EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD);
    String keystorePassword = envVars.get(EnvVars.SOLR_SSL_KEY_STORE_PASSWORD);

    String clientTruststorePassword = envVars.get(EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD);
    String truststorePassword = envVars.get(EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD);

    if (isEmpty(System.getProperty(SysProps.SSL_KEY_STORE_PASSWORD))
        && !(isEmpty(clientKeystorePassword) && isEmpty(keystorePassword))) {
      log.debug("Setting {} based on env var", SysProps.SSL_KEY_STORE_PASSWORD);
      System.setProperty(SysProps.SSL_KEY_STORE_PASSWORD, clientKeystorePassword != null ? clientKeystorePassword : keystorePassword);
    }
    if (isEmpty(System.getProperty(SysProps.SSL_TRUST_STORE_PASSWORD))
        && !(isEmpty(clientTruststorePassword) && isEmpty(truststorePassword))) {
      log.debug("Setting {} based on env var", SysProps.SSL_TRUST_STORE_PASSWORD);
      System.setProperty(SysProps.SSL_TRUST_STORE_PASSWORD, clientTruststorePassword != null ? clientTruststorePassword : truststorePassword);
    }
  }

  private boolean isEmpty(String str) {
    return StringUtils.isEmpty(str);
  }
}
