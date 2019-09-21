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

package org.apache.solr.util.configuration.providers;

import java.util.EnumMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import static org.apache.solr.util.configuration.SSLCredentialProvider.CredentialType.SSL_CLIENT_KEY_STORE_PASSWORD;
import static org.apache.solr.util.configuration.SSLCredentialProvider.CredentialType.SSL_CLIENT_TRUST_STORE_PASSWORD;
import static org.apache.solr.util.configuration.SSLCredentialProvider.CredentialType.SSL_KEY_STORE_PASSWORD;
import static org.apache.solr.util.configuration.SSLCredentialProvider.CredentialType.SSL_TRUST_STORE_PASSWORD;


/**
 * Environment variable based SSL configuration provider
 */
public class EnvSSLCredentialProvider extends AbstractSSLCredentialProvider {

  public static class EnvVars {
    public static final String SOLR_SSL_CLIENT_KEY_STORE_PASSWORD = "SOLR_SSL_CLIENT_KEY_STORE_PASSWORD";
    public static final String SOLR_SSL_KEY_STORE_PASSWORD = "SOLR_SSL_KEY_STORE_PASSWORD";
    public static final String SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD = "SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD";
    public static final String SOLR_SSL_TRUST_STORE_PASSWORD = "SOLR_SSL_TRUST_STORE_PASSWORD";
  }

  private Map<String, String> envVars;

  public EnvSSLCredentialProvider() {
    this.envVars = System.getenv();
  }

  protected EnumMap<CredentialType, String> getCredentialKeyMap() {
    return Maps.newEnumMap(ImmutableMap.of(
        SSL_KEY_STORE_PASSWORD, EnvVars.SOLR_SSL_KEY_STORE_PASSWORD,
        SSL_TRUST_STORE_PASSWORD, EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD,
        SSL_CLIENT_KEY_STORE_PASSWORD, EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD,
        SSL_CLIENT_TRUST_STORE_PASSWORD, EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD
    ));
  }

  protected String getCredential(String envKey) {
    if (envVars.containsKey(envKey)) {
      return envVars.get(envKey);
    } else {
      return null;
    }
  }

  @VisibleForTesting
  public void setEnvVars(Map<String, String> envVars) {
    this.envVars = envVars;
  }
}
