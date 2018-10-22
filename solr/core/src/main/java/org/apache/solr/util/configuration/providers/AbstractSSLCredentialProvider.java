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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.solr.util.configuration.SSLCredentialProvider;

import static org.apache.solr.util.configuration.SSLCredentialProvider.CredentialType.SSL_CLIENT_KEY_STORE_PASSWORD;
import static org.apache.solr.util.configuration.SSLCredentialProvider.CredentialType.SSL_CLIENT_TRUST_STORE_PASSWORD;
import static org.apache.solr.util.configuration.SSLCredentialProvider.CredentialType.SSL_KEY_STORE_PASSWORD;
import static org.apache.solr.util.configuration.SSLCredentialProvider.CredentialType.SSL_TRUST_STORE_PASSWORD;

/**
 * Abstract provider with default implementation
 */
abstract public class AbstractSSLCredentialProvider implements SSLCredentialProvider {
  public static final EnumMap<CredentialType, String> DEFAULT_CREDENTIAL_KEY_MAP = Maps.newEnumMap(ImmutableMap.of(
      SSL_KEY_STORE_PASSWORD, "solr.jetty.keystore.password",
      SSL_TRUST_STORE_PASSWORD, "solr.jetty.truststore.password",
      SSL_CLIENT_KEY_STORE_PASSWORD, "javax.net.ssl.keyStorePassword",
      SSL_CLIENT_TRUST_STORE_PASSWORD, "javax.net.ssl.trustStorePassword"
  ));
  private final EnumMap<CredentialType, String> credentialKeyMap;

  public AbstractSSLCredentialProvider() {
    credentialKeyMap = getCredentialKeyMap();
  }

  abstract protected String getCredential(String key);

  abstract protected EnumMap<CredentialType, String> getCredentialKeyMap();

  @Override
  public String getCredential(CredentialType type) {
    return getCredential(credentialKeyMap.get(type));
  }

}
