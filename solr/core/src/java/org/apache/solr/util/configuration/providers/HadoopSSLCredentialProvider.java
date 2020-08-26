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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.EnumMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.solr.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH;

/**
 * System property based SSL configuration provider
 */
public class HadoopSSLCredentialProvider extends AbstractSSLCredentialProvider {

  private Configuration hadoopConfigurationProvider;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public HadoopSSLCredentialProvider() {
    this(new Configuration());
  }

  public HadoopSSLCredentialProvider(Configuration hadoopConfigurationProvider) {
    if (StringUtils.isEmpty(System.getProperty(CREDENTIAL_PROVIDER_PATH))) {
      throw new RuntimeException("Cannot initialize Hadoop configuration provider without credential provider path. Use " + CREDENTIAL_PROVIDER_PATH + " system property to configure.");
    }
    this.hadoopConfigurationProvider = hadoopConfigurationProvider;
    hadoopConfigurationProvider.set(CREDENTIAL_PROVIDER_PATH, System.getProperty(CREDENTIAL_PROVIDER_PATH));
  }

  @Override
  protected EnumMap<CredentialType, String> getCredentialKeyMap() {
    return DEFAULT_CREDENTIAL_KEY_MAP;
  }

  protected String getCredential(String keystoreKey) {
    try {
      char[] password = hadoopConfigurationProvider.getPassword(keystoreKey);
      return password == null ? null : String.valueOf(password);
    } catch (IOException e) {
      log.error("Could not read password from Hadoop Credential Store: {}", keystoreKey, e);
      return null;
    }
  }
}
