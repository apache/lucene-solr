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

import java.util.Map;

import org.apache.lucene.util.TestRuleRestoreSystemProperties;
import org.apache.solr.util.configuration.SSLConfigurations;
import org.apache.solr.util.configuration.SSLCredentialProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.apache.solr.util.configuration.providers.AbstractSSLCredentialProvider.DEFAULT_CREDENTIAL_KEY_MAP;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 */
public class SysPropSSLCredentialProviderTest {

  @Rule
  public TestRule syspropRestore = new TestRuleRestoreSystemProperties(
      SSLConfigurations.SysProps.SSL_KEY_STORE_PASSWORD,
      SSLConfigurations.SysProps.SSL_TRUST_STORE_PASSWORD,
      SSLConfigurations.SysProps.SSL_CLIENT_KEY_STORE_PASSWORD,
      SSLConfigurations.SysProps.SSL_CLIENT_TRUST_STORE_PASSWORD
  );

  @Test
  public void testGetCredentials() throws Exception {
    int cnt = 0;
    SysPropSSLCredentialProvider sut = new SysPropSSLCredentialProvider();
    for (Map.Entry<SSLCredentialProvider.CredentialType, String> set : DEFAULT_CREDENTIAL_KEY_MAP.entrySet()) {
      String pw = "pw" + ++cnt;
      System.setProperty(set.getValue(), pw);
      assertThat(sut.getCredential(set.getKey()), is(pw));
    }
  }


  @Test
  public void testGetCredentialsWithoutSetup() throws Exception {
    SysPropSSLCredentialProvider sut = new SysPropSSLCredentialProvider();
    // assuming not to fail
    sut.getCredential(SSLCredentialProvider.CredentialType.SSL_KEY_STORE_PASSWORD);
    sut.getCredential(SSLCredentialProvider.CredentialType.SSL_CLIENT_KEY_STORE_PASSWORD);
    sut.getCredential(SSLCredentialProvider.CredentialType.SSL_TRUST_STORE_PASSWORD);
    sut.getCredential(SSLCredentialProvider.CredentialType.SSL_CLIENT_TRUST_STORE_PASSWORD);
  }
}
