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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.util.TestRuleRestoreSystemProperties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.configuration.SSLCredentialProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

import static org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH;
import static org.apache.solr.util.configuration.providers.AbstractSSLCredentialProvider.DEFAULT_CREDENTIAL_KEY_MAP;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

/**
 */
public class HadoopSSLCredentialProviderTest {

  @Rule
  public TestRule syspropRestore = new TestRuleRestoreSystemProperties(
      CREDENTIAL_PROVIDER_PATH
  );

  @Test(expected = RuntimeException.class)
  public void testConstructorRequiresCredPath() {
    new HadoopSSLCredentialProvider(getMockHadoopConfiguration());
  }

  @Test
  public void testGetCredentials() throws Exception {
    int cnt = 0;
    for (Map.Entry<SSLCredentialProvider.CredentialType, String> set : DEFAULT_CREDENTIAL_KEY_MAP.entrySet()) {
      String pw = "pw" + ++cnt;
      HadoopSSLCredentialProvider sut = new HadoopSSLCredentialProvider(getMockedHadoopCredentialProvider(set.getValue(), pw));
      assertThat(sut.getCredential(set.getKey()), is(pw));
    }
  }

  private Configuration getMockedHadoopCredentialProvider(String key, String pw) throws IOException {
    Configuration mockHadoopConfiguration = getMockHadoopConfiguration();
    when(mockHadoopConfiguration.getPassword(key))
        .then(invocationOnMock -> invocationOnMock.getArguments()[0].equals(key) ? pw.toCharArray() : null);
    System.setProperty(CREDENTIAL_PROVIDER_PATH, "/some/path"); // enables HCP
    return mockHadoopConfiguration;
  }

  private Configuration getMockHadoopConfiguration() {
    SolrTestCaseJ4.assumeWorkingMockito();
    return Mockito.mock(Configuration.class);
  }
}
