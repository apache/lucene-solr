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

import java.util.List;

import org.apache.lucene.util.TestRuleRestoreSystemProperties;
import org.apache.solr.util.configuration.providers.EnvSSLCredentialProvider;
import org.apache.solr.util.configuration.providers.SysPropSSLCredentialProvider;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

/**
 */
public class SSLCredentialProviderFactoryTest {

  @Rule
  public TestRule syspropRestore = new TestRuleRestoreSystemProperties(
      SSLCredentialProviderFactory.PROVIDER_CHAIN_KEY
  );

  public static <T> Matcher<T> isA(Class<?> type) {
    final Matcher<T> typeMatcher = instanceOf(type);
    return is(typeMatcher);
  }

  @Test
  public void testGetProvidersOrder() {
    SSLCredentialProviderFactory sut = getSut("sysprop;env");
    List<SSLCredentialProvider> providers = sut.getProviders();
    assertThat(providers.get(0), isA(SysPropSSLCredentialProvider.class));
    assertThat(providers.get(1), isA(EnvSSLCredentialProvider.class));

    sut = getSut("env;sysprop");
    providers = sut.getProviders();
    assertThat(providers.get(0), isA(EnvSSLCredentialProvider.class));
    assertThat(providers.get(1), isA(SysPropSSLCredentialProvider.class));
  }

  @Test
  public void testGetProvidersWithCustomProvider() {
    SSLCredentialProviderFactory sut = getSut("sysprop;class://" + CustomSSLCredentialProvider.class.getName() + ";env");
    List<SSLCredentialProvider> providers = sut.getProviders();
    assertThat(providers.get(0), isA(SysPropSSLCredentialProvider.class));
    assertThat(providers.get(1), isA(CustomSSLCredentialProvider.class));
    assertThat(providers.get(2), isA(EnvSSLCredentialProvider.class));
  }

  @Test(expected = RuntimeException.class)
  public void testGetProvidersInvalidProvider() {
    getSut("sysprop;DoesNotExists").getProviders();
  }

  @Test
  public void testGetProvidersBySysprop() {
    String chain = "sysprop;class://" + CustomSSLCredentialProvider.class.getName() + ";env";
    System.setProperty(SSLCredentialProviderFactory.PROVIDER_CHAIN_KEY, chain);
    SSLCredentialProviderFactory sut = new SSLCredentialProviderFactory();
    List<SSLCredentialProvider> providers = sut.getProviders();
    assertThat(providers.get(0), isA(SysPropSSLCredentialProvider.class));
    assertThat(providers.get(1), isA(CustomSSLCredentialProvider.class));
    assertThat(providers.get(2), isA(EnvSSLCredentialProvider.class));
  }

  private SSLCredentialProviderFactory getSut(String providerChain) {
    return new SSLCredentialProviderFactory(providerChain);
  }

  static public class CustomSSLCredentialProvider implements SSLCredentialProvider {
    @Override
    public String getCredential(CredentialType type) {
      return null;
    }
  }

}
