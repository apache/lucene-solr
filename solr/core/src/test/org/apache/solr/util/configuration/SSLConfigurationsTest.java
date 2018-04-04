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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.TestRuleRestoreSystemProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SSLConfigurationsTest {
  private Map<String, String> envs;
  private SSLConfigurations sut;

  public static final String SAMPLE_PW1 = "pw123";
  public static final String SAMPLE_PW2 = "pw456";
  public static final String SAMPLE_PW3 = "pw789";
  public static final String KEY_STORE_PASSWORD = SSLConfigurations.SysProps.SSL_KEY_STORE_PASSWORD;
  public static final String TRUST_STORE_PASSWORD = SSLConfigurations.SysProps.SSL_TRUST_STORE_PASSWORD;

  @Rule
  public TestRule syspropRestore = new TestRuleRestoreSystemProperties(
      SSLConfigurations.SysProps.SSL_KEY_STORE_PASSWORD,
      SSLConfigurations.SysProps.SSL_TRUST_STORE_PASSWORD
  );

  @Before
  public void setUp() {
    envs = new HashMap<>();
  }

  private SSLConfigurations createSut() {
    sut = new SSLConfigurations(envs);
    return sut;
  }

  @Test
  public void testSslConfigKeystorePwFromKeystoreEnvVar() {
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_KEY_STORE_PASSWORD, SAMPLE_PW1);
    createSut().init();
    assertThat(System.getProperty(KEY_STORE_PASSWORD), is(SAMPLE_PW1));
  }

  @Test
  public void testSslConfigKeystorePwFromClientKeystoreEnvVar() {
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(KEY_STORE_PASSWORD), is(SAMPLE_PW2));
  }

  @Test
  public void testSslConfigKeystorePwFromBothEnvVars() {
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_KEY_STORE_PASSWORD, SAMPLE_PW1);
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(KEY_STORE_PASSWORD), is(SAMPLE_PW2));
  }

  @Test
  public void testSslConfigKeystorePwNotOverwrittenIfExists() {
    System.setProperty(KEY_STORE_PASSWORD, SAMPLE_PW3);
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_KEY_STORE_PASSWORD, SAMPLE_PW1);
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_CLIENT_KEY_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(KEY_STORE_PASSWORD), is(SAMPLE_PW3)); // unchanged
  }


  @Test
  public void testSslConfigTruststorePwFromKeystoreEnvVar() {
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD, SAMPLE_PW1);
    createSut().init();
    assertThat(System.getProperty(TRUST_STORE_PASSWORD), is(SAMPLE_PW1));
  }

  @Test
  public void testSslConfigTruststorePwFromClientKeystoreEnvVar() {
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(TRUST_STORE_PASSWORD), is(SAMPLE_PW2));
  }

  @Test
  public void testSslConfigTruststorePwFromBothEnvVars() {
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD, SAMPLE_PW1);
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(TRUST_STORE_PASSWORD), is(SAMPLE_PW2));
  }

  @Test
  public void testSslConfigTruststorePwNotOverwrittenIfExists() {
    System.setProperty(TRUST_STORE_PASSWORD, SAMPLE_PW3);
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_TRUST_STORE_PASSWORD, SAMPLE_PW1);
    envs.put(SSLConfigurations.EnvVars.SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD, SAMPLE_PW2);
    createSut().init();
    assertThat(System.getProperty(TRUST_STORE_PASSWORD), is(SAMPLE_PW3)); // unchanged
  }

}
