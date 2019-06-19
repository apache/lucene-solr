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
package org.apache.solr.client.solrj.impl;

import javax.net.ssl.HostnameVerifier;
import java.io.IOException;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.impl.HttpClientUtil.SchemaRegistryProvider;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.lucene.util.TestRuleRestoreSystemProperties;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.Test;

public class HttpClientUtilTest extends SolrTestCase {

  @Rule
  public TestRule syspropRestore = new TestRuleRestoreSystemProperties
    (HttpClientUtil.SYS_PROP_CHECK_PEER_NAME);

  @After
  public void resetHttpClientBuilder() {
    HttpClientUtil.resetHttpClientBuilder();
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testSSLSystemProperties() throws IOException {

    assertNotNull("HTTPS scheme could not be created using system defaults",
                  HttpClientUtil.getSchemaRegisteryProvider().getSchemaRegistry().lookup("https"));

    assertSSLHostnameVerifier(DefaultHostnameVerifier.class, HttpClientUtil.getSchemaRegisteryProvider());

    System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "true");
    resetHttpClientBuilder();
    assertSSLHostnameVerifier(DefaultHostnameVerifier.class, HttpClientUtil.getSchemaRegisteryProvider());

    System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "");
    resetHttpClientBuilder();
    assertSSLHostnameVerifier(DefaultHostnameVerifier.class, HttpClientUtil.getSchemaRegisteryProvider());
    
    System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "false");
    resetHttpClientBuilder();
    assertSSLHostnameVerifier(NoopHostnameVerifier.class, HttpClientUtil.getSchemaRegisteryProvider());
  }

  private void assertSSLHostnameVerifier(Class<? extends HostnameVerifier> expected,
                                         SchemaRegistryProvider provider) {
    ConnectionSocketFactory socketFactory = provider.getSchemaRegistry().lookup("https");
    assertNotNull("unable to lookup https", socketFactory);
    assertTrue("socketFactory is not an SSLConnectionSocketFactory: " + socketFactory.getClass(),
               socketFactory instanceof SSLConnectionSocketFactory);
    SSLConnectionSocketFactory sslSocketFactory = (SSLConnectionSocketFactory) socketFactory;
    try {
      Object hostnameVerifier = FieldUtils.readField(sslSocketFactory, "hostnameVerifier", true);
      assertNotNull("sslSocketFactory has null hostnameVerifier", hostnameVerifier);
      assertEquals("sslSocketFactory does not have expected hostnameVerifier impl",
                   expected, hostnameVerifier.getClass());
    } catch (IllegalAccessException e) {
      throw new AssertionError("Unexpected access error reading hostnameVerifier field", e);
    }
  }
  
  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testToBooleanDefaultIfNull() throws Exception {
    assertFalse(HttpClientUtil.toBooleanDefaultIfNull(Boolean.FALSE, true));
    assertTrue(HttpClientUtil.toBooleanDefaultIfNull(Boolean.TRUE, false));
    assertFalse(HttpClientUtil.toBooleanDefaultIfNull(null, false));
    assertTrue(HttpClientUtil.toBooleanDefaultIfNull(null, true));
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testToBooleanObject() throws Exception {
    assertEquals(Boolean.TRUE, HttpClientUtil.toBooleanObject("true"));
    assertEquals(Boolean.TRUE, HttpClientUtil.toBooleanObject("TRUE"));
    assertEquals(Boolean.TRUE, HttpClientUtil.toBooleanObject("tRuE"));

    assertEquals(Boolean.FALSE, HttpClientUtil.toBooleanObject("false"));
    assertEquals(Boolean.FALSE, HttpClientUtil.toBooleanObject("FALSE"));
    assertEquals(Boolean.FALSE, HttpClientUtil.toBooleanObject("fALSE"));

    assertEquals(null, HttpClientUtil.toBooleanObject("t"));
    assertEquals(null, HttpClientUtil.toBooleanObject("f"));
    assertEquals(null, HttpClientUtil.toBooleanObject("foo"));
    assertEquals(null, HttpClientUtil.toBooleanObject(null));
  }
}
