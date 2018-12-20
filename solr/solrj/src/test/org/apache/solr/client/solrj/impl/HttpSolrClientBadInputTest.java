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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.hamcrest.core.StringContains.containsString;

/**
 * Tests {@link HttpSolrClient}'s response to a variety of bad inputs.
 */
public class HttpSolrClientBadInputTest extends SolrJettyTestBase {
  private static final List<String> NULL_STR_LIST = null;
  private static final List<String> EMPTY_STR_LIST = new ArrayList<>();
  private static final String ANY_COLLECTION = "ANY_COLLECTION";
  private static final int ANY_COMMIT_WITHIN_TIME = -1;

  @BeforeClass
  public static void beforeTest() throws Exception {
    JettyConfig jettyConfig = JettyConfig.builder()
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build();
    createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);
  }

  private void assertExceptionThrownWithMessageContaining(Class expectedType, List<String> expectedStrings, ThrowingRunnable runnable) {
    Throwable thrown = expectThrows(expectedType, runnable);

    if (expectedStrings != null) {
      for (String expectedString : expectedStrings) {
        assertThat(thrown.getMessage(), containsString(expectedString));
      }
    }
  }

  @Test
  public void testDeleteByIdReportsInvalidIdLists() throws Exception {
    try (SolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/" + ANY_COLLECTION)) {
      assertExceptionThrownWithMessageContaining(IllegalArgumentException.class, Lists.newArrayList("ids", "null"), () -> {
        client.deleteById(NULL_STR_LIST);
      });
      assertExceptionThrownWithMessageContaining(IllegalArgumentException.class, Lists.newArrayList("ids", "empty"), () -> {
        client.deleteById(EMPTY_STR_LIST);
      });
      assertExceptionThrownWithMessageContaining(IllegalArgumentException.class, Lists.newArrayList("ids", "null"), () -> {
        client.deleteById(NULL_STR_LIST, ANY_COMMIT_WITHIN_TIME);
      });
      assertExceptionThrownWithMessageContaining(IllegalArgumentException.class, Lists.newArrayList("ids", "empty"), () -> {
        client.deleteById(EMPTY_STR_LIST, ANY_COMMIT_WITHIN_TIME);
      });
    }

    try (SolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString())) {
      assertExceptionThrownWithMessageContaining(IllegalArgumentException.class, Lists.newArrayList("ids", "null"), () -> {
        client.deleteById(ANY_COLLECTION, NULL_STR_LIST);
      });
      assertExceptionThrownWithMessageContaining(IllegalArgumentException.class, Lists.newArrayList("ids", "empty"), () -> {
        client.deleteById(ANY_COLLECTION, EMPTY_STR_LIST);
      });
      assertExceptionThrownWithMessageContaining(IllegalArgumentException.class, Lists.newArrayList("ids", "null"), () -> {
        client.deleteById(ANY_COLLECTION, NULL_STR_LIST, ANY_COMMIT_WITHIN_TIME);
      });
      assertExceptionThrownWithMessageContaining(IllegalArgumentException.class, Lists.newArrayList("ids", "empty"), () -> {
        client.deleteById(ANY_COLLECTION, EMPTY_STR_LIST, ANY_COMMIT_WITHIN_TIME);
      });
    }
  }



}
