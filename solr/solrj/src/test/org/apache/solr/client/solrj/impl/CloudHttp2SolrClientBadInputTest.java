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
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;

public class CloudHttp2SolrClientBadInputTest extends SolrCloudTestCase {
  private static final List<String> NULL_STR_LIST = null;
  private static final List<String> EMPTY_STR_LIST = new ArrayList<>();
  private static final String ANY_COLLECTION = "ANY_COLLECTION";
  private static final int ANY_COMMIT_WITHIN_TIME = -1;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .configure();

    final List<String> solrUrls = new ArrayList<>();
  }

  @Test
  public void testDeleteByIdReportsInvalidIdLists() throws Exception {
    try (SolrClient client = getCloudHttp2SolrClient(cluster)) {
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

  private void assertExceptionThrownWithMessageContaining(@SuppressWarnings({"rawtypes"})Class expectedType,
                                                          List<String> expectedStrings, LuceneTestCase.ThrowingRunnable runnable) {
    @SuppressWarnings({"unchecked"})
    Throwable thrown = expectThrows(expectedType, runnable);

    if (expectedStrings != null) {
      for (String expectedString : expectedStrings) {
        assertThat(thrown.getMessage(), containsString(expectedString));
      }
    }
  }
}