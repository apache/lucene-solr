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

package org.apache.solr.client.solrj.request.json;

import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;

public class DomainMapTest extends SolrTestCaseJ4 {

  @Test
  public void testRejectsInvalidFilters() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new DomainMap()
          .withFilter(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresFilterWithCorrectKey() {
    final DomainMap domain = new DomainMap()
        .withFilter("name:Solr");
    @SuppressWarnings({"unchecked"})
    final List<String> filterList = (List<String>) domain.get("filter");

    assertTrue("Expected filter list to contain provided filter", filterList.contains("name:Solr"));
  }

  @Test
  public void testStoresMultipleFilters() {
    final DomainMap domain = new DomainMap()
        .withFilter("name:Solr")
        .withFilter("cat:search");
    @SuppressWarnings({"unchecked"})
    final List<String> filterList = (List<String>) domain.get("filter");

    assertTrue("Expected filter list to contain 1st provided filter", filterList.contains("name:Solr"));
    assertTrue("Expected filter list to contain 2nd provided filter", filterList.contains("cat:search"));
  }

  @Test
  public void testRejectsInvalidQueries() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new DomainMap()
          .withQuery(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresQueryWithCorrectKey() {
    final DomainMap domain = new DomainMap()
        .withQuery("name:Solr");
    @SuppressWarnings({"unchecked"})
    final List<String> queryList = (List<String>) domain.get("query");

    assertTrue("Expected query list to contain provided query", queryList.contains("name:Solr"));
  }

  @Test
  public void testStoresMultipleQueries() {
    final DomainMap domain = new DomainMap()
        .withQuery("name:Solr")
        .withQuery("cat:search");
    @SuppressWarnings({"unchecked"})
    final List<String> queryList = (List<String>) domain.get("query");

    assertTrue("Expected query list to contain 1st provided query", queryList.contains("name:Solr"));
    assertTrue("Expected query list to contain 2nd provided query", queryList.contains("cat:search"));
  }

  @Test
  public void testRejectsInvalidTagsToExclude() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new DomainMap()
          .withTagsToExclude(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresTagsToExcludeWithCorrectKey() {
    final DomainMap domain = new DomainMap()
        .withTagsToExclude("BRAND");
    @SuppressWarnings({"unchecked"})
    final List<String> exclusionList = (List<String>) domain.get("excludeTags");

    assertTrue("Expected tag-exclusion list to contain provided tag", exclusionList.contains("BRAND"));
  }

  @Test
  public void testStoresMultipleTagExclusionStrings() {
    final DomainMap domain = new DomainMap()
        .withTagsToExclude("BRAND")
        .withTagsToExclude("COLOR");
    @SuppressWarnings({"unchecked"})
    final List<String> exclusionList = (List<String>) domain.get("excludeTags");

    assertTrue("Expected tag-exclusion list to contain provided 1st tag", exclusionList.contains("BRAND"));
    assertTrue("Expected tag-exclusion list to contain provided 2nd tag", exclusionList.contains("COLOR"));
  }

  @Test
  public void testRejectsInvalidBlockParentQuery() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new DomainMap()
          .setBlockParentQuery(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresBlockParentQueryWithCorrectKey() {
    final DomainMap domain = new DomainMap()
        .setBlockParentQuery("content_type:product");
    assertEquals("content_type:product", domain.get("blockParent"));
  }

  @Test
  public void testRejectsInvalidBlockChildrenQuery() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new DomainMap()
          .setBlockChildQuery(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresBlockChildrenQueryWithCorrectKey() {
    final DomainMap domain = new DomainMap()
        .setBlockChildQuery("content_type:productColors");
    assertEquals("content_type:productColors", domain.get("blockChildren"));
  }

  @Test
  public void testRejectsInvalidJoinFromParam() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new DomainMap()
          .setJoinTransformation(null, "valid-to-field");
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testRejectsInvalidJoinToParam() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new DomainMap()
          .setJoinTransformation("valid-from-field", null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresJoinValuesWithCorrectKey() {
    final DomainMap domain = new DomainMap()
        .setJoinTransformation("any-from-field", "any-to-field");

    assertTrue(domain.containsKey("join"));
    @SuppressWarnings({"unchecked"})
    final Map<String, Object> joinParams = (Map<String, Object>) domain.get("join");
    assertEquals("any-from-field", joinParams.get("from"));
    assertEquals("any-to-field", joinParams.get("to"));
  }
}
