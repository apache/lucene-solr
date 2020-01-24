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

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;
import static org.hamcrest.core.StringContains.containsString;


public class TermsFacetMapTest extends SolrTestCaseJ4 {
  private static final String ANY_FIELD_NAME = "ANY_FIELD_NAME";

  @Test
  public void testSetsFacetTypeToTerm() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME);
    assertEquals("terms", termsFacet.get("type"));
  }

  @Test
  public void testStoresFieldWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME);
    assertEquals(ANY_FIELD_NAME, termsFacet.get("field"));
  }

  @Test
  public void testRejectsNegativeBucketOffset() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .setBucketOffset(-1);
    });
    assertThat(thrown.getMessage(), containsString("must be non-negative"));
  }

  @Test
  public void testStoresBucketOffsetWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .setBucketOffset(2);
    assertEquals(2, termsFacet.get("offset"));

  }

  @Test
  public void testStoresBucketLimitWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .setLimit(3);
    assertEquals(3, termsFacet.get("limit"));
  }

  @Test
  public void testRejectsInvalidSortString() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
          .setSort(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresSortWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .setSort("price asc");
    assertEquals("price asc", termsFacet.get("sort"));
  }

  @Test
  public void testRejectInvalidOverRequestBuckets() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
          .setOverRequest(-2);
    });
    assertThat(thrown.getMessage(), containsString("must be >= -1"));
  }

  @Test
  public void testStoresOverRequestBucketsWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .setOverRequest(4);
    assertEquals(4, termsFacet.get("overrequest"));
  }

  @Test
  public void testStoresRefinementFlagWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .useDistributedFacetRefining(true);
    assertEquals(true, termsFacet.get("refine"));
  }

  @Test
  public void testRejectInvalidOverRefineBuckets() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
          .setOverRefine(-2);
    });
    assertThat(thrown.getMessage(), containsString("must be >= -1"));
  }

  @Test
  public void testStoresOverRefineBucketsWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .setOverRefine(5);
    assertEquals(5, termsFacet.get("overrefine"));
  }

  @Test
  public void testRejectInvalidMinCount() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
          .setMinCount(-1);
    });
    assertThat(thrown.getMessage(), containsString("must be a non-negative integer"));
  }

  @Test
  public void testStoresMinCountWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .setMinCount(6);
    assertEquals(6, termsFacet.get("mincount"));
    termsFacet.setMinCount(0);
    assertEquals(0, termsFacet.get("mincount"));
  }

  @Test
  public void testStoresNumBucketsFlagWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .includeTotalNumBuckets(true);
    assertEquals(true, termsFacet.get("numBuckets"));
  }

  @Test
  public void testStoresAllBucketsFlagWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .includeAllBucketsUnionBucket(true);
    assertEquals(true, termsFacet.get("allBuckets"));
  }

  @Test
  public void testRejectInvalidTermPrefix() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
          .setTermPrefix(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresTermPrefixWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .setTermPrefix("ANY_PREF");
    assertEquals("ANY_PREF", termsFacet.get("prefix"));
  }

  @Test
  public void testRejectsInvalidMethod() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
          .setFacetMethod(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresMethodWithCorrectKey() {
    final TermsFacetMap termsFacet = new TermsFacetMap(ANY_FIELD_NAME)
        .setFacetMethod(TermsFacetMap.FacetMethod.STREAM);
    assertEquals("stream", termsFacet.get("method"));
  }
}
