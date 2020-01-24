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

/**
 * Unit tests for {@link RangeFacetMap}
 */
public class RangeFacetMapTest extends SolrTestCaseJ4 {
  @Test
  public void testRejectsInvalidFieldName() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new RangeFacetMap(null, 1, 2, 3);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testRejectsInvalidStartEndBounds() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new RangeFacetMap("ANY_FIELD_NAME", 1, -1, 3);
    });
    assertThat(thrown.getMessage(), containsString("'end' must be greater than parameter 'start'"));
  }

  @Test
  public void testRejectsInvalidGap() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new RangeFacetMap("ANY_FIELD_NAME", 1, 2, -1);
    });
    assertThat(thrown.getMessage(), containsString("must be a positive integer"));
  }

  @Test
  public void testStoresRequiredValuesWithCorrectKeys() {
    final RangeFacetMap rangeFacet = new RangeFacetMap("ANY_FIELD_NAME", 1, 2, 3);
    assertEquals("ANY_FIELD_NAME", rangeFacet.get("field"));
    assertEquals(1L, rangeFacet.get("start"));
    assertEquals(2L, rangeFacet.get("end"));
    assertEquals(3L, rangeFacet.get("gap"));
  }

  @Test
  public void testStoresHardEndWithCorrectKey() {
    final RangeFacetMap rangeFacet = new RangeFacetMap("ANY_FIELD_NAME", 1, 2, 3)
        .setHardEnd(true);
    assertEquals(true, rangeFacet.get("hardend"));
  }

  @Test
  public void testRejectsInvalidOtherBuckets() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new RangeFacetMap("ANY_FIELD_NAME", 1, 2, 3)
          .setOtherBuckets(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresOtherBucketsValueWithCorrectKey() {
    final RangeFacetMap rangeFacet = new RangeFacetMap("ANY_FIELD_NAME", 1, 2, 3)
        .setOtherBuckets(RangeFacetMap.OtherBuckets.BETWEEN);
    assertEquals("between", rangeFacet.get("other"));
  }
}
