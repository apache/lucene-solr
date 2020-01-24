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


public class HeatmapFacetMapTest extends SolrTestCaseJ4 {

  @Test
  public void testRejectsInvalidFieldName() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new HeatmapFacetMap(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresFieldNameWithCorrectKey() {
    final HeatmapFacetMap heatmapFacet = new HeatmapFacetMap("ANY_FIELD_NAME");
    assertEquals("ANY_FIELD_NAME", heatmapFacet.get("field"));
  }

  @Test
  public void testDoesntSupportSubfacets() {
    final Throwable thrown = expectThrows(UnsupportedOperationException.class, () -> {
      new HeatmapFacetMap("ANY_FIELD_NAME")
          .withSubFacet("ANY_NAME", new TermsFacetMap("ANY_OTHER_FIELD_NAME"));
    });
    assertThat(thrown.getMessage(), containsString("doesn't currently support subfacets"));
  }

  @Test
  public void testRejectsInvalidRegionQueries() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new HeatmapFacetMap("ANY_FIELD_NAME")
          .setRegionQuery(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresRegionQueryWithCorrectKey() {
    final HeatmapFacetMap heatmapFacet = new HeatmapFacetMap("ANY_FIELD_NAME")
        .setRegionQuery("[-120,-35 TO 50,60]");
    assertEquals("[-120,-35 TO 50,60]", heatmapFacet.get("geom"));
  }

  @Test
  public void testRejectsInvalidCellSize() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new HeatmapFacetMap("ANY_FIELD_NAME")
          .setGridLevel(0);
    });
    assertThat(thrown.getMessage(), containsString("must be a positive integer"));
  }

  @Test
  public void testStoresCellSizeWithCorrectKey() {
    final HeatmapFacetMap heatmapFacet = new HeatmapFacetMap("ANY_FIELD_NAME")
        .setGridLevel(42);
    assertEquals(42, heatmapFacet.get("gridLevel"));
  }

  @Test
  public void testRejectsInvalidDistanceError() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new HeatmapFacetMap("ANY_FIELD_NAME")
          .setDistErr(-1.0);
    });
    assertThat(thrown.getMessage(), containsString("must be non-negative"));
  }

  @Test
  public void testStoresDistanceErrorWithCorrectKey() {
    final HeatmapFacetMap heatmapFacet = new HeatmapFacetMap("ANY_FIELD_NAME")
        .setDistErr(4.5);
    assertEquals(4.5, heatmapFacet.get("distErr"));
  }

  @Test
  public void testRejectsInvalidDistanceErrorPercentageWithCorrectKey() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new HeatmapFacetMap("ANY_FIELD_NAME")
          .setDistErrPct(2.0);
    });
    assertThat(thrown.getMessage(), containsString("must be between 0.0 and 1.0"));
  }

  @Test
  public void testStoresDistanceErrorPercentageWithCorrectKey() {
    final HeatmapFacetMap heatmapFacet = new HeatmapFacetMap("ANY_FIELD_NAME")
        .setDistErrPct(0.45);
    assertEquals(0.45, heatmapFacet.get("distErrPct"));
  }

  @Test
  public void testRejectsInvalidHeatmapFormat() {
    final Throwable thrown = expectThrows(IllegalArgumentException.class, () -> {
      new HeatmapFacetMap("ANY_FIELD_NAME")
          .setHeatmapFormat(null);
    });
    assertThat(thrown.getMessage(), containsString("must be non-null"));
  }

  @Test
  public void testStoresHeatmapFormatWithCorrectKey() {
    final HeatmapFacetMap heatmapFacet = new HeatmapFacetMap("ANY_FIELD_NAME")
        .setHeatmapFormat(HeatmapFacetMap.HeatmapFormat.PNG);
    assertEquals("png", heatmapFacet.get("format"));
  }
}
