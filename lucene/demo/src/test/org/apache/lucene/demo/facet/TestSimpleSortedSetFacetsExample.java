package org.apache.lucene.demo.facet;

import java.util.List;

import org.apache.lucene.facet.collections.ObjectToIntMap;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

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

public class TestSimpleSortedSetFacetsExample extends LuceneTestCase {

  private static final ObjectToIntMap<CategoryPath> expectedCounts = new ObjectToIntMap<CategoryPath>();
  static {
    expectedCounts.put(new CategoryPath("Publish Year", "2012"), 2);
    expectedCounts.put(new CategoryPath("Publish Year", "2010"), 2);
    expectedCounts.put(new CategoryPath("Publish Year", "1999"), 1);
    expectedCounts.put(new CategoryPath("Author", "Lisa"), 2);
    expectedCounts.put(new CategoryPath("Author", "Frank"), 1);
    expectedCounts.put(new CategoryPath("Author", "Susan"), 1);
    expectedCounts.put(new CategoryPath("Author", "Bob"), 1);
  }
  
  private static final ObjectToIntMap<CategoryPath> expectedCountsDrillDown = new ObjectToIntMap<CategoryPath>();
  static {
    expectedCountsDrillDown.put(new CategoryPath("Author", "Lisa"), 1);
    expectedCountsDrillDown.put(new CategoryPath("Author", "Bob"), 1);
  }
  
  private void assertExpectedCounts(List<FacetResult> facetResults, ObjectToIntMap<CategoryPath> expCounts) {
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      for (FacetResultNode node : root.subResults) {
        assertEquals("incorrect count for " + node.label, expCounts.get(node.label), (int) node.value);
      }
    }
  }
  
  @Test
  public void testSimple() throws Exception {
    assumeTrue("Test requires SortedSetDV support", defaultCodecSupportsSortedSet());
    List<FacetResult> facetResults = new SimpleSortedSetFacetsExample().runSearch();
    assertEquals(2, facetResults.size());
    assertExpectedCounts(facetResults, expectedCounts);
  }

  @Test
  public void testDrillDown() throws Exception {
    assumeTrue("Test requires SortedSetDV support", defaultCodecSupportsSortedSet());
    List<FacetResult> facetResults = new SimpleSortedSetFacetsExample().runDrillDown();
    assertEquals(1, facetResults.size());
    assertExpectedCounts(facetResults, expectedCountsDrillDown);
  }
  
}
