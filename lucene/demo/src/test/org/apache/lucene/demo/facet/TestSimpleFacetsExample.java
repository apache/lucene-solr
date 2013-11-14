package org.apache.lucene.demo.facet;

import java.util.List;

import org.apache.lucene.facet.collections.ObjectToIntMap;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.taxonomy.FacetLabel;
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

public class TestSimpleFacetsExample extends LuceneTestCase {

  private static final ObjectToIntMap<FacetLabel> expectedCounts = new ObjectToIntMap<FacetLabel>();
  static {
    expectedCounts.put(new FacetLabel("Publish Date", "2012"), 2);
    expectedCounts.put(new FacetLabel("Publish Date", "2010"), 2);
    expectedCounts.put(new FacetLabel("Publish Date", "1999"), 1);
    expectedCounts.put(new FacetLabel("Author", "Lisa"), 2);
    expectedCounts.put(new FacetLabel("Author", "Frank"), 1);
    expectedCounts.put(new FacetLabel("Author", "Susan"), 1);
    expectedCounts.put(new FacetLabel("Author", "Bob"), 1);
  }
  
  private static final ObjectToIntMap<FacetLabel> expectedCountsDrillDown = new ObjectToIntMap<FacetLabel>();
  static {
    expectedCountsDrillDown.put(new FacetLabel("Author", "Lisa"), 1);
    expectedCountsDrillDown.put(new FacetLabel("Author", "Bob"), 1);
  }
  
  private void assertExpectedCounts(List<FacetResult> facetResults, ObjectToIntMap<FacetLabel> expCounts) {
    for (FacetResult res : facetResults) {
      FacetResultNode root = res.getFacetResultNode();
      for (FacetResultNode node : root.subResults) {
        assertEquals("incorrect count for " + node.label, expCounts.get(node.label), (int) node.value);
      }
    }
  }
  
  @Test
  public void testSimple() throws Exception {
    List<FacetResult> facetResults = new SimpleFacetsExample().runSearch();
    assertEquals(2, facetResults.size());
    assertExpectedCounts(facetResults, expectedCounts);
  }

  @Test
  public void testDrillDown() throws Exception {
    List<FacetResult> facetResults = new SimpleFacetsExample().runDrillDown();
    assertEquals(1, facetResults.size());
    assertExpectedCounts(facetResults, expectedCountsDrillDown);
  }
  
}
