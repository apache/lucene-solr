package org.apache.lucene.demo.facet;

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

import java.util.List;

import org.apache.lucene.facet.collections.ObjectToIntMap;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.RangeFacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

@SuppressCodecs("Lucene3x")
public class TestRangeFacetsExample extends LuceneTestCase {

  private static final ObjectToIntMap<FacetLabel> expectedCounts = new ObjectToIntMap<FacetLabel>();
  static {
    expectedCounts.put(new FacetLabel("timestamp", "Past hour"), 4);
    expectedCounts.put(new FacetLabel("timestamp", "Past six hours"), 22);
    expectedCounts.put(new FacetLabel("timestamp", "Past day"), 87);
  }
  
  private void assertExpectedCounts(FacetResult res, ObjectToIntMap<FacetLabel> expCounts) {
    FacetResultNode root = res.getFacetResultNode();
    for (FacetResultNode node : root.subResults) {
      assertEquals("incorrect count for " + node.label, expCounts.get(node.label), (int) node.value);
    }
  }
  
  @Test
  public void testSimple() throws Exception {
    RangeFacetsExample example = new RangeFacetsExample();
    example.index();
    List<FacetResult> facetResults = example.search();
    assertEquals(1, facetResults.size());
    assertExpectedCounts(facetResults.get(0), expectedCounts);
    example.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDrillDown() throws Exception {
    RangeFacetsExample example = new RangeFacetsExample();
    example.index();
    List<FacetResult> facetResults = example.search();
    TopDocs hits = example.drillDown((LongRange) ((RangeFacetRequest<LongRange>) facetResults.get(0).getFacetRequest()).ranges[1]);
    assertEquals(22, hits.totalHits);
    example.close();
  }
}
