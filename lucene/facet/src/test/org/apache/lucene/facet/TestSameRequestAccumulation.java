package org.apache.lucene.facet;

import java.util.List;

import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.After;
import org.junit.Before;

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

public class TestSameRequestAccumulation extends FacetTestBase {
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    initIndex();
  }
  
  // Following LUCENE-4461 - ensure requesting the (exact) same request more
  // than once does not alter the results
  public void testTwoSameRequests() throws Exception {
    final CountFacetRequest facetRequest = new CountFacetRequest(new CategoryPath("root"), 10);
    FacetSearchParams fsp = new FacetSearchParams(facetRequest);
    
    FacetsCollector fc = new FacetsCollector(fsp, indexReader, taxoReader);
    searcher.search(new MatchAllDocsQuery(), fc);
    
    final String expected = fc.getFacetResults().get(0).toString();

    // now add the same facet request with duplicates (same instance and same one)
    fsp = new FacetSearchParams(facetRequest, facetRequest, new CountFacetRequest(new CategoryPath("root"), 10));

    // make sure the search params holds 3 requests now
    assertEquals(3, fsp.getFacetRequests().size());
    
    fc = new FacetsCollector(fsp, indexReader, taxoReader);
    searcher.search(new MatchAllDocsQuery(), fc);
    List<FacetResult> actual = fc.getFacetResults();

    // all 3 results should have the same toString()
    assertEquals("same FacetRequest but different result?", expected, actual.get(0).toString());
    assertEquals("same FacetRequest but different result?", expected, actual.get(1).toString());
    assertEquals("same FacetRequest but different result?", expected, actual.get(2).toString());
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    closeAll();
    super.tearDown();
  }
}
