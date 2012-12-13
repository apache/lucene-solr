package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.ParallelAtomicReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.lucene.facet.FacetTestBase;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.ScoredDocIdCollector;
import org.apache.lucene.facet.search.StandardFacetsAccumulator;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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

/**
 * Test that complementsworks as expected.
 * We place this test under *.facet.search rather than *.search
 * because the test actually does faceted search.
 */
public class TestFacetsAccumulatorWithComplement extends FacetTestBase {
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    initIndex();
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    closeAll();
    super.tearDown();
  }
  
  /**
   * Test that complements does not cause a failure when using a parallel reader
   */
  @Test
  public void testComplementsWithParallerReader() throws Exception {
    IndexReader origReader = indexReader; 
    ParallelAtomicReader pr = new ParallelAtomicReader(SlowCompositeReaderWrapper.wrap(origReader));
    indexReader = pr;
    try {
      doTestComplements();
    } finally {
      indexReader = origReader;
    }
  }

  /**
   * Test that complements works with MultiReader
   */
  @Test
  public void testComplementsWithMultiReader() throws Exception {
    final IndexReader origReader = indexReader; 
    indexReader = new MultiReader(origReader);
    try {
      doTestComplements();
    } finally {
      indexReader = origReader;
    }
  }
  
  /**
   * Test that score is indeed constant when using a constant score
   */
  @Test
  public void testComplements() throws Exception {
    doTestComplements();
  }
  
  private void doTestComplements() throws Exception {
    Query q = new MatchAllDocsQuery(); //new TermQuery(new Term(TEXT,"white"));
    if (VERBOSE) {
      System.out.println("Query: "+q);
    }
    ScoredDocIdCollector dCollector = 
      ScoredDocIdCollector.create(indexReader.maxDoc(),false); // scoring is disabled
    searcher.search(q, dCollector);
    
    // verify by facet values
    List<FacetResult> countResWithComplement = findFacets(dCollector.getScoredDocIDs(), true);
    List<FacetResult> countResNoComplement = findFacets(dCollector.getScoredDocIDs(), false);
    
    assertEquals("Wrong number of facet count results with complement!",1,countResWithComplement.size());
    assertEquals("Wrong number of facet count results no complement!",1,countResNoComplement.size());
    
    FacetResultNode parentResWithComp = countResWithComplement.get(0).getFacetResultNode();
    FacetResultNode parentResNoComp = countResWithComplement.get(0).getFacetResultNode();
    
    assertEquals("Wrong number of top count aggregated categories with complement!",3,parentResWithComp.getNumSubResults());
    assertEquals("Wrong number of top count aggregated categories no complement!",3,parentResNoComp.getNumSubResults());
    
  }
  
  private FacetSearchParams getFacetSearchParams() {
    return new FacetSearchParams(new CountFacetRequest(new CategoryPath("root","a"), 10));
  }
  
  /** compute facets with certain facet requests and docs */
  private List<FacetResult> findFacets(ScoredDocIDs sDocids, boolean withComplement) throws IOException {
    
    FacetsAccumulator fAccumulator = 
      new StandardFacetsAccumulator(getFacetSearchParams(), indexReader, taxoReader);
    
    fAccumulator.setComplementThreshold(
        withComplement ? 
            FacetsAccumulator.FORCE_COMPLEMENT: 
              FacetsAccumulator.DISABLE_COMPLEMENT);
    
    List<FacetResult> res = fAccumulator.accumulate(sDocids);
    
    // Results are ready, printing them...
    int i = 0;
    for (FacetResult facetResult : res) {
      if (VERBOSE) {
        System.out.println("Res "+(i++)+": "+facetResult);
      }
    }
    
    assertEquals(withComplement, ((StandardFacetsAccumulator) fAccumulator).isUsingComplements);
    
    return res;
  }
  
}