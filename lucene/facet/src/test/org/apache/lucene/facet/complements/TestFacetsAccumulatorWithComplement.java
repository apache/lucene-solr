package org.apache.lucene.facet.complements;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.facet.FacetTestBase;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.StandardFacetsAccumulator;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.ParallelAtomicReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.junit.After;
import org.junit.Before;
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

public class TestFacetsAccumulatorWithComplement extends FacetTestBase {
  
  private FacetIndexingParams fip;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    fip = getFacetIndexingParams(Integer.MAX_VALUE);
    initIndex(fip);
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
    // verify by facet values
    List<FacetResult> countResWithComplement = findFacets(true);
    List<FacetResult> countResNoComplement = findFacets(false);
    
    assertEquals("Wrong number of facet count results with complement!",1,countResWithComplement.size());
    assertEquals("Wrong number of facet count results no complement!",1,countResNoComplement.size());
    
    FacetResultNode parentResWithComp = countResWithComplement.get(0).getFacetResultNode();
    FacetResultNode parentResNoComp = countResWithComplement.get(0).getFacetResultNode();
    
    assertEquals("Wrong number of top count aggregated categories with complement!",3,parentResWithComp.subResults.size());
    assertEquals("Wrong number of top count aggregated categories no complement!",3,parentResNoComp.subResults.size());
  }
  
  /** compute facets with certain facet requests and docs */
  private List<FacetResult> findFacets(boolean withComplement) throws IOException {
    FacetSearchParams fsp = new FacetSearchParams(fip, new CountFacetRequest(new CategoryPath("root","a"), 10));
    StandardFacetsAccumulator sfa = new StandardFacetsAccumulator(fsp, indexReader, taxoReader);
    sfa.setComplementThreshold(withComplement ? StandardFacetsAccumulator.FORCE_COMPLEMENT : StandardFacetsAccumulator.DISABLE_COMPLEMENT);
    FacetsCollector fc = FacetsCollector.create(sfa);
    searcher.search(new MatchAllDocsQuery(), fc);
    
    List<FacetResult> res = fc.getFacetResults();
    
    // Results are ready, printing them...
    int i = 0;
    for (FacetResult facetResult : res) {
      if (VERBOSE) {
        System.out.println("Res "+(i++)+": "+facetResult);
      }
    }
    
    assertEquals(withComplement, sfa.isUsingComplements());
    
    return res;
  }
  
}
