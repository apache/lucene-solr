package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.MatchAllDocsQuery;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.lucene.facet.FacetTestBase;
import org.apache.lucene.facet.index.params.CategoryListParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.cache.CategoryListCache;
import org.apache.lucene.facet.search.cache.CategoryListData;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
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

public class TestCategoryListCache extends FacetTestBase {

  public TestCategoryListCache() {
    super();
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    initIndex();
  }
  
  @After
  @Override
  public void tearDown() throws Exception {
    closeAll();
    super.tearDown();
  }
  
  @Test
  public void testNoClCache() throws Exception {
    doTest(false,false);
  }

  @Test
  public void testCorrectClCache() throws Exception {
    doTest(true,false);
  }
  
  @Test
  public void testWrongClCache() throws Exception {
    doTest(true,true);
  }
  
  private void doTest(boolean withCache, boolean plantWrongData) throws Exception {
    Map<CategoryPath,Integer> truth = facetCountsTruth();
    CategoryPath cp = (CategoryPath) truth.keySet().toArray()[0]; // any category path will do for this test 
    CountFacetRequest frq = new CountFacetRequest(cp, 10);
    FacetSearchParams sParams = getFacetedSearchParams();
    sParams.addFacetRequest(frq);
    if (withCache) {
      //let's use a cached cl data
      FacetIndexingParams iparams = sParams.getFacetIndexingParams();
      CategoryListParams clp = new CategoryListParams(); // default term ok as only single list
      CategoryListCache clCache = new CategoryListCache();
      clCache.loadAndRegister(clp, indexReader, taxoReader, iparams);
      if (plantWrongData) {
        // let's mess up the cached data and then expect a wrong result...
        messCachedData(clCache, clp);
      }
      sParams.setClCache(clCache);
    }
    FacetsCollector fc = new FacetsCollector(sParams, indexReader, taxoReader);
    searcher.search(new MatchAllDocsQuery(), fc);
    List<FacetResult> res = fc.getFacetResults();
    try {
      assertCountsAndCardinality(truth, res);
      assertFalse("Correct results not expected when wrong data was cached", plantWrongData);
    } catch (Throwable e) {
      assertTrue("Wrong results not expected unless wrong data was cached", withCache);
      assertTrue("Wrong results not expected unless wrong data was cached", plantWrongData);
    }
  }

  /** Mess the cached data for this {@link CategoryListParams} */
  private void messCachedData(CategoryListCache clCache, CategoryListParams clp) {
    final CategoryListData cld = clCache.get(clp);
    CategoryListData badCld = new CategoryListData() {
      @Override
      public CategoryListIterator iterator(int partition)  throws IOException {
        final CategoryListIterator it = cld.iterator(partition);
        return new CategoryListIterator() {              
          public boolean skipTo(int docId) throws IOException {
            return it.skipTo(docId);
          }
          public long nextCategory() throws IOException {
            long res = it.nextCategory();
            if (res>Integer.MAX_VALUE) {
              return res;
            }
            return res>1 ? res-1 : res+1;
          }
          public boolean init() throws IOException {
            return it.init();
          }
        };
      }
    };
    clCache.register(clp, badCld);
  }
  
}
