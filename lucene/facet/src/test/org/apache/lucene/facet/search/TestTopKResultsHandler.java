package org.apache.lucene.facet.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.search.FacetRequest.ResultMode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.search.MatchAllDocsQuery;
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

public class TestTopKResultsHandler extends BaseTestTopK {

  private static final CategoryPath[] CATEGORIES = {
    new CategoryPath( "a", "b"),
    new CategoryPath( "a", "b", "1"),
    new CategoryPath( "a", "b", "1"),
    new CategoryPath( "a", "b", "2"),
    new CategoryPath( "a", "b", "2"),
    new CategoryPath( "a", "b", "3"),
    new CategoryPath( "a", "b", "4"),
    new CategoryPath( "a", "c"),
    new CategoryPath( "a", "c"),
    new CategoryPath( "a", "c"),
    new CategoryPath( "a", "c"),
    new CategoryPath( "a", "c"),
    new CategoryPath( "a", "c", "1"),
  };

  @Override
  protected String getContent(int doc) {
    return ALPHA;
  }
  
  @Override
  protected int numDocsToIndex() {
    return CATEGORIES.length;
  }
  
  @Override
  protected List<CategoryPath> getCategories(int doc) {
    return Arrays.asList(CATEGORIES[doc]);
  }
  
  /**
   * Straightforward test: Adding specific documents with specific facets and
   * counting them in the most basic form.
   */
  @Test
  public void testSimple() throws Exception {
    for (int partitionSize : partitionSizes) {
      FacetIndexingParams fip = getFacetIndexingParams(partitionSize);
      OrdinalPolicy op = fip.getCategoryListParams(null).getOrdinalPolicy(null);
      initIndex(fip);

      List<FacetRequest> facetRequests = new ArrayList<FacetRequest>();
      facetRequests.add(new CountFacetRequest(new CategoryPath("a"), 100));
      CountFacetRequest cfra = new CountFacetRequest(new CategoryPath("a"), 100);
      cfra.setDepth(3);
      // makes it easier to check the results in the test.
      cfra.setResultMode(ResultMode.GLOBAL_FLAT);
      facetRequests.add(cfra);
      facetRequests.add(new CountFacetRequest(new CategoryPath("a", "b"), 100));
      facetRequests.add(new CountFacetRequest(new CategoryPath("a", "b", "1"), 100));
      facetRequests.add(new CountFacetRequest(new CategoryPath("a", "c"), 100));
      
      // do different facet counts and compare to control
      FacetSearchParams sParams = getFacetSearchParams(facetRequests, fip);
      FacetsCollector fc = FacetsCollector.create(sParams, indexReader, taxoReader);
      
      searcher.search(new MatchAllDocsQuery(), fc);
      List<FacetResult> facetResults = fc.getFacetResults();
      
      FacetResult fr = facetResults.get(0);
      FacetResultNode parentRes = fr.getFacetResultNode();
      if (op == OrdinalPolicy.ALL_PARENTS) {
        assertEquals(13.0, parentRes.value, Double.MIN_VALUE);
      }
      FacetResultNode[] frn = resultNodesAsArray(parentRes);
      assertEquals(7.0, frn[0].value, Double.MIN_VALUE);
      assertEquals(6.0, frn[1].value, Double.MIN_VALUE);

      fr = facetResults.get(1);
      parentRes = fr.getFacetResultNode();
      if (op == OrdinalPolicy.ALL_PARENTS) {
        assertEquals(13.0, parentRes.value, Double.MIN_VALUE);
      }
      frn = resultNodesAsArray(parentRes);
      assertEquals(7.0, frn[0].value, Double.MIN_VALUE);
      assertEquals(6.0, frn[1].value, Double.MIN_VALUE);
      assertEquals(2.0, frn[2].value, Double.MIN_VALUE);
      assertEquals(2.0, frn[3].value, Double.MIN_VALUE);
      assertEquals(1.0, frn[4].value, Double.MIN_VALUE);
      assertEquals(1.0, frn[5].value, Double.MIN_VALUE);

      fr = facetResults.get(2);
      parentRes = fr.getFacetResultNode();
      if (op == OrdinalPolicy.ALL_PARENTS) {
        assertEquals(7.0, parentRes.value, Double.MIN_VALUE);
      }
      frn = resultNodesAsArray(parentRes);
      assertEquals(2.0, frn[0].value, Double.MIN_VALUE);
      assertEquals(2.0, frn[1].value, Double.MIN_VALUE);
      assertEquals(1.0, frn[2].value, Double.MIN_VALUE);
      assertEquals(1.0, frn[3].value, Double.MIN_VALUE);

      fr = facetResults.get(3);
      parentRes = fr.getFacetResultNode();
      if (op == OrdinalPolicy.ALL_PARENTS) {
        assertEquals(2.0, parentRes.value, Double.MIN_VALUE);
      }
      frn = resultNodesAsArray(parentRes);
      assertEquals(0, frn.length);

      fr = facetResults.get(4);
      parentRes = fr.getFacetResultNode();
      if (op == OrdinalPolicy.ALL_PARENTS) {
        assertEquals(6.0, parentRes.value, Double.MIN_VALUE);
      }
      frn = resultNodesAsArray(parentRes);
      assertEquals(1.0, frn[0].value, Double.MIN_VALUE);
      closeAll();
    }
  }
  
  /**
   * Creating an index, matching the results of an top K = Integer.MAX_VALUE and top-1000 requests
   */
  @Test
  public void testGetMaxIntFacets() throws Exception {
    for (int partitionSize : partitionSizes) {
      FacetIndexingParams fip = getFacetIndexingParams(partitionSize);
      initIndex(fip);

      // do different facet counts and compare to control
      CategoryPath path = new CategoryPath("a", "b");
      FacetSearchParams sParams = getFacetSearchParams(fip, new CountFacetRequest(path, Integer.MAX_VALUE));
      FacetsCollector fc = FacetsCollector.create(sParams, indexReader, taxoReader);
      
      searcher.search(new MatchAllDocsQuery(), fc);
      List<FacetResult> results = fc.getFacetResults();

      assertEquals("Should only be one result as there's only one request", 1, results.size());
      FacetResult res = results.get(0);
      assertEquals(path + " should only have 4 desendants", 4, res.getNumValidDescendants());

      // As a control base results, ask for top-1000 results
      FacetSearchParams sParams2 = getFacetSearchParams(fip, new CountFacetRequest(path, Integer.MAX_VALUE));
      FacetsCollector fc2 = FacetsCollector.create(sParams2, indexReader, taxoReader);
      
      searcher.search(new MatchAllDocsQuery(), fc2);
      List<FacetResult> baseResults = fc2.getFacetResults();
      FacetResult baseRes = baseResults.get(0);

      // Removing the first line which holds the REQUEST and this is surly different between the two
      String baseResultString = baseRes.toString();
      baseResultString = baseResultString.substring(baseResultString.indexOf('\n'));
      
      // Removing the first line
      String resultString = res.toString();
      resultString = resultString.substring(resultString.indexOf('\n'));
      
      assertTrue("Results for k=MAX_VALUE do not match the regular results for k=1000!!",
          baseResultString.equals(resultString));
      
      closeAll();
    }
  }

  @Test
  public void testSimpleSearchForNonexistentFacet() throws Exception {
    for (int partitionSize : partitionSizes) {
      FacetIndexingParams fip = getFacetIndexingParams(partitionSize);
      initIndex(fip);

      CategoryPath path = new CategoryPath("Miau Hattulla");
      FacetSearchParams sParams = getFacetSearchParams(fip, new CountFacetRequest(path, 10));

      FacetsCollector fc = FacetsCollector.create(sParams, indexReader, taxoReader);
      
      searcher.search(new MatchAllDocsQuery(), fc);
      
      List<FacetResult> facetResults = fc.getFacetResults();
      
      assertEquals("Shouldn't have found anything for a FacetRequest "
          + "of a facet that doesn't exist in the index.", 1, facetResults.size());
      assertEquals("Miau Hattulla", facetResults.get(0).getFacetResultNode().label.components[0]);
      closeAll();
    }
  }
}
