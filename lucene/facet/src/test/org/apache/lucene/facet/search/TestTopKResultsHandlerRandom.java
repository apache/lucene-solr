package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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

public class TestTopKResultsHandlerRandom extends BaseTestTopK {
  
  private List<FacetResult> countFacets(int partitionSize, int numResults, final boolean doComplement)
      throws IOException, IllegalAccessException, InstantiationException {
    Query q = new MatchAllDocsQuery();
    FacetSearchParams facetSearchParams = searchParamsWithRequests(numResults, partitionSize);
    FacetsCollector fc = new FacetsCollector(facetSearchParams, indexReader, taxoReader) {
      @Override
      protected FacetsAccumulator initFacetsAccumulator(
          FacetSearchParams facetSearchParams, IndexReader indexReader,
          TaxonomyReader taxonomyReader) {
        FacetsAccumulator accumulator = new StandardFacetsAccumulator(facetSearchParams, indexReader, taxonomyReader);
        double complement = doComplement ? FacetsAccumulator.FORCE_COMPLEMENT : FacetsAccumulator.DISABLE_COMPLEMENT;
        accumulator.setComplementThreshold(complement);
        return accumulator;
      }
    };
    searcher.search(q, fc);
    List<FacetResult> facetResults = fc.getFacetResults();
    return facetResults;
  }

  /**
   * Test that indeed top results are returned, ordered same as all results 
   * also when some facets have the same counts.
   */
  @Test
  public void testTopCountsOrder() throws Exception {
    for (int partitionSize : partitionSizes) {
      initIndex(partitionSize);
      
      /*
       * Try out faceted search in it's most basic form (no sampling nor complement
       * that is). In this test lots (and lots..) of randomly generated data is
       * being indexed, and later on an "over-all" faceted search is performed. The
       * results are checked against the DF of each facet by itself
       */
      List<FacetResult> facetResults = countFacets(partitionSize, 100000, false);
      assertCountsAndCardinality(facetCountsTruth(), facetResults);
      
      /*
       * Try out faceted search with complements. In this test lots (and lots..) of
       * randomly generated data is being indexed, and later on, a "beta" faceted
       * search is performed - retrieving ~90% of the documents so complements takes
       * place in here. The results are checked against the a regular (a.k.a
       * no-complement, no-sampling) faceted search with the same parameters.
       */
      facetResults = countFacets(partitionSize, 100000, true);
      assertCountsAndCardinality(facetCountsTruth(), facetResults);
      
      List<FacetResult> allFacetResults = countFacets(partitionSize, 100000, false);
      
      HashMap<String,Integer> all = new HashMap<String,Integer>();
      int maxNumNodes = 0;
      int k = 0;
      for (FacetResult fr : allFacetResults) {
        FacetResultNode topResNode = fr.getFacetResultNode();
        maxNumNodes = Math.max(maxNumNodes, topResNode.getNumSubResults());
        int prevCount = Integer.MAX_VALUE;
        int pos = 0;
        for (FacetResultNode frn: topResNode.getSubResults()) {
          assertTrue("wrong counts order: prev="+prevCount+" curr="+frn.getValue(), prevCount>=frn.getValue());
          prevCount = (int) frn.getValue();
          String key = k+"--"+frn.getLabel()+"=="+frn.getValue();
          if (VERBOSE) {
            System.out.println(frn.getLabel() + " - " + frn.getValue() + "  "+key+"  "+pos);
          }
          all.put(key, pos++); // will use this later to verify order of sub-results
        }
        k++;
      }
      
      // verify that when asking for less results, they are always of highest counts
      // also verify that the order is stable
      for (int n=1; n<maxNumNodes; n++) {
        if (VERBOSE) {
          System.out.println("-------  verify for "+n+" top results");
        }
        List<FacetResult> someResults = countFacets(partitionSize, n, false);
        k = 0;
        for (FacetResult fr : someResults) {
          FacetResultNode topResNode = fr.getFacetResultNode();
          assertTrue("too many results: n="+n+" but got "+topResNode.getNumSubResults(), n>=topResNode.getNumSubResults());
          int pos = 0;
          for (FacetResultNode frn: topResNode.getSubResults()) {
            String key = k+"--"+frn.getLabel()+"=="+frn.getValue();
            if (VERBOSE) {
              System.out.println(frn.getLabel() + " - " + frn.getValue() + "  "+key+"  "+pos);
            }
            Integer origPos = all.get(key);
            assertNotNull("missing in all results: "+frn,origPos);
            assertEquals("wrong order of sub-results!",pos++, origPos.intValue()); // verify order of sub-results
          }
          k++;
        }
      }
      
      closeAll(); // done with this partition
    }
  }

  @Override
  protected int numDocsToIndex() {
    return TEST_NIGHTLY ? 20000 : 1000;
  }

}
