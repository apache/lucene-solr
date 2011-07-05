package org.apache.lucene.facet.search.sampling;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.facet.search.BaseTestTopK;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.ScoredDocIdCollector;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

/**
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

public abstract class BaseSampleTestTopK extends BaseTestTopK {
  
  /** Number of top results */
  protected static final int K = 2; 
  
  /** since there is a chance that this test would fail even if the code is correct, retry the sampling */
  protected static final int RETRIES = 4; 
  
  protected abstract FacetsAccumulator getSamplingAccumulator(Sampler sampler,
      TaxonomyReader taxoReader, IndexReader indexReader,
      FacetSearchParams searchParams);
  
  /**
   * Try out faceted search with sampling enabled and complements either disabled or enforced
   * Lots of randomly generated data is being indexed, and later on a "90% docs" faceted search
   * is performed. The results are compared to non-sampled ones.
   */
  public void testCountUsingSamping() throws Exception, IOException {
    for (int partitionSize : partitionSizes) {
      initIndex(partitionSize);
      
      // Get all of the documents and run the query, then do different
      // facet counts and compare to control
      Query q = new TermQuery(new Term(CONTENT_FIELD, BETA)); // 90% of the docs
      ScoredDocIdCollector docCollector = ScoredDocIdCollector.create(searcher.maxDoc(), false);
      
      FacetSearchParams expectedSearchParams = searchParamsWithRequests(K, partitionSize); 
      FacetsCollector fc = new FacetsCollector(expectedSearchParams, indexReader, taxoReader);
      
      searcher.search(q, MultiCollector.wrap(docCollector, fc));
      
      List<FacetResult> expectedResults = fc.getFacetResults();
      
      // complement with sampling!
      final Sampler sampler = createSampler(docCollector.getScoredDocIDs());
      
      FacetSearchParams samplingSearchParams = searchParamsWithRequests(K, partitionSize); 

      assertSampling(expectedResults, q, sampler, samplingSearchParams, false);
      assertSampling(expectedResults, q, sampler, samplingSearchParams, true);

      closeAll();
    }
  }
  
  private void assertSampling(List<FacetResult> expected, Query q, Sampler sampler, FacetSearchParams params, boolean complement) throws Exception {
    // try several times in case of failure, because the test has a chance to fail 
    // if the top K facets are not sufficiently common with the sample set
    for (int n=RETRIES; n>0; n--) {
      FacetsCollector samplingFC = samplingCollector(false, sampler, params);
      
      searcher.search(q, samplingFC);
      List<FacetResult> sampledResults = samplingFC.getFacetResults();
      
      try {
        assertSameResults(expected, sampledResults);
        break; // succeeded
      } catch (Exception e) {
        if (n<=1) { // otherwise try again
          throw e; 
        }
      }
    }
  }
  
  private FacetsCollector samplingCollector(
      final boolean complement,
      final Sampler sampler,
      FacetSearchParams samplingSearchParams) {
    FacetsCollector samplingFC = new FacetsCollector(samplingSearchParams, indexReader, taxoReader) {
      @Override
      protected FacetsAccumulator initFacetsAccumulator(
          FacetSearchParams facetSearchParams, IndexReader indexReader,
          TaxonomyReader taxonomyReader) {
        FacetsAccumulator acc = getSamplingAccumulator(sampler, taxonomyReader, indexReader, facetSearchParams);
        acc.setComplementThreshold(complement ? FacetsAccumulator.FORCE_COMPLEMENT : FacetsAccumulator.DISABLE_COMPLEMENT);
        return acc;
      }
    };
    return samplingFC;
  }
  
  private Sampler createSampler(ScoredDocIDs scoredDocIDs) {
    SamplingParams samplingParams = new SamplingParams();
    samplingParams.setSampleRatio(0.8);
    samplingParams.setMinSampleSize(100);
    samplingParams.setMaxSampleSize(10000);
    samplingParams.setSampingThreshold(11000); //force sampling 
    samplingParams.setOversampleFactor(5.0);
    Sampler sampler = new Sampler(samplingParams);
    assertTrue("must enable sampling for this test!",sampler.shouldSample(scoredDocIDs));
    return sampler;
  }
}
