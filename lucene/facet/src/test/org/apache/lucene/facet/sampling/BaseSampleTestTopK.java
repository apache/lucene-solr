package org.apache.lucene.facet.sampling;

import java.util.List;
import java.util.Random;

import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.sampling.RandomSampler;
import org.apache.lucene.facet.sampling.RepeatableSampler;
import org.apache.lucene.facet.sampling.Sampler;
import org.apache.lucene.facet.sampling.SamplingParams;
import org.apache.lucene.facet.search.BaseTestTopK;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.StandardFacetsAccumulator;
import org.apache.lucene.facet.search.FacetRequest.ResultMode;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

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

public abstract class BaseSampleTestTopK extends BaseTestTopK {
  
  /** Number of top results */
  protected static final int K = 2; 
  
  /** since there is a chance that this test would fail even if the code is correct, retry the sampling */
  protected static final int RETRIES = 10;
  
  @Override
  protected FacetSearchParams searchParamsWithRequests(int numResults, FacetIndexingParams fip) {
    FacetSearchParams res = super.searchParamsWithRequests(numResults, fip);
    for (FacetRequest req : res.facetRequests) {
      // randomize the way we aggregate results
      if (random().nextBoolean()) {
        req.setResultMode(ResultMode.GLOBAL_FLAT);
      } else {
        req.setResultMode(ResultMode.PER_NODE_IN_TREE);
      }
    }
    return res;
  }
  
  protected abstract StandardFacetsAccumulator getSamplingAccumulator(Sampler sampler, TaxonomyReader taxoReader, 
      IndexReader indexReader, FacetSearchParams searchParams);
  
  /**
   * Try out faceted search with sampling enabled and complements either disabled or enforced
   * Lots of randomly generated data is being indexed, and later on a "90% docs" faceted search
   * is performed. The results are compared to non-sampled ones.
   */
  public void testCountUsingSampling() throws Exception {
    boolean useRandomSampler = random().nextBoolean();
    for (int partitionSize : partitionSizes) {
      try {
        // complements return counts for all ordinals, so force ALL_PARENTS indexing
        // so that it's easier to compare
        FacetIndexingParams fip = getFacetIndexingParams(partitionSize, true);
        initIndex(fip);
        // Get all of the documents and run the query, then do different
        // facet counts and compare to control
        Query q = new TermQuery(new Term(CONTENT_FIELD, BETA)); // 90% of the docs
        
        FacetSearchParams expectedSearchParams = searchParamsWithRequests(K, fip); 
        FacetsCollector fc = FacetsCollector.create(expectedSearchParams, indexReader, taxoReader);
        
        searcher.search(q, fc);
        
        List<FacetResult> expectedResults = fc.getFacetResults();
        
        FacetSearchParams samplingSearchParams = searchParamsWithRequests(K, fip); 
        
        // try several times in case of failure, because the test has a chance to fail 
        // if the top K facets are not sufficiently common with the sample set
        for (int nTrial = 0; nTrial < RETRIES; nTrial++) {
          try {
            // complement with sampling!
            final Sampler sampler = createSampler(nTrial, useRandomSampler);
            
            assertSampling(expectedResults, q, sampler, samplingSearchParams, false);
            assertSampling(expectedResults, q, sampler, samplingSearchParams, true);
            
            break; // succeeded
          } catch (AssertionError e) {
            if (nTrial >= RETRIES - 1) {
              throw e; // no more retries allowed, must fail
            }
          }
        }
      } finally { 
        closeAll();
      }
    }
  }
  
  private void assertSampling(List<FacetResult> expected, Query q, Sampler sampler, FacetSearchParams params, boolean complement) throws Exception {
    FacetsCollector samplingFC = samplingCollector(complement, sampler, params);
    
    searcher.search(q, samplingFC);
    List<FacetResult> sampledResults = samplingFC.getFacetResults();
    
    assertSameResults(expected, sampledResults);
  }
  
  private FacetsCollector samplingCollector(final boolean complement, final Sampler sampler,
      FacetSearchParams samplingSearchParams) {
    StandardFacetsAccumulator sfa = getSamplingAccumulator(sampler, taxoReader, indexReader, samplingSearchParams);
    sfa.setComplementThreshold(complement ? StandardFacetsAccumulator.FORCE_COMPLEMENT : StandardFacetsAccumulator.DISABLE_COMPLEMENT);
    return FacetsCollector.create(sfa);
  }
  
  private Sampler createSampler(int nTrial, boolean useRandomSampler) {
    SamplingParams samplingParams = new SamplingParams();
    
    final double retryFactor = Math.pow(1.01, nTrial);
    samplingParams.setSampleRatio(0.8 * retryFactor);
    samplingParams.setMinSampleSize((int) (100 * retryFactor));
    samplingParams.setMaxSampleSize((int) (10000 * retryFactor));
    samplingParams.setOversampleFactor(5.0 * retryFactor);
    samplingParams.setSamplingThreshold(11000); //force sampling

    Sampler sampler = useRandomSampler ? 
        new RandomSampler(samplingParams, new Random(random().nextLong())) :
          new RepeatableSampler(samplingParams);
    return sampler;
  }
  
}
