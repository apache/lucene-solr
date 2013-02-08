package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.sampling.RandomSampler;
import org.apache.lucene.facet.sampling.Sampler;
import org.apache.lucene.facet.sampling.SamplingAccumulator;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.IndexReader;

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
 * {@link FacetsAccumulator} whose behavior regarding complements, sampling,
 * etc. is not set up front but rather is determined at accumulation time
 * according to the statistics of the accumulated set of documents and the
 * index.
 * <p>
 * Note: Sampling accumulation (Accumulation over a sampled-set of the results),
 * does not guarantee accurate values for
 * {@link FacetResult#getNumValidDescendants()}.
 * 
 * @lucene.experimental
 */
public final class AdaptiveFacetsAccumulator extends StandardFacetsAccumulator {
  
  private Sampler sampler = new RandomSampler();

  /**
   * Create an {@link AdaptiveFacetsAccumulator} 
   * @see StandardFacetsAccumulator#StandardFacetsAccumulator(FacetSearchParams, IndexReader, TaxonomyReader)
   */
  public AdaptiveFacetsAccumulator(FacetSearchParams searchParams, IndexReader indexReader, 
      TaxonomyReader taxonomyReader) {
    super(searchParams, indexReader, taxonomyReader);
  }

  /**
   * Create an {@link AdaptiveFacetsAccumulator}
   * 
   * @see StandardFacetsAccumulator#StandardFacetsAccumulator(FacetSearchParams,
   *      IndexReader, TaxonomyReader, FacetArrays)
   */
  public AdaptiveFacetsAccumulator(FacetSearchParams searchParams, IndexReader indexReader,
      TaxonomyReader taxonomyReader, FacetArrays facetArrays) {
    super(searchParams, indexReader, taxonomyReader, facetArrays);
  }

  /**
   * Set the sampler.
   * @param sampler sampler to set
   */
  public void setSampler(Sampler sampler) {
    this.sampler = sampler;
  }
  
  @Override
  public List<FacetResult> accumulate(ScoredDocIDs docids) throws IOException {
    StandardFacetsAccumulator delegee = appropriateFacetCountingAccumulator(docids);

    if (delegee == this) {
      return super.accumulate(docids);
    }

    return delegee.accumulate(docids);
  }

  /**
   * Compute the appropriate facet accumulator to use.
   * If no special/clever adaptation is possible/needed return this (self).
   */
  private StandardFacetsAccumulator appropriateFacetCountingAccumulator(ScoredDocIDs docids) {
    // Verify that searchPareams permit sampling/complement/etc... otherwise do default
    if (!mayComplement()) {
      return this;
    }
    
    // Now we're sure we can use the sampling methods as we're in a counting only mode
    
    // Verify that sampling is enabled and required ... otherwise do default
    if (sampler == null || !sampler.shouldSample(docids)) {
      return this;
    }
    
    SamplingAccumulator samplingAccumulator = new SamplingAccumulator(sampler, searchParams, indexReader, taxonomyReader);
    samplingAccumulator.setComplementThreshold(getComplementThreshold());
    return samplingAccumulator;
  }

  /**
   * @return the sampler in effect
   */
  public final Sampler getSampler() {
    return sampler;
  }
}