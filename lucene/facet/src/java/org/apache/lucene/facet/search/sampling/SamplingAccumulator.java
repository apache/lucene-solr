package org.apache.lucene.facet.search.sampling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetResultsHandler;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.SamplingWrapper;
import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.StandardFacetsAccumulator;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.search.sampling.Sampler.SampleResult;
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
 * Facets accumulation with sampling.<br>
 * <p>
 * Note two major differences between this class and {@link SamplingWrapper}:
 * <ol>
 * <li>Latter can wrap any other {@link FacetsAccumulator} while this class
 * directly extends {@link StandardFacetsAccumulator}.</li>
 * <li>This class can effectively apply sampling on the complement set of
 * matching document, thereby working efficiently with the complement
 * optimization - see {@link FacetsAccumulator#getComplementThreshold()}.</li>
 * </ol>
 * <p>
 * Note: Sampling accumulation (Accumulation over a sampled-set of the results),
 * does not guarantee accurate values for
 * {@link FacetResult#getNumValidDescendants()} &
 * {@link FacetResultNode#getResidue()}.
 * 
 * @see Sampler
 * @lucene.experimental
 */
public class SamplingAccumulator extends StandardFacetsAccumulator {
  
  private double samplingRatio = -1d;
  private final Sampler sampler;
  
  public SamplingAccumulator(Sampler sampler, FacetSearchParams searchParams,
      IndexReader indexReader, TaxonomyReader taxonomyReader,
      FacetArrays facetArrays) {
    super(searchParams, indexReader, taxonomyReader, facetArrays);
    this.sampler = sampler;
  }

  /**
   * Constructor...
   */
  public SamplingAccumulator(
      Sampler sampler,
      FacetSearchParams searchParams,
      IndexReader indexReader, TaxonomyReader taxonomyReader) {
    super(searchParams, indexReader, taxonomyReader);
    this.sampler = sampler;
  }

  @Override
  public List<FacetResult> accumulate(ScoredDocIDs docids) throws IOException {
    // first let delegee accumulate without labeling at all (though
    // currently it doesn't matter because we have to label all returned anyhow)
    boolean origAllowLabeling = isAllowLabeling();
    setAllowLabeling(false);
    
    // Replacing the original searchParams with the over-sampled
    FacetSearchParams original = searchParams;
    searchParams = sampler.overSampledSearchParams(original);
    
    List<FacetResult> sampleRes = super.accumulate(docids);
    setAllowLabeling(origAllowLabeling);
    
    List<FacetResult> fixedRes = new ArrayList<FacetResult>();
    for (FacetResult fres : sampleRes) {
      // for sure fres is not null because this is guaranteed by the delegee.
      FacetResultsHandler frh = fres.getFacetRequest().createFacetResultsHandler(
          taxonomyReader);
      // fix the result of current request
      sampler.getSampleFixer(indexReader, taxonomyReader, searchParams)
          .fixResult(docids, fres);
      
      fres = frh.rearrangeFacetResult(fres); // let delegee's handler do any

      // Using the sampler to trim the extra (over-sampled) results
      fres = sampler.trimResult(fres);
                                              // arranging it needs to
      // final labeling if allowed (because labeling is a costly operation)
      if (isAllowLabeling()) {
        frh.labelResult(fres);
      }
      fixedRes.add(fres); // add to final results
    }
    
    searchParams = original; // Back to original params
    
    return fixedRes; 
  }

  @Override
  protected ScoredDocIDs actualDocsToAccumulate(ScoredDocIDs docids) throws IOException {
    SampleResult sampleRes = sampler.getSampleSet(docids);
    samplingRatio = sampleRes.actualSampleRatio;
    return sampleRes.docids;
  }
  
  @Override
  protected double getTotalCountsFactor() {
    if (samplingRatio<0) {
      throw new IllegalStateException("Total counts ratio unavailable because actualDocsToAccumulate() was not invoked");
    }
    return samplingRatio;
  }
}
