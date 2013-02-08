package org.apache.lucene.facet.sampling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.partitions.PartitionsFacetResultsHandler;
import org.apache.lucene.facet.sampling.Sampler.SampleResult;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.ScoredDocIDs;
import org.apache.lucene.facet.search.StandardFacetsAccumulator;
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
 * optimization - see {@link StandardFacetsAccumulator#getComplementThreshold()}
 * .</li>
 * </ol>
 * <p>
 * Note: Sampling accumulation (Accumulation over a sampled-set of the results),
 * does not guarantee accurate values for
 * {@link FacetResult#getNumValidDescendants()}.
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
    // Replacing the original searchParams with the over-sampled
    FacetSearchParams original = searchParams;
    searchParams = sampler.overSampledSearchParams(original);
    
    List<FacetResult> sampleRes = super.accumulate(docids);
    
    List<FacetResult> fixedRes = new ArrayList<FacetResult>();
    for (FacetResult fres : sampleRes) {
      // for sure fres is not null because this is guaranteed by the delegee.
      PartitionsFacetResultsHandler frh = createFacetResultsHandler(fres.getFacetRequest());
      // fix the result of current request
      sampler.getSampleFixer(indexReader, taxonomyReader, searchParams).fixResult(docids, fres);
      
      fres = frh.rearrangeFacetResult(fres); // let delegee's handler do any arranging it needs to

      // Using the sampler to trim the extra (over-sampled) results
      fres = sampler.trimResult(fres);

      // final labeling if allowed (because labeling is a costly operation)
      frh.labelResult(fres);
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
