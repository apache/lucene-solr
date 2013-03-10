package org.apache.lucene.facet.sampling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.Aggregator;
import org.apache.lucene.facet.search.FacetArrays;
import org.apache.lucene.facet.search.FacetRequest;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetResultNode;
import org.apache.lucene.facet.search.ScoredDocIDs;
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
 * Sampling definition for facets accumulation
 * <p>
 * The Sampler uses TAKMI style counting to provide a 'best guess' top-K result
 * set of the facets accumulated.
 * <p>
 * Note: Sampling accumulation (Accumulation over a sampled-set of the results),
 * does not guarantee accurate values for
 * {@link FacetResult#getNumValidDescendants()}.
 * 
 * @lucene.experimental
 */
public abstract class Sampler {

  protected final SamplingParams samplingParams;
  
  /**
   * Construct with {@link SamplingParams}
   */
  public Sampler() {
    this(new SamplingParams()); 
  }
  
  /**
   * Construct with certain {@link SamplingParams}
   * 
   * @param params sampling params in effect
   * @throws IllegalArgumentException if the provided SamplingParams are not valid 
   */
  public Sampler(SamplingParams params) throws IllegalArgumentException {
    if (!params.validate()) {
      throw new IllegalArgumentException("The provided SamplingParams are not valid!!");
    }
    this.samplingParams = params;
  }

  /**
   * Check if this sampler would complement for the input docIds
   */
  public boolean shouldSample(ScoredDocIDs docIds) {
    return docIds.size() > samplingParams.getSamplingThreshold();
  }
  
  /**
   * Compute a sample set out of the input set, based on the {@link SamplingParams#getSampleRatio()}
   * in effect. Sub classes can override to alter how the sample set is
   * computed.
   * <p> 
   * If the input set is of size smaller than {@link SamplingParams#getMinSampleSize()}, 
   * the input set is returned (no sampling takes place).
   * <p>
   * Other than that, the returned set size will not be larger than {@link SamplingParams#getMaxSampleSize()} 
   * nor smaller than {@link SamplingParams#getMinSampleSize()}.  
   * @param docids
   *          full set of matching documents out of which a sample is needed.
   */
  public SampleResult getSampleSet(ScoredDocIDs docids) throws IOException {
    if (!shouldSample(docids)) {
      return new SampleResult(docids, 1d);
    }

    int actualSize = docids.size();
    int sampleSetSize = (int) (actualSize * samplingParams.getSampleRatio());
    sampleSetSize = Math.max(sampleSetSize, samplingParams.getMinSampleSize());
    sampleSetSize = Math.min(sampleSetSize, samplingParams.getMaxSampleSize());

    return createSample(docids, actualSize, sampleSetSize);
  }

  /**
   * Create and return a sample of the input set
   * @param docids input set out of which a sample is to be created 
   * @param actualSize original size of set, prior to sampling
   * @param sampleSetSize required size of sample set
   * @return sample of the input set in the required size
   */
  protected abstract SampleResult createSample(ScoredDocIDs docids, int actualSize, int sampleSetSize) 
      throws IOException;

  /**
   * Get a fixer of sample facet accumulation results. Default implementation
   * returns a <code>TakmiSampleFixer</code> which is adequate only for
   * counting. For any other accumulator, provide a different fixer.
   */
  public SampleFixer getSampleFixer(IndexReader indexReader, TaxonomyReader taxonomyReader,
      FacetSearchParams searchParams) {
    return new TakmiSampleFixer(indexReader, taxonomyReader, searchParams);
  }
  
  /**
   * Result of sample computation
   */
  public final static class SampleResult {
    public final ScoredDocIDs docids;
    public final double actualSampleRatio;
    protected SampleResult(ScoredDocIDs docids, double actualSampleRatio) {
      this.docids = docids;
      this.actualSampleRatio = actualSampleRatio;
    }
  }
  
  /**
   * Return the sampling params in effect
   */
  public final SamplingParams getSamplingParams() {
    return samplingParams;
  }

  /**
   * Trim the input facet result.<br>
   * Note: It is only valid to call this method with result obtained for a
   * facet request created through {@link #overSampledSearchParams(FacetSearchParams)}.
   * 
   * @throws IllegalArgumentException
   *             if called with results not obtained for requests created
   *             through {@link #overSampledSearchParams(FacetSearchParams)}
   */
  public FacetResult trimResult(FacetResult facetResult) throws IllegalArgumentException {
    double overSampleFactor = getSamplingParams().getOversampleFactor();
    if (overSampleFactor <= 1) { // no factoring done?
      return facetResult;
    }
    
    OverSampledFacetRequest sampledFreq = null;
    
    try {
      sampledFreq = (OverSampledFacetRequest) facetResult.getFacetRequest();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          "It is only valid to call this method with result obtained for a " +
          "facet request created through sampler.overSamlpingSearchParams()",
          e);
    }
    
    FacetRequest origFrq = sampledFreq.orig;

    FacetResultNode trimmedRootNode = facetResult.getFacetResultNode();
    trimSubResults(trimmedRootNode, origFrq.numResults);
    
    return new FacetResult(origFrq, trimmedRootNode, facetResult.getNumValidDescendants());
  }
  
  /** Trim sub results to a given size. */
  private void trimSubResults(FacetResultNode node, int size) {
    if (node.subResults == FacetResultNode.EMPTY_SUB_RESULTS || node.subResults.size() == 0) {
      return;
    }

    ArrayList<FacetResultNode> trimmed = new ArrayList<FacetResultNode>(size);
    for (int i = 0; i < node.subResults.size() && i < size; i++) {
      FacetResultNode trimmedNode = node.subResults.get(i);
      trimSubResults(trimmedNode, size);
      trimmed.add(trimmedNode);
    }
    
    node.subResults = trimmed;
  }

  /**
   * Over-sampled search params, wrapping each request with an over-sampled one.
   */
  public FacetSearchParams overSampledSearchParams(FacetSearchParams original) {
    FacetSearchParams res = original;
    // So now we can sample -> altering the searchParams to accommodate for the statistical error for the sampling
    double overSampleFactor = getSamplingParams().getOversampleFactor();
    if (overSampleFactor > 1) { // any factoring to do?
      List<FacetRequest> facetRequests = new ArrayList<FacetRequest>();
      for (FacetRequest frq : original.facetRequests) {
        int overSampledNumResults = (int) Math.ceil(frq.numResults * overSampleFactor);
        facetRequests.add(new OverSampledFacetRequest(frq, overSampledNumResults));
      }
      res = new FacetSearchParams(original.indexingParams, facetRequests);
    }
    return res;
  }
  
  /**
   * Wrapping a facet request for over sampling.
   * Implementation detail: even if the original request is a count request, no 
   * statistics will be computed for it as the wrapping is not a count request.
   * This is ok, as the sampling accumulator is later computing the statistics
   * over the original requests.
   */
  private static class OverSampledFacetRequest extends FacetRequest {
    final FacetRequest orig;
    public OverSampledFacetRequest(FacetRequest orig, int num) {
      super(orig.categoryPath, num);
      this.orig = orig;
      setDepth(orig.getDepth());
      setNumLabel(orig.getNumLabel());
      setResultMode(orig.getResultMode());
      setSortOrder(orig.getSortOrder());
    }
    
    @Override
    public Aggregator createAggregator(boolean useComplements, FacetArrays arrays, TaxonomyReader taxonomy) 
        throws IOException {
      return orig.createAggregator(useComplements, arrays, taxonomy);
    }

    @Override
    public FacetArraysSource getFacetArraysSource() {
      return orig.getFacetArraysSource();
    }

    @Override
    public double getValueOf(FacetArrays arrays, int idx) {
      return orig.getValueOf(arrays, idx);
    }
  }

}
