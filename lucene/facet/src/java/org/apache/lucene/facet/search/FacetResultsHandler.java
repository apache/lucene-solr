package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.facet.search.params.FacetRequest;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.search.results.IntermediateFacetResult;
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

/**
 * Handler for facet results.
 * <p>
 * The facet results handler provided by the {@link FacetRequest} to 
 * a {@link FacetsAccumulator}.
 * <p>
 * First it is used by {@link FacetsAccumulator} to obtain a temporary 
 * facet result for each partition and to merge results of several partitions.
 * <p>
 * Later the accumulator invokes the handler to render the results, creating 
 * {@link FacetResult} objects.
 * <p>
 * Last the accumulator invokes the handler to label final results. 
 * 
 * @lucene.experimental
 */
public abstract class FacetResultsHandler {

  /** Taxonomy for which facets are handled */
  protected final TaxonomyReader taxonomyReader;

  /**
   * Facet request served by this handler.
   */
  protected final FacetRequest facetRequest;

  /**
   * Create a faceted search handler.
   * @param taxonomyReader See {@link #getTaxonomyReader()}.
   * @param facetRequest See {@link #getFacetRequest()}.
   */
  public FacetResultsHandler(TaxonomyReader taxonomyReader,
                              FacetRequest facetRequest) {
    this.taxonomyReader = taxonomyReader;
    this.facetRequest = facetRequest;
  }

  /**
   * Fetch results of a single partition, given facet arrays for that partition,
   * and based on the matching documents and faceted search parameters.
   * 
   * @param arrays
   *          facet arrays for the certain partition
   * @param offset
   *          offset in input arrays where partition starts
   * @return temporary facet result, potentially, to be passed back to
   *         <b>this</b> result handler for merging, or <b>null</b> in case that
   *         constructor parameter, <code>facetRequest</code>, requests an
   *         illegal FacetResult, like, e.g., a root node category path that
   *         does not exist in constructor parameter <code>taxonomyReader</code>
   *         .
   * @throws IOException
   *           on error
   */
  public abstract IntermediateFacetResult fetchPartitionResult(FacetArrays arrays, int offset) throws IOException;

  /**
   * Merge results of several facet partitions. Logic of the merge is undefined
   * and open for interpretations. For example, a merge implementation could
   * keep top K results. Passed {@link IntermediateFacetResult} must be ones
   * that were created by this handler otherwise a {@link ClassCastException} is
   * thrown. In addition, all passed {@link IntermediateFacetResult} must have
   * the same {@link FacetRequest} otherwise an {@link IllegalArgumentException}
   * is thrown.
   * 
   * @param tmpResults one or more temporary results created by <b>this</b>
   *        handler.
   * @return temporary facet result that represents to union, as specified by
   *         <b>this</b> handler, of the input temporary facet results.
   * @throws IOException on error.
   * @throws ClassCastException if the temporary result passed was not created
   *         by this handler
   * @throws IllegalArgumentException if passed <code>facetResults</code> do not
   *         have the same {@link FacetRequest}
   * @see IntermediateFacetResult#getFacetRequest()
   */
  public abstract IntermediateFacetResult mergeResults(IntermediateFacetResult... tmpResults) 
  throws IOException, ClassCastException, IllegalArgumentException;

  /**
   * Create a facet result from the temporary result.
   * @param tmpResult temporary result to be rendered as a {@link FacetResult}
   * @throws IOException on error.
   */
  public abstract FacetResult renderFacetResult(IntermediateFacetResult tmpResult) throws IOException ;

  /**
   * Perform any rearrangement as required on a facet result that has changed after
   * it was rendered.
   * <P>
   * Possible use case: a sampling facets accumulator invoked another 
   * other facets accumulator on a sample set of documents, obtained
   * rendered facet results, fixed their counts, and now it is needed 
   * to sort the results differently according to the fixed counts. 
   * @param facetResult result to be rearranged.
   * @see FacetResultNode#setValue(double)
   */
  public abstract FacetResult rearrangeFacetResult(FacetResult facetResult);

  /**
   * Label results according to settings in {@link FacetRequest}, 
   * such as {@link FacetRequest#getNumLabel()}. 
   * Usually invoked by {@link FacetsAccumulator#accumulate(ScoredDocIDs)}
   * @param facetResult facet result to be labeled. 
   * @throws IOException on error 
   */
  public abstract void labelResult (FacetResult facetResult) throws IOException;

  /** Return taxonomy reader used for current facets accumulation operation. */
  public final TaxonomyReader getTaxonomyReader() {
    return this.taxonomyReader;
  }

  /** Return the facet request served by this handler. */
  public final FacetRequest getFacetRequest() {
    return this.facetRequest;
  }

  /**
   * Check if an array contains the partition which contains ordinal
   * 
   * @param ordinal
   *          checked facet
   * @param facetArrays
   *          facet arrays for the certain partition
   * @param offset
   *          offset in input arrays where partition starts
   */
  protected boolean isSelfPartition (int ordinal, FacetArrays facetArrays, int offset) {
    int partitionSize = facetArrays.arrayLength;
    return ordinal / partitionSize == offset / partitionSize;
  }

}
