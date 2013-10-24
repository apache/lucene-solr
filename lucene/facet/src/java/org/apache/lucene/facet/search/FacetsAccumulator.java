package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.old.OldFacetsAccumulator;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.range.RangeAccumulator;
import org.apache.lucene.facet.range.RangeFacetRequest;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesAccumulator;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
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
 * Accumulates the facets defined in the {@link FacetSearchParams}.
 * 
 * @lucene.experimental
 */
public abstract class FacetsAccumulator {

  // TODO this should be final, but currently SamplingAccumulator modifies the params.
  // need to review the class and if it's resolved, make it final
  public /*final*/ FacetSearchParams searchParams;

  /** Constructor with the given search params. */
  protected FacetsAccumulator(FacetSearchParams fsp) {
    this.searchParams = fsp;
  }

  /**
   * Creates a {@link FacetsAccumulator} for the given facet requests. This
   * method supports {@link RangeAccumulator} and
   * {@link TaxonomyFacetsAccumulator} by dividing the facet requests into
   * {@link RangeFacetRequest} and the rest.
   * <p>
   * If both types of facet requests are used, it returns a
   * {@link MultiFacetsAccumulator} and the facet results returned from
   * {@link #accumulate(List)} may not be in the same order as the given facet
   * requests.
   * 
   * @param fsp
   *          the search params define the facet requests and the
   *          {@link FacetIndexingParams}
   * @param indexReader
   *          the {@link IndexReader} used for search
   * @param taxoReader
   *          the {@link TaxonomyReader} used for search
   * @param arrays
   *          the {@link FacetArrays} which the accumulator should use to store
   *          the categories weights in. Can be {@code null}.
   */
  public static FacetsAccumulator create(FacetSearchParams fsp, IndexReader indexReader, TaxonomyReader taxoReader, 
      FacetArrays arrays) {
    if (fsp.indexingParams.getPartitionSize() != Integer.MAX_VALUE) {
      return new OldFacetsAccumulator(fsp, indexReader, taxoReader, arrays);
    }
    
    List<FacetRequest> rangeRequests = new ArrayList<FacetRequest>();
    List<FacetRequest> nonRangeRequests = new ArrayList<FacetRequest>();
    for (FacetRequest fr : fsp.facetRequests) {
      if (fr instanceof RangeFacetRequest) {
        rangeRequests.add(fr);
      } else {
        nonRangeRequests.add(fr);
      }
    }

    if (rangeRequests.isEmpty()) {
      return new TaxonomyFacetsAccumulator(fsp, indexReader, taxoReader, arrays);
    } else if (nonRangeRequests.isEmpty()) {
      return new RangeAccumulator(rangeRequests);
    } else {
      FacetSearchParams searchParams = new FacetSearchParams(fsp.indexingParams, nonRangeRequests);
      FacetsAccumulator accumulator = new TaxonomyFacetsAccumulator(searchParams, indexReader, taxoReader, arrays);
      RangeAccumulator rangeAccumulator = new RangeAccumulator(rangeRequests);
      return MultiFacetsAccumulator.wrap(accumulator, rangeAccumulator);
    }
  }
  
  /**
   * Creates a {@link FacetsAccumulator} for the given facet requests. This
   * method supports {@link RangeAccumulator} and
   * {@link SortedSetDocValuesAccumulator} by dividing the facet requests into
   * {@link RangeFacetRequest} and the rest.
   * <p>
   * If both types of facet requests are used, it returns a
   * {@link MultiFacetsAccumulator} and the facet results returned from
   * {@link #accumulate(List)} may not be in the same order as the given facet
   * requests.
   * 
   * @param fsp
   *          the search params define the facet requests and the
   *          {@link FacetIndexingParams}
   * @param state
   *          the {@link SortedSetDocValuesReaderState} needed for accumulating
   *          the categories
   * @param arrays
   *          the {@link FacetArrays} which the accumulator should use to
   *          store the categories weights in. Can be {@code null}.
   */
  public static FacetsAccumulator create(FacetSearchParams fsp, SortedSetDocValuesReaderState state, FacetArrays arrays) throws IOException {
    if (fsp.indexingParams.getPartitionSize() != Integer.MAX_VALUE) {
      throw new IllegalArgumentException("only default partition size is supported by this method: " + fsp.indexingParams.getPartitionSize());
    }
    
    List<FacetRequest> rangeRequests = new ArrayList<FacetRequest>();
    List<FacetRequest> nonRangeRequests = new ArrayList<FacetRequest>();
    for (FacetRequest fr : fsp.facetRequests) {
      if (fr instanceof RangeFacetRequest) {
        rangeRequests.add(fr);
      } else {
        nonRangeRequests.add(fr);
      }
    }
    
    if (rangeRequests.isEmpty()) {
      return new SortedSetDocValuesAccumulator(state, fsp, arrays);
    } else if (nonRangeRequests.isEmpty()) {
      return new RangeAccumulator(rangeRequests);
    } else {
      FacetSearchParams searchParams = new FacetSearchParams(fsp.indexingParams, nonRangeRequests);
      FacetsAccumulator accumulator = new SortedSetDocValuesAccumulator(state, searchParams, arrays);
      RangeAccumulator rangeAccumulator = new RangeAccumulator(rangeRequests);
      return MultiFacetsAccumulator.wrap(accumulator, rangeAccumulator);
    }
  }
  
  /** Returns an empty {@link FacetResult}. */
  protected static FacetResult emptyResult(int ordinal, FacetRequest fr) {
    FacetResultNode root = new FacetResultNode(ordinal, 0);
    root.label = fr.categoryPath;
    return new FacetResult(fr, root, 0);
  }
  
  /**
   * Used by {@link FacetsCollector} to build the list of {@link FacetResult
   * facet results} that match the {@link FacetRequest facet requests} that were
   * given in the constructor.
   * 
   * @param matchingDocs
   *          the documents that matched the query, per-segment.
   */
  public abstract List<FacetResult> accumulate(List<MatchingDocs> matchingDocs) throws IOException;

  /**
   * Used by {@link FacetsCollector} to determine if document scores need to be
   * collected in addition to matching documents.
   */
  public abstract boolean requiresDocScores();
  
}
