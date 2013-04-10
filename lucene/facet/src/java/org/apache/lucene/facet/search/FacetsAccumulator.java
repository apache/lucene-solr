package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.facet.encoding.DGapVInt8IntDecoder;
import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.search.FacetRequest.FacetArraysSource;
import org.apache.lucene.facet.search.FacetRequest.ResultMode;
import org.apache.lucene.facet.search.FacetRequest.SortOrder;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
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
 * Driver for Accumulating facets of faceted search requests over given
 * documents.
 * 
 * @lucene.experimental
 */
public class FacetsAccumulator {

  public final TaxonomyReader taxonomyReader;
  public final IndexReader indexReader;
  public final FacetArrays facetArrays;
  public FacetSearchParams searchParams;

  /**
   * Initializes the accumulator with the given search params, index reader and
   * taxonomy reader. This constructor creates the default {@link FacetArrays},
   * which do not support reuse. If you want to use {@link ReusingFacetArrays},
   * you should use the
   * {@link #FacetsAccumulator(FacetSearchParams, IndexReader, TaxonomyReader, FacetArrays)}
   * constructor.
   */
  public FacetsAccumulator(FacetSearchParams searchParams, IndexReader indexReader, TaxonomyReader taxonomyReader) {
    this(searchParams, indexReader, taxonomyReader, null);
  }

  /**
   * Creates an appropriate {@link FacetsAccumulator},
   * returning {@link FacetsAccumulator} when all requests
   * are {@link CountFacetRequest} and only one partition is
   * in use, otherwise {@link StandardFacetsAccumulator}.
   */
  public static FacetsAccumulator create(FacetSearchParams fsp, IndexReader indexReader, TaxonomyReader taxoReader) {
    if (fsp.indexingParams.getPartitionSize() != Integer.MAX_VALUE) {
      return new StandardFacetsAccumulator(fsp, indexReader, taxoReader);
    }
    
    for (FacetRequest fr : fsp.facetRequests) {
      if (!(fr instanceof CountFacetRequest)) {
        return new StandardFacetsAccumulator(fsp, indexReader, taxoReader);
      }
    }
    
    return new FacetsAccumulator(fsp, indexReader, taxoReader);
  }
  
  /** Returns an empty {@link FacetResult}. */
  protected static FacetResult emptyResult(int ordinal, FacetRequest fr) {
    FacetResultNode root = new FacetResultNode(ordinal, 0);
    root.label = fr.categoryPath;
    return new FacetResult(fr, root, 0);
  }
  
  /**
   * Initializes the accumulator with the given parameters as well as
   * {@link FacetArrays}. Note that the accumulator doesn't call
   * {@link FacetArrays#free()}. If you require that (only makes sense if you
   * use {@link ReusingFacetArrays}, you should do it after you've finished with
   * the accumulator.
   */
  public FacetsAccumulator(FacetSearchParams searchParams, IndexReader indexReader, TaxonomyReader taxonomyReader, 
      FacetArrays facetArrays) {
    if (facetArrays == null) {
      facetArrays = new FacetArrays(taxonomyReader.getSize());
    }
    this.facetArrays = facetArrays;
    this.indexReader = indexReader;
    this.taxonomyReader = taxonomyReader;
    this.searchParams = searchParams;
  }
  
  /**
   * Returns the {@link FacetsAggregator} to use for aggregating the categories
   * found in the result documents. The default implementation returns
   * {@link CountingFacetsAggregator}, or {@link FastCountingFacetsAggregator}
   * if all categories can be decoded with {@link DGapVInt8IntDecoder}.
   */
  public FacetsAggregator getAggregator() {
    if (FastCountingFacetsAggregator.verifySearchParams(searchParams)) {
      return new FastCountingFacetsAggregator();
    } else {
      return new CountingFacetsAggregator();
    }
  }
  
  /**
   * Creates a {@link FacetResultsHandler} that matches the given
   * {@link FacetRequest}.
   */
  protected FacetResultsHandler createFacetResultsHandler(FacetRequest fr) {
    if (fr.getDepth() == 1 && fr.getSortOrder() == SortOrder.DESCENDING) {
      FacetArraysSource fas = fr.getFacetArraysSource();
      if (fas == FacetArraysSource.INT) {
        return new IntFacetResultsHandler(taxonomyReader, fr, facetArrays);
      }
      
      if (fas == FacetArraysSource.FLOAT) {
        return new FloatFacetResultsHandler(taxonomyReader, fr, facetArrays);
      }
    }

    if (fr.getResultMode() == ResultMode.PER_NODE_IN_TREE) {
      return new TopKInEachNodeHandler(taxonomyReader, fr, facetArrays);
    } 
    return new TopKFacetResultsHandler(taxonomyReader, fr, facetArrays);
  }

  protected Set<CategoryListParams> getCategoryLists() {
    if (searchParams.indexingParams.getAllCategoryListParams().size() == 1) {
      return Collections.singleton(searchParams.indexingParams.getCategoryListParams(null));
    }
    
    HashSet<CategoryListParams> clps = new HashSet<CategoryListParams>();
    for (FacetRequest fr : searchParams.facetRequests) {
      clps.add(searchParams.indexingParams.getCategoryListParams(fr.categoryPath));
    }
    return clps;
  }

  /**
   * Used by {@link FacetsCollector} to build the list of {@link FacetResult
   * facet results} that match the {@link FacetRequest facet requests} that were
   * given in the constructor.
   * 
   * @param matchingDocs
   *          the documents that matched the query, per-segment.
   */
  public List<FacetResult> accumulate(List<MatchingDocs> matchingDocs) throws IOException {
    // aggregate facets per category list (usually onle one category list)
    FacetsAggregator aggregator = getAggregator();
    for (CategoryListParams clp : getCategoryLists()) {
      for (MatchingDocs md : matchingDocs) {
        aggregator.aggregate(md, clp, facetArrays);
      }
    }
    
    ParallelTaxonomyArrays arrays = taxonomyReader.getParallelTaxonomyArrays();
    
    // compute top-K
    final int[] children = arrays.children();
    final int[] siblings = arrays.siblings();
    List<FacetResult> res = new ArrayList<FacetResult>();
    for (FacetRequest fr : searchParams.facetRequests) {
      int rootOrd = taxonomyReader.getOrdinal(fr.categoryPath);
      if (rootOrd == TaxonomyReader.INVALID_ORDINAL) { // category does not exist
        // Add empty FacetResult
        res.add(emptyResult(rootOrd, fr));
        continue;
      }
      CategoryListParams clp = searchParams.indexingParams.getCategoryListParams(fr.categoryPath);
      if (fr.categoryPath.length > 0) { // someone might ask to aggregate the ROOT category
        OrdinalPolicy ordinalPolicy = clp.getOrdinalPolicy(fr.categoryPath.components[0]);
        if (ordinalPolicy == OrdinalPolicy.NO_PARENTS) {
          // rollup values
          aggregator.rollupValues(fr, rootOrd, children, siblings, facetArrays);
        }
      }
      
      FacetResultsHandler frh = createFacetResultsHandler(fr);
      res.add(frh.compute());
    }
    return res;
  }

}
