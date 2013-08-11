package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.FacetRequest.ResultMode;
import org.apache.lucene.facet.search.FacetRequest.SortOrder;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.CategoryPath;
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
 * A {@link FacetsAccumulator} suitable for accumulating categories that were
 * indexed into a taxonomy index.
 * 
 * @lucene.experimental
 */
public class TaxonomyFacetsAccumulator extends FacetsAccumulator {

  public final TaxonomyReader taxonomyReader;
  public final IndexReader indexReader;
  public final FacetArrays facetArrays;
  
  /**
   * Initializes the accumulator with the given search params, index reader and
   * taxonomy reader. This constructor creates the default {@link FacetArrays},
   * which do not support reuse. If you want to use {@link ReusingFacetArrays},
   * you should use the
   * {@link #TaxonomyFacetsAccumulator(FacetSearchParams, IndexReader, TaxonomyReader)}
   * constructor.
   */
  public TaxonomyFacetsAccumulator(FacetSearchParams searchParams, IndexReader indexReader, 
      TaxonomyReader taxonomyReader) {
    this(searchParams, indexReader, taxonomyReader, null);
  }

  /**
   * Initializes the accumulator with the given parameters as well as
   * {@link FacetArrays}. Note that the accumulator doesn't call
   * {@link FacetArrays#free()}. If you require that (only makes sense if you
   * use {@link ReusingFacetArrays}, you should do it after you've finished with
   * the accumulator.
   */
  public TaxonomyFacetsAccumulator(FacetSearchParams searchParams, IndexReader indexReader, 
      TaxonomyReader taxonomyReader, FacetArrays facetArrays) {
    super(searchParams);
    this.facetArrays = facetArrays == null ? new FacetArrays(taxonomyReader.getSize()) : facetArrays;
    this.indexReader = indexReader;
    this.taxonomyReader = taxonomyReader;
  }

  /** Group all requests that belong to the same {@link CategoryListParams}. */
  protected Map<CategoryListParams,List<FacetRequest>> groupRequests() {
    if (searchParams.indexingParams.getAllCategoryListParams().size() == 1) {
      return Collections.singletonMap(searchParams.indexingParams.getCategoryListParams(null), searchParams.facetRequests);
    }
    
    HashMap<CategoryListParams,List<FacetRequest>> requestsPerCLP = new HashMap<CategoryListParams,List<FacetRequest>>();
    for (FacetRequest fr : searchParams.facetRequests) {
      CategoryListParams clp = searchParams.indexingParams.getCategoryListParams(fr.categoryPath);
      List<FacetRequest> requests = requestsPerCLP.get(clp);
      if (requests == null) {
        requests = new ArrayList<FacetRequest>();
        requestsPerCLP.put(clp, requests);
      }
      requests.add(fr);
    }
    return requestsPerCLP;
  }

  /**
   * Returns the {@link FacetsAggregator} to use for aggregating the categories
   * found in the result documents.
   */
  public FacetsAggregator getAggregator() {
    Map<CategoryListParams,List<FacetRequest>> requestsPerCLP = groupRequests();

    // optimize for all-CountFacetRequest and single category list (common case)
    if (requestsPerCLP.size() == 1) {
      boolean allCount = true;
      for (FacetRequest fr : searchParams.facetRequests) {
        if (!(fr instanceof CountFacetRequest)) {
          allCount = false;
          break;
        }
      }
      if (allCount) {
        return requestsPerCLP.values().iterator().next().get(0).createFacetsAggregator(searchParams.indexingParams);
      }
    }
    
    // If we're here it means the facet requests are spread across multiple
    // category lists, or there are multiple types of facet requests, or both.
    // Therefore create a per-CategoryList mapping of FacetsAggregators.
    Map<CategoryListParams,FacetsAggregator> perCLPAggregator = new HashMap<CategoryListParams,FacetsAggregator>();
    for (Entry<CategoryListParams,List<FacetRequest>> e : requestsPerCLP.entrySet()) {
      CategoryListParams clp = e.getKey();
      List<FacetRequest> requests = e.getValue();
      Map<Class<? extends FacetsAggregator>,FacetsAggregator> aggClasses = new HashMap<Class<? extends FacetsAggregator>,FacetsAggregator>();
      Map<CategoryPath,FacetsAggregator> perCategoryAggregator = new HashMap<CategoryPath,FacetsAggregator>();
      for (FacetRequest fr : requests) {
        FacetsAggregator fa = fr.createFacetsAggregator(searchParams.indexingParams);
        if (fa == null) {
          throw new IllegalArgumentException("this accumulator only supports requests that create a FacetsAggregator: " + fr);
        }
        Class<? extends FacetsAggregator> faClass = fa.getClass();
        if (!aggClasses.containsKey(faClass)) {
          aggClasses.put(faClass, fa);
        } else {
          fa = aggClasses.get(faClass);
        }
        perCategoryAggregator.put(fr.categoryPath, fa);
      }
      
      if (aggClasses.size() == 1) { // only one type of facet request
        perCLPAggregator.put(clp, aggClasses.values().iterator().next());
      } else {
        perCLPAggregator.put(clp, new MultiFacetsAggregator(perCategoryAggregator));
      }
    }

    return new PerCategoryListAggregator(perCLPAggregator, searchParams.indexingParams);
  }
  
  /**
   * Creates a {@link FacetResultsHandler} that matches the given
   * {@link FacetRequest}, using the {@link OrdinalValueResolver}.
   */
  protected FacetResultsHandler createFacetResultsHandler(FacetRequest fr, OrdinalValueResolver resolver) {
    if (fr.getDepth() == 1 && fr.getSortOrder() == SortOrder.DESCENDING) {
      return new DepthOneFacetResultsHandler(taxonomyReader, fr, facetArrays, resolver);
    }

    if (fr.getResultMode() == ResultMode.PER_NODE_IN_TREE) {
      return new TopKInEachNodeHandler(taxonomyReader, fr, resolver, facetArrays);
    } else {
      return new TopKFacetResultsHandler(taxonomyReader, fr, resolver, facetArrays);
    }
  }

  /**
   * Used by {@link FacetsCollector} to build the list of {@link FacetResult
   * facet results} that match the {@link FacetRequest facet requests} that were
   * given in the constructor.
   * 
   * @param matchingDocs
   *          the documents that matched the query, per-segment.
   */
  @Override
  public List<FacetResult> accumulate(List<MatchingDocs> matchingDocs) throws IOException {
    // aggregate facets per category list (usually onle one category list)
    FacetsAggregator aggregator = getAggregator();
    for (CategoryListParams clp : groupRequests().keySet()) {
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
      
      FacetResultsHandler frh = createFacetResultsHandler(fr, aggregator.createOrdinalValueResolver(fr, facetArrays));
      res.add(frh.compute());
    }
    return res;
  }

  @Override
  public boolean requiresDocScores() {
    return getAggregator().requiresDocScores();
  }
}
