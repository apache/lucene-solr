package org.apache.lucene.facet.search.params;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.index.params.DefaultFacetIndexingParams;
import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.cache.CategoryListCache;
import org.apache.lucene.facet.search.results.FacetResult;

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
 * Faceted search parameters indicate for which facets should info be gathered.
 * <p>
 * The contained facet requests define for which facets should info be gathered.
 * <p>
 * Contained faceted indexing parameters provide required info on how
 * to read and interpret the underlying faceted information in the search index.   
 * 
 * @lucene.experimental
 */
public class FacetSearchParams {

  protected final FacetIndexingParams indexingParams;
  protected final List<FacetRequest> facetRequests;
  private CategoryListCache clCache = null;

  /**
   * Construct with specific faceted indexing parameters.
   * It is important to know the indexing parameters so as to e.g. 
   * read facets data correctly from the index.
   * {@link #addFacetRequest(FacetRequest)} must be called at least once 
   * for this faceted search to find any faceted result.
   * @param indexingParams Indexing faceted parameters which were used at indexing time.
   * @see #addFacetRequest(FacetRequest)
   */
  public FacetSearchParams(FacetIndexingParams indexingParams) {
    this.indexingParams = indexingParams;
    facetRequests = new ArrayList<FacetRequest>();
  }

  /**
   * Construct with default faceted indexing parameters.
   * Usage of this constructor is valid only if also during indexing the 
   * default faceted indexing parameters were used.   
   * {@link #addFacetRequest(FacetRequest)} must be called at least once 
   * for this faceted search to find any faceted result.
   * @see #addFacetRequest(FacetRequest)
   */
  public FacetSearchParams() {
    this(new DefaultFacetIndexingParams());
  }

  /**
   * A list of {@link FacetRequest} objects, determining what to count.
   * If the returned collection is empty, the faceted search will return no facet results!
   */
  public final FacetIndexingParams getFacetIndexingParams() {
    return indexingParams;
  }

  /**
   * Parameters which controlled the indexing of facets, and which are also
   * needed during search.
   */
  public final List<FacetRequest> getFacetRequests() {
    return facetRequests;
  }

  /**
   * Add a facet request to apply for this faceted search.
   * This method must be called at least once for faceted search 
   * to find any faceted result. <br>
   * NOTE: The order of addition implies the order of the {@link FacetResult}s
   * @param facetRequest facet request to be added.
   */
  public void addFacetRequest(FacetRequest facetRequest) {
    if (facetRequest == null) {
      throw new IllegalArgumentException("Provided facetRequest must not be null");
    }
    facetRequests.add(facetRequest);
  }
  
  @Override
  public String toString() {
    final char TAB = '\t';
    final char NEWLINE = '\n';

    StringBuilder sb = new StringBuilder("IndexingParams: ");
    sb.append(NEWLINE).append(TAB).append(getFacetIndexingParams());
    
    sb.append(NEWLINE).append("FacetRequests:");
    for (FacetRequest facetRequest : getFacetRequests()) {
      sb.append(NEWLINE).append(TAB).append(facetRequest);
    }
    
    return sb.toString();
  }

  /**
   * @return the cldCache in effect
   */
  public CategoryListCache getClCache() {
    return clCache;
  }

  /**
   * Set Cached Category Lists data to be used in Faceted search.
   * @param clCache the cldCache to set
   */
  public void setClCache(CategoryListCache clCache) {
    this.clCache = clCache;
  }
}
