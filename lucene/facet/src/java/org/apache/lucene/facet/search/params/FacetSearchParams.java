package org.apache.lucene.facet.search.params;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.facet.index.params.FacetIndexingParams;
import org.apache.lucene.facet.search.cache.CategoryListCache;

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
 * Defines parameters that are needed for faceted search. The list of
 * {@link FacetRequest facet requests} denotes the facets for which aggregated
 * should be done.
 * <p>
 * One can pass {@link FacetIndexingParams} in order to tell the search code how
 * to read the facets information. Note that you must use the same
 * {@link FacetIndexingParams} that were used for indexing.
 * 
 * @lucene.experimental
 */
public class FacetSearchParams {

  protected final FacetIndexingParams indexingParams;
  protected final List<FacetRequest> facetRequests;
  
  /**
   * Initializes with the given {@link FacetRequest requests} and default
   * {@link FacetIndexingParams#ALL_PARENTS}. If you used a different
   * {@link FacetIndexingParams}, you should use
   * {@link #FacetSearchParams(List, FacetIndexingParams)}.
   */
  public FacetSearchParams(FacetRequest... facetRequests) {
    this(Arrays.asList(facetRequests), FacetIndexingParams.ALL_PARENTS);
  }
  
  /**
   * Initializes with the given {@link FacetRequest requests} and default
   * {@link FacetIndexingParams#ALL_PARENTS}. If you used a different
   * {@link FacetIndexingParams}, you should use
   * {@link #FacetSearchParams(List, FacetIndexingParams)}.
   */
  public FacetSearchParams(List<FacetRequest> facetRequests) {
    this(facetRequests, FacetIndexingParams.ALL_PARENTS);
  }

  /**
   * Initilizes with the given {@link FacetRequest requests} and
   * {@link FacetIndexingParams}.
   */
  public FacetSearchParams(List<FacetRequest> facetRequests, FacetIndexingParams indexingParams) {
    if (facetRequests == null || facetRequests.size() == 0) {
      throw new IllegalArgumentException("at least one FacetRequest must be defined");
    }
    this.indexingParams = indexingParams;
    this.facetRequests = facetRequests;
  }

  /**
   * Returns the {@link CategoryListCache}. By default returns {@code null}, you
   * should override if you want to use a cache.
   */
  public CategoryListCache getCategoryListCache() {
    return null;
  }

  /**
   * Returns the {@link FacetIndexingParams} that were passed to the
   * constructor.
   */
  public FacetIndexingParams getFacetIndexingParams() {
    return indexingParams;
  }
  
  /**
   * Returns the list of {@link FacetRequest facet requests} that were passed to
   * the constructor.
   */
  public List<FacetRequest> getFacetRequests() {
    return facetRequests;
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
}
