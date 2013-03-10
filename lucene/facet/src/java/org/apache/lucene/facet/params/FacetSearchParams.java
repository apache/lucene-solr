package org.apache.lucene.facet.params;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.facet.search.FacetRequest;

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

  public final FacetIndexingParams indexingParams;
  public final List<FacetRequest> facetRequests;
  
  /**
   * Initializes with the given {@link FacetRequest requests} and default
   * {@link FacetIndexingParams#DEFAULT}. If you used a different
   * {@link FacetIndexingParams}, you should use
   * {@link #FacetSearchParams(FacetIndexingParams, List)}.
   */
  public FacetSearchParams(FacetRequest... facetRequests) {
    this(FacetIndexingParams.DEFAULT, Arrays.asList(facetRequests));
  }
  
  /**
   * Initializes with the given {@link FacetRequest requests} and default
   * {@link FacetIndexingParams#DEFAULT}. If you used a different
   * {@link FacetIndexingParams}, you should use
   * {@link #FacetSearchParams(FacetIndexingParams, List)}.
   */
  public FacetSearchParams(List<FacetRequest> facetRequests) {
    this(FacetIndexingParams.DEFAULT, facetRequests);
  }
  
  /**
   * Initializes with the given {@link FacetRequest requests} and
   * {@link FacetIndexingParams}.
   */
  public FacetSearchParams(FacetIndexingParams indexingParams, FacetRequest... facetRequests) {
    this(indexingParams, Arrays.asList(facetRequests));
  }

  /**
   * Initializes with the given {@link FacetRequest requests} and
   * {@link FacetIndexingParams}.
   */
  public FacetSearchParams(FacetIndexingParams indexingParams, List<FacetRequest> facetRequests) {
    if (facetRequests == null || facetRequests.size() == 0) {
      throw new IllegalArgumentException("at least one FacetRequest must be defined");
    }
    this.facetRequests = facetRequests;
    this.indexingParams = indexingParams;
  }
  
  @Override
  public String toString() {
    final String INDENT = "  ";
    final char NEWLINE = '\n';

    StringBuilder sb = new StringBuilder("IndexingParams: ");
    sb.append(NEWLINE).append(INDENT).append(indexingParams);
    
    sb.append(NEWLINE).append("FacetRequests:");
    for (FacetRequest facetRequest : facetRequests) {
      sb.append(NEWLINE).append(INDENT).append(facetRequest);
    }
    
    return sb.toString();
  }
  
}
