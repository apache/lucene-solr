package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.FacetIndexingParams;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;

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
 * A {@link FacetsAggregator} which invokes the proper aggregator per
 * {@link CategoryListParams}.
 */
public class PerCategoryListAggregator implements FacetsAggregator {
  
  private final Map<CategoryListParams,FacetsAggregator> aggregators;
  private final FacetIndexingParams fip;
  
  public PerCategoryListAggregator(Map<CategoryListParams,FacetsAggregator> aggregators, FacetIndexingParams fip) {
    this.aggregators = aggregators;
    this.fip = fip;
  }
  
  @Override
  public void aggregate(MatchingDocs matchingDocs, CategoryListParams clp, FacetArrays facetArrays) throws IOException {
    aggregators.get(clp).aggregate(matchingDocs, clp, facetArrays);
  }
  
  @Override
  public void rollupValues(FacetRequest fr, int ordinal, int[] children, int[] siblings, FacetArrays facetArrays) {
    CategoryListParams clp = fip.getCategoryListParams(fr.categoryPath);
    aggregators.get(clp).rollupValues(fr, ordinal, children, siblings, facetArrays);
  }
  
  @Override
  public boolean requiresDocScores() {
    for (FacetsAggregator aggregator : aggregators.values()) {
      if (aggregator.requiresDocScores()) {
        return true;
      }
    }
    return false;
  }
  
}
