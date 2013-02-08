package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
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
 * Aggregates categories that were found in result documents (specified by
 * {@link MatchingDocs}). If the aggregator requires document scores too, it
 * should return {@code true} from {@link #requiresDocScores()}.
 * 
 * @lucene.experimental
 */
public interface FacetsAggregator {
  
  /** Aggregate the facets found in the given matching documents. */
  public void aggregate(MatchingDocs matchingDocs, CategoryListParams clp, FacetArrays facetArrays) throws IOException;
  
  /**
   * Rollup the values of the given ordinal. This method is called when a
   * category was indexed with {@link OrdinalPolicy#NO_PARENTS}. The given
   * ordinal is the requested category, and you should use the children and
   * siblings arrays to traverse its sub-tree.
   */
  public void rollupValues(FacetRequest fr, int ordinal, int[] children, int[] siblings, FacetArrays facetArrays);
  
  /** Returns {@code true} if this aggregator requires document scores. */
  public boolean requiresDocScores();
  
}
