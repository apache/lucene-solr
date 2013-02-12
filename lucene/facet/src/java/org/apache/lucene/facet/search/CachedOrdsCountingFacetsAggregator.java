package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.search.OrdinalsCache.CachedOrds;

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
 * A {@link FacetsAggregator} which updates categories values by counting their
 * occurrences in matching documents. Uses {@link OrdinalsCache} to obtain the
 * category ordinals of each segment.
 * 
 * @lucene.experimental
 */
public class CachedOrdsCountingFacetsAggregator extends IntRollupFacetsAggregator {
  
  @Override
  public void aggregate(MatchingDocs matchingDocs, CategoryListParams clp, FacetArrays facetArrays) throws IOException {
    final CachedOrds ords = OrdinalsCache.getCachedOrds(matchingDocs.context, clp);
    if (ords == null) {
      return; // this segment has no ordinals for the given category list
    }
    final int[] counts = facetArrays.getIntArray();
    int doc = 0;
    int length = matchingDocs.bits.length();
    while (doc < length && (doc = matchingDocs.bits.nextSetBit(doc)) != -1) {
      int start = ords.offsets[doc];
      int end = ords.offsets[doc + 1];
      for (int i = start; i < end; i++) {
        ++counts[ords.ordinals[i]];
      }
      ++doc;
    }
  }
  
}
