package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.util.IntsRef;

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
 * A {@link FacetsAggregator} which counts the number of times each category
 * appears in the given set of documents. This aggregator uses the
 * {@link CategoryListIterator} to read the encoded categories. If you used the
 * default settings while idnexing, you can use
 * {@link FastCountingFacetsAggregator} for better performance.
 * 
 * @lucene.experimental
 */
public class CountingFacetsAggregator extends IntRollupFacetsAggregator {
  
  private final IntsRef ordinals = new IntsRef(32);
  
  @Override
  public void aggregate(MatchingDocs matchingDocs, CategoryListParams clp, FacetArrays facetArrays) throws IOException {
    final CategoryListIterator cli = clp.createCategoryListIterator(0);
    if (!cli.setNextReader(matchingDocs.context)) {
      return;
    }
    
    final int length = matchingDocs.bits.length();
    final int[] counts = facetArrays.getIntArray();
    int doc = 0;
    while (doc < length && (doc = matchingDocs.bits.nextSetBit(doc)) != -1) {
      cli.getOrdinals(doc, ordinals);
      final int upto = ordinals.offset + ordinals.length;
      for (int i = ordinals.offset; i < upto; i++) {
        ++counts[ordinals.ints[i]];
      }
      ++doc;
    }
  }
  
}
