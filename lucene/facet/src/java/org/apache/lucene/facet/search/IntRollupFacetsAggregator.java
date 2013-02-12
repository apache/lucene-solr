package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

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
 * A {@link FacetsAggregator} which implements
 * {@link #rollupValues(FacetRequest, int, int[], int[], FacetArrays)} by
 * summing the values from {@link FacetArrays#getIntArray()}. Extending classes
 * should only implement {@link #aggregate}. Also, {@link #requiresDocScores()}
 * always returns false.
 * 
 * @lucene.experimental
 */
public abstract class IntRollupFacetsAggregator implements FacetsAggregator {
  
  @Override
  public abstract void aggregate(MatchingDocs matchingDocs, CategoryListParams clp, FacetArrays facetArrays) throws IOException;
  
  private int rollupValues(int ordinal, int[] children, int[] siblings, int[] values) {
    int value = 0;
    while (ordinal != TaxonomyReader.INVALID_ORDINAL) {
      int childValue = values[ordinal];
      childValue += rollupValues(children[ordinal], children, siblings, values);
      values[ordinal] = childValue;
      value += childValue;
      ordinal = siblings[ordinal];
    }
    return value;
  }

  @Override
  public final void rollupValues(FacetRequest fr, int ordinal, int[] children, int[] siblings, FacetArrays facetArrays) {
    final int[] values = facetArrays.getIntArray();
    values[ordinal] += rollupValues(children[ordinal], children, siblings, values);
  }
  
  @Override
  public final boolean requiresDocScores() {
    return false;
  }
  
}
