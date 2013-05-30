package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.facet.params.CategoryListParams;
import org.apache.lucene.facet.search.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
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
 * A {@link FacetsAggregator} which updates the weight of a category by summing the
 * scores of documents it was found in.
 */
public class SumScoreFacetsAggregator implements FacetsAggregator {
  
  private final IntsRef ordinals = new IntsRef(32);
  
  @Override
  public void aggregate(MatchingDocs matchingDocs, CategoryListParams clp, FacetArrays facetArrays) throws IOException {
    CategoryListIterator cli = clp.createCategoryListIterator(0);
    if (!cli.setNextReader(matchingDocs.context)) {
      return;
    }
    
    int doc = 0;
    int length = matchingDocs.bits.length();
    float[] scores = facetArrays.getFloatArray();
    int scoresIdx = 0;
    while (doc < length && (doc = matchingDocs.bits.nextSetBit(doc)) != -1) {
      cli.getOrdinals(doc, ordinals);
      int upto = ordinals.offset + ordinals.length;
      final float score = matchingDocs.scores[scoresIdx++];
      for (int i = ordinals.offset; i < upto; i++) {
        scores[ordinals.ints[i]] += score;
      }
      ++doc;
    }
  }
  
  private float rollupScores(int ordinal, int[] children, int[] siblings, float[] scores) {
    float score = 0f;
    while (ordinal != TaxonomyReader.INVALID_ORDINAL) {
      float childScore = scores[ordinal];
      childScore += rollupScores(children[ordinal], children, siblings, scores);
      scores[ordinal] = childScore;
      score += childScore;
      ordinal = siblings[ordinal];
    }
    return score;
  }

  @Override
  public void rollupValues(FacetRequest fr, int ordinal, int[] children, int[] siblings, FacetArrays facetArrays) {
    float[] scores = facetArrays.getFloatArray();
    scores[ordinal] += rollupScores(children[ordinal], children, siblings, scores);
  }
  
  @Override
  public boolean requiresDocScores() {
    return true;
  }
  
}
