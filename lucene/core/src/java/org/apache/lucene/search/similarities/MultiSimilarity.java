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
package org.apache.lucene.search.similarities;


import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;

/**
 * Implements the CombSUM method for combining evidence from multiple
 * similarity values described in: Joseph A. Shaw, Edward A. Fox. 
 * In Text REtrieval Conference (1993), pp. 243-252
 * @lucene.experimental
 */
public class MultiSimilarity extends Similarity {
  /** the sub-similarities used to create the combined score */
  protected final Similarity sims[];
  
  /** Creates a MultiSimilarity which will sum the scores
   * of the provided <code>sims</code>. */
  public MultiSimilarity(Similarity sims[]) {
    this.sims = sims;
  }
  
  @Override
  public long computeNorm(FieldInvertState state) {
    return sims[0].computeNorm(state);
  }

  @Override
  public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    SimScorer subScorers[] = new SimScorer[sims.length];
    for (int i = 0; i < subScorers.length; i++) {
      subScorers[i] = sims[i].scorer(boost, collectionStats, termStats);
    }
    return new MultiSimScorer(subScorers);
  }
  
  static class MultiSimScorer extends SimScorer {
    private final SimScorer subScorers[];
    
    MultiSimScorer(SimScorer subScorers[]) {
      this.subScorers = subScorers;
    }
    
    @Override
    public float score(float freq, long norm) {
      float sum = 0.0f;
      for (SimScorer subScorer : subScorers) {
        sum += subScorer.score(freq, norm);
      }
      return sum;
    }

    @Override
    public Explanation explain(Explanation freq, long norm) {
      List<Explanation> subs = new ArrayList<>();
      for (SimScorer subScorer : subScorers) {
        subs.add(subScorer.explain(freq, norm));
      }
      return Explanation.match(score(freq.getValue().floatValue(), norm), "sum of:", subs);
    }

  }
}
