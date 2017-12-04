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


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.LeafReaderContext;
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
  public SimWeight computeWeight(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    SimWeight subStats[] = new SimWeight[sims.length];
    for (int i = 0; i < subStats.length; i++) {
      subStats[i] = sims[i].computeWeight(boost, collectionStats, termStats);
    }
    return new MultiStats(subStats);
  }

  @Override
  public SimScorer simScorer(SimWeight stats, LeafReaderContext context) throws IOException {
    SimScorer subScorers[] = new SimScorer[sims.length];
    for (int i = 0; i < subScorers.length; i++) {
      subScorers[i] = sims[i].simScorer(((MultiStats)stats).subStats[i], context);
    }
    return new MultiSimScorer(subScorers);
  }
  
  static class MultiSimScorer extends SimScorer {
    private final SimScorer subScorers[];
    
    MultiSimScorer(SimScorer subScorers[]) {
      this.subScorers = subScorers;
    }
    
    @Override
    public float score(int doc, float freq) throws IOException {
      float sum = 0.0f;
      for (SimScorer subScorer : subScorers) {
        sum += subScorer.score(doc, freq);
      }
      return sum;
    }

    @Override
    public Explanation explain(int doc, Explanation freq) throws IOException {
      List<Explanation> subs = new ArrayList<>();
      for (SimScorer subScorer : subScorers) {
        subs.add(subScorer.explain(doc, freq));
      }
      return Explanation.match(score(doc, freq.getValue()), "sum of:", subs);
    }

  }

  static class MultiStats extends SimWeight {
    final SimWeight subStats[];
    
    MultiStats(SimWeight subStats[]) {
      this.subStats = subStats;
    }
  }
}
