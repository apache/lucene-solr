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

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;

class IntervalScorer extends Scorer {

  private final IntervalIterator intervals;
  private final Similarity.SimScorer simScorer;
  private final float boost;
  private final int minExtent;

  private float freq;
  private int lastScoredDoc = -1;

  IntervalScorer(
      Weight weight,
      IntervalIterator intervals,
      int minExtent,
      float boost,
      IntervalScoreFunction scoreFunction) {
    super(weight);
    this.intervals = intervals;
    this.minExtent = minExtent;
    this.boost = boost;
    this.simScorer = scoreFunction.scorer(boost);
  }

  @Override
  public int docID() {
    return intervals.docID();
  }

  @Override
  public float score() throws IOException {
    ensureFreq();
    return simScorer.score(freq, 1);
  }

  float freq() throws IOException {
    ensureFreq();
    return freq;
  }

  private void ensureFreq() throws IOException {
    if (lastScoredDoc != docID()) {
      lastScoredDoc = docID();
      freq = 0;
      do {
        int length = (intervals.end() - intervals.start() + 1);
        freq += 1.0 / Math.max(length - minExtent + 1, 1);
      } while (intervals.nextInterval() != IntervalIterator.NO_MORE_INTERVALS);
    }
  }

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return new TwoPhaseIterator(intervals) {
      @Override
      public boolean matches() throws IOException {
        return intervals.nextInterval() != IntervalIterator.NO_MORE_INTERVALS;
      }

      @Override
      public float matchCost() {
        return intervals.matchCost();
      }
    };
  }

  @Override
  public float getMaxScore(int upTo) {
    return boost;
  }
}
