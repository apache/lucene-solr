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

package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.search.similarities.Similarity;

class PhraseScorer extends Scorer {

  final PhraseMatcher matcher;
  final boolean needsScores;
  private final Similarity.SimScorer simScorer;
  final float matchCost;

  private float freq = 0;

  PhraseScorer(Weight weight, PhraseMatcher matcher, boolean needsScores, Similarity.SimScorer simScorer) {
    super(weight);
    this.matcher = matcher;
    this.needsScores = needsScores;
    this.simScorer = simScorer;
    this.matchCost = matcher.getMatchCost();
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return new TwoPhaseIterator(matcher.approximation) {
      @Override
      public boolean matches() throws IOException {
        matcher.reset();
        freq = 0;
        return matcher.nextMatch();
      }

      @Override
      public float matchCost() {
        return matchCost;
      }
    };
  }

  @Override
  public int docID() {
    return matcher.approximation.docID();
  }

  @Override
  public float score() throws IOException {
    if (freq == 0) {
      freq = matcher.sloppyWeight(simScorer);
      while (matcher.nextMatch()) {
        freq += matcher.sloppyWeight(simScorer);
      }
    }
    return simScorer.score(docID(), freq);
  }

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public String toString() {
    return "PhraseScorer(" + weight + ")";
  }


}
