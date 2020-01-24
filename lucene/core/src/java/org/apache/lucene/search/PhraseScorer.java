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

class PhraseScorer extends Scorer {

  final DocIdSetIterator approximation;
  final ImpactsDISI impactsApproximation;
  final PhraseMatcher matcher;
  final ScoreMode scoreMode;
  private final LeafSimScorer simScorer;
  final float matchCost;

  private float minCompetitiveScore = 0;
  private float freq = 0;

  PhraseScorer(Weight weight, PhraseMatcher matcher, ScoreMode scoreMode, LeafSimScorer simScorer) {
    super(weight);
    this.matcher = matcher;
    this.scoreMode = scoreMode;
    this.simScorer = simScorer;
    this.matchCost = matcher.getMatchCost();
    this.approximation = matcher.approximation();
    this.impactsApproximation = matcher.impactsApproximation();
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return new TwoPhaseIterator(approximation) {
      @Override
      public boolean matches() throws IOException {
        matcher.reset();
        if (scoreMode == ScoreMode.TOP_SCORES && minCompetitiveScore > 0) {
          float maxFreq = matcher.maxFreq();
          if (simScorer.score(docID(), maxFreq) < minCompetitiveScore) {
            // The maximum score we could get is less than the min competitive score
            return false;
          }
        }
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
    return approximation.docID();
  }

  @Override
  public float score() throws IOException {
    if (freq == 0) {
      freq = matcher.sloppyWeight();
      while (matcher.nextMatch()) {
        freq += matcher.sloppyWeight();
      }
    }
    return simScorer.score(docID(), freq);
  }

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public void setMinCompetitiveScore(float minScore) {
    this.minCompetitiveScore = minScore;
    impactsApproximation.setMinCompetitiveScore(minScore);
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    return impactsApproximation.advanceShallow(target);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return impactsApproximation.getMaxScore(upTo);
  }

  @Override
  public String toString() {
    return "PhraseScorer(" + weight + ")";
  }

}
