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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** A Scorer for queries with a required part and an optional part.
 * Delays skipTo() on the optional part until a score() is needed.
 */
class ReqOptSumScorer extends Scorer {
  private final Scorer reqScorer;
  private final Scorer optScorer;
  private final DocIdSetIterator reqApproximation;
  private final DocIdSetIterator optApproximation;
  private final TwoPhaseIterator optTwoPhase;
  private final DocIdSetIterator approximation;
  private final TwoPhaseIterator twoPhase;

  private final MaxScoreSumPropagator maxScorePropagator;
  private float minScore = 0;
  private float reqMaxScore;
  private boolean optIsRequired;

  /**
   * Construct a <code>ReqOptScorer</code>.
   *
   * @param reqScorer The required scorer. This must match.
   * @param optScorer The optional scorer. This is used for scoring only.
   * @param scoreMode  How the produced scorers will be consumed.
   */
  public ReqOptSumScorer(Scorer reqScorer, Scorer optScorer, ScoreMode scoreMode) throws IOException {
    super(reqScorer.weight);
    assert reqScorer != null;
    assert optScorer != null;
    if (scoreMode == ScoreMode.TOP_SCORES) {
      this.maxScorePropagator = new MaxScoreSumPropagator(Arrays.asList(reqScorer, optScorer));
    } else {
      this.maxScorePropagator = null;
    }
    this.reqScorer = reqScorer;
    this.optScorer = optScorer;

    final TwoPhaseIterator reqTwoPhase = reqScorer.twoPhaseIterator();
    this.optTwoPhase = optScorer.twoPhaseIterator();
    if (reqTwoPhase == null) {
      reqApproximation = reqScorer.iterator();
    } else {
      reqApproximation = reqTwoPhase.approximation();
    }
    if (optTwoPhase == null) {
      optApproximation = optScorer.iterator();
    } else {
      optApproximation = optTwoPhase.approximation();
    }
    if (scoreMode != ScoreMode.TOP_SCORES) {
      approximation = reqApproximation;
      this.reqMaxScore = Float.POSITIVE_INFINITY;
    } else {
      reqScorer.advanceShallow(0);
      optScorer.advanceShallow(0);
      this.reqMaxScore = reqScorer.getMaxScore(NO_MORE_DOCS);
      this.approximation = new DocIdSetIterator() {
        int upTo = -1;
        float maxScore;

        private void moveToNextBlock(int target) throws IOException {
          upTo = advanceShallow(target);
          float reqMaxScoreBlock = reqScorer.getMaxScore(upTo);
          maxScore = getMaxScore(upTo);

          // Potentially move to a conjunction
          optIsRequired = reqMaxScoreBlock < minScore;
        }

        private int advanceImpacts(int target) throws IOException {
          if (target > upTo) {
            moveToNextBlock(target);
          }

          while (true) {
            if (maxScore >= minScore) {
              return target;
            }

            if (upTo == NO_MORE_DOCS) {
              return NO_MORE_DOCS;
            }

            target = upTo + 1;

            moveToNextBlock(target);
          }
        }

        @Override
        public int nextDoc() throws IOException {
          return advanceInternal(reqApproximation.docID()+1);
        }

        @Override
        public int advance(int target) throws IOException {
          return advanceInternal(target);
        }

        private int advanceInternal(int target) throws IOException {
          if (target == NO_MORE_DOCS) {
            reqApproximation.advance(target);
            return NO_MORE_DOCS;
          }
          int reqDoc = target;
          advanceHead: for (;;) {
            if (minScore != 0) {
              reqDoc = advanceImpacts(reqDoc);
            }
            if (reqApproximation.docID() < reqDoc) {
              reqDoc = reqApproximation.advance(reqDoc);
            }
            if (reqDoc == NO_MORE_DOCS || optIsRequired == false) {
              return reqDoc;
            }

            int upperBound = reqMaxScore < minScore ? NO_MORE_DOCS : upTo;
            if (reqDoc > upperBound) {
              continue;
            }

            // Find the next common doc within the current block
            for (;;) { // invariant: reqDoc >= optDoc
              int optDoc = optApproximation.docID();
              if (optDoc < reqDoc) {
                optDoc = optApproximation.advance(reqDoc);
              }
              if (optDoc > upperBound) {
                reqDoc = upperBound + 1;
                continue advanceHead;
              }

              if (optDoc != reqDoc) {
                reqDoc = reqApproximation.advance(optDoc);
                if (reqDoc > upperBound) {
                  continue advanceHead;
                }
              }

              if (reqDoc == NO_MORE_DOCS || optDoc == reqDoc) {
                return reqDoc;
              }
            }
          }
        }

        @Override
        public int docID() {
          return reqApproximation.docID();
        }

        @Override
        public long cost() {
          return reqApproximation.cost();
        }
      };
    }

    if (reqTwoPhase == null && optTwoPhase == null) {
      this.twoPhase = null;
    } else {
      this.twoPhase = new TwoPhaseIterator(approximation) {

        @Override
        public boolean matches() throws IOException {
          if (reqTwoPhase != null && reqTwoPhase.matches() == false) {
            return false;
          }
          if (optTwoPhase != null) {
            if (optIsRequired) {
              // The below condition is rare and can only happen if we transitioned to optIsRequired=true
              // after the opt approximation was advanced and before it was confirmed.
              if (reqScorer.docID() != optApproximation.docID()) {
                if (optApproximation.docID() < reqScorer.docID()) {
                  optApproximation.advance(reqScorer.docID());
                }
                if (reqScorer.docID() != optApproximation.docID()) {
                  return false;
                }
              }
              if (optTwoPhase.matches() == false) {
                // Advance the iterator to make it clear it doesn't match the current doc id
                optApproximation.nextDoc();
                return false;
              }
            } else if (optApproximation.docID() == reqScorer.docID() && optTwoPhase.matches() == false) {
              // Advance the iterator to make it clear it doesn't match the current doc id
              optApproximation.nextDoc();
            }
          }
          return true;
        }

        @Override
        public float matchCost() {
          float matchCost = 1;
          if (reqTwoPhase != null) {
            matchCost += reqTwoPhase.matchCost();
          }
          if (optTwoPhase != null) {
            matchCost += optTwoPhase.matchCost();
          }
          return matchCost;
        }
      };
    }
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return twoPhase;
  }

  @Override
  public DocIdSetIterator iterator() {
    if (twoPhase == null) {
      return approximation;
    } else {
      return TwoPhaseIterator.asDocIdSetIterator(twoPhase);
    }
  }

  @Override
  public int docID() {
    return reqScorer.docID();
  }

  @Override
  public float score() throws IOException {
    // TODO: sum into a double and cast to float if we ever send required clauses to BS1
    int curDoc = reqScorer.docID();
    float score = reqScorer.score();

    int optScorerDoc = optApproximation.docID();
    if (optScorerDoc < curDoc) {
      optScorerDoc = optApproximation.advance(curDoc);
      if (optTwoPhase != null && optScorerDoc == curDoc && optTwoPhase.matches() == false) {
        optScorerDoc = optApproximation.nextDoc();
      }
    }
    if (optScorerDoc == curDoc) {
      score += optScorer.score();
    }

    return score;
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    int upTo = reqScorer.advanceShallow(target);
    if (optScorer.docID() <= target) {
      upTo = Math.min(upTo, optScorer.advanceShallow(target));
    } else if (optScorer.docID() != NO_MORE_DOCS) {
      upTo = Math.min(upTo, optScorer.docID() - 1);
    }
    return upTo;
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    float maxScore = reqScorer.getMaxScore(upTo);
    if (optScorer.docID() <= upTo) {
      maxScore += optScorer.getMaxScore(upTo);
    }
    return maxScore;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    this.minScore = minScore;
    // Potentially move to a conjunction
    if (reqMaxScore < minScore) {
      optIsRequired = true;
    }
    maxScorePropagator.setMinCompetitiveScore(minScore);
  }

  @Override
  public Collection<ChildScorable> getChildren() {
    ArrayList<ChildScorable> children = new ArrayList<>(2);
    children.add(new ChildScorable(reqScorer, "MUST"));
    children.add(new ChildScorable(optScorer, "SHOULD"));
    return children;
  }
}
