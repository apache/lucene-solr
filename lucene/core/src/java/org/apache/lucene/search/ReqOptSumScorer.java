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

/** A Scorer for queries with a required part and an optional part.
 * Delays skipTo() on the optional part until a score() is needed.
 */
class ReqOptSumScorer extends Scorer {
  /** The scorers passed from the constructor.
   * These are set to null as soon as their next() or skipTo() returns false.
   */
  private final Scorer reqScorer;
  private final Scorer optScorer;
  private final float reqMaxScore;
  private final DocIdSetIterator optApproximation;
  private final TwoPhaseIterator optTwoPhase;
  private boolean optIsRequired;
  private final DocIdSetIterator approximation;
  private final TwoPhaseIterator twoPhase;
  final MaxScoreSumPropagator maxScorePropagator;

  /** Construct a <code>ReqOptScorer</code>.
   * @param reqScorer The required scorer. This must match.
   * @param optScorer The optional scorer. This is used for scoring only.
   */
  public ReqOptSumScorer(
      Scorer reqScorer,
      Scorer optScorer) throws IOException
  {
    super(reqScorer.weight);
    assert reqScorer != null;
    assert optScorer != null;
    this.reqScorer = reqScorer;
    this.optScorer = optScorer;

    this.reqMaxScore = reqScorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS);
    this.maxScorePropagator = new MaxScoreSumPropagator(Arrays.asList(reqScorer, optScorer));

    final TwoPhaseIterator reqTwoPhase = reqScorer.twoPhaseIterator();
    this.optTwoPhase = optScorer.twoPhaseIterator();
    final DocIdSetIterator reqApproximation;
    if (reqTwoPhase == null) {
      reqApproximation = reqScorer.iterator();
    } else {
      reqApproximation= reqTwoPhase.approximation();
    }
    if (optTwoPhase == null) {
      optApproximation = optScorer.iterator();
    } else {
      optApproximation= optTwoPhase.approximation();
    }

    approximation = new DocIdSetIterator() {

      private int nextCommonDoc(int reqDoc) throws IOException {
        int optDoc = optApproximation.docID();
        if (optDoc > reqDoc) {
          reqDoc = reqApproximation.advance(optDoc);
        }

        while (true) { // invariant: reqDoc >= optDoc
          if (reqDoc == optDoc) {
            return reqDoc;
          }

          optDoc = optApproximation.advance(reqDoc);
          if (optDoc == reqDoc) {
            return reqDoc;
          }
          reqDoc = reqApproximation.advance(optDoc);
        }
      }

      @Override
      public int nextDoc() throws IOException {
        int doc = reqApproximation.nextDoc();
        if (optIsRequired) {
          doc = nextCommonDoc(doc);
        }
        return doc;
      }

      @Override
      public int advance(int target) throws IOException {
        int doc = reqApproximation.advance(target);
        if (optIsRequired) {
          doc = nextCommonDoc(doc);
        }
        return doc;
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
  public float getMaxScore(int upTo) throws IOException {
    float maxScore = reqScorer.getMaxScore(upTo);
    if (optScorer.docID() <= upTo) {
      maxScore += optScorer.getMaxScore(upTo);
    }
    return maxScore;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) {
    // Potentially move to a conjunction
    if (optIsRequired == false && minScore > reqMaxScore) {
      optIsRequired = true;
    }
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<>(2);
    children.add(new ChildScorer(reqScorer, "MUST"));
    children.add(new ChildScorer(optScorer, "SHOULD"));
    return children;
  }

}
