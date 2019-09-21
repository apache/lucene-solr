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
import java.util.Comparator;
import java.util.List;

/**
 * Scorer for conjunctions that checks the maximum scores of each clause in
 * order to potentially skip over blocks that can't have competitive matches.
 */
final class BlockMaxConjunctionScorer extends Scorer {
  final Scorer[] scorers;
  final DocIdSetIterator[] approximations;
  final TwoPhaseIterator[] twoPhases;
  final MaxScoreSumPropagator maxScorePropagator;
  float minScore;

  /** Create a new {@link BlockMaxConjunctionScorer} from scoring clauses. */
  BlockMaxConjunctionScorer(Weight weight, Collection<Scorer> scorersList) throws IOException {
    super(weight);
    this.scorers = scorersList.toArray(new Scorer[scorersList.size()]);
    // Sort scorer by cost
    Arrays.sort(this.scorers, Comparator.comparingLong(s -> s.iterator().cost()));
    this.maxScorePropagator = new MaxScoreSumPropagator(Arrays.asList(scorers));

    this.approximations = new DocIdSetIterator[scorers.length];
    List<TwoPhaseIterator> twoPhaseList = new ArrayList<>();
    for (int i = 0; i < scorers.length; i++) {
      Scorer scorer = scorers[i];
      TwoPhaseIterator twoPhase = scorer.twoPhaseIterator();
      if (twoPhase != null) {
        twoPhaseList.add(twoPhase);
        approximations[i] = twoPhase.approximation();
      } else {
        approximations[i] = scorer.iterator();
      }
      scorer.advanceShallow(0);
    }
    this.twoPhases = twoPhaseList.toArray(new TwoPhaseIterator[twoPhaseList.size()]);
    Arrays.sort(this.twoPhases, Comparator.comparingDouble(TwoPhaseIterator::matchCost));
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    if (twoPhases.length == 0) {
      return null;
    }
    float matchCost = (float) Arrays.stream(twoPhases)
        .mapToDouble(TwoPhaseIterator::matchCost)
        .sum();
    final DocIdSetIterator approx = approximation();
    return new TwoPhaseIterator(approx) {
      @Override
      public boolean matches() throws IOException {
        for (TwoPhaseIterator twoPhase : twoPhases) {
          assert twoPhase.approximation().docID() == docID();
          if (twoPhase.matches() == false) {
            return false;
          }
        }
        return true;
      }

      @Override
      public float matchCost() {
        return matchCost;
      }
    };
  }

  @Override
  public DocIdSetIterator iterator() {
    return twoPhases.length == 0 ? approximation() :
        TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  private DocIdSetIterator approximation() {
    final DocIdSetIterator lead = approximations[0];

    return new DocIdSetIterator() {

      float maxScore;
      int upTo = -1;

      @Override
      public int docID() {
        return lead.docID();
      }

      @Override
      public long cost() {
        return lead.cost();
      }

      private void moveToNextBlock(int target) throws IOException {
        upTo = advanceShallow(target);
        maxScore = getMaxScore(upTo);
      }

      private int advanceTarget(int target) throws IOException {
        if (target > upTo) {
          moveToNextBlock(target);
        }

        while (true) {
          assert upTo >= target;

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
        return advance(docID() + 1);
      }

      @Override
      public int advance(int target) throws IOException {
        return doNext(lead.advance(advanceTarget(target)));
      }

      private int doNext(int doc) throws IOException {
        advanceHead: for(;;) {
          assert doc == lead.docID();

          if (doc == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
          }

          if (doc > upTo) {
            // This check is useful when scorers return information about blocks
            // that do not actually have any matches. Otherwise `doc` will always
            // be in the current block already since it is always the result of
            // lead.advance(advanceTarget(some_doc_id))
            final int nextTarget = advanceTarget(doc);
            if (nextTarget != doc) {
              doc = lead.advance(nextTarget);
              continue;
            }
          }

          assert doc <= upTo;

          // then find agreement with other iterators
          for (int i = 1; i < approximations.length; ++i) {
            final DocIdSetIterator other = approximations[i];
            // other.doc may already be equal to doc if we "continued advanceHead"
            // on the previous iteration and the advance on the lead scorer exactly matched.
            if (other.docID() < doc) {
              final int next = other.advance(doc);

              if (next > doc) {
                // iterator beyond the current doc - advance lead and continue to the new highest doc.
                doc = lead.advance(advanceTarget(next));
                continue advanceHead;
              }
            }

            assert other.docID() == doc;
          }

          // success - all iterators are on the same doc and the score is competitive
          return doc;
        }
      }
    };
  }

  @Override
  public int docID() {
    return scorers[0].docID();
  }

  @Override
  public float score() throws IOException {
    double score = 0;
    for (Scorer scorer : scorers) {
      score += scorer.score();
    }
    return (float) score;
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    // We use block boundaries of the lead scorer.
    // It is tempting to fold in other clauses as well to have better bounds of
    // the score, but then there is a risk of not progressing fast enough.
    int result = scorers[0].advanceShallow(target);
    // But we still need to shallow-advance other clauses, in order to have
    // better score upper bounds
    for (int i = 1; i < scorers.length; ++i) {
      scorers[i].advanceShallow(target);
    }
    return result;
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    double sum = 0;
    for (Scorer scorer : scorers) {
      sum += scorer.getMaxScore(upTo);
    }
    return (float) sum;
  }

  @Override
  public void setMinCompetitiveScore(float score) throws IOException {
    minScore = score;
    maxScorePropagator.setMinCompetitiveScore(score);
  }

  @Override
  public Collection<ChildScorable> getChildren() {
    ArrayList<ChildScorable> children = new ArrayList<>();
    for (Scorer scorer : scorers) {
      children.add(new ChildScorable(scorer, "MUST"));
    }
    return children;
  }
}
