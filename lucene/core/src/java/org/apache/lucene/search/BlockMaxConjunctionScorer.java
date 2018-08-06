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

/**
 * Scorer for conjunctions that checks the maximum scores of each clause in
 * order to potentially skip over blocks that can'h have competitive matches.
 */
final class BlockMaxConjunctionScorer extends Scorer {

  final Scorer[] scorers;
  final MaxScoreSumPropagator maxScorePropagator;
  float minScore;
  final double[] minScores; // stores the min value of the sum of scores between 0..i for a hit to be competitive
  double score;

  /** Create a new {@link BlockMaxConjunctionScorer} from scoring clauses. */
  BlockMaxConjunctionScorer(Weight weight, Collection<Scorer> scorersList) throws IOException {
    super(weight);
    this.scorers = scorersList.toArray(new Scorer[scorersList.size()]);
    for (Scorer scorer : scorers) {
      scorer.advanceShallow(0);
    }
    this.maxScorePropagator = new MaxScoreSumPropagator(scorersList);

    // Put scorers with the higher max scores first
    // We tie-break on cost
    Comparator<Scorer> comparator = (s1, s2) -> {
      int cmp;
      try {
        cmp = Float.compare(s2.getMaxScore(DocIdSetIterator.NO_MORE_DOCS), s1.getMaxScore(DocIdSetIterator.NO_MORE_DOCS));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (cmp == 0) {
        cmp = Long.compare(s1.iterator().cost(), s2.iterator().cost());
      }
      return cmp;
    };
    Arrays.sort(this.scorers, comparator);
    minScores = new double[this.scorers.length];
  }

  @Override
  public DocIdSetIterator iterator() {
    // TODO: support two-phase
    final Scorer leadScorer = this.scorers[0]; // higher max score
    final DocIdSetIterator[] iterators = Arrays.stream(this.scorers)
        .map(Scorer::iterator)
        .toArray(DocIdSetIterator[]::new);
    final DocIdSetIterator lead = iterators[0];

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

        // Also compute the minimum required scores for a hit to be competitive
        // A double that is less than 'score' might still be converted to 'score'
        // when casted to a float, so we go to the previous float to avoid this issue
        minScores[minScores.length - 1] = minScore > 0 ? Math.nextDown(minScore) : 0;
        for (int i = scorers.length - 1; i > 0; --i) {
          double minScore = minScores[i];
          float clauseMaxScore = scorers[i].getMaxScore(upTo);
          if (minScore > clauseMaxScore) {
            minScores[i - 1] = minScore - clauseMaxScore;
            assert minScores[i - 1] + clauseMaxScore <= minScore;
          } else {
            minScores[i - 1] = 0;
          }
        }
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

          if (minScore > 0) {
            score = leadScorer.score();
            if (score < minScores[0]) {
              // computing a score is usually less costly than advancing other clauses
              doc = lead.advance(advanceTarget(doc + 1));
              continue;
            }
          }

          // then find agreement with other iterators
          for (int i = 1; i < iterators.length; ++i) {
            final DocIdSetIterator other = iterators[i];
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
            if (minScore > 0) {
              score += scorers[i].score();

              if (score < minScores[i]) {
                // computing a score is usually less costly than advancing the next clause
                doc = lead.advance(advanceTarget(doc + 1));
                continue advanceHead;
              }
            }
          }

          if (minScore > 0 == false) {
            // the score hasn't been computed on the fly, do it now
            score = 0;
            for (Scorer scorer : scorers) {
              score += scorer.score();
            }
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
  public void setMinCompetitiveScore(float score) {
    minScore = score;
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<>();
    for (Scorer scorer : scorers) {
      children.add(new ChildScorer(scorer, "MUST"));
    }
    return children;
  }

}
