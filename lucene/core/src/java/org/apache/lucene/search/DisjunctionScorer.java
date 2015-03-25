package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.search.ScorerPriorityQueue.ScorerWrapper;

/**
 * Base class for Scorers that score disjunctions.
 */
abstract class DisjunctionScorer extends Scorer {

  private final boolean needsScores;
  private final ScorerPriorityQueue subScorers;
  private final long cost;

  /** Linked list of scorers which are on the current doc */
  private ScorerWrapper topScorers;

  protected DisjunctionScorer(Weight weight, List<Scorer> subScorers, boolean needsScores) {
    super(weight);
    if (subScorers.size() <= 1) {
      throw new IllegalArgumentException("There must be at least 2 subScorers");
    }
    this.subScorers = new ScorerPriorityQueue(subScorers.size());
    long cost = 0;
    for (Scorer scorer : subScorers) {
      final ScorerWrapper w = new ScorerWrapper(scorer);
      cost += w.cost;
      this.subScorers.add(w);
    }
    this.cost = cost;
    this.needsScores = needsScores;
  }

  /**
   * A {@link DocIdSetIterator} which is a disjunction of the approximations of
   * the provided iterators.
   */
  private static class DisjunctionDISIApproximation extends DocIdSetIterator {

    final ScorerPriorityQueue subScorers;
    final long cost;

    DisjunctionDISIApproximation(ScorerPriorityQueue subScorers) {
      this.subScorers = subScorers;
      long cost = 0;
      for (ScorerWrapper w : subScorers) {
        cost += w.cost;
      }
      this.cost = cost;
    }

    @Override
    public long cost() {
      return cost;
    }

    @Override
    public int docID() {
     return subScorers.top().doc;
    }

    @Override
    public int nextDoc() throws IOException {
      ScorerWrapper top = subScorers.top();
      final int doc = top.doc;
      do {
        top.doc = top.approximation.nextDoc();
        top = subScorers.updateTop();
      } while (top.doc == doc);

      return top.doc;
    }

    @Override
    public int advance(int target) throws IOException {
      ScorerWrapper top = subScorers.top();
      do {
        top.doc = top.approximation.advance(target);
        top = subScorers.updateTop();
      } while (top.doc < target);

      return top.doc;
    }
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    boolean hasApproximation = false;
    for (ScorerWrapper w : subScorers) {
      if (w.twoPhaseView != null) {
        hasApproximation = true;
        break;
      }
    }

    if (hasApproximation == false) {
      // none of the sub scorers supports approximations
      return null;
    }

    // note it is important to share the same pq as this scorer so that
    // rebalancing the pq through the approximation will also rebalance
    // the pq in this scorer.
    return new TwoPhaseIterator(new DisjunctionDISIApproximation(subScorers)) {

      @Override
      public boolean matches() throws IOException {
        ScorerWrapper topScorers = subScorers.topList();
        // remove the head of the list as long as it does not match
        while (topScorers.twoPhaseView != null && topScorers.twoPhaseView.matches() == false) {
          topScorers = topScorers.next;
          if (topScorers == null) {
            return false;
          }
        }
        // now we know we have at least one match since the first element of 'matchList' matches
        if (needsScores) {
          // if scores or freqs are needed, we also need to remove scorers
          // from the top list that do not actually match
          ScorerWrapper previous = topScorers;
          for (ScorerWrapper w = topScorers.next; w != null; w = w.next) {
            if (w.twoPhaseView != null && w.twoPhaseView.matches() == false) {
              // w does not match, remove it
              previous.next = w.next;
            } else {
              previous = w;
            }
          }
        } else {
          // since we don't need scores, let's pretend we have a single match
          topScorers.next = null;
        }

        // We need to explicitely set the list of top scorers to avoid the
        // laziness of DisjunctionScorer.score() that would take all scorers
        // positioned on the same doc as the top of the pq, including
        // non-matching scorers
        DisjunctionScorer.this.topScorers = topScorers;
        return true;
      }
    };
  }

  @Override
  public final long cost() {
    return cost;
  }

  @Override
  public final int docID() {
   return subScorers.top().doc;
  }

  @Override
  public final int nextDoc() throws IOException {
    topScorers = null;
    ScorerWrapper top = subScorers.top();
    final int doc = top.doc;
    do {
      top.doc = top.scorer.nextDoc();
      top = subScorers.updateTop();
    } while (top.doc == doc);

    return top.doc;
  }

  @Override
  public final int advance(int target) throws IOException {
    topScorers = null;
    ScorerWrapper top = subScorers.top();
    do {
      top.doc = top.scorer.advance(target);
      top = subScorers.updateTop();
    } while (top.doc < target);

    return top.doc;
  }

  @Override
  public final int freq() throws IOException {
    if (topScorers == null) {
      topScorers = subScorers.topList();
    }
    int freq = 1;
    for (ScorerWrapper w = topScorers.next; w != null; w = w.next) {
      freq += 1;
    }
    return freq;
  }

  @Override
  public final float score() throws IOException {
    if (topScorers == null) {
      topScorers = subScorers.topList();
    }
    return score(topScorers);
  }

  /** Compute the score for the given linked list of scorers. */
  protected abstract float score(ScorerWrapper topList) throws IOException;

  @Override
  public final Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<>();
    for (ScorerWrapper scorer : subScorers) {
      children.add(new ChildScorer(scorer.scorer, "SHOULD"));
    }
    return children;
  }

}
