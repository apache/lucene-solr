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

import static org.apache.lucene.search.DisiPriorityQueue.leftNode;
import static org.apache.lucene.search.DisiPriorityQueue.parentNode;
import static org.apache.lucene.search.DisiPriorityQueue.rightNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;

/**
 * This implements the WAND (Weak AND) algorithm for dynamic pruning
 * described in "Efficient Query Evaluation using a Two-Level Retrieval
 * Process" by Broder, Carmel, Herscovici, Soffer and Zien.
 * This scorer maintains a feedback loop with the collector in order to
 * know at any time the minimum score that is required in order for a hit
 * to be competitive. Then it leverages the {@link Scorer#maxScore() max score}
 * from each scorer in order to know when it may call
 * {@link DocIdSetIterator#advance} rather than {@link DocIdSetIterator#nextDoc}
 * to move to the next competitive hit.
 * Implementation is similar to {@link MinShouldMatchSumScorer} except that
 * instead of enforcing that {@code freq >= minShouldMatch}, we enforce that
 * {@code ∑ max_score >= minCompetitiveScore}.
 */
final class WANDScorer extends Scorer {

  /** Return a scaling factor for the given float so that
   *  f x 2^scalingFactor would be in ]2^15, 2^16]. Special
   *  cases:
   *    scalingFactor(0) = scalingFactor(MIN_VALUE) - 1
   *    scalingFactor(+Infty) = scalingFactor(MAX_VALUE) + 1
   */
  static int scalingFactor(float f) {
    if (f < 0) {
      throw new IllegalArgumentException("");
    } else if (f == 0) {
      return scalingFactor(Float.MIN_VALUE) - 1;
    } else if (Float.isInfinite(f)) {
      return scalingFactor(Float.MAX_VALUE) + 1;
    } else {
      double d = f;
      // Since doubles have more amplitude than floats for the
      // exponent, the cast produces a normal value.
      assert d == 0 || Math.getExponent(d) >= Double.MIN_EXPONENT; // normal double
      return 15 - Math.getExponent(Math.nextDown(d));
    }
  }

  /**
   * Scale max scores in an unsigned integer to avoid overflows
   * (only the lower 32 bits of the long are used) as well as
   * floating-point arithmetic errors. Those are rounded up in order
   * to make sure we do not miss any matches.
   */
  private static long scaleMaxScore(float maxScore, int scalingFactor) {
    assert Float.isNaN(maxScore) == false;
    assert maxScore >= 0;

    if (Float.isInfinite(maxScore)) {
      return (1L << 32) - 1; // means +Infinity in practice for this scorer
    }

    // NOTE: because doubles have more amplitude than floats for the
    // exponent, the scalb call produces an accurate value.
    double scaled = Math.scalb((double) maxScore, scalingFactor);
    assert scaled <= 1 << 16 : scaled + " " + maxScore; // regular values of max_score go into 0..2^16
    return (long) Math.ceil(scaled); // round up, cast is accurate since value is <= 2^16
  }

  /**
   * Scale min competitive scores the same way as max scores but this time
   * by rounding down in order to make sure that we do not miss any matches.
   */
  private static long scaleMinScore(float minScore, int scalingFactor) {
    assert Float.isNaN(minScore) == false;
    assert minScore >= 0;

    // like for scaleMaxScore, this scalb call is accurate
    double scaled = Math.scalb((double) minScore, scalingFactor);
    return (long) Math.floor(scaled); // round down, cast might lower the value again if scaled > Long.MAX_VALUE, which is fine
  }

  private final int scalingFactor;
  // scaled min competitive score
  private long minCompetitiveScore = 0;

  // list of scorers which 'lead' the iteration and are currently
  // positioned on 'doc'. This is sometimes called the 'pivot' in
  // some descriptions of WAND (Weak AND).
  DisiWrapper lead;
  int doc;  // current doc ID of the leads
  long leadMaxScore; // sum of the max scores of scorers in 'lead'

  // priority queue of scorers that are too advanced compared to the current
  // doc. Ordered by doc ID.
  final DisiPriorityQueue head;

  // priority queue of scorers which are behind the current doc.
  // Ordered by maxScore.
  final DisiWrapper[] tail;
  long tailMaxScore; // sum of the max scores of scorers in 'tail'
  int tailSize;

  final long cost;
  final MaxScoreSumPropagator maxScorePropagator;

  WANDScorer(Weight weight, Collection<Scorer> scorers) {
    super(weight);

    this.minCompetitiveScore = 0;
    this.doc = -1;

    head = new DisiPriorityQueue(scorers.size());
    // there can be at most num_scorers - 1 scorers beyond the current position
    tail = new DisiWrapper[scorers.size() - 1];

    OptionalInt scalingFactor = OptionalInt.empty();
    for (Scorer scorer : scorers) {
      float maxScore = scorer.maxScore();
      if (maxScore != 0 && Float.isFinite(maxScore)) {
        // 0 and +Infty should not impact the scale
        scalingFactor = OptionalInt.of(Math.min(scalingFactor.orElse(Integer.MAX_VALUE), scalingFactor(maxScore)));
      }
    }
    // Use a scaling factor of 0 if all max scores are either 0 or +Infty
    this.scalingFactor = scalingFactor.orElse(0);
    
    for (Scorer scorer : scorers) {
      DisiWrapper w = new DisiWrapper(scorer);
      float maxScore = scorer.maxScore();
      w.maxScore = scaleMaxScore(maxScore, this.scalingFactor);
      addLead(w);
    }

    long cost = 0;
    for (DisiWrapper w = lead; w != null; w = w.next) {
      cost += w.cost;
    }
    this.cost = cost;
    this.maxScorePropagator = new MaxScoreSumPropagator(scorers);
  }

  // returns a boolean so that it can be called from assert
  // the return value is useless: it always returns true
  private boolean ensureConsistent() {
    long maxScoreSum = 0;
    for (int i = 0; i < tailSize; ++i) {
      assert tail[i].doc < doc;
      maxScoreSum = Math.addExact(maxScoreSum, tail[i].maxScore);
    }
    assert maxScoreSum == tailMaxScore : maxScoreSum + " " + tailMaxScore;

    maxScoreSum = 0;
    for (DisiWrapper w = lead; w != null; w = w.next) {
      assert w.doc == doc;
      maxScoreSum = Math.addExact(maxScoreSum, w.maxScore);
    }
    assert maxScoreSum == leadMaxScore : maxScoreSum + " " + leadMaxScore;

    for (DisiWrapper w : head) {
      assert w.doc > doc;
    }

    assert tailSize == 0 || tailMaxScore < minCompetitiveScore;

    return true;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) {
    // Let this disjunction know about the new min score so that it can skip
    // over clauses that produce low scores.
    assert minScore >= 0;
    long scaledMinScore = scaleMinScore(minScore, scalingFactor);
    assert scaledMinScore >= minCompetitiveScore;
    minCompetitiveScore = scaledMinScore;

    // And also propagate to sub clauses.
    maxScorePropagator.setMinCompetitiveScore(minScore);
  }

  @Override
  public final Collection<ChildScorer> getChildren() throws IOException {
    List<ChildScorer> matchingChildren = new ArrayList<>();
    updateFreq();
    for (DisiWrapper s = lead; s != null; s = s.next) {
      matchingChildren.add(new ChildScorer(s.scorer, "SHOULD"));
    }
    return matchingChildren;
  }

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    DocIdSetIterator approximation = new DocIdSetIterator() {

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public int nextDoc() throws IOException {
        return advance(doc + 1);
      }

      @Override
      public int advance(int target) throws IOException {
        assert ensureConsistent();

        // Move 'lead' iterators back to the tail
        pushBackLeads(target);

        // Advance 'head' as well
        advanceHead(target);

        // Pop the new 'lead' from the 'head'
        setDocAndFreq();

        assert ensureConsistent();

        // Advance to the next possible match
        return doNextCandidate();
      }

      @Override
      public long cost() {
        return cost;
      }
    };
    return new TwoPhaseIterator(approximation) {

      @Override
      public boolean matches() throws IOException {
        while (leadMaxScore < minCompetitiveScore) {
          if (leadMaxScore + tailMaxScore >= minCompetitiveScore) {
            // a match on doc is still possible, try to
            // advance scorers from the tail
            advanceTail();
          } else {
            return false;
          }
        }
        return true;
      }

      @Override
      public float matchCost() {
        // maximum number of scorer that matches() might advance
        return tail.length;
      }

    };
  }

  private void addLead(DisiWrapper lead) {
    lead.next = this.lead;
    this.lead = lead;
    leadMaxScore += lead.maxScore;
  }

  private void pushBackLeads(int target) throws IOException {
    for (DisiWrapper s = lead; s != null; s = s.next) {
      final DisiWrapper evicted = insertTailWithOverFlow(s);
      if (evicted != null) {
        evicted.doc = evicted.iterator.advance(target);
        head.add(evicted);
      }
    }
  }

  private void advanceHead(int target) throws IOException {
    DisiWrapper headTop = head.top();
    while (headTop.doc < target) {
      final DisiWrapper evicted = insertTailWithOverFlow(headTop);
      if (evicted != null) {
        evicted.doc = evicted.iterator.advance(target);
        headTop = head.updateTop(evicted);
      } else {
        head.pop();
        headTop = head.top();
      }
    }
  }

  private void advanceTail(DisiWrapper disi) throws IOException {
    disi.doc = disi.iterator.advance(doc);
    if (disi.doc == doc) {
      addLead(disi);
    } else {
      head.add(disi);
    }
  }

  private void advanceTail() throws IOException {
    final DisiWrapper top = popTail();
    advanceTail(top);
  }

  /** Reinitializes head, freq and doc from 'head' */
  private void setDocAndFreq() {
    assert head.size() > 0;

    // The top of `head` defines the next potential match
    // pop all documents which are on this doc
    lead = head.pop();
    lead.next = null;
    leadMaxScore = lead.maxScore;
    doc = lead.doc;
    while (head.size() > 0 && head.top().doc == doc) {
      addLead(head.pop());
    }
  }

  /** Move iterators to the tail until there is a potential match. */
  private int doNextCandidate() throws IOException {
    while (leadMaxScore + tailMaxScore < minCompetitiveScore) {
      // no match on doc is possible, move to the next potential match
      if (head.size() == 0) {
        // special case: the total max score is less than the min competitive score, there are no more matches
        return doc = DocIdSetIterator.NO_MORE_DOCS;
      }
      pushBackLeads(doc + 1);
      setDocAndFreq();
      assert ensureConsistent();
    }

    return doc;
  }

  /** Advance all entries from the tail to know about all matches on the
   *  current doc. */
  private void updateFreq() throws IOException {
    // we return the next doc when the sum of the scores of the potential
    // matching clauses is high enough but some of the clauses in 'tail' might
    // match as well
    // in general we want to advance least-costly clauses first in order to
    // skip over non-matching documents as fast as possible. However here,
    // we are advancing everything anyway so iterating over clauses in
    // (roughly) cost-descending order might help avoid some permutations in
    // the head heap
    for (int i = tailSize - 1; i >= 0; --i) {
      advanceTail(tail[i]);
    }
    tailSize = 0;
    tailMaxScore = 0;
    assert ensureConsistent();
  }

  @Override
  public float score() throws IOException {
    // we need to know about all matches
    updateFreq();
    double score = 0;
    for (DisiWrapper s = lead; s != null; s = s.next) {
      score += s.scorer.score();
    }
    return (float) score;
  }

  @Override
  public float maxScore() {
    return maxScorePropagator.maxScore();
  }

  @Override
  public int docID() {
    return doc;
  }

  /** Insert an entry in 'tail' and evict the least-costly scorer if full. */
  private DisiWrapper insertTailWithOverFlow(DisiWrapper s) {
    if (tailSize < tail.length && tailMaxScore + s.maxScore < minCompetitiveScore) {
      // we have free room for this new entry
      addTail(s);
      tailMaxScore += s.maxScore;
      return null;
    } else if (tailSize == 0) {
      return s;
    } else {
      final DisiWrapper top = tail[0];
      if (greaterMaxScore(top, s) == false) {
        return s;
      }
      // Swap top and s
      tail[0] = s;
      downHeapMaxScore(tail, tailSize);
      tailMaxScore = tailMaxScore - top.maxScore + s.maxScore;
      return top;
    }
  }

  /** Add an entry to 'tail'. Fails if over capacity. */
  private void addTail(DisiWrapper s) {
    tail[tailSize] = s;
    upHeapMaxScore(tail, tailSize);
    tailSize += 1;
  }

  /** Pop the least-costly scorer from 'tail'. */
  private DisiWrapper popTail() {
    assert tailSize > 0;
    final DisiWrapper result = tail[0];
    tail[0] = tail[--tailSize];
    downHeapMaxScore(tail, tailSize);
    tailMaxScore -= result.maxScore;
    return result;
  }

  /** Heap helpers */

  private static void upHeapMaxScore(DisiWrapper[] heap, int i) {
    final DisiWrapper node = heap[i];
    int j = parentNode(i);
    while (j >= 0 && greaterMaxScore(node, heap[j])) {
      heap[i] = heap[j];
      i = j;
      j = parentNode(j);
    }
    heap[i] = node;
  }

  private static void downHeapMaxScore(DisiWrapper[] heap, int size) {
    int i = 0;
    final DisiWrapper node = heap[0];
    int j = leftNode(i);
    if (j < size) {
      int k = rightNode(j);
      if (k < size && greaterMaxScore(heap[k], heap[j])) {
        j = k;
      }
      if (greaterMaxScore(heap[j], node)) {
        do {
          heap[i] = heap[j];
          i = j;
          j = leftNode(i);
          k = rightNode(j);
          if (k < size && greaterMaxScore(heap[k], heap[j])) {
            j = k;
          }
        } while (j < size && greaterMaxScore(heap[j], node));
        heap[i] = node;
      }
    }
  }

  /**
   * In the tail, we want to get first entries that produce the maximum scores
   * and in case of ties (eg. constant-score queries), those that have the least
   * cost so that they are likely to advance further.
   */
  private static boolean greaterMaxScore(DisiWrapper w1, DisiWrapper w2) {
    if (w1.maxScore > w2.maxScore) {
      return true;
    } else if (w1.maxScore < w2.maxScore) {
      return false;
    } else {
      return w1.cost < w2.cost;
    }
  }

}
