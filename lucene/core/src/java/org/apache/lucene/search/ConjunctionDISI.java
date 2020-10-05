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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.search.spans.Spans;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.CollectionUtil;

/** A conjunction of DocIdSetIterators.
 * This iterates over the doc ids that are present in each given DocIdSetIterator.
 * <br>Public only for use in {@link org.apache.lucene.search.spans}.
 * @lucene.internal
 */
public final class ConjunctionDISI extends DocIdSetIterator {

  /** Create a conjunction over the provided {@link Scorer}s. Note that the
   * returned {@link DocIdSetIterator} might leverage two-phase iteration in
   * which case it is possible to retrieve the {@link TwoPhaseIterator} using
   * {@link TwoPhaseIterator#unwrap}. */
  public static DocIdSetIterator intersectScorers(Collection<Scorer> scorers) {
    if (scorers.size() < 2) {
      throw new IllegalArgumentException("Cannot make a ConjunctionDISI of less than 2 iterators");
    }
    final List<DocIdSetIterator> allIterators = new ArrayList<>();
    final List<TwoPhaseIterator> twoPhaseIterators = new ArrayList<>();
    for (Scorer scorer : scorers) {
      addScorer(scorer, allIterators, twoPhaseIterators);
    }

    return createConjunction(allIterators, twoPhaseIterators);
  }

  /** Create a conjunction over the provided DocIdSetIterators. Note that the
   * returned {@link DocIdSetIterator} might leverage two-phase iteration in
   * which case it is possible to retrieve the {@link TwoPhaseIterator} using
   * {@link TwoPhaseIterator#unwrap}. */
  public static DocIdSetIterator intersectIterators(List<DocIdSetIterator> iterators) {
    if (iterators.size() < 2) {
      throw new IllegalArgumentException("Cannot make a ConjunctionDISI of less than 2 iterators");
    }
    final List<DocIdSetIterator> allIterators = new ArrayList<>();
    final List<TwoPhaseIterator> twoPhaseIterators = new ArrayList<>();
    for (DocIdSetIterator iterator : iterators) {
      addIterator(iterator, allIterators, twoPhaseIterators);
    }

    return createConjunction(allIterators, twoPhaseIterators);
  }

  /** Create a conjunction over the provided {@link Spans}. Note that the
   * returned {@link DocIdSetIterator} might leverage two-phase iteration in
   * which case it is possible to retrieve the {@link TwoPhaseIterator} using
   * {@link TwoPhaseIterator#unwrap}. */
  public static DocIdSetIterator intersectSpans(List<Spans> spanList) {
    if (spanList.size() < 2) {
      throw new IllegalArgumentException("Cannot make a ConjunctionDISI of less than 2 iterators");
    }
    final List<DocIdSetIterator> allIterators = new ArrayList<>();
    final List<TwoPhaseIterator> twoPhaseIterators = new ArrayList<>();
    for (Spans spans : spanList) {
      addSpans(spans, allIterators, twoPhaseIterators);
    }

    return createConjunction(allIterators, twoPhaseIterators);
  }

  /** Adds the scorer, possibly splitting up into two phases or collapsing if it is another conjunction */
  private static void addScorer(Scorer scorer, List<DocIdSetIterator> allIterators, List<TwoPhaseIterator> twoPhaseIterators) {
    TwoPhaseIterator twoPhaseIter = scorer.twoPhaseIterator();
    if (twoPhaseIter != null) {
      addTwoPhaseIterator(twoPhaseIter, allIterators, twoPhaseIterators);
    } else { // no approximation support, use the iterator as-is
      addIterator(scorer.iterator(), allIterators, twoPhaseIterators);
    }
  }

  /** Adds the Spans. */
  private static void addSpans(Spans spans, List<DocIdSetIterator> allIterators, List<TwoPhaseIterator> twoPhaseIterators) {
    TwoPhaseIterator twoPhaseIter = spans.asTwoPhaseIterator();
    if (twoPhaseIter != null) {
      addTwoPhaseIterator(twoPhaseIter, allIterators, twoPhaseIterators);
    } else { // no approximation support, use the iterator as-is
      addIterator(spans, allIterators, twoPhaseIterators);
    }
  }

  private static void addIterator(DocIdSetIterator disi, List<DocIdSetIterator> allIterators, List<TwoPhaseIterator> twoPhaseIterators) {
    TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(disi);
    if (twoPhase != null) {
      addTwoPhaseIterator(twoPhase, allIterators, twoPhaseIterators);
    } else if (disi.getClass() == ConjunctionDISI.class) { // Check for exactly this class for collapsing
      ConjunctionDISI conjunction = (ConjunctionDISI) disi;
      // subconjuctions have already split themselves into two phase iterators and others, so we can take those
      // iterators as they are and move them up to this conjunction
      allIterators.add(conjunction.lead1);
      allIterators.add(conjunction.lead2);
      Collections.addAll(allIterators, conjunction.others);
    } else if (disi.getClass() == BitSetConjunctionDISI.class) {
      BitSetConjunctionDISI conjunction = (BitSetConjunctionDISI) disi;
      allIterators.add(conjunction.lead);
      Collections.addAll(allIterators, conjunction.bitSetIterators);
    } else {
      allIterators.add(disi);
    }
  }

  private static void addTwoPhaseIterator(TwoPhaseIterator twoPhaseIter, List<DocIdSetIterator> allIterators, List<TwoPhaseIterator> twoPhaseIterators) {
    addIterator(twoPhaseIter.approximation(), allIterators, twoPhaseIterators);
    if (twoPhaseIter.getClass() == ConjunctionTwoPhaseIterator.class) { // Check for exactly this class for collapsing
      Collections.addAll(twoPhaseIterators, ((ConjunctionTwoPhaseIterator) twoPhaseIter).twoPhaseIterators);
    } else {
      twoPhaseIterators.add(twoPhaseIter);
    }
  }

  private static DocIdSetIterator createConjunction(
      List<DocIdSetIterator> allIterators,
      List<TwoPhaseIterator> twoPhaseIterators) {
    long minCost = allIterators.stream().mapToLong(DocIdSetIterator::cost).min().getAsLong();
    List<BitSetIterator> bitSetIterators = new ArrayList<>();
    List<DocIdSetIterator> iterators = new ArrayList<>();
    for (DocIdSetIterator iterator : allIterators) {
      if (iterator.cost() > minCost && iterator instanceof BitSetIterator) {
        // we put all bitset iterators into bitSetIterators
        // except if they have the minimum cost, since we need
        // them to lead the iteration in that case
        bitSetIterators.add((BitSetIterator) iterator);
      } else {
        iterators.add(iterator);
      }
    }

    DocIdSetIterator disi;
    if (iterators.size() == 1) {
      disi = iterators.get(0);
    } else {
      disi = new ConjunctionDISI(iterators);
    }

    if (bitSetIterators.size() > 0) {
      disi = new BitSetConjunctionDISI(disi, bitSetIterators);
    }

    if (twoPhaseIterators.isEmpty() == false) {
      disi = TwoPhaseIterator.asDocIdSetIterator(new ConjunctionTwoPhaseIterator(disi, twoPhaseIterators));
    }

    return disi;
  }

  final DocIdSetIterator lead1, lead2;
  final DocIdSetIterator[] others;

  private ConjunctionDISI(List<? extends DocIdSetIterator> iterators) {
    assert iterators.size() >= 2;
    // Sort the array the first time to allow the least frequent DocsEnum to
    // lead the matching.
    CollectionUtil.timSort(iterators, new Comparator<DocIdSetIterator>() {
      @Override
      public int compare(DocIdSetIterator o1, DocIdSetIterator o2) {
        return Long.compare(o1.cost(), o2.cost());
      }
    });
    lead1 = iterators.get(0);
    lead2 = iterators.get(1);
    others = iterators.subList(2, iterators.size()).toArray(new DocIdSetIterator[0]);
  }

  private int doNext(int doc) throws IOException {
    advanceHead: for(;;) {
      assert doc == lead1.docID();

      // find agreement between the two iterators with the lower costs
      // we special case them because they do not need the
      // 'other.docID() < doc' check that the 'others' iterators need
      final int next2 = lead2.advance(doc);
      if (next2 != doc) {
        doc = lead1.advance(next2);
        if (next2 != doc) {
          continue;
        }
      }

      // then find agreement with other iterators
      for (DocIdSetIterator other : others) {
        // other.doc may already be equal to doc if we "continued advanceHead"
        // on the previous iteration and the advance on the lead scorer exactly matched.
        if (other.docID() < doc) {
          final int next = other.advance(doc);

          if (next > doc) {
            // iterator beyond the current doc - advance lead and continue to the new highest doc.
            doc = lead1.advance(next);
            continue advanceHead;
          }
        }
      }

      // success - all iterators are on the same doc
      return doc;
    }
  }

  @Override
  public int advance(int target) throws IOException {
    return doNext(lead1.advance(target));
  }

  @Override
  public int docID() {
    return lead1.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return doNext(lead1.nextDoc());
  }

  @Override
  public long cost() {
    return lead1.cost(); // overestimate
  }

  /** Conjunction between a {@link DocIdSetIterator} and one or more {@link BitSetIterator}s. */
  private static class BitSetConjunctionDISI extends DocIdSetIterator {

    private final DocIdSetIterator lead;
    private final BitSetIterator[] bitSetIterators;
    private final BitSet[] bitSets;
    private final int minLength;

    BitSetConjunctionDISI(DocIdSetIterator lead, Collection<BitSetIterator> bitSetIterators) {
      this.lead = lead;
      assert bitSetIterators.size() > 0;
      this.bitSetIterators = bitSetIterators.toArray(new BitSetIterator[0]);
      // Put the least costly iterators first so that we exit as soon as possible
      ArrayUtil.timSort(this.bitSetIterators, (a, b) -> Long.compare(a.cost(), b.cost()));
      this.bitSets = new BitSet[this.bitSetIterators.length];
      int minLen = Integer.MAX_VALUE;
      for (int i = 0; i < this.bitSetIterators.length; ++i) {
        BitSet bitSet = this.bitSetIterators[i].getBitSet();
        this.bitSets[i] = bitSet;
        minLen = Math.min(minLen, bitSet.length());
      }
      this.minLength = minLen;
    }

    @Override
    public int docID() {
      return lead.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return doNext(lead.nextDoc());
    }

    @Override
    public int advance(int target) throws IOException {
      return doNext(lead.advance(target));
    }

    private int doNext(int doc) throws IOException {
      advanceLead: for (;; doc = lead.nextDoc()) {
        if (doc >= minLength) {
          return NO_MORE_DOCS;
        }
        for (BitSet bitSet : bitSets) {
          if (bitSet.get(doc) == false) {
            continue advanceLead;
          }
        }
        for (BitSetIterator iterator : bitSetIterators) {
          iterator.setDocId(doc);
        }
        return doc;
      }
    }

    @Override
    public long cost() {
      return lead.cost();
    }

  }

  /**
   * {@link TwoPhaseIterator} implementing a conjunction.
   */
  private static final class ConjunctionTwoPhaseIterator extends TwoPhaseIterator {

    private final TwoPhaseIterator[] twoPhaseIterators;
    private final float matchCost;

    private ConjunctionTwoPhaseIterator(DocIdSetIterator approximation,
        List<? extends TwoPhaseIterator> twoPhaseIterators) {
      super(approximation);
      assert twoPhaseIterators.size() > 0;

      CollectionUtil.timSort(twoPhaseIterators, new Comparator<TwoPhaseIterator>() {
        @Override
        public int compare(TwoPhaseIterator o1, TwoPhaseIterator o2) {
          return Float.compare(o1.matchCost(), o2.matchCost());
        }
      });

      this.twoPhaseIterators = twoPhaseIterators.toArray(new TwoPhaseIterator[twoPhaseIterators.size()]);

      // Compute the matchCost as the total matchCost of the sub iterators.
      // TODO: This could be too high because the matching is done cheapest first: give the lower matchCosts a higher weight.
      float totalMatchCost = 0;
      for (TwoPhaseIterator tpi : twoPhaseIterators) {
        totalMatchCost += tpi.matchCost();
      }
      matchCost = totalMatchCost;
    }

    @Override
    public boolean matches() throws IOException {
      for (TwoPhaseIterator twoPhaseIterator : twoPhaseIterators) { // match cheapest first
        if (twoPhaseIterator.matches() == false) {
          return false;
        }
      }
      return true;
    }

    @Override
    public float matchCost() {
      return matchCost;
    }

  }

}
