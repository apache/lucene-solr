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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestConjunctionDISI extends LuceneTestCase {

  private static TwoPhaseIterator approximation(DocIdSetIterator iterator, final FixedBitSet confirmed) {
    DocIdSetIterator approximation;
    if (random().nextBoolean()) {
      approximation = anonymizeIterator(iterator);
    } else {
      approximation = iterator;
    }
    return new TwoPhaseIterator(approximation) {

      @Override
      public boolean matches() throws IOException {
        return confirmed.get(approximation.docID());
      }

      @Override
      public float matchCost() {
        return 5; // #operations in FixedBitSet#get()
      }
    };
  }

  /** Return an anonym class so that ConjunctionDISI cannot optimize it
   *  like it does eg. for BitSetIterators. */
  private static DocIdSetIterator anonymizeIterator(DocIdSetIterator it) {
    return new DocIdSetIterator() {
      
      @Override
      public int nextDoc() throws IOException {
        return it.nextDoc();
      }
      
      @Override
      public int docID() {
        return it.docID();
      }
      
      @Override
      public long cost() {
        return it.docID();
      }
      
      @Override
      public int advance(int target) throws IOException {
        return it.advance(target);
      }
    };
  }

  private static Scorer scorer(TwoPhaseIterator twoPhaseIterator) {
    return scorer(TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator), twoPhaseIterator);
  }

  /**
   * Create a {@link Scorer} that wraps the given {@link DocIdSetIterator}. It
   * also accepts a {@link TwoPhaseIterator} view, which is exposed in
   * {@link Scorer#twoPhaseIterator()}. When the two-phase view is not null,
   * then {@link DocIdSetIterator#nextDoc()} and {@link DocIdSetIterator#advance(int)} will raise
   * an exception in order to make sure that {@link ConjunctionDISI} takes
   * advantage of the {@link TwoPhaseIterator} view.
   */
  private static Scorer scorer(DocIdSetIterator it, TwoPhaseIterator twoPhaseIterator) {
    return new Scorer(null) {

      @Override
      public DocIdSetIterator iterator() {
        return new DocIdSetIterator() {

          @Override
          public int docID() {
            return it.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            if (twoPhaseIterator != null) {
              throw new UnsupportedOperationException("ConjunctionDISI should call the two-phase iterator");
            }
            return it.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            if (twoPhaseIterator != null) {
              throw new UnsupportedOperationException("ConjunctionDISI should call the two-phase iterator");
            }
            return it.advance(target);
          }

          @Override
          public long cost() {
            if (twoPhaseIterator != null) {
              throw new UnsupportedOperationException("ConjunctionDISI should call the two-phase iterator");
            }
            return it.cost();
          }
        };
      }

      @Override
      public TwoPhaseIterator twoPhaseIterator() {
        return twoPhaseIterator;
      }

      @Override
      public int docID() {
        if (twoPhaseIterator != null) {
          throw new UnsupportedOperationException("ConjunctionDISI should call the two-phase iterator");
        }
        return it.docID();
      }

      @Override
      public float score() throws IOException {
        return 0;
      }

      @Override
      public int freq() throws IOException {
        return 0;
      }

    };
  }

  private static FixedBitSet randomSet(int maxDoc) {
    final int step = TestUtil.nextInt(random(), 1, 10);
    FixedBitSet set = new FixedBitSet(maxDoc);
    for (int doc = random().nextInt(step); doc < maxDoc; doc += TestUtil.nextInt(random(), 1, step)) {
      set.set(doc);
    }
    return set;
  }

  private static FixedBitSet clearRandomBits(FixedBitSet other) {
    final FixedBitSet set = new FixedBitSet(other.length());
    set.or(other);
    for (int i = 0; i < set.length(); ++i) {
      if (random().nextBoolean()) {
        set.clear(i);
      }
    }
    return set;
  }

  private static FixedBitSet intersect(FixedBitSet[] bitSets) {
    final FixedBitSet intersection = new FixedBitSet(bitSets[0].length());
    intersection.or(bitSets[0]);
    for (int i = 1; i < bitSets.length; ++i) {
      intersection.and(bitSets[i]);
    }
    return intersection;
  }

  private static FixedBitSet toBitSet(int maxDoc, DocIdSetIterator iterator) throws IOException {
    final FixedBitSet set = new FixedBitSet(maxDoc);
    for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
      set.set(doc);
    }
    return set;
  }

  // Test that the conjunction iterator is correct
  public void testConjunction() throws IOException {
    final int iters = atLeast(100);
    for (int iter = 0; iter < iters; ++iter) {
      final int maxDoc = TestUtil.nextInt(random(), 100, 10000);
      final int numIterators = TestUtil.nextInt(random(), 2, 5);
      final FixedBitSet[] sets = new FixedBitSet[numIterators];
      final Scorer[] iterators = new Scorer[numIterators];
      for (int i = 0; i < iterators.length; ++i) {
        final FixedBitSet set = randomSet(maxDoc);
        switch (random().nextInt(3)) {
          case 0:
            // simple iterator
            sets[i] = set;
            iterators[i] = new ConstantScoreScorer(null, 0f, anonymizeIterator(new BitDocIdSet(set).iterator()));
            break;
          case 1:
            // bitSet iterator
            sets[i] = set;
            iterators[i] = new ConstantScoreScorer(null, 0f, new BitDocIdSet(set).iterator());
            break;
          default:
            // scorer with approximation
            final FixedBitSet confirmed = clearRandomBits(set);
            sets[i] = confirmed;
            final TwoPhaseIterator approximation = approximation(new BitDocIdSet(set).iterator(), confirmed);
            iterators[i] = scorer(approximation);
            break;
        }
      }

      final DocIdSetIterator conjunction = ConjunctionDISI.intersectScorers(Arrays.asList(iterators));
      assertEquals(intersect(sets), toBitSet(maxDoc, conjunction));
    }
  }

  // Test that the conjunction approximation is correct
  public void testConjunctionApproximation() throws IOException {
    final int iters = atLeast(100);
    for (int iter = 0; iter < iters; ++iter) {
      final int maxDoc = TestUtil.nextInt(random(), 100, 10000);
      final int numIterators = TestUtil.nextInt(random(), 2, 5);
      final FixedBitSet[] sets = new FixedBitSet[numIterators];
      final Scorer[] iterators = new Scorer[numIterators];
      boolean hasApproximation = false;
      for (int i = 0; i < iterators.length; ++i) {
        final FixedBitSet set = randomSet(maxDoc);
        if (random().nextBoolean()) {
          // simple iterator
          sets[i] = set;
          iterators[i] = new ConstantScoreScorer(null, 0f, new BitDocIdSet(set).iterator());
        } else {
          // scorer with approximation
          final FixedBitSet confirmed = clearRandomBits(set);
          sets[i] = confirmed;
          final TwoPhaseIterator approximation = approximation(new BitDocIdSet(set).iterator(), confirmed);
          iterators[i] = scorer(approximation);
          hasApproximation = true;
        }
      }

      final DocIdSetIterator conjunction = ConjunctionDISI.intersectScorers(Arrays.asList(iterators));
      TwoPhaseIterator twoPhaseIterator = TwoPhaseIterator.unwrap(conjunction);
      assertEquals(hasApproximation, twoPhaseIterator != null);
      if (hasApproximation) {
        assertEquals(intersect(sets), toBitSet(maxDoc, TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator)));
      }
    }
  }

  // This test makes sure that when nesting scorers with ConjunctionDISI, confirmations are pushed to the root.
  public void testRecursiveConjunctionApproximation() throws IOException {
    final int iters = atLeast(100);
    for (int iter = 0; iter < iters; ++iter) {
      final int maxDoc = TestUtil.nextInt(random(), 100, 10000);
      final int numIterators = TestUtil.nextInt(random(), 2, 5);
      final FixedBitSet[] sets = new FixedBitSet[numIterators];
      Scorer conjunction = null;
      boolean hasApproximation = false;
      for (int i = 0; i < numIterators; ++i) {
        final FixedBitSet set = randomSet(maxDoc);
        final Scorer newIterator;
        switch (random().nextInt(3)) {
          case 0:
            // simple iterator
            sets[i] = set;
            newIterator = new ConstantScoreScorer(null, 0f, anonymizeIterator(new BitDocIdSet(set).iterator()));
            break;
          case 1:
            // bitSet iterator
            sets[i] = set;
            newIterator = new ConstantScoreScorer(null, 0f, new BitDocIdSet(set).iterator());
            break;
          default:
            // scorer with approximation
            final FixedBitSet confirmed = clearRandomBits(set);
            sets[i] = confirmed;
            final TwoPhaseIterator approximation = approximation(new BitDocIdSet(set).iterator(), confirmed);
            newIterator = scorer(approximation);
            hasApproximation = true;
            break;
        }
        if (conjunction == null) {
          conjunction = newIterator;
        } else {
          final DocIdSetIterator conj = ConjunctionDISI.intersectScorers(Arrays.asList(conjunction, newIterator));
          conjunction = scorer(conj, TwoPhaseIterator.unwrap(conj));
        }
      }

      TwoPhaseIterator twoPhaseIterator = conjunction.twoPhaseIterator();
      assertEquals(hasApproximation, twoPhaseIterator != null);
      if (hasApproximation) {
        assertEquals(intersect(sets), toBitSet(maxDoc, TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator)));
      } else {
        assertEquals(intersect(sets), toBitSet(maxDoc, conjunction.iterator()));
      }
    }
  }

  public void testCollapseSubConjunctions(boolean wrapWithScorer) throws IOException {
    final int iters = atLeast(100);
    for (int iter = 0; iter < iters; ++iter) {
      final int maxDoc = TestUtil.nextInt(random(), 100, 10000);
      final int numIterators = TestUtil.nextInt(random(), 5, 10);
      final FixedBitSet[] sets = new FixedBitSet[numIterators];
      final List<Scorer> scorers = new LinkedList<>();
      for (int i = 0; i < numIterators; ++i) {
        final FixedBitSet set = randomSet(maxDoc);
        if (random().nextBoolean()) {
          // simple iterator
          sets[i] = set;
          scorers.add(new ConstantScoreScorer(null, 0f, new BitDocIdSet(set).iterator()));
        } else {
          // scorer with approximation
          final FixedBitSet confirmed = clearRandomBits(set);
          sets[i] = confirmed;
          final TwoPhaseIterator approximation = approximation(new BitDocIdSet(set).iterator(), confirmed);
          scorers.add(scorer(approximation));
        }
      }

      // make some sub sequences into sub conjunctions
      final int subIters = atLeast(3);
      for (int subIter = 0; subIter < subIters && scorers.size() > 3; ++subIter) {
        final int subSeqStart = TestUtil.nextInt(random(), 0, scorers.size() - 2);
        final int subSeqEnd = TestUtil.nextInt(random(), subSeqStart + 2, scorers.size());
        List<Scorer> subIterators = scorers.subList(subSeqStart, subSeqEnd);
        Scorer subConjunction;
        if (wrapWithScorer) {
          subConjunction = new ConjunctionScorer(null, subIterators, Collections.emptyList(), 1f);
        } else {
          subConjunction = new ConstantScoreScorer(null, 0f, ConjunctionDISI.intersectScorers(subIterators));
        }
        scorers.set(subSeqStart, subConjunction);
        int toRemove = subSeqEnd - subSeqStart - 1;
        while (toRemove-- > 0) {
          scorers.remove(subSeqStart + 1);
        }
      }
      if (scorers.size() == 1) {
        // ConjunctionDISI needs two iterators
        scorers.add(new ConstantScoreScorer(null, 0f, DocIdSetIterator.all(maxDoc)));
      }


      final DocIdSetIterator conjunction = ConjunctionDISI.intersectScorers(scorers);
      assertEquals(intersect(sets), toBitSet(maxDoc, conjunction));
    }
  }

  public void testCollapseSubConjunctionDISIs() throws IOException {
    testCollapseSubConjunctions(false);
  }

  public void testCollapseSubConjunctionScorers() throws IOException {
    testCollapseSubConjunctions(true);
  }
}
