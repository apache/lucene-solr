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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestConjunctionDISI extends LuceneTestCase {

  private static TwoPhaseIterator approximation(final DocIdSetIterator iterator, final FixedBitSet confirmed) {
    return new TwoPhaseIterator(iterator) {

      @Override
      public boolean matches() throws IOException {
        return confirmed.get(iterator.docID());
      }
    };
  }

  private static Scorer scorer(TwoPhaseIterator twoPhaseIterator) {
    return scorer(TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator), twoPhaseIterator);
  }

  /**
   * Create a {@link Scorer} that wraps the given {@link DocIdSetIterator}. It
   * also accepts a {@link TwoPhaseIterator} view, which is exposed in
   * {@link Scorer#asTwoPhaseIterator()}. When the two-phase view is not null,
   * then {@link Scorer#nextDoc()} and {@link Scorer#advance(int)} will raise
   * an exception in order to make sure that {@link ConjunctionDISI} takes
   * advantage of the {@link TwoPhaseIterator} view.
   */
  private static Scorer scorer(final DocIdSetIterator it, final TwoPhaseIterator twoPhaseIterator) {
    return new Scorer(null) {

      @Override
      public TwoPhaseIterator asTwoPhaseIterator() {
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
      final DocIdSetIterator[] iterators = new DocIdSetIterator[numIterators];
      for (int i = 0; i < iterators.length; ++i) {
        final FixedBitSet set = randomSet(maxDoc);
        if (random().nextBoolean()) {
          // simple iterator
          sets[i] = set;
          iterators[i] = new BitDocIdSet(set).iterator();
        } else {
          // scorer with approximation
          final FixedBitSet confirmed = clearRandomBits(set);
          sets[i] = confirmed;
          final TwoPhaseIterator approximation = approximation(new BitDocIdSet(set).iterator(), confirmed);
          iterators[i] = scorer(approximation);
        }
      }

      final ConjunctionDISI conjunction = ConjunctionDISI.intersect(Arrays.asList(iterators));
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
      final DocIdSetIterator[] iterators = new DocIdSetIterator[numIterators];
      boolean hasApproximation = false;
      for (int i = 0; i < iterators.length; ++i) {
        final FixedBitSet set = randomSet(maxDoc);
        if (random().nextBoolean()) {
          // simple iterator
          sets[i] = set;
          iterators[i] = new BitDocIdSet(set).iterator();
        } else {
          // scorer with approximation
          final FixedBitSet confirmed = clearRandomBits(set);
          sets[i] = confirmed;
          final TwoPhaseIterator approximation = approximation(new BitDocIdSet(set).iterator(), confirmed);
          iterators[i] = scorer(approximation);
          hasApproximation = true;
        }
      }

      final ConjunctionDISI conjunction = ConjunctionDISI.intersect(Arrays.asList(iterators));
      TwoPhaseIterator twoPhaseIterator = conjunction.asTwoPhaseIterator();
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
      DocIdSetIterator conjunction = null;
      boolean hasApproximation = false;
      for (int i = 0; i < numIterators; ++i) {
        final FixedBitSet set = randomSet(maxDoc);
        final DocIdSetIterator newIterator;
        if (random().nextBoolean()) {
          // simple iterator
          sets[i] = set;
          newIterator = new BitDocIdSet(set).iterator();
        } else {
          // scorer with approximation
          final FixedBitSet confirmed = clearRandomBits(set);
          sets[i] = confirmed;
          final TwoPhaseIterator approximation = approximation(new BitDocIdSet(set).iterator(), confirmed);
          newIterator = scorer(approximation);
          hasApproximation = true;
        }

        if (conjunction == null) {
          conjunction = newIterator;
        } else {
          final ConjunctionDISI conj = ConjunctionDISI.intersect(Arrays.asList(conjunction, newIterator));
          conjunction = scorer(conj, conj.asTwoPhaseIterator());
        }
      }

      TwoPhaseIterator twoPhaseIterator = ((Scorer) conjunction).asTwoPhaseIterator();
      assertEquals(hasApproximation, twoPhaseIterator != null);
      if (hasApproximation) {
        assertEquals(intersect(sets), toBitSet(maxDoc, TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator)));
      } else {
        assertEquals(intersect(sets), toBitSet(maxDoc, conjunction));
      }
    }
  }

  public void testCollapseSubConjunctions() throws IOException {
    final int iters = atLeast(100);
    for (int iter = 0; iter < iters; ++iter) {
      final int maxDoc = TestUtil.nextInt(random(), 100, 10000);
      final int numIterators = TestUtil.nextInt(random(), 5, 10);
      final FixedBitSet[] sets = new FixedBitSet[numIterators];
      final List<DocIdSetIterator> iterators = new LinkedList<>();
      for (int i = 0; i < numIterators; ++i) {
        final FixedBitSet set = randomSet(maxDoc);
        if (random().nextBoolean()) {
          // simple iterator
          sets[i] = set;
          iterators.add(new BitDocIdSet(set).iterator());
        } else {
          // scorer with approximation
          final FixedBitSet confirmed = clearRandomBits(set);
          sets[i] = confirmed;
          final TwoPhaseIterator approximation = approximation(new BitDocIdSet(set).iterator(), confirmed);
          iterators.add(scorer(approximation));
        }
      }

      // make some sub sequences into sub conjunctions
      final int subIters = atLeast(3);
      for (int subIter = 0; subIter < subIters && iterators.size() > 3; ++subIter) {
        final int subSeqStart = TestUtil.nextInt(random(), 0, iterators.size() - 2);
        final int subSeqEnd = TestUtil.nextInt(random(), subSeqStart + 2, iterators.size());
        final ConjunctionDISI subConjunction = ConjunctionDISI.intersect(iterators.subList(subSeqStart, subSeqEnd));
        iterators.set(subSeqStart, subConjunction);
        int toRemove = subSeqEnd - subSeqStart - 1;
        while (toRemove-- > 0) {
          iterators.remove(subSeqStart + 1);
        }
      }
      if (iterators.size() == 1) {
        // ConjunctionDISI needs two iterators
        iterators.add(DocIdSetIterator.all(maxDoc));
      }


      final ConjunctionDISI conjunction = ConjunctionDISI.intersect(iterators);
      assertEquals(intersect(sets), toBitSet(maxDoc, conjunction));
    }
  }

}
