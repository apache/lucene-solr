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
import java.util.Collection;
import java.util.Collections;
import java.util.Random;

/** Wraps a Scorer with additional checks */
public class AssertingScorer extends Scorer {

  static enum IteratorState { START, APPROXIMATING, ITERATING, FINISHED };

  public static Scorer wrap(Random random, Scorer other, boolean canScore) {
    if (other == null) {
      return null;
    }
    return new AssertingScorer(random, other, canScore);
  }

  final Random random;
  final Scorer in;
  final boolean needsScores;

  IteratorState state = IteratorState.START;
  int doc;

  private AssertingScorer(Random random, Scorer in, boolean needsScores) {
    super(in.weight);
    this.random = random;
    this.in = in;
    this.needsScores = needsScores;
    this.doc = in.docID();
  }

  public Scorer getIn() {
    return in;
  }

  boolean iterating() {
    // we cannot assert that state == ITERATING because of CachingScorerWrapper
    switch (docID()) {
    case -1:
    case DocIdSetIterator.NO_MORE_DOCS:
      return false;
    default:
      return state != IteratorState.APPROXIMATING; // Matches must be confirmed before calling freq() or score()
    }
  }

  @Override
  public float score() throws IOException {
    assert needsScores;
    assert iterating();
    final float score = in.score();
    assert !Float.isNaN(score) : "NaN score for in="+in;
    return score;
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    // We cannot hide that we hold a single child, else
    // collectors (e.g. ToParentBlockJoinCollector) that
    // need to walk the scorer tree will miss/skip the
    // Scorer we wrap:
    return Collections.singletonList(new ChildScorer(in, "SHOULD"));
  }

  @Override
  public int freq() throws IOException {
    assert needsScores;
    assert iterating();
    return in.freq();
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public String toString() {
    return "AssertingScorer(" + in + ")";
  }

  @Override
  public DocIdSetIterator iterator() {
    final DocIdSetIterator in = this.in.iterator();
    assert in != null;
    return new DocIdSetIterator() {
      
      @Override
      public int docID() {
        assert AssertingScorer.this.in.docID() == in.docID();
        return in.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        assert state != IteratorState.FINISHED : "nextDoc() called after NO_MORE_DOCS";
        int nextDoc = in.nextDoc();
        assert nextDoc > doc : "backwards nextDoc from " + doc + " to " + nextDoc + " " + in;
        if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
          state = IteratorState.FINISHED;
        } else {
          state = IteratorState.ITERATING;
        }
        assert in.docID() == nextDoc;
        assert AssertingScorer.this.in.docID() == in.docID();
        return doc = nextDoc;
      }

      @Override
      public int advance(int target) throws IOException {
        assert state != IteratorState.FINISHED : "advance() called after NO_MORE_DOCS";
        assert target > doc : "target must be > docID(), got " + target + " <= " + doc;
        int advanced = in.advance(target);
        assert advanced >= target : "backwards advance from: " + target + " to: " + advanced;
        if (advanced == DocIdSetIterator.NO_MORE_DOCS) {
          state = IteratorState.FINISHED;
        } else {
          state = IteratorState.ITERATING;
        }
        assert in.docID() == advanced;
        assert AssertingScorer.this.in.docID() == in.docID();
        return doc = advanced;
      }

      @Override
      public long cost() {
        return in.cost();
      }
    };
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    final TwoPhaseIterator in = this.in.twoPhaseIterator();
    if (in == null) {
      return null;
    }
    final DocIdSetIterator inApproximation = in.approximation();
    assert inApproximation.docID() == doc;
    final DocIdSetIterator assertingApproximation = new DocIdSetIterator() {

      @Override
      public int docID() {
        return inApproximation.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        assert state != IteratorState.FINISHED : "advance() called after NO_MORE_DOCS";
        final int nextDoc = inApproximation.nextDoc();
        assert nextDoc > doc : "backwards advance from: " + doc + " to: " + nextDoc;
        if (nextDoc == NO_MORE_DOCS) {
          state = IteratorState.FINISHED;
        } else {
          state = IteratorState.APPROXIMATING;
        }
        assert inApproximation.docID() == nextDoc;
        return doc = nextDoc;
      }

      @Override
      public int advance(int target) throws IOException {
        assert state != IteratorState.FINISHED : "advance() called after NO_MORE_DOCS";
        assert target > doc : "target must be > docID(), got " + target + " <= " + doc;
        final int advanced = inApproximation.advance(target);
        assert advanced >= target : "backwards advance from: " + target + " to: " + advanced;
        if (advanced == NO_MORE_DOCS) {
          state = IteratorState.FINISHED;
        } else {
          state = IteratorState.APPROXIMATING;
        }
        assert inApproximation.docID() == advanced;
        return doc = advanced;
      }

      @Override
      public long cost() {
        return inApproximation.cost();
      }

    };
    return new TwoPhaseIterator(assertingApproximation) {
      @Override
      public boolean matches() throws IOException {
        assert state == IteratorState.APPROXIMATING;
        final boolean matches = in.matches();
        if (matches) {
          assert AssertingScorer.this.in.iterator().docID() == inApproximation.docID() : "Approximation and scorer don't advance synchronously";
          doc = inApproximation.docID();
          state = IteratorState.ITERATING;
        }
        return matches;
      }

      @Override
      public float matchCost() {
        float matchCost = in.matchCost();
        assert ! Float.isNaN(matchCost);
        assert matchCost >= 0;
        return matchCost;
      }

      @Override
      public String toString() {
        return "AssertingScorer@asTwoPhaseIterator(" + in + ")";
      }
    };
  }
}

