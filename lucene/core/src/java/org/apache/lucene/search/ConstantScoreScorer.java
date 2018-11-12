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

/**
 * A constant-scoring {@link Scorer}.
 * @lucene.internal
 */
public final class ConstantScoreScorer extends Scorer {

  private int doc = -1;

  private class TwoPhaseIteratorWrapper extends TwoPhaseIterator {
    final TwoPhaseIterator delegate;

    TwoPhaseIteratorWrapper(TwoPhaseIterator delegate, DocIdSetIterator approximation) {
      super(approximation);
      this.delegate = delegate;
    }

    @Override
    public boolean matches() throws IOException {
      return delegate.matches();
    }

    @Override
    public float matchCost() {
      return delegate.matchCost();
    }
  }

  private class DocIdSetIteratorWrapper extends DocIdSetIterator {
    DocIdSetIterator delegate;

    DocIdSetIteratorWrapper(DocIdSetIterator delegate) {
      this.delegate = delegate;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return doc = delegate.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return doc = delegate.advance(target);
    }

    @Override
    public long cost() {
      return delegate.cost();
    }
  }

  private final float score;
  private final DocIdSetIteratorWrapper approximation;
  private final TwoPhaseIterator twoPhaseIterator;
  private final DocIdSetIteratorWrapper disi;

  /** Constructor based on a {@link DocIdSetIterator} which will be used to
   *  drive iteration. Two phase iteration will not be supported.
   *  @param weight the parent weight
   *  @param score the score to return on each document
   *  @param disi the iterator that defines matching documents */
  public ConstantScoreScorer(Weight weight, float score, DocIdSetIterator disi) {
    super(weight);
    this.score = score;
    this.approximation = null;
    this.twoPhaseIterator = null;
    this.disi = new DocIdSetIteratorWrapper(disi);
  }

  /** Constructor based on a {@link TwoPhaseIterator}. In that case the
   *  {@link Scorer} will support two-phase iteration.
   *  @param weight the parent weight
   *  @param score the score to return on each document
   *  @param twoPhaseIterator the iterator that defines matching documents */
  public ConstantScoreScorer(Weight weight, float score, TwoPhaseIterator twoPhaseIterator) {
    super(weight);
    this.score = score;
    this.approximation = new DocIdSetIteratorWrapper(twoPhaseIterator.approximation());
    this.twoPhaseIterator = new TwoPhaseIteratorWrapper(twoPhaseIterator, this.approximation);
    this.disi = new DocIdSetIteratorWrapper(TwoPhaseIterator.asDocIdSetIterator(this.twoPhaseIterator));
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return score;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    if (minScore > score) {
      if (twoPhaseIterator == null) {
        disi.delegate = DocIdSetIterator.empty();
      } else {
        approximation.delegate = DocIdSetIterator.empty();
      }
    }
  }

  @Override
  public DocIdSetIterator iterator() {
    return disi;
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return twoPhaseIterator;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public float score() throws IOException {
    return score;
  }

}
