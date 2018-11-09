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

  private final float score;
  private final TwoPhaseIterator twoPhaseIterator;
  private DocIdSetIterator disi;
  private int doc = -1;

  /** Constructor based on a {@link DocIdSetIterator} which will be used to
   *  drive iteration. Two phase iteration will not be supported.
   *  @param weight the parent weight
   *  @param score the score to return on each document
   *  @param disi the iterator that defines matching documents */
  public ConstantScoreScorer(Weight weight, float score, DocIdSetIterator disi) {
    super(weight);
    this.score = score;
    this.twoPhaseIterator = null;
    this.disi = disi;
  }

  /** Constructor based on a {@link TwoPhaseIterator}. In that case the
   *  {@link Scorer} will support two-phase iteration.
   *  @param weight the parent weight
   *  @param score the score to return on each document
   *  @param twoPhaseIterator the iterator that defines matching documents */
  public ConstantScoreScorer(Weight weight, float score, TwoPhaseIterator twoPhaseIterator) {
    super(weight);

    DocIdSetIterator approximation = new DocIdSetIterator() {
      @Override
      public int nextDoc() throws IOException {
        return doc = twoPhaseIterator.approximation().nextDoc();
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public long cost() {
        return twoPhaseIterator.approximation().cost();
      }

      @Override
      public int advance(int target) throws IOException {
        return doc = twoPhaseIterator.approximation().advance(target);
      }
    };

    this.score = score;
    this.twoPhaseIterator = new TwoPhaseIterator(approximation) {
      @Override
      public boolean matches() throws IOException {
        return twoPhaseIterator.matches();
      }

      @Override
      public float matchCost() {
        return twoPhaseIterator.matchCost();
      }
    };
    this.disi = TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return score;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    if (minScore > score) {
      disi = DocIdSetIterator.empty();
    }
  }

  @Override
  public DocIdSetIterator iterator() {
    return new DocIdSetIterator() {
      @Override
      public int nextDoc() throws IOException {
        return doc = disi.nextDoc();
      }

      @Override
      public int docID() {
        return doc;
      }

      @Override
      public long cost() {
        return disi.cost();
      }

      @Override
      public int advance(int target) throws IOException {
        return doc = disi.advance(target);
      }
    };
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return twoPhaseIterator;
  }

  @Override
  public int docID() {
    return twoPhaseIterator != null ? twoPhaseIterator.approximation().docID() : doc;
  }

  @Override
  public float score() throws IOException {
    return score;
  }

}
