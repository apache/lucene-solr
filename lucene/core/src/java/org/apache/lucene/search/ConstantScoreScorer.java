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
 *
 * @lucene.internal
 */
public final class ConstantScoreScorer extends Scorer {

  private class DocIdSetIteratorWrapper extends DocIdSetIterator {
    int doc = -1;
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
  private final ScoreMode scoreMode;
  private final DocIdSetIterator approximation;
  private final TwoPhaseIterator twoPhaseIterator;
  private final DocIdSetIterator disi;

  /**
   * Constructor based on a {@link DocIdSetIterator} which will be used to drive iteration. Two
   * phase iteration will not be supported.
   *
   * @param weight the parent weight
   * @param score the score to return on each document
   * @param scoreMode the score mode
   * @param disi the iterator that defines matching documents
   */
  public ConstantScoreScorer(
      Weight weight, float score, ScoreMode scoreMode, DocIdSetIterator disi) {
    super(weight);
    this.score = score;
    this.scoreMode = scoreMode;
    this.approximation =
        scoreMode == ScoreMode.TOP_SCORES ? new DocIdSetIteratorWrapper(disi) : disi;
    this.twoPhaseIterator = null;
    this.disi = this.approximation;
  }

  /**
   * Constructor based on a {@link TwoPhaseIterator}. In that case the {@link Scorer} will support
   * two-phase iteration.
   *
   * @param weight the parent weight
   * @param score the score to return on each document
   * @param scoreMode the score mode
   * @param twoPhaseIterator the iterator that defines matching documents
   */
  public ConstantScoreScorer(
      Weight weight, float score, ScoreMode scoreMode, TwoPhaseIterator twoPhaseIterator) {
    super(weight);
    this.score = score;
    this.scoreMode = scoreMode;
    if (scoreMode == ScoreMode.TOP_SCORES) {
      this.approximation = new DocIdSetIteratorWrapper(twoPhaseIterator.approximation());
      this.twoPhaseIterator =
          new TwoPhaseIterator(this.approximation) {
            @Override
            public boolean matches() throws IOException {
              return twoPhaseIterator.matches();
            }

            @Override
            public float matchCost() {
              return twoPhaseIterator.matchCost();
            }
          };
    } else {
      this.approximation = twoPhaseIterator.approximation();
      this.twoPhaseIterator = twoPhaseIterator;
    }
    this.disi = TwoPhaseIterator.asDocIdSetIterator(this.twoPhaseIterator);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return score;
  }

  @Override
  public void setMinCompetitiveScore(float minScore) throws IOException {
    if (scoreMode == ScoreMode.TOP_SCORES && minScore > score) {
      ((DocIdSetIteratorWrapper) approximation).delegate = DocIdSetIterator.empty();
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
    return disi.docID();
  }

  @Override
  public float score() throws IOException {
    return score;
  }
}
