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

import org.apache.lucene.index.Impacts;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.ImpactsSource;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

/**
 * {@link DocIdSetIterator} that skips non-competitive docs thanks to the
 * indexed impacts. Call {@link #setMinCompetitiveScore(float)} in order to
 * give this iterator the ability to skip low-scoring documents.
 * @lucene.internal
 */
public final class ImpactsDISI extends DocIdSetIterator {

  private final DocIdSetIterator in;
  private final ImpactsSource impactsSource;
  private final MaxScoreCache maxScoreCache;
  private final float globalMaxScore;
  private float minCompetitiveScore = 0;
  private int upTo = DocIdSetIterator.NO_MORE_DOCS;
  private float maxScore = Float.MAX_VALUE;

  /**
   * Sole constructor.
   * @param in            wrapped iterator
   * @param impactsSource source of impacts
   * @param scorer        scorer
   */
  public ImpactsDISI(DocIdSetIterator in, ImpactsSource impactsSource, SimScorer scorer) {
    this.in = in;
    this.impactsSource = impactsSource;
    this.maxScoreCache = new MaxScoreCache(impactsSource, scorer);
    this.globalMaxScore = scorer.score(Float.MAX_VALUE, 1L);
  }

  /**
   * Set the minimum competitive score.
   * @see Scorer#setMinCompetitiveScore(float)
   */
  public void setMinCompetitiveScore(float minCompetitiveScore) {
    assert minCompetitiveScore >= this.minCompetitiveScore;
    if (minCompetitiveScore > this.minCompetitiveScore) {
      this.minCompetitiveScore = minCompetitiveScore;
      // force upTo and maxScore to be recomputed so that we will skip documents
      // if the current block of documents is not competitive - only if the min
      // competitive score actually increased
      upTo = -1;
    }
  }

  /**
   * Implement the contract of {@link Scorer#advanceShallow(int)} based on the
   * wrapped {@link ImpactsEnum}.
   * @see Scorer#advanceShallow(int)
   */
  public int advanceShallow(int target) throws IOException {
    impactsSource.advanceShallow(target);
    Impacts impacts = impactsSource.getImpacts();
    return impacts.getDocIdUpTo(0);
  }

  /**
   * Implement the contract of {@link Scorer#getMaxScore(int)} based on the
   * wrapped {@link ImpactsEnum} and {@link Scorer}.
   * @see Scorer#getMaxScore(int)
   */
  public float getMaxScore(int upTo) throws IOException {
    final int level = maxScoreCache.getLevel(upTo);
    if (level == -1) {
      return globalMaxScore;
    } else {
      return maxScoreCache.getMaxScoreForLevel(level);
    }
  }

  private int advanceTarget(int target) throws IOException {
    if (target <= upTo) {
      // we are still in the current block, which is considered competitive
      // according to impacts, no skipping
      return target;
    }

    upTo = advanceShallow(target);
    maxScore = maxScoreCache.getMaxScoreForLevel(0);

    while (true) {
      assert upTo >= target;

      if (maxScore >= minCompetitiveScore) {
        return target;
      }

      if (upTo == NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }

      final int skipUpTo = maxScoreCache.getSkipUpTo(minCompetitiveScore);
      if (skipUpTo == -1) { // no further skipping
        target = upTo + 1;
      } else if (skipUpTo == NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      } else {
        target = skipUpTo + 1;
      }
      upTo = advanceShallow(target);
      maxScore = maxScoreCache.getMaxScoreForLevel(0);
    }
  }

  @Override
  public int advance(int target) throws IOException {
    return in.advance(advanceTarget(target));
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(in.docID() + 1);
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public long cost() {
    return in.cost();
  }

}
