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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.index.TermsEnum;

/** Expert: A <code>Scorer</code> for documents matching a <code>Term</code>.
 */
final class TermScorer extends Scorer {
  private final PostingsEnum postingsEnum;
  private final ImpactsEnum impactsEnum;
  private final DocIdSetIterator iterator;
  private final LeafSimScorer docScorer;
  private final MaxScoreCache maxScoreCache;
  private float minCompetitiveScore;

  /**
   * Construct a <code>TermScorer</code>.
   *
   * @param weight
   *          The weight of the <code>Term</code> in the query.
   * @param te
   *          A {@link TermsEnum} positioned on the expected term.
   * @param docScorer
   *          A {@link LeafSimScorer} for the appropriate field.
   */
  TermScorer(Weight weight, TermsEnum te, ScoreMode scoreMode, LeafSimScorer docScorer) throws IOException {
    super(weight);
    this.docScorer = docScorer;
    if (scoreMode == ScoreMode.TOP_SCORES) {
      impactsEnum = te.impacts(PostingsEnum.FREQS);
      maxScoreCache = new MaxScoreCache(impactsEnum, docScorer.getSimScorer());
      postingsEnum = impactsEnum;
      iterator = new DocIdSetIterator() {

        int upTo = -1;
        float maxScore;

        private int advanceTarget(int target) throws IOException {
          if (minCompetitiveScore == 0) {
            // no potential for skipping
            return target;
          }

          if (target > upTo) {
            impactsEnum.advanceShallow(target);
            Impacts impacts = impactsEnum.getImpacts();
            upTo = impacts.getDocIdUpTo(0);
            maxScore = maxScoreCache.getMaxScoreForLevel(0);
          }

          while (true) {
            assert upTo >= target;

            if (maxScore >= minCompetitiveScore) {
              return target;
            }

            if (upTo == NO_MORE_DOCS) {
              return NO_MORE_DOCS;
            }

            impactsEnum.advanceShallow(upTo + 1);
            Impacts impacts = impactsEnum.getImpacts();
            final int level = maxScoreCache.getSkipLevel(minCompetitiveScore);
            if (level >= 0) {
              // we can skip more docs
              int newUpTo = impacts.getDocIdUpTo(level);
              if (newUpTo == NO_MORE_DOCS) {
                return NO_MORE_DOCS;
              }
              target = newUpTo + 1;
              impactsEnum.advanceShallow(target);
              impacts = impactsEnum.getImpacts();
            } else {
              target = upTo + 1;
            }
            upTo = impacts.getDocIdUpTo(0);
            maxScore = maxScoreCache.getMaxScoreForLevel(0);
          }
        }

        @Override
        public int advance(int target) throws IOException {
          return impactsEnum.advance(advanceTarget(target));
        }

        @Override
        public int nextDoc() throws IOException {
          return advance(impactsEnum.docID() + 1);
        }

        @Override
        public int docID() {
          return impactsEnum.docID();
        }

        @Override
        public long cost() {
          return impactsEnum.cost();
        }
      };
    } else {
      postingsEnum = te.postings(null, scoreMode.needsScores() ? PostingsEnum.FREQS : PostingsEnum.NONE);
      impactsEnum = new SlowImpactsEnum(postingsEnum);
      maxScoreCache = new MaxScoreCache(impactsEnum, docScorer.getSimScorer());
      iterator = postingsEnum;
    }
  }

  @Override
  public int docID() {
    return postingsEnum.docID();
  }

  final int freq() throws IOException {
    return postingsEnum.freq();
  }

  @Override
  public DocIdSetIterator iterator() {
    return iterator;
  }

  @Override
  public float score() throws IOException {
    assert docID() != DocIdSetIterator.NO_MORE_DOCS;
    return docScorer.score(postingsEnum.docID(), postingsEnum.freq());
  }

  @Override
  public int advanceShallow(int target) throws IOException {
    return maxScoreCache.advanceShallow(target);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return maxScoreCache.getMaxScore(upTo);
  }

  @Override
  public void setMinCompetitiveScore(float minScore) {
    this.minCompetitiveScore = minScore;
  }

  /** Returns a string representation of this <code>TermScorer</code>. */
  @Override
  public String toString() { return "scorer(" + weight + ")[" + super.toString() + "]"; }

}
