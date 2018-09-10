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

import org.apache.lucene.index.LeafReaderContext;

/**
 * A {@link Collector} implementation that collects the top-scoring hits,
 * returning them as a {@link TopDocs}. This is used by {@link IndexSearcher} to
 * implement {@link TopDocs}-based search. Hits are sorted by score descending
 * and then (when the scores are tied) docID ascending. When you create an
 * instance of this collector you should know in advance whether documents are
 * going to be collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and
 * {@link Float#NEGATIVE_INFINITY} are not valid scores.  This
 * collector will not properly collect hits with such
 * scores.
 */
public abstract class TopScoreDocCollector extends TopDocsCollector<ScoreDoc> {

  abstract static class ScorerLeafCollector implements LeafCollector {

    Scorable scorer;

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer;
    }

  }

  private static class SimpleTopScoreDocCollector extends TopScoreDocCollector {

    private final int totalHitsThreshold;

    SimpleTopScoreDocCollector(int numHits, int totalHitsThreshold) {
      super(numHits);
      this.totalHitsThreshold = totalHitsThreshold;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context)
        throws IOException {
      final int docBase = context.docBase;
      return new ScorerLeafCollector() {

        private void updateMinCompetitiveScore() throws IOException {
          // since we tie-break on doc id and collect in doc id order, we can require
          // the next float
          scorer.setMinCompetitiveScore(Math.nextUp(pqTop.score));
          totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
          super.setScorer(scorer);
          if (totalHits >= totalHitsThreshold
              && pqTop != null
              && pqTop.score != Float.NEGATIVE_INFINITY) {
            updateMinCompetitiveScore();
          }
        }

        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();

          // This collector relies on the fact that scorers produce positive values:
          assert score >= 0; // NOTE: false for NaN

          totalHits++;
          if (score <= pqTop.score) {
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO && totalHits >= totalHitsThreshold) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore();
            }
            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }
          pqTop.doc = doc + docBase;
          pqTop.score = score;
          pqTop = pq.updateTop();
          if (totalHits >= totalHitsThreshold && pqTop.score != Float.NEGATIVE_INFINITY) { // -Infinity is the score of sentinels
            updateMinCompetitiveScore();
          }
        }

      };
    }

    @Override
    public ScoreMode scoreMode() {
      return totalHitsThreshold == Integer.MAX_VALUE ? ScoreMode.COMPLETE : ScoreMode.TOP_SCORES;
    }
  }

  private static class PagingTopScoreDocCollector extends TopScoreDocCollector {

    private final ScoreDoc after;
    private int collectedHits;

    PagingTopScoreDocCollector(int numHits, ScoreDoc after) {
      super(numHits);
      this.after = after;
      this.collectedHits = 0;
    }

    @Override
    protected int topDocsSize() {
      return collectedHits < pq.size() ? collectedHits : pq.size();
    }

    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      return results == null
          ? new TopDocs(new TotalHits(totalHits, totalHitsRelation), new ScoreDoc[0])
          : new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      final int docBase = context.docBase;
      final int afterDoc = after.doc - context.docBase;
      return new ScorerLeafCollector() {
        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();

          // This collector relies on the fact that scorers produce positive values:
          assert score >= 0; // NOTE: false for NaN

          totalHits++;

          if (score > after.score || (score == after.score && doc <= afterDoc)) {
            // hit was collected on a previous page
            return;
          }

          if (score <= pqTop.score) {
            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }
          collectedHits++;
          pqTop.doc = doc + docBase;
          pqTop.score = score;
          pqTop = pq.updateTop();
        }
      };
    }
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect and the number of hits to count accurately.
   *
   * <p><b>NOTE</b>: If the total hit count of the top docs is less than
   * {@code totalHitsThreshold} then this value is accurate. On the other hand,
   * if the {@link TopDocs#totalHits} value is greater than or equal to
   * {@code totalHitsThreshold} then its value is a lower bound of the hit
   * count. A value of {@link Integer#MAX_VALUE} will make the hit count
   * accurate but will also likely make query processing slower.
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, int totalHitsThreshold) {
    return create(numHits, null, totalHitsThreshold);
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect, the bottom of the previous page, and the number of hits to count
   * accurately.
   *
   * <p><b>NOTE</b>: If the total hit count of the top docs is less than
   * {@code totalHitsThreshold} then this value is accurate. On the other hand,
   * if the {@link TopDocs#totalHits} value is greater than or equal to
   * {@code totalHitsThreshold} then its value is a lower bound of the hit
   * count. A value of {@link Integer#MAX_VALUE} will make the hit count
   * accurate but will also likely make query processing slower.
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, ScoreDoc after, int totalHitsThreshold) {

    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    if (totalHitsThreshold <= 0) {
      throw new IllegalArgumentException("totalHitsThreshold must be > 0, got " + totalHitsThreshold);
    }

    if (after == null) {
      return new SimpleTopScoreDocCollector(numHits, totalHitsThreshold);
    } else {
      return new PagingTopScoreDocCollector(numHits, after);
    }
  }

  ScoreDoc pqTop;

  // prevents instantiation
  TopScoreDocCollector(int numHits) {
    super(new HitQueue(numHits, true));
    // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
    // that at this point top() is already initialized.
    pqTop = pq.top();
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      return EMPTY_TOPDOCS;
    }

    return new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE;
  }
}
