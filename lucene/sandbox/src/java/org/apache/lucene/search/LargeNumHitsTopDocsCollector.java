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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;

import static org.apache.lucene.search.TopDocsCollector.EMPTY_TOPDOCS;

/**
 * Optimized collector for large number of hits.
 * The collector maintains an ArrayList of hits until it accumulates
 * the requested number of hits. Post that, it builds a Priority Queue
 * and starts filtering further hits based on the minimum competitive
 * score.
 */
public final class LargeNumHitsTopDocsCollector implements Collector {
  private final int requestedHitCount;
  private List<ScoreDoc> hits = new ArrayList<>();
  // package private for testing
  HitQueue pq;
  ScoreDoc pqTop;
  int totalHits;

  public LargeNumHitsTopDocsCollector(int requestedHitCount) {
    this.requestedHitCount = requestedHitCount;
    this.totalHits = 0;
  }

  // We always return COMPLETE since this collector should ideally
  // be used only with large number of hits case
  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) {
    final int docBase = context.docBase;
    return new TopScoreDocCollector.ScorerLeafCollector() {

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        super.setScorer(scorer);
      }

      @Override
      public void collect(int doc) throws IOException {
        float score = scorer.score();

        // This collector relies on the fact that scorers produce positive values:
        assert score >= 0; // NOTE: false for NaN

        if (totalHits < requestedHitCount) {
          hits.add(new ScoreDoc(doc + docBase, score));
          totalHits++;
          return;
        } else if (totalHits == requestedHitCount) {
          // Convert the list to a priority queue

          // We should get here only when priority queue
          // has not been built
          assert pq == null;
          assert pqTop == null;
          pq = new HitQueue(requestedHitCount, false);

          for (ScoreDoc scoreDoc : hits) {
            pq.add(scoreDoc);
          }

          pqTop = pq.top();
          hits = null;
        }

        if (score > pqTop.score) {
          pqTop.doc = doc + docBase;
          pqTop.score = score;
          pqTop = pq.updateTop();
        }
        ++totalHits;
      }
    };
  }

  /** Returns the top docs that were collected by this collector. */
  public TopDocs topDocs(int howMany) {

    if (howMany <= 0 || howMany > totalHits) {
      throw new IllegalArgumentException("Incorrect number of hits requested");
    }

    ScoreDoc[] results = new ScoreDoc[howMany];

    // Get the requested results from either hits list or PQ
    populateResults(results, howMany);

    return newTopDocs(results);
  }

  /**
   * Populates the results array with the ScoreDoc instances. This can be
   * overridden in case a different ScoreDoc type should be returned.
   */
  protected void populateResults(ScoreDoc[] results, int howMany) {
    if (pq != null) {
      assert totalHits >= requestedHitCount;
      for (int i = howMany - 1; i >= 0; i--) {
        results[i] = pq.pop();
      }
      return;
    }

    // Total number of hits collected were less than requestedHitCount
    assert totalHits < requestedHitCount;
    Collections.sort(hits, Comparator.comparing((ScoreDoc scoreDoc) ->
        scoreDoc.score).reversed().thenComparing(scoreDoc -> scoreDoc.doc));

    for (int i = 0; i < howMany; i++) {
      results[i] = hits.get(i);
    }
  }

  /**
   * Returns a {@link TopDocs} instance containing the given results. If
   * <code>results</code> is null it means there are no results to return,
   * either because there were 0 calls to collect() or because the arguments to
   * topDocs were invalid.
   */
  protected TopDocs newTopDocs(ScoreDoc[] results) {
    return results == null ? EMPTY_TOPDOCS : new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), results);
  }

  /** Returns the top docs that were collected by this collector. */
  public TopDocs topDocs() {
    return topDocs(Math.min(totalHits, requestedHitCount));
  }
}
