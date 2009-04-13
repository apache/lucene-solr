package org.apache.lucene.search;

/**
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

import org.apache.lucene.index.IndexReader;

/**
 * A {@link Collector} implementation that collects the
 * top-scoring hits, returning them as a {@link
 * TopDocs}. This is used by {@link IndexSearcher} to
 * implement {@link TopDocs}-based search.  Hits are sorted
 * by score descending and then (when the scores are tied)
 * docID ascending.
 */
public final class TopScoreDocCollector extends TopDocsCollector {

  private ScoreDoc reusableSD;
  private int docBase = 0;
  private Scorer scorer;
    
  /** Construct to collect a given number of hits.
   * @param numHits the maximum number of hits to collect
   */
  public TopScoreDocCollector(int numHits) {
    super(new HitQueue(numHits));
  }

  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      return EMPTY_TOPDOCS;
    }
    
    // We need to compute maxScore in order to set it in TopDocs. If start == 0,
    // it means the largest element is already in results, use its score as
    // maxScore. Otherwise pop everything else, until the largest element is
    // extracted and use its score as maxScore.
    float maxScore = Float.NaN;
    if (start == 0) {
      maxScore = results[0].score;
    } else {
      for (int i = pq.size(); i > 1; i--) { pq.pop(); }
      maxScore = ((ScoreDoc) pq.pop()).score;
    }
    
    return new TopDocs(totalHits, results, maxScore);
  }
  
  // javadoc inherited
  public void collect(int doc) throws IOException {
    float score = scorer.score();
    totalHits++;
    if (reusableSD == null) {
      reusableSD = new ScoreDoc(doc + docBase, score);
    } else if (score >= reusableSD.score) {
      // reusableSD holds the last "rejected" entry, so, if
      // this new score is not better than that, there's no
      // need to try inserting it
      reusableSD.doc = doc + docBase;
      reusableSD.score = score;
    } else {
      return;
    }
    reusableSD = (ScoreDoc) pq.insertWithOverflow(reusableSD);
  }

  public void setNextReader(IndexReader reader, int base) {
    docBase = base;
  }
  
  public void setScorer(Scorer scorer) throws IOException {
    this.scorer = scorer;
  }
}
