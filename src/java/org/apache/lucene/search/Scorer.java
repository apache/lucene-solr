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

/**
 * Expert: Common scoring functionality for different types of queries.
 *
 * <p>
 * A <code>Scorer</code> iterates over documents matching a
 * query in increasing order of doc Id.
 * </p>
 * <p>
 * Document scores are computed using a given <code>Similarity</code>
 * implementation.
 * </p>
 *
 * <p><b>NOTE</b>: The values Float.Nan,
 * Float.NEGATIVE_INFINITY and Float.POSITIVE_INFINITY are
 * not valid scores.  Certain collectors (eg {@link
 * TopScoreDocCollector}) will not properly collect hits
 * with these scores.
 *
 * @see BooleanQuery#setAllowDocsOutOfOrder
 */
public abstract class Scorer extends DocIdSetIterator {
  private Similarity similarity;

  /** Constructs a Scorer.
   * @param similarity The <code>Similarity</code> implementation used by this scorer.
   */
  protected Scorer(Similarity similarity) {
    this.similarity = similarity;
  }

  /** Returns the Similarity implementation used by this scorer. */
  public Similarity getSimilarity() {
    return this.similarity;
  }

  /** Scores and collects all matching documents.
   * @param hc The collector to which all matching documents are passed through
   * {@link HitCollector#collect(int, float)}.
   * <br>When this method is used the {@link #explain(int)} method should not be used.
   * @deprecated use {@link #score(Collector)} instead.
   */
  public void score(HitCollector hc) throws IOException {
    score(new HitCollectorWrapper(hc));
  }
  
  /** Scores and collects all matching documents.
   * @param collector The collector to which all matching documents are passed.
   * <br>When this method is used the {@link #explain(int)} method should not be used.
   */
  public void score(Collector collector) throws IOException {
    collector.setScorer(this);
    int doc;
    while ((doc = nextDoc()) != NO_MORE_DOCS) {
      collector.collect(doc);
    }
  }

  /** Expert: Collects matching documents in a range.  Hook for optimization.
   * Note that {@link #next()} must be called once before this method is called
   * for the first time.
   * @param hc The collector to which all matching documents are passed through
   * {@link HitCollector#collect(int, float)}.
   * @param max Do not score documents past this.
   * @return true if more matching documents may remain.
   * @deprecated use {@link #score(Collector, int, int)} instead.
   */
  protected boolean score(HitCollector hc, int max) throws IOException {
    return score(new HitCollectorWrapper(hc), max, docID());
  }
  
  /**
   * Expert: Collects matching documents in a range. Hook for optimization.
   * Note, <code>firstDocID</code> is added to ensure that {@link #nextDoc()}
   * was called before this method.
   * 
   * @param collector
   *          The collector to which all matching documents are passed.
   * @param max
   *          Do not score documents past this.
   * @param firstDocID
   *          The first document ID (ensures {@link #nextDoc()} is called before
   *          this method.
   * @return true if more matching documents may remain.
   */
  protected boolean score(Collector collector, int max, int firstDocID) throws IOException {
    collector.setScorer(this);
    int doc = firstDocID;
    while (doc < max) {
      collector.collect(doc);
      doc = nextDoc();
    }
    return doc != NO_MORE_DOCS;
  }
  
  /** Returns the score of the current document matching the query.
   * Initially invalid, until {@link #next()} or {@link #skipTo(int)}
   * is called the first time, or when called from within
   * {@link Collector#collect}.
   */
  public abstract float score() throws IOException;

  /** Returns an explanation of the score for a document.
   * <br>When this method is used, the {@link #next()}, {@link #skipTo(int)} and
   * {@link #score(HitCollector)} methods should not be used.
   * @param doc The document number for the explanation.
   *
   * @deprecated Please use {@link IndexSearcher#explain}
   * or {@link Weight#explain} instead.
   */
  public Explanation explain(int doc) throws IOException {
    throw new UnsupportedOperationException();
  }

}
