package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/** Expert: Common scoring functionality for different types of queries.
 * <br>A <code>Scorer</code> iterates over all documents matching a query,
 * or provides an explanation of the score for a query for a given document.
 * <br>Scores are computed using a given <code>Similarity</code> implementation.
 */
public abstract class Scorer {
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
   */
  public void score(HitCollector hc) throws IOException {
    while (next()) {
      hc.collect(doc(), score());
    }
  }

  /** Advances to the next document matching the query.
   * @return true iff there is another document matching the query.
   */
  public abstract boolean next() throws IOException;

  /** Returns the current document number matching the query.
   * Initially invalid, until {@link #next()} is called the first time.
   */
  public abstract int doc();

  /** Returns the score of the current document matching the query.
   * Initially invalid, until {@link #next()} is called the first time.
   */
  public abstract float score() throws IOException;

  /** Skips to the first match beyond the current whose document number is
   * greater than or equal to a given target. 
   * @param target The target document number.
   * @return true iff there is such a match.
   * <p>Behaves as if written: <pre>
   *   boolean skipTo(int target) {
   *     do {
   *       if (!next())
   * 	     return false;
   *     } while (target > doc());
   *     return true;
   *   }
   * </pre>
   * Most implementations are considerably more efficient than that.
   */
  public abstract boolean skipTo(int target) throws IOException;

  /** Returns an explanation of the score for a document.
   * <br>When this method is used, the {@link #next()} method
   * and the {@link #score(HitCollector)} method should not be used.
   * @param doc The document number for the explanation.
   */
  public abstract Explanation explain(int doc) throws IOException;

}
