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

/** Expert: Implements scoring for a class of queries. */
public abstract class Scorer {
  private Similarity similarity;

  /** Constructs a Scorer. */
  protected Scorer(Similarity similarity) {
    this.similarity = similarity;
  }

  /** Returns the Similarity implementation used by this scorer. */
  public Similarity getSimilarity() {
    return this.similarity;
  }

  /** Scores all documents and passes them to a collector. */
  public void score(HitCollector hc) throws IOException {
    while (next()) {
      hc.collect(doc(), score());
    }
  }

  /** Advance to the next document matching the query.  Returns true iff there
   * is another match. */
  public abstract boolean next() throws IOException;

  /** Returns the current document number.  Initially invalid, until {@link
   * #next()} is called the first time. */
  public abstract int doc();

  /** Returns the score of the current document.  Initially invalid, until
   * {@link #next()} is called the first time. */
  public abstract float score() throws IOException;

  /** Skips to the first match beyond the current whose document number is
   * greater than or equal to <i>target</i>. <p>Returns true iff there is such
   * a match.  <p>Behaves as if written: <pre>
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

  /** Returns an explanation of the score for <code>doc</code>. */
  public abstract Explanation explain(int doc) throws IOException;

}
