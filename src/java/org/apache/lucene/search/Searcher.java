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

/** An abstract base class for search implementations.
 * Implements some common utility methods.
 */
public abstract class Searcher implements Searchable {

  /** Returns the documents matching <code>query</code>. 
   * @throws BooleanQuery.TooManyClauses
   */
  public final Hits search(Query query) throws IOException {
    return search(query, (Filter)null);
  }

  /** Returns the documents matching <code>query</code> and
   * <code>filter</code>.
   * @throws BooleanQuery.TooManyClauses
   */
  public Hits search(Query query, Filter filter) throws IOException {
    return new Hits(this, query, filter);
  }

  /** Returns documents matching <code>query</code> sorted by
   * <code>sort</code>.
   * @throws BooleanQuery.TooManyClauses
   */
  public Hits search(Query query, Sort sort)
    throws IOException {
    return new Hits(this, query, null, sort);
  }

  /** Returns documents matching <code>query</code> and <code>filter</code>,
   * sorted by <code>sort</code>.
   * @throws BooleanQuery.TooManyClauses
   */
  public Hits search(Query query, Filter filter, Sort sort)
    throws IOException {
    return new Hits(this, query, filter, sort);
  }

  /** Lower-level search API.
   *
   * <p>{@link HitCollector#collect(int,float)} is called for every non-zero
   * scoring document.
   *
   * <p>Applications should only use this if they need <i>all</i> of the
   * matching documents.  The high-level search API ({@link
   * Searcher#search(Query)}) is usually more efficient, as it skips
   * non-high-scoring hits.
   * <p>Note: The <code>score</code> passed to this method is a raw score.
   * In other words, the score will not necessarily be a float whose value is
   * between 0 and 1.
   * @throws BooleanQuery.TooManyClauses
   */
  public void search(Query query, HitCollector results)
    throws IOException {
    search(query, (Filter)null, results);
  }    

  /** The Similarity implementation used by this searcher. */
  private Similarity similarity = Similarity.getDefault();

  /** Expert: Set the Similarity implementation used by this Searcher.
   *
   * @see Similarity#setDefault(Similarity)
   */
  public void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
  }

  /** Expert: Return the Similarity implementation used by this Searcher.
   *
   * <p>This defaults to the current value of {@link Similarity#getDefault()}.
   */
  public Similarity getSimilarity() {
    return this.similarity;
  }
}
