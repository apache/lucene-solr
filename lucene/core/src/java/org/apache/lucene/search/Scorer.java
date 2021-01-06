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
import java.util.Objects;

/**
 * Expert: Common scoring functionality for different types of queries.
 *
 * <p>A <code>Scorer</code> exposes an {@link #iterator()} over documents matching a query in
 * increasing order of doc Id.
 *
 * <p>Document scores are computed using a given <code>Similarity</code> implementation.
 *
 * <p><b>NOTE</b>: The values Float.Nan, Float.NEGATIVE_INFINITY and Float.POSITIVE_INFINITY are not
 * valid scores. Certain collectors (eg {@link TopScoreDocCollector}) will not properly collect hits
 * with these scores.
 */
public abstract class Scorer extends Scorable {

  /** the Scorer's parent Weight */
  protected final Weight weight;

  /**
   * Constructs a Scorer
   *
   * @param weight The scorers <code>Weight</code>.
   */
  protected Scorer(Weight weight) {
    this.weight = Objects.requireNonNull(weight);
  }

  /**
   * returns parent Weight
   *
   * @lucene.experimental
   */
  public Weight getWeight() {
    return weight;
  }

  /**
   * Return a {@link DocIdSetIterator} over matching documents.
   *
   * <p>The returned iterator will either be positioned on {@code -1} if no documents have been
   * scored yet, {@link DocIdSetIterator#NO_MORE_DOCS} if all documents have been scored already, or
   * the last document id that has been scored otherwise.
   *
   * <p>The returned iterator is a view: calling this method several times will return iterators
   * that have the same state.
   */
  public abstract DocIdSetIterator iterator();

  /**
   * Optional method: Return a {@link TwoPhaseIterator} view of this {@link Scorer}. A return value
   * of {@code null} indicates that two-phase iteration is not supported.
   *
   * <p>Note that the returned {@link TwoPhaseIterator}'s {@link TwoPhaseIterator#approximation()
   * approximation} must advance synchronously with the {@link #iterator()}: advancing the
   * approximation must advance the iterator and vice-versa.
   *
   * <p>Implementing this method is typically useful on {@link Scorer}s that have a high
   * per-document overhead in order to confirm matches.
   *
   * <p>The default implementation returns {@code null}.
   */
  public TwoPhaseIterator twoPhaseIterator() {
    return null;
  }

  /**
   * Advance to the block of documents that contains {@code target} in order to get scoring
   * information about this block. This method is implicitly called by {@link
   * DocIdSetIterator#advance(int)} and {@link DocIdSetIterator#nextDoc()} on the returned doc ID.
   * Calling this method doesn't modify the current {@link DocIdSetIterator#docID()}. It returns a
   * number that is greater than or equal to all documents contained in the current block, but less
   * than any doc IDS of the next block. {@code target} must be &gt;= {@link #docID()} as well as
   * all targets that have been passed to {@link #advanceShallow(int)} so far.
   */
  public int advanceShallow(int target) throws IOException {
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  /**
   * Return the maximum score that documents between the last {@code target} that this iterator was
   * {@link #advanceShallow(int) shallow-advanced} to included and {@code upTo} included.
   */
  public abstract float getMaxScore(int upTo) throws IOException;
}
