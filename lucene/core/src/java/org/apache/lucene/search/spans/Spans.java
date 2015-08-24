package org.apache.lucene.search.spans;

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

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

/** Iterates through combinations of start/end positions per-doc.
 *  Each start/end position represents a range of term positions within the current document.
 *  These are enumerated in order, by increasing document number, within that by
 *  increasing start position and finally by increasing end position.
 */
public abstract class Spans extends DocIdSetIterator {
  public static final int NO_MORE_POSITIONS = Integer.MAX_VALUE;

  /**
   * Returns the next start position for the current doc.
   * There is always at least one start/end position per doc.
   * After the last start/end position at the current doc this returns {@link #NO_MORE_POSITIONS}.
   */
  public abstract int nextStartPosition() throws IOException;

  /**
   * Returns the start position in the current doc, or -1 when {@link #nextStartPosition} was not yet called on the current doc.
   * After the last start/end position at the current doc this returns {@link #NO_MORE_POSITIONS}.
   */
  public abstract int startPosition();

  /**
   * Returns the end position for the current start position, or -1 when {@link #nextStartPosition} was not yet called on the current doc.
   * After the last start/end position at the current doc this returns {@link #NO_MORE_POSITIONS}.
   */
  public abstract int endPosition();

  /**
   * Return the width of the match, which is typically used to compute
   * the {@link SimScorer#computeSlopFactor(int) slop factor}. It is only legal
   * to call this method when the iterator is on a valid doc ID and positioned.
   * The return value must be positive, and lower values means that the match is
   * better.
   */
  public abstract int width();

  /**
   * Collect postings data from the leaves of the current Spans.
   *
   * This method should only be called after {@link #nextStartPosition()}, and before
   * {@link #NO_MORE_POSITIONS} has been reached.
   *
   * @param collector a SpanCollector
   *
   * @lucene.experimental
   */
  public abstract void collect(SpanCollector collector) throws IOException;

  /**
   * Optional method: Return a {@link TwoPhaseIterator} view of this
   * {@link Spans}. A return value of {@code null} indicates that
   * two-phase iteration is not supported.
   *
   * Note that the returned {@link TwoPhaseIterator}'s
   * {@link TwoPhaseIterator#approximation() approximation} must
   * advance documents synchronously with this iterator:
   * advancing the approximation must
   * advance this iterator and vice-versa.
   *
   * Implementing this method is typically useful on a {@link Spans}
   * that has a high per-document overhead for confirming matches.
   *
   * The default implementation returns {@code null}.
   */
  public TwoPhaseIterator asTwoPhaseIterator() {
    return null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Class<? extends Spans> clazz = getClass();
    sb.append(clazz.isAnonymousClass() ? clazz.getName() : clazz.getSimpleName());
    sb.append("(doc=").append(docID());
    sb.append(",start=").append(startPosition());
    sb.append(",end=").append(endPosition());
    sb.append(")");
    return sb.toString();
  }

}
