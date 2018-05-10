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

import org.apache.lucene.search.similarities.Similarity;

/**
 * Base class for exact and sloppy phrase matching
 *
 * To find matches on a document, first advance {@link #approximation} to the
 * relevant document, then call {@link #reset()}.  Clients can then call
 * {@link #nextMatch()} to iterate over the matches
 */
abstract class PhraseMatcher {

  protected final DocIdSetIterator approximation;
  private final float matchCost;

  PhraseMatcher(DocIdSetIterator approximation, float matchCost) {
    assert TwoPhaseIterator.unwrap(approximation) == null;
    this.approximation = approximation;
    this.matchCost = matchCost;
  }

  /**
   * An upper bound on the number of possible matches on this document
   */
  abstract float maxFreq() throws IOException;

  /**
   * Called after {@link #approximation} has been advanced
   */
  public abstract void reset() throws IOException;

  /**
   * Find the next match on the current document, returning {@code false} if there
   * are none.
   */
  public abstract boolean nextMatch() throws IOException;

  /**
   * The slop-adjusted weight of the current match
   *
   * The sum of the slop-adjusted weights is used as the freq for scoring
   */
  abstract float sloppyWeight(Similarity.SimScorer simScorer);

  /**
   * The start position of the current match
   */
  abstract int startPosition();

  /**
   * The end position of the current match
   */
  abstract int endPosition();

  /**
   * The start offset of the current match
   */
  abstract int startOffset() throws IOException;

  /**
   * The end offset of the current match
   */
  abstract int endOffset() throws IOException;

  /**
   * An estimate of the average cost of finding all matches on a document
   *
   * @see TwoPhaseIterator#matchCost()
   */
  public float getMatchCost() {
    return matchCost;
  }
}
