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
package org.apache.lucene.queries.function;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * {@link Scorer} which returns the result of {@link FunctionValues#floatVal(int)} as
 * the score for a document, and which filters out documents that don't match {@link #matches(int)}.
 * This Scorer has a {@link TwoPhaseIterator}.  This is similar to {@link FunctionQuery},
 * with an added filter.
 * <p>
 * Note: If the scores are needed, then the underlying value will probably be
 * fetched/computed twice -- once to filter and next to return the score.  If that's non-trivial then
 * consider wrapping it in an implementation that will cache the current value.
 * </p>
 *
 * @see FunctionQuery
 * @lucene.experimental
 */
public abstract class ValueSourceScorer extends Scorer {
  // Fixed cost for a single iteration of the TwoPhaseIterator instance
  private static final int DEF_COST = 5;

  protected final FunctionValues values;
  private final TwoPhaseIterator twoPhaseIterator;
  private final DocIdSetIterator disi;
  private Float matchCost;

  protected ValueSourceScorer(Weight weight, LeafReaderContext readerContext, FunctionValues values) {
    super(weight);
    this.values = values;
    final DocIdSetIterator approximation = DocIdSetIterator.all(readerContext.reader().maxDoc()); // no approximation!
    this.twoPhaseIterator = new TwoPhaseIterator(approximation) {
      @Override
      public boolean matches() throws IOException {
        return ValueSourceScorer.this.matches(approximation.docID());
      }

      @Override
      public float matchCost() {
        // If an external cost is set, use that
        if (matchCost != 0.0) {
          return matchCost;
        }

        return costEvaluationFunction();
      }
    };
    this.disi = TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator);
  }

  /** Override to decide if this document matches. It's called by {@link TwoPhaseIterator#matches()}. */
  public abstract boolean matches(int doc) throws IOException;

  @Override
  public DocIdSetIterator iterator() {
    return disi;
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return twoPhaseIterator;
  }

  @Override
  public int docID() {
    return disi.docID();
  }

  @Override
  public float score() throws IOException {
    // (same as FunctionQuery, but no qWeight)  TODO consider adding configurable qWeight
    float score = values.floatVal(disi.docID());
    // Current Lucene priority queues can't handle NaN and -Infinity, so
    // map to -Float.MAX_VALUE. This conditional handles both -infinity
    // and NaN since comparisons with NaN are always false.
    return score > Float.NEGATIVE_INFINITY ? score : -Float.MAX_VALUE;
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return Float.POSITIVE_INFINITY;
  }

  /**
   * Used to externally set a mutable cost for this instance. If set, this cost gets preference over the FunctionValues's cost
   * The value set here is used by {@link TwoPhaseIterator#matchCost()} for the TwoPhaseIterator owned by this class
   *
   * @lucene.experimental
   */
  public void setMatchCost(float cost) {
    matchCost = cost;
  }

  /**
   * Cost evaluation function which defines the cost of access for the TwoPhaseIterator for this class
   * This method should be overridden for specifying custom cost methods. Used by {@link TwoPhaseIterator#matchCost()}
   * for the instance owned by this class
   *
   * @return cost of access
   *
   * @lucene.experimental
   */
  protected float costEvaluationFunction() {
    // Cost of iteration is fixed cost + cost exposed by delegated FunctionValues instance
    return DEF_COST + values.cost();
  }
}
