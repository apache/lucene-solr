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
  protected final FunctionValues values;
  private final TwoPhaseIterator twoPhaseIterator;
  private final DocIdSetIterator disi;

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
        return 100; // TODO: use cost of ValueSourceScorer.this.matches()
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

}
