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

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

/**
 * {@link SimScorer} on a specific {@link LeafReader}.
 */
public final class LeafSimScorer {

  private final SimScorer scorer;
  private final NumericDocValues norms;

  /**
   * Sole constructor: Score documents of {@code reader} with {@code scorer}.
   */
  public LeafSimScorer(SimScorer scorer, LeafReader reader, String field, boolean needsScores) throws IOException {
    this.scorer = Objects.requireNonNull(scorer);
    norms = needsScores ? reader.getNormValues(field) : null;
  }

  /** Return the wrapped {@link SimScorer}. */
  public SimScorer getSimScorer() {
    return scorer;
  }

  private long getNormValue(int doc) throws IOException {
    if (norms != null) {
      boolean found = norms.advanceExact(doc);
      assert found;
      return norms.longValue();
    } else {
      return 1L; // default norm
    }
  }

  /** Score the provided document assuming the given term document frequency.
   *  This method must be called on non-decreasing sequences of doc ids.
   *  @see SimScorer#score(float, long) */
  public float score(int doc, float freq) throws IOException {
    return scorer.score(freq, getNormValue(doc));
  }

  /** Explain the score for the provided document assuming the given term document frequency.
   *  This method must be called on non-decreasing sequences of doc ids.
   *  @see SimScorer#explain(Explanation, long) */
  public Explanation explain(int doc, Explanation freqExpl) throws IOException {
    return scorer.explain(freqExpl, getNormValue(doc));
  }

}
