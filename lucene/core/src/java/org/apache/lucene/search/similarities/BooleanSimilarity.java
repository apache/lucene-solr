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
package org.apache.lucene.search.similarities;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;

/**
 * Simple similarity that gives terms a score that is equal to their query boost. This similarity is
 * typically used with disabled norms since neither document statistics nor index statistics are
 * used for scoring. That said, if norms are enabled, they will be computed the same way as {@link
 * SimilarityBase} and {@link BM25Similarity} with {@link
 * SimilarityBase#setDiscountOverlaps(boolean) discounted overlaps} so that the {@link Similarity}
 * can be changed after the index has been created.
 */
public class BooleanSimilarity extends Similarity {

  private static final Similarity BM25_SIM = new BM25Similarity();

  /** Sole constructor */
  public BooleanSimilarity() {}

  @Override
  public long computeNorm(FieldInvertState state) {
    return BM25_SIM.computeNorm(state);
  }

  @Override
  public SimScorer scorer(
      float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    return new BooleanWeight(boost);
  }

  private static class BooleanWeight extends SimScorer {
    final float boost;

    BooleanWeight(float boost) {
      this.boost = boost;
    }

    @Override
    public float score(float freq, long norm) {
      return boost;
    }

    @Override
    public Explanation explain(Explanation freq, long norm) {
      Explanation queryBoostExpl = Explanation.match(boost, "boost, query boost");
      return Explanation.match(
          queryBoostExpl.getValue(),
          "score(" + getClass().getSimpleName() + "), computed from:",
          queryBoostExpl);
    }
  }
}
