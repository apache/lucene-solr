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
import java.util.Random;
import org.apache.lucene.index.LeafReaderContext;

import static org.apache.lucene.util.LuceneTestCase.usually;

class AssertingWeight extends FilterWeight {

  final Random random;
  final ScoreMode scoreMode;

  AssertingWeight(Random random, Weight in, ScoreMode scoreMode) {
    super(in);
    this.random = random;
    this.scoreMode = scoreMode;
  }

  @Override
  public Matches matches(LeafReaderContext context, int doc) throws IOException {
    Matches matches = in.matches(context, doc);
    if (matches == null)
      return null;
    return new AssertingMatches(matches);
  }

  @Override
  public Scorer scorer(LeafReaderContext context) throws IOException {
    if (random.nextBoolean()) {
      final Scorer inScorer = in.scorer(context);
      assert inScorer == null || inScorer.docID() == -1;
      return AssertingScorer.wrap(new Random(random.nextLong()), inScorer, scoreMode);
    } else {
      final ScorerSupplier scorerSupplier = scorerSupplier(context);
      if (scorerSupplier == null) {
        return null;
      }
      if (random.nextBoolean()) {
        // Evil: make sure computing the cost has no side effects
        scorerSupplier.cost();
      }
      return scorerSupplier.get(Long.MAX_VALUE);
    }
  }

  @Override
  public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
    final ScorerSupplier inScorerSupplier = in.scorerSupplier(context);
    if (inScorerSupplier == null) {
      return null;
    }
    return new ScorerSupplier() {
      private boolean getCalled = false;
      @Override
      public Scorer get(long leadCost) throws IOException {
        assert getCalled == false;
        getCalled = true;
        assert leadCost >= 0 : leadCost;
        return AssertingScorer.wrap(new Random(random.nextLong()), inScorerSupplier.get(leadCost), scoreMode);
      }

      @Override
      public long cost() {
        final long cost = inScorerSupplier.cost();
        assert cost >= 0;
        return cost;
      }
    };
  }

  @Override
  public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
    BulkScorer inScorer;
    // We explicitly test both the delegate's bulk scorer, and also the normal scorer.
    // This ensures that normal scorers are sometimes tested with an asserting wrapper.
    if (usually(random)) {
      inScorer = in.bulkScorer(context);
    } else {
      inScorer = super.bulkScorer(context);
    }

    if (inScorer == null) {
      return null;
    }
    return AssertingBulkScorer.wrap(new Random(random.nextLong()), inScorer, context.reader().maxDoc(), scoreMode);
  }
}
