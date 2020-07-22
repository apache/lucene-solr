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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;

/**
 * A query that wraps another query and simply returns a constant score equal to
 * 1 for every document that matches the query.
 * It therefore simply strips of all scores and always returns 1.
 */
public final class ConstantScoreQuery extends Query {
  private final Query query;

  /** Strips off scores from the passed in Query. The hits will get a constant score
   * of 1. */
  public ConstantScoreQuery(Query query) {
    this.query = Objects.requireNonNull(query, "Query must not be null");
  }

  /** Returns the encapsulated query. */
  public Query getQuery() {
    return query;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = query.rewrite(reader);

    if (rewritten != query) {
      return new ConstantScoreQuery(rewritten);
    }

    if (rewritten.getClass() == ConstantScoreQuery.class) {
      return rewritten;
    }

    if (rewritten.getClass() == BoostQuery.class) {
      return new ConstantScoreQuery(((BoostQuery) rewritten).getQuery());
    }

    return super.rewrite(reader);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor.getSubVisitor(BooleanClause.Occur.FILTER, this));
  }

  /** We return this as our {@link BulkScorer} so that if the CSQ
   *  wraps a query with its own optimized top-level
   *  scorer (e.g. BooleanScorer) we can use that
   *  top-level scorer. */
  protected static class ConstantBulkScorer extends BulkScorer {
    final BulkScorer bulkScorer;
    final Weight weight;
    final float theScore;

    public ConstantBulkScorer(BulkScorer bulkScorer, Weight weight, float theScore) {
      this.bulkScorer = bulkScorer;
      this.weight = weight;
      this.theScore = theScore;
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
      return bulkScorer.score(wrapCollector(collector), acceptDocs, min, max);
    }

    private LeafCollector wrapCollector(LeafCollector collector) {
      return new FilterLeafCollector(collector) {
        @Override
        public void setScorer(Scorable scorer) throws IOException {
          // we must wrap again here, but using the scorer passed in as parameter:
          in.setScorer(new FilterScorable(scorer) {
            @Override
            public float score() {
              return theScore;
            }
          });
        }
      };
    }

    @Override
    public long cost() {
      return bulkScorer.cost();
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    final Weight innerWeight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
    if (scoreMode.needsScores()) {
      return new ConstantScoreWeight(this, boost) {
        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
          if (scoreMode.isExhaustive() == false) {
            return super.bulkScorer(context);
          }
          final BulkScorer innerScorer = innerWeight.bulkScorer(context);
          if (innerScorer == null) {
            return null;
          }
          return new ConstantBulkScorer(innerScorer, this, score());
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
          ScorerSupplier innerScorerSupplier = innerWeight.scorerSupplier(context);
          if (innerScorerSupplier == null) {
            return null;
          }
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
              final Scorer innerScorer = innerScorerSupplier.get(leadCost);
              final TwoPhaseIterator twoPhaseIterator = innerScorer.twoPhaseIterator();
              if (twoPhaseIterator == null) {
                return new ConstantScoreScorer(innerWeight, score(), scoreMode, innerScorer.iterator());
              } else {
                return new ConstantScoreScorer(innerWeight, score(), scoreMode, twoPhaseIterator);
              }
            }

            @Override
            public long cost() {
              return innerScorerSupplier.cost();
            }
          };
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
          return innerWeight.matches(context, doc);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          ScorerSupplier scorerSupplier = scorerSupplier(context);
          if (scorerSupplier == null) {
            return null;
          }
          return scorerSupplier.get(Long.MAX_VALUE);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return innerWeight.isCacheable(ctx);
        }

      };
    } else {
      return innerWeight;
    }
  }

  @Override
  public String toString(String field) {
    return new StringBuilder("ConstantScore(")
      .append(query.toString(field))
      .append(')')
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           query.equals(((ConstantScoreQuery) other).query);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + query.hashCode();
  }
}
