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

package org.apache.solr.search;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/** Wraps a {@link Query} to customize the {@link TwoPhaseIterator#matchCost()}. */
public class MatchCostQuery extends Query {
  private final Query delegate;
  private final float matchCost;

  public MatchCostQuery(Query delegate, float matchCost) {
    this.delegate = delegate;
    this.matchCost = matchCost;
    assert matchCost >= 0;
  }

  @Override
  public String toString(String field) {
    return delegate.toString(field);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    // don't visit our wrapper, just delegate
    delegate.visit(visitor);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
            Objects.equals(delegate, ((MatchCostQuery) other).delegate);
  }

  @Override
  public int hashCode() {
    return 31 * classHash() + delegate.hashCode();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query rewrite = delegate.rewrite(reader);
    if (delegate == rewrite) {
      return this; // unchanged
    }
    return new MatchCostQuery(rewrite, matchCost);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new Weight(this) {
      final Weight weight = delegate.createWeight(searcher, scoreMode, boost);

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return weight.isCacheable(ctx);
      }

      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {
        return weight.matches(context, doc);
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return weight.explain(context, doc);
      }

      // do not delegate scorerSupplier(); use the default implementation that calls our
      // scorer() so that we can wrap TPI

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final Scorer scorer = weight.scorer(context);
        if (scorer == null) {
          return null;
        }
        final TwoPhaseIterator tpi = scorer.twoPhaseIterator();
        if (tpi == null || tpi.matchCost() == matchCost) {
          return scorer; // needn't wrap/delegate
        }
        return new Scorer(weight) { // pass delegated weight

          @Override
          public TwoPhaseIterator twoPhaseIterator() {
            return new TwoPhaseIterator(tpi.approximation()) {
              @Override
              public boolean matches() throws IOException {
                return tpi.matches();
              }

              @Override
              public float matchCost() {
                return matchCost;
              }
            };
          }

          @Override
          public DocIdSetIterator iterator() {
            return scorer.iterator();
          }

          @Override
          public float getMaxScore(int upTo) throws IOException {
            return scorer.getMaxScore(upTo);
          }

          @Override
          public float score() throws IOException {
            return scorer.score();
          }

          @Override
          public int docID() {
            return scorer.docID();
          }
        };
      }

      // delegate because thus there's no need to care about TPI matchCost if called
      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        return weight.bulkScorer(context);
      }
    };

  }
}
