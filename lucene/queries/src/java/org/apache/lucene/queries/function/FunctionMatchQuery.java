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
import java.util.Objects;
import java.util.function.DoublePredicate;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * A query that retrieves all documents with a {@link DoubleValues} value matching a predicate
 *
 * This query works by a linear scan of the index, and is best used in
 * conjunction with other queries that can restrict the number of
 * documents visited
 */
public final class FunctionMatchQuery extends Query {

  static final float DEFAULT_MATCH_COST = 100;

  private final DoubleValuesSource source;
  private final DoublePredicate filter;
  private final float matchCost; // not used in equals/hashCode

  /**
   * Create a FunctionMatchQuery with default TwoPhaseIterator matchCost -
   * {@link #DEFAULT_MATCH_COST} = {@value #DEFAULT_MATCH_COST}
   * @param source  a {@link DoubleValuesSource} to use for values
   * @param filter  the predicate to match against
   */
  public FunctionMatchQuery(DoubleValuesSource source, DoublePredicate filter) {
    this(source, filter, DEFAULT_MATCH_COST);
  }

  /**
   * Create a FunctionMatchQuery
   * @param source     a {@link DoubleValuesSource} to use for values
   * @param filter     the predicate to match against
   * @param matchCost  to be returned by {@link TwoPhaseIterator#matchCost()}
   */
  public FunctionMatchQuery(DoubleValuesSource source, DoublePredicate filter, float matchCost) {
    this.source = source;
    this.filter = filter;
    this.matchCost = matchCost;
  }

  @Override
  public String toString(String field) {
    return "FunctionMatchQuery(" + source.toString() + ")";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    DoubleValuesSource vs = source.rewrite(searcher);
    return new ConstantScoreWeight(this, boost) {
      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        DoubleValues values = vs.getValues(context, null);
        DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
        TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
          @Override
          public boolean matches() throws IOException {
            return values.advanceExact(approximation.docID()) && filter.test(values.doubleValue());
          }

          @Override
          public float matchCost() {
            return matchCost; // TODO maybe DoubleValuesSource should have a matchCost?
          }
        };
        return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return source.isCacheable(ctx);
      }

    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FunctionMatchQuery that = (FunctionMatchQuery) o;
    return Objects.equals(source, that.source) && Objects.equals(filter, that.filter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, filter);
  }

}
