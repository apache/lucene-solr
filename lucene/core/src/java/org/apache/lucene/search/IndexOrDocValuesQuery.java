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
import java.util.Set;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;

/**
 * A query that uses either an index structure (points or terms) or doc values
 * in order to run a query, depending which one is more efficient. This is
 * typically useful for range queries, whose {@link Weight#scorer} is costly
 * to create since it usually needs to sort large lists of doc ids. For
 * instance, for a field that both indexed {@link LongPoint}s and
 * {@link SortedNumericDocValuesField}s with the same values, an efficient
 * range query could be created by doing:
 * <pre class="prettyprint">
 *   String field;
 *   long minValue, maxValue;
 *   Query pointQuery = LongPoint.newRangeQuery(field, minValue, maxValue);
 *   Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, minValue, maxValue);
 *   Query query = new IndexOrDocValuesQuery(pointQuery, dvQuery);
 * </pre>
 * The above query will be efficient as it will use points in the case that they
 * perform better, ie. when we need a good lead iterator that will be almost
 * entirely consumed; and doc values otherwise, ie. in the case that another
 * part of the query is already leading iteration but we still need the ability
 * to verify that some documents match.
 * <p><b>NOTE</b>This query currently only works well with point range/exact
 * queries and their equivalent doc values queries.
 * @lucene.experimental
 */
public final class IndexOrDocValuesQuery extends Query {

  private final Query indexQuery, dvQuery;

  /**
   * Create an {@link IndexOrDocValuesQuery}. Both provided queries must match
   * the same documents and give the same scores.
   * @param indexQuery a query that has a good iterator but whose scorer may be costly to create
   * @param dvQuery a query whose scorer is cheap to create that can quickly check whether a given document matches
   */
  public IndexOrDocValuesQuery(Query indexQuery, Query dvQuery) {
    this.indexQuery = indexQuery;
    this.dvQuery = dvQuery;
  }

  /** Return the wrapped query that may be costly to initialize but has a good
   *  iterator. */
  public Query getIndexQuery() {
    return indexQuery;
  }

  /** Return the wrapped query that may be slow at identifying all matching
   *  documents, but which is cheap to initialize and can efficiently
   *  verify that some documents match. */
  public Query getRandomAccessQuery() {
    return dvQuery;
  }

  @Override
  public String toString(String field) {
    return indexQuery.toString(field);
  }

  @Override
  public boolean equals(Object obj) {
    if (sameClassAs(obj) == false) {
      return false;
    }
    IndexOrDocValuesQuery that = (IndexOrDocValuesQuery) obj;
    return indexQuery.equals(that.indexQuery) && dvQuery.equals(that.dvQuery);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + indexQuery.hashCode();
    h = 31 * h + dvQuery.hashCode();
    return h;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query indexRewrite = indexQuery.rewrite(reader);
    Query dvRewrite = dvQuery.rewrite(reader);
    if (indexQuery != indexRewrite || dvQuery != dvRewrite) {
      return new IndexOrDocValuesQuery(indexRewrite, dvRewrite);
    }
    return this;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.MUST, this);
    indexQuery.visit(v);
    dvQuery.visit(v);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    final Weight indexWeight = indexQuery.createWeight(searcher, scoreMode, boost);
    final Weight dvWeight = dvQuery.createWeight(searcher, scoreMode, boost);
    return new Weight(this) {
      @Override
      public void extractTerms(Set<Term> terms) {
        indexWeight.extractTerms(terms);
      }

      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {
        // We need to check a single doc, so the dv query should perform better
        return dvWeight.matches(context, doc);
      }

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        // We need to check a single doc, so the dv query should perform better
        return dvWeight.explain(context, doc);
      }

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        // Bulk scorers need to consume the entire set of docs, so using an
        // index structure should perform better
        return indexWeight.bulkScorer(context);
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final ScorerSupplier indexScorerSupplier = indexWeight.scorerSupplier(context);
        final ScorerSupplier dvScorerSupplier = dvWeight.scorerSupplier(context);
        if (indexScorerSupplier == null || dvScorerSupplier == null) {
          return null;
        }
        return new ScorerSupplier() {
          @Override
          public Scorer get(long leadCost) throws IOException {
            // At equal costs, doc values tend to be worse than points since they
            // still need to perform one comparison per document while points can
            // do much better than that given how values are organized. So we give
            // an arbitrary 8x penalty to doc values.
            final long threshold = cost() >>> 3;
            if (threshold <= leadCost) {
              return indexScorerSupplier.get(leadCost);
            } else {
              return dvScorerSupplier.get(leadCost);
            }
          }

          @Override
          public long cost() {
            return indexScorerSupplier.cost();
          }
        };
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
        // Both index and dv query should return the same values, so we can use
        // the index query's cachehelper here
        return indexWeight.isCacheable(ctx);
      }

    };
  }

}
