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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

/**
 * A query that uses either an index (points or terms) or doc values in order
 * to run a range query, depending which one is more efficient.
 */
public final class IndexOrDocValuesQuery extends Query {

  private final Query indexQuery, dvQuery;

  /**
   * Constructor that takes both a query that executes on an index structure
   * like the inverted index or the points tree, and another query that
   * executes on doc values. Both queries must match the same documents and
   * attribute constant scores.
   */
  public IndexOrDocValuesQuery(Query indexQuery, Query dvQuery) {
    this.indexQuery = indexQuery;
    this.dvQuery = dvQuery;
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
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    final Weight indexWeight = indexQuery.createWeight(searcher, needsScores, boost);
    final Weight dvWeight = dvQuery.createWeight(searcher, needsScores, boost);
    return new ConstantScoreWeight(this, boost) {
      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
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
          public Scorer get(boolean randomAccess) throws IOException {
            return (randomAccess ? dvScorerSupplier : indexScorerSupplier).get(randomAccess);
          }

          @Override
          public long cost() {
            return Math.min(indexScorerSupplier.cost(), dvScorerSupplier.cost());
          }
        };
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(false);
      }
    };
  }

}
