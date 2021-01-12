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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.search.Matches;

/** Query wrapper that forces its wrapped Query to use the default doc-by-doc BulkScorer. */
class ForceNoBulkScoringQuery extends Query {

  private final Query inner;

  public ForceNoBulkScoringQuery(Query inner) {
    this.inner = inner;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = inner.rewrite(reader);
    if (rewritten != inner) return new ForceNoBulkScoringQuery(rewritten);
    return super.rewrite(reader);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    inner.visit(visitor);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ForceNoBulkScoringQuery that = (ForceNoBulkScoringQuery) o;
    return Objects.equals(inner, that.inner);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inner);
  }

  public Query getWrappedQuery() {
    return inner;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {

    final Weight innerWeight = inner.createWeight(searcher, scoreMode, boost);

    return new Weight(ForceNoBulkScoringQuery.this) {

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return innerWeight.isCacheable(ctx);
      }

      @Override
      public Explanation explain(LeafReaderContext leafReaderContext, int i) throws IOException {
        return innerWeight.explain(leafReaderContext, i);
      }

      @Override
      public Scorer scorer(LeafReaderContext leafReaderContext) throws IOException {
        return innerWeight.scorer(leafReaderContext);
      }

      @Override
      public Matches matches(LeafReaderContext context, int doc) throws IOException {
        return innerWeight.matches(context, doc);
      }
    };
  }

  @Override
  public String toString(String s) {
    return "NoBulkScorer(" + inner.toString(s) + ")";
  }
}
