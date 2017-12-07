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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * Query that is boosted by a ValueSource
 */
// TODO: BoostedQuery and BoostingQuery in the same module? 
// something has to give
public final class BoostedQuery extends Query {
  private final Query q;
  private final ValueSource boostVal; // optional, can be null

  public BoostedQuery(Query subQuery, ValueSource boostVal) {
    this.q = subQuery;
    this.boostVal = boostVal;
  }

  public Query getQuery() { return q; }
  public ValueSource getValueSource() { return boostVal; }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query newQ = q.rewrite(reader);
    if (newQ != q) {
      return new BoostedQuery(newQ, boostVal);
    }
    return super.rewrite(reader);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new BoostedQuery.BoostedWeight(searcher, scoreMode, boost);
  }

  private class BoostedWeight extends Weight {
    Weight qWeight;
    Map fcontext;

    public BoostedWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      super(BoostedQuery.this);
      this.qWeight = searcher.createWeight(q, scoreMode, boost);
      this.fcontext = ValueSource.newContext(searcher);
      boostVal.createWeight(fcontext,searcher);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      qWeight.extractTerms(terms);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      Scorer subQueryScorer = qWeight.scorer(context);
      if (subQueryScorer == null) {
        return null;
      }
      return new BoostedQuery.CustomScorer(context, this, subQueryScorer, boostVal);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }

    @Override
    public Explanation explain(LeafReaderContext readerContext, int doc) throws IOException {
      Explanation subQueryExpl = qWeight.explain(readerContext,doc);
      if (!subQueryExpl.isMatch()) {
        return subQueryExpl;
      }
      FunctionValues vals = boostVal.getValues(fcontext, readerContext);
      float factor = vals.floatVal(doc);
      Explanation factorExpl = vals.explain(doc);
      if (factor < 0) {
        factor = 0;
        factorExpl = Explanation.match(0, "truncated score, max of:",
            Explanation.match(0f, "minimum score"), factorExpl);
      } else if (Float.isNaN(factor)) {
        factor = 0;
        factorExpl = Explanation.match(0, "score, computed as (score == NaN ? 0 : score) since NaN is an illegal score from:", factorExpl);
      }
      
      float sc = subQueryExpl.getValue() * factor;
      return Explanation.match(sc, BoostedQuery.this.toString() + ", product of:",
          subQueryExpl, factorExpl);
    }
  }


  private class CustomScorer extends FilterScorer {
    private final BoostedQuery.BoostedWeight weight;
    private final ValueSource vs;
    private final FunctionValues vals;
    private final LeafReaderContext readerContext;

    private CustomScorer(LeafReaderContext readerContext, BoostedQuery.BoostedWeight w,
        Scorer scorer, ValueSource vs) throws IOException {
      super(scorer);
      this.weight = w;
      this.readerContext = readerContext;
      this.vs = vs;
      this.vals = vs.getValues(weight.fcontext, readerContext);
    }

    @Override   
    public float score() throws IOException {
      float factor = vals.floatVal(in.docID());
      if (factor >= 0 == false) { // covers NaN as well
        factor = 0;
      }
      return in.score() * factor;
    }

    @Override
    public float maxScore() {
      return Float.POSITIVE_INFINITY;
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(in, "CUSTOM"));
    }
  }


  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    sb.append("boost(").append(q.toString(field)).append(',').append(boostVal).append(')');
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(BoostedQuery other) {
    return q.equals(other.q) &&
           boostVal.equals(other.boostVal);
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + q.hashCode();
    h = 31 * h + boostVal.hashCode();
    return h;
  }

}
