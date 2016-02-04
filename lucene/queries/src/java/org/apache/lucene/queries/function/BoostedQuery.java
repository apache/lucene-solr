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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ToStringUtils;

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
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    Query newQ = q.rewrite(reader);
    if (newQ != q) {
      return new BoostedQuery(newQ, boostVal);
    }
    return super.rewrite(reader);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new BoostedQuery.BoostedWeight(searcher, needsScores);
  }

  private class BoostedWeight extends Weight {
    Weight qWeight;
    Map fcontext;

    public BoostedWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
      super(BoostedQuery.this);
      this.qWeight = searcher.createWeight(q, needsScores);
      this.fcontext = ValueSource.newContext(searcher);
      boostVal.createWeight(fcontext,searcher);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      qWeight.extractTerms(terms);
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return qWeight.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float boost) {
      qWeight.normalize(norm, boost);
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
    public Explanation explain(LeafReaderContext readerContext, int doc) throws IOException {
      Explanation subQueryExpl = qWeight.explain(readerContext,doc);
      if (!subQueryExpl.isMatch()) {
        return subQueryExpl;
      }
      FunctionValues vals = boostVal.getValues(fcontext, readerContext);
      float sc = subQueryExpl.getValue() * vals.floatVal(doc);
      return Explanation.match(sc, BoostedQuery.this.toString() + ", product of:", subQueryExpl, vals.explain(doc));
    }
  }


  private class CustomScorer extends FilterScorer {
    private final BoostedQuery.BoostedWeight weight;
    private final FunctionValues vals;
    private final LeafReaderContext readerContext;

    private CustomScorer(LeafReaderContext readerContext, BoostedQuery.BoostedWeight w,
        Scorer scorer, ValueSource vs) throws IOException {
      super(scorer);
      this.weight = w;
      this.readerContext = readerContext;
      this.vals = vs.getValues(weight.fcontext, readerContext);
    }

    @Override   
    public float score() throws IOException {
      float score = in.score() * vals.floatVal(in.docID());

      // Current Lucene priority queues can't handle NaN and -Infinity, so
      // map to -Float.MAX_VALUE. This conditional handles both -infinity
      // and NaN since comparisons with NaN are always false.
      return score>Float.NEGATIVE_INFINITY ? score : -Float.MAX_VALUE;
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(in, "CUSTOM"));
    }

    public Explanation explain(int doc) throws IOException {
      Explanation subQueryExpl = weight.qWeight.explain(readerContext ,doc);
      if (!subQueryExpl.isMatch()) {
        return subQueryExpl;
      }
      float sc = subQueryExpl.getValue() * vals.floatVal(doc);
      return Explanation.match(sc, BoostedQuery.this.toString() + ", product of:", subQueryExpl, vals.explain(doc));
    }

  }


  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    sb.append("boost(").append(q.toString(field)).append(',').append(boostVal).append(')');
    sb.append(ToStringUtils.boost(getBoost()));
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
  if (!super.equals(o)) return false;
    BoostedQuery other = (BoostedQuery)o;
    return this.q.equals(other.q)
           && this.boostVal.equals(other.boostVal);
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = 31 * h + q.hashCode();
    h = 31 * h + boostVal.hashCode();
    return h;
  }

}
