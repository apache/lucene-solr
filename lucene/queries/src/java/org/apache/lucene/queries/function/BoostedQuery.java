package org.apache.lucene.queries.function;

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

import org.apache.lucene.search.*;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.Map;

/**
 * Query that is boosted by a ValueSource
 */
// TODO: BoostedQuery and BoostingQuery in the same module? 
// something has to give
public class BoostedQuery extends Query {
  private Query q;
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
    if (newQ == q) return this;
    BoostedQuery bq = (BoostedQuery)this.clone();
    bq.q = newQ;
    return bq;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    q.extractTerms(terms);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new BoostedQuery.BoostedWeight(searcher);
  }

  private class BoostedWeight extends Weight {
    final IndexSearcher searcher;
    Weight qWeight;
    Map fcontext;

    public BoostedWeight(IndexSearcher searcher) throws IOException {
      this.searcher = searcher;
      this.qWeight = q.createWeight(searcher);
      this.fcontext = ValueSource.newContext(searcher);
      boostVal.createWeight(fcontext,searcher);
    }

    @Override
    public Query getQuery() {
      return BoostedQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      float sum = qWeight.getValueForNormalization();
      sum *= getBoost() * getBoost();
      return sum ;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      topLevelBoost *= getBoost();
      qWeight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {
      // we are gonna advance() the subscorer
      Scorer subQueryScorer = qWeight.scorer(context, true, false, acceptDocs);
      if(subQueryScorer == null) {
        return null;
      }
      return new BoostedQuery.CustomScorer(context, this, getBoost(), subQueryScorer, boostVal);
    }

    @Override
    public Explanation explain(AtomicReaderContext readerContext, int doc) throws IOException {
      Explanation subQueryExpl = qWeight.explain(readerContext,doc);
      if (!subQueryExpl.isMatch()) {
        return subQueryExpl;
      }
      FunctionValues vals = boostVal.getValues(fcontext, readerContext);
      float sc = subQueryExpl.getValue() * vals.floatVal(doc);
      Explanation res = new ComplexExplanation(
        true, sc, BoostedQuery.this.toString() + ", product of:");
      res.addDetail(subQueryExpl);
      res.addDetail(vals.explain(doc));
      return res;
    }
  }


  private class CustomScorer extends Scorer {
    private final BoostedQuery.BoostedWeight weight;
    private final float qWeight;
    private final Scorer scorer;
    private final FunctionValues vals;
    private final AtomicReaderContext readerContext;

    private CustomScorer(AtomicReaderContext readerContext, BoostedQuery.BoostedWeight w, float qWeight,
        Scorer scorer, ValueSource vs) throws IOException {
      super(w);
      this.weight = w;
      this.qWeight = qWeight;
      this.scorer = scorer;
      this.readerContext = readerContext;
      this.vals = vs.getValues(weight.fcontext, readerContext);
    }

    @Override
    public int docID() {
      return scorer.docID();
    }

    @Override
    public int advance(int target) throws IOException {
      return scorer.advance(target);
    }

    @Override
    public int nextDoc() throws IOException {
      return scorer.nextDoc();
    }

    @Override   
    public float score() throws IOException {
      float score = qWeight * scorer.score() * vals.floatVal(scorer.docID());

      // Current Lucene priority queues can't handle NaN and -Infinity, so
      // map to -Float.MAX_VALUE. This conditional handles both -infinity
      // and NaN since comparisons with NaN are always false.
      return score>Float.NEGATIVE_INFINITY ? score : -Float.MAX_VALUE;
    }

    @Override
    public int freq() throws IOException {
      return scorer.freq();
    }

    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(scorer, "CUSTOM"));
    }

    public Explanation explain(int doc) throws IOException {
      Explanation subQueryExpl = weight.qWeight.explain(readerContext ,doc);
      if (!subQueryExpl.isMatch()) {
        return subQueryExpl;
      }
      float sc = subQueryExpl.getValue() * vals.floatVal(doc);
      Explanation res = new ComplexExplanation(
        true, sc, BoostedQuery.this.toString() + ", product of:");
      res.addDetail(subQueryExpl);
      res.addDetail(vals.explain(doc));
      return res;
    }

    @Override
    public long cost() {
      return scorer.cost();
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
    int h = q.hashCode();
    h ^= (h << 17) | (h >>> 16);
    h += boostVal.hashCode();
    h ^= (h << 8) | (h >>> 25);
    h += Float.floatToIntBits(getBoost());
    return h;
  }

}
