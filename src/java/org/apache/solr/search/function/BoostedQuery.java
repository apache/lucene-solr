/**
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

package org.apache.solr.search.function;

import org.apache.lucene.search.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Set;

/**
 * Query that is boosted by a ValueSource
 */
public class BoostedQuery extends Query {
  private Query q;
  private ValueSource boostVal; // optional, can be null

  public BoostedQuery(Query subQuery, ValueSource boostVal) {
    this.q = subQuery;
    this.boostVal = boostVal;
  }

  public Query getQuery() { return q; }
  public ValueSource getValueSource() { return boostVal; }

  public Query rewrite(IndexReader reader) throws IOException {
    return q.rewrite(reader);
  }

  public void extractTerms(Set terms) {
    q.extractTerms(terms);
  }

  protected Weight createWeight(Searcher searcher) throws IOException {
    return new BoostedQuery.BoostedWeight(searcher);
  }

  private class BoostedWeight implements Weight {
    Searcher searcher;
    Weight weight;
    boolean qStrict;

    public BoostedWeight(Searcher searcher) throws IOException {
      this.searcher = searcher;
      this.weight = q.weight(searcher);
    }

    public Query getQuery() {
      return BoostedQuery.this;
    }

    public float getValue() {
      return getBoost();
    }

    public float sumOfSquaredWeights() throws IOException {
      float sum = weight.sumOfSquaredWeights();
      sum *= getBoost() * getBoost();
      return sum ;
    }

    public void normalize(float norm) {
      norm *= getBoost();
      weight.normalize(norm);
    }

    public Scorer scorer(IndexReader reader) throws IOException {
      Scorer subQueryScorer = weight.scorer(reader);
      return new BoostedQuery.CustomScorer(getSimilarity(searcher), reader, this, subQueryScorer, boostVal);
    }

    public Explanation explain(IndexReader reader, int doc) throws IOException {
      return scorer(reader).explain(doc);
    }
  }


  private class CustomScorer extends Scorer {
    private final BoostedQuery.BoostedWeight weight;
    private final float qWeight;
    private final Scorer scorer;
    private final DocValues vals;
    private final IndexReader reader;

    private CustomScorer(Similarity similarity, IndexReader reader, BoostedQuery.BoostedWeight w,
        Scorer scorer, ValueSource vs) throws IOException {
      super(similarity);
      this.weight = w;
      this.qWeight = w.getValue();
      this.scorer = scorer;
      this.reader = reader;
      this.vals = vs.getValues(reader);
    }

    public boolean next() throws IOException {
      return scorer.next();
    }

    public int doc() {
      return scorer.doc();
    }

    public float score() throws IOException {
      return qWeight * scorer.score() * vals.floatVal(scorer.doc());
    }

    public boolean skipTo(int target) throws IOException {
      return scorer.skipTo(target);
    }

    public Explanation explain(int doc) throws IOException {
      Explanation subQueryExpl = weight.weight.explain(reader,doc);
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
  }


  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    sb.append("boost(").append(q.toString(field)).append(',').append(boostVal).append(')');
    sb.append(ToStringUtils.boost(getBoost()));
    return sb.toString();
  }

  public boolean equals(Object o) {
    if (getClass() != o.getClass()) return false;
    BoostedQuery other = (BoostedQuery)o;
    return this.getBoost() == other.getBoost()
           && this.q.equals(other.q)
           && this.boostVal.equals(other.boostVal);
  }

  public int hashCode() {
    int h = q.hashCode();
    h ^= (h << 17) | (h >>> 16);
    h += boostVal.hashCode();
    h ^= (h << 8) | (h >>> 25);
    h += Float.floatToIntBits(getBoost());
    return h;
  }

}
