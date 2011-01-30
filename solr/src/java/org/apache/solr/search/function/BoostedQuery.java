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
import org.apache.solr.search.SolrIndexReader;

import java.io.IOException;
import java.util.Set;
import java.util.Map;

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

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query newQ = q.rewrite(reader);
    if (newQ == q) return this;
    BoostedQuery bq = (BoostedQuery)this.clone();
    bq.q = newQ;
    return bq;
  }

  @Override
  public void extractTerms(Set terms) {
    q.extractTerms(terms);
  }

  @Override
  public Weight createWeight(Searcher searcher) throws IOException {
    return new BoostedQuery.BoostedWeight(searcher);
  }

  private class BoostedWeight extends Weight {
    Searcher searcher;
    Weight qWeight;
    Map context;

    public BoostedWeight(Searcher searcher) throws IOException {
      this.searcher = searcher;
      this.qWeight = q.weight(searcher);
      this.context = boostVal.newContext();
      boostVal.createWeight(context,searcher);
    }

    @Override
    public Query getQuery() {
      return BoostedQuery.this;
    }

    @Override
    public float getValue() {
      return getBoost();
    }

    @Override
    public float sumOfSquaredWeights() throws IOException {
      float sum = qWeight.sumOfSquaredWeights();
      sum *= getBoost() * getBoost();
      return sum ;
    }

    @Override
    public void normalize(float norm) {
      norm *= getBoost();
      qWeight.normalize(norm);
    }

    @Override
    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
      Scorer subQueryScorer = qWeight.scorer(reader, true, false);
      if(subQueryScorer == null) {
        return null;
      }
      return new BoostedQuery.CustomScorer(getSimilarity(searcher), searcher, reader, this, subQueryScorer, boostVal);
    }

    @Override
    public Explanation explain(IndexReader reader, int doc) throws IOException {
      SolrIndexReader topReader = (SolrIndexReader)reader;
      SolrIndexReader[] subReaders = topReader.getLeafReaders();
      int[] offsets = topReader.getLeafOffsets();
      int readerPos = SolrIndexReader.readerIndex(doc, offsets);
      int readerBase = offsets[readerPos];

      Explanation subQueryExpl = qWeight.explain(reader,doc);
      if (!subQueryExpl.isMatch()) {
        return subQueryExpl;
      }

      DocValues vals = boostVal.getValues(context, subReaders[readerPos]);
      float sc = subQueryExpl.getValue() * vals.floatVal(doc-readerBase);
      Explanation res = new ComplexExplanation(
        true, sc, BoostedQuery.this.toString() + ", product of:");
      res.addDetail(subQueryExpl);
      res.addDetail(vals.explain(doc-readerBase));
      return res;
    }
  }


  private class CustomScorer extends Scorer {
    private final BoostedQuery.BoostedWeight weight;
    private final float qWeight;
    private final Scorer scorer;
    private final DocValues vals;
    private final IndexReader reader;
    private final Searcher searcher;

    private CustomScorer(Similarity similarity, Searcher searcher, IndexReader reader, BoostedQuery.BoostedWeight w,
        Scorer scorer, ValueSource vs) throws IOException {
      super(similarity, w);
      this.weight = w;
      this.qWeight = w.getValue();
      this.scorer = scorer;
      this.reader = reader;
      this.searcher = searcher; // for explain
      this.vals = vs.getValues(weight.context, reader);
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

    public Explanation explain(int doc) throws IOException {
      Explanation subQueryExpl = weight.qWeight.explain(reader,doc);
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


  @Override
  public String toString(String field) {
    StringBuilder sb = new StringBuilder();
    sb.append("boost(").append(q.toString(field)).append(',').append(boostVal).append(')');
    sb.append(ToStringUtils.boost(getBoost()));
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) return false;
    BoostedQuery other = (BoostedQuery)o;
    return this.getBoost() == other.getBoost()
           && this.q.equals(other.q)
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
