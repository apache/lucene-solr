package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ToStringUtils;

import java.util.Set;

/**
 * A query that matches all documents.
 *
 */
public class MatchAllDocsQuery extends Query {

  public MatchAllDocsQuery() {
  }

  private class MatchAllScorer extends Scorer {

    final IndexReader reader;
    int id;
    final int maxId;
    final float score;

    MatchAllScorer(IndexReader reader, Similarity similarity, Weight w) {
      super(similarity);
      this.reader = reader;
      id = -1;
      maxId = reader.maxDoc() - 1;
      score = w.getValue();
    }

    public Explanation explain(int doc) {
      return null; // not called... see MatchAllDocsWeight.explain()
    }

    public int doc() {
      return id;
    }

    public boolean next() {
      while (id < maxId) {
        id++;
        if (!reader.isDeleted(id)) {
          return true;
        }
      }
      return false;
    }

    public float score() {
      return score;
    }

    public boolean skipTo(int target) {
      id = target - 1;
      return next();
    }

  }

  private class MatchAllDocsWeight implements Weight {
    private Similarity similarity;
    private float queryWeight;
    private float queryNorm;

    public MatchAllDocsWeight(Searcher searcher) {
      this.similarity = searcher.getSimilarity();
    }

    public String toString() {
      return "weight(" + MatchAllDocsQuery.this + ")";
    }

    public Query getQuery() {
      return MatchAllDocsQuery.this;
    }

    public float getValue() {
      return queryWeight;
    }

    public float sumOfSquaredWeights() {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    public void normalize(float queryNorm) {
      this.queryNorm = queryNorm;
      queryWeight *= this.queryNorm;
    }

    public Scorer scorer(IndexReader reader) {
      return new MatchAllScorer(reader, similarity, this);
    }

    public Explanation explain(IndexReader reader, int doc) {
      // explain query weight
      Explanation queryExpl = new ComplexExplanation
        (true, getValue(), "MatchAllDocsQuery, product of:");
      if (getBoost() != 1.0f) {
        queryExpl.addDetail(new Explanation(getBoost(),"boost"));
      }
      queryExpl.addDetail(new Explanation(queryNorm,"queryNorm"));

      return queryExpl;
    }
  }

  protected Weight createWeight(Searcher searcher) {
    return new MatchAllDocsWeight(searcher);
  }

  public void extractTerms(Set terms) {
  }

  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("MatchAllDocsQuery");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  public boolean equals(Object o) {
    if (!(o instanceof MatchAllDocsQuery))
      return false;
    MatchAllDocsQuery other = (MatchAllDocsQuery) o;
    return this.getBoost() == other.getBoost();
  }

  public int hashCode() {
    return Float.floatToIntBits(getBoost()) ^ 0x1AA71190;
  }
}
