package org.apache.lucene.search;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/**
 * A query that matches all documents.
 * 
 * @author John Wang
 */
public class MatchAllDocsQuery extends Query {

  public MatchAllDocsQuery() {
  }

  private class MatchAllScorer extends Scorer {

    IndexReader reader;
    int count;
    int maxDoc;

    MatchAllScorer(IndexReader reader, Similarity similarity) {
      super(similarity);
      this.reader = reader;
      count = -1;
      maxDoc = reader.maxDoc();
    }

    public int doc() {
      return count;
    }

    public Explanation explain(int doc) {
      Explanation explanation = new Explanation();
      explanation.setValue(1.0f);
      explanation.setDescription("MatchAllDocsQuery");
      return explanation;
    }

    public boolean next() {
      while (count < (maxDoc - 1)) {
        count++;
        if (!reader.isDeleted(count)) {
          return true;
        }
      }
      return false;
    }

    public float score() {
      return 1.0f;
    }

    public boolean skipTo(int target) {
      count = target - 1;
      return next();
    }

  }

  private class MatchAllDocsWeight implements Weight {
    private Searcher searcher;

    public MatchAllDocsWeight(Searcher searcher) {
      this.searcher = searcher;
    }

    public String toString() {
      return "weight(" + MatchAllDocsQuery.this + ")";
    }

    public Query getQuery() {
      return MatchAllDocsQuery.this;
    }

    public float getValue() {
      return 1.0f;
    }

    public float sumOfSquaredWeights() {
      return 1.0f;
    }

    public void normalize(float queryNorm) {
    }

    public Scorer scorer(IndexReader reader) {
      return new MatchAllScorer(reader, getSimilarity(searcher));
    }

    public Explanation explain(IndexReader reader, int doc) {
      // explain query weight
      Explanation queryExpl = new Explanation();
      queryExpl.setDescription("MatchAllDocsQuery:");

      Explanation boostExpl = new Explanation(getBoost(), "boost");
      if (getBoost() != 1.0f)
        queryExpl.addDetail(boostExpl);
      queryExpl.setValue(boostExpl.getValue());

      return queryExpl;
    }
  }

  protected Weight createWeight(Searcher searcher) {
    return new MatchAllDocsWeight(searcher);
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
    return Float.floatToIntBits(getBoost());
  }
}
