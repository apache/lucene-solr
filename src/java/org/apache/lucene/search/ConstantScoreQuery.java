package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
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

import java.io.IOException;
import java.util.BitSet;
import java.util.Set;

/**
 * A query that wraps a filter and simply returns a constant score equal to the
 * query boost for every document in the filter.
 *
 * @author yonik
 * @version $Id$
 */
public class ConstantScoreQuery extends Query {
  protected final Filter filter;

  public ConstantScoreQuery(Filter filter) {
    this.filter=filter;
  }

  public Query rewrite(IndexReader reader) throws IOException {
    return this;
  }

  public void extractTerms(Set terms) {
    // OK to not add any terms when used for MultiSearcher,
    // but may not be OK for highlighting
  }

  protected class ConstantWeight implements Weight {
    private Similarity similarity;
    private float queryNorm;
    private float queryWeight;

    public ConstantWeight(Searcher searcher) {
      this.similarity = getSimilarity(searcher);
    }

    public Query getQuery() {
      return ConstantScoreQuery.this;
    }

    public float getValue() {
      return queryWeight;
    }

    public float sumOfSquaredWeights() throws IOException {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    public void normalize(float norm) {
      this.queryNorm = norm;
      queryWeight *= this.queryNorm;
    }

    public Scorer scorer(IndexReader reader) throws IOException {
      return new ConstantScorer(similarity, reader, this);
    }

    public Explanation explain(IndexReader reader, int doc) throws IOException {

      ConstantScorer cs = (ConstantScorer)scorer(reader);
      boolean exists = cs.bits.get(doc);

      ComplexExplanation result = new ComplexExplanation();

      if (exists) {
        result.setDescription("ConstantScoreQuery(" + filter
        + "), product of:");
        result.setValue(queryWeight);
        result.setMatch(Boolean.TRUE);
        result.addDetail(new Explanation(getBoost(), "boost"));
        result.addDetail(new Explanation(queryNorm,"queryNorm"));
      } else {
        result.setDescription("ConstantScoreQuery(" + filter
        + ") doesn't match id " + doc);
        result.setValue(0);
        result.setMatch(Boolean.FALSE);
      }
      return result;
    }
  }

  protected class ConstantScorer extends Scorer {
    final BitSet bits;
    final float theScore;
    int doc=-1;

    public ConstantScorer(Similarity similarity, IndexReader reader, Weight w) throws IOException {
      super(similarity);
      theScore = w.getValue();
      bits = filter.bits(reader);
    }

    public boolean next() throws IOException {
      doc = bits.nextSetBit(doc+1);
      return doc >= 0;
    }

    public int doc() {
      return doc;
    }

    public float score() throws IOException {
      return theScore;
    }

    public boolean skipTo(int target) throws IOException {
      doc = bits.nextSetBit(target);  // requires JDK 1.4
      return doc >= 0;
    }

    public Explanation explain(int doc) throws IOException {
      throw new UnsupportedOperationException();
    }
  }


  protected Weight createWeight(Searcher searcher) {
    return new ConstantScoreQuery.ConstantWeight(searcher);
  }


  /** Prints a user-readable version of this query. */
  public String toString(String field)
  {
    return "ConstantScore(" + filter.toString()
      + (getBoost()==1.0 ? ")" : "^" + getBoost());
  }

  /** Returns true if <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ConstantScoreQuery)) return false;
    ConstantScoreQuery other = (ConstantScoreQuery)o;
    return this.getBoost()==other.getBoost() && filter.equals(other.filter);
  }

  /** Returns a hash code value for this object. */
  public int hashCode() {
    // Simple add is OK since no existing filter hashcode has a float component.
    return filter.hashCode() + Float.floatToIntBits(getBoost());
  }

}


