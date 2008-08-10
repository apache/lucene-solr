package org.apache.lucene.search.function;

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
import org.apache.lucene.search.*;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Set;

/**
 * Expert: A Query that sets the scores of document to the
 * values obtained from a {@link org.apache.lucene.search.function.ValueSource ValueSource}.
 * <p>   
 * The value source can be based on a (cached) value of an indexed field, but it
 * can also be based on an external source, e.g. values read from an external database. 
 * <p>
 * Score is set as: Score(doc,query) = query.getBoost()<sup>2</sup> * valueSource(doc).  
 *
 * <p><font color="#FF0000">
 * WARNING: The status of the <b>search.function</b> package is experimental. 
 * The APIs introduced here might change in the future and will not be 
 * supported anymore in such a case.</font>
 */
public class ValueSourceQuery extends Query {
  ValueSource valSrc;

  /**
   * Create a value source query
   * @param valSrc provides the values defines the function to be used for scoring
   */
  public ValueSourceQuery(ValueSource valSrc) {
    this.valSrc=valSrc;
  }

  /*(non-Javadoc) @see org.apache.lucene.search.Query#rewrite(org.apache.lucene.index.IndexReader) */
  public Query rewrite(IndexReader reader) throws IOException {
    return this;
  }

  /*(non-Javadoc) @see org.apache.lucene.search.Query#extractTerms(java.util.Set) */
  public void extractTerms(Set terms) {
    // no terms involved here
  }

  private class ValueSourceWeight implements Weight {
    Similarity similarity;
    float queryNorm;
    float queryWeight;

    public ValueSourceWeight(Searcher searcher) {
      this.similarity = getSimilarity(searcher);
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#getQuery() */
    public Query getQuery() {
      return ValueSourceQuery.this;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#getValue() */
    public float getValue() {
      return queryWeight;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#sumOfSquaredWeights() */
    public float sumOfSquaredWeights() throws IOException {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#normalize(float) */
    public void normalize(float norm) {
      this.queryNorm = norm;
      queryWeight *= this.queryNorm;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#scorer(org.apache.lucene.index.IndexReader) */
    public Scorer scorer(IndexReader reader) throws IOException {
      return new ValueSourceScorer(similarity, reader, this);
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Weight#explain(org.apache.lucene.index.IndexReader, int) */
    public Explanation explain(IndexReader reader, int doc) throws IOException {
      return scorer(reader).explain(doc);
    }
  }

  /**
   * A scorer that (simply) matches all documents, and scores each document with 
   * the value of the value soure in effect. As an example, if the value source 
   * is a (cached) field source, then value of that field in that document will 
   * be used. (assuming field is indexed for this doc, with a single token.)   
   */
  private class ValueSourceScorer extends Scorer {
    private final IndexReader reader;
    private final ValueSourceWeight weight;
    private final int maxDoc;
    private final float qWeight;
    private int doc=-1;
    private final DocValues vals;

    // constructor
    private ValueSourceScorer(Similarity similarity, IndexReader reader, ValueSourceWeight w) throws IOException {
      super(similarity);
      this.weight = w;
      this.qWeight = w.getValue();
      this.reader = reader;
      this.maxDoc = reader.maxDoc();
      // this is when/where the values are first created.
      vals = valSrc.getValues(reader);
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Scorer#next() */
    public boolean next() throws IOException {
      for(;;) {
        ++doc;
        if (doc>=maxDoc) {
          return false;
        }
        if (reader.isDeleted(doc)) {
          continue;
        }
        return true;
      }
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Scorer#doc()
     */
    public int doc() {
      return doc;
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Scorer#score() */
    public float score() throws IOException {
      return qWeight * vals.floatVal(doc);
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Scorer#skipTo(int) */
    public boolean skipTo(int target) throws IOException {
      doc=target-1;
      return next();
    }

    /*(non-Javadoc) @see org.apache.lucene.search.Scorer#explain(int) */
    public Explanation explain(int doc) throws IOException {
      float sc = qWeight * vals.floatVal(doc);

      Explanation result = new ComplexExplanation(
        true, sc, ValueSourceQuery.this.toString() + ", product of:");

      result.addDetail(vals.explain(doc));
      result.addDetail(new Explanation(getBoost(), "boost"));
      result.addDetail(new Explanation(weight.queryNorm,"queryNorm"));
      return result;
    }
  }

  /*(non-Javadoc) @see org.apache.lucene.search.Query#createWeight(org.apache.lucene.search.Searcher) */
  protected Weight createWeight(Searcher searcher) {
    return new ValueSourceQuery.ValueSourceWeight(searcher);
  }

  /* (non-Javadoc) @see org.apache.lucene.search.Query#toString(java.lang.String) */
  public String toString(String field) {
    return valSrc.toString() + ToStringUtils.boost(getBoost());
  }

  /** Returns true if <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (getClass() != o.getClass()) {
      return false;
    }
    ValueSourceQuery other = (ValueSourceQuery)o;
    return this.getBoost() == other.getBoost()
           && this.valSrc.equals(other.valSrc);
  }

  /** Returns a hash code value for this object. */
  public int hashCode() {
    return (getClass().hashCode() + valSrc.hashCode()) ^ Float.floatToIntBits(getBoost());
  }

}
