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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import java.io.IOException;
import java.util.Set;


/**
 * Returns a score for each document based on a ValueSource,
 * often some function of the value of a field.
 *
 * @author yonik
 * @version $Id$
 */
public class FunctionQuery extends Query {
  ValueSource func;

  /**
   *
   * @param func defines the function to be used for scoring
   */
  public FunctionQuery(ValueSource func) {
    this.func=func;
  }

  public Query rewrite(IndexReader reader) throws IOException {
    return this;
  }

  public void extractTerms(Set terms) {}

  protected class FunctionWeight implements Weight {
    Searcher searcher;
    float queryNorm;
    float queryWeight;

    public FunctionWeight(Searcher searcher) {
      this.searcher = searcher;
    }

    public Query getQuery() {
      return FunctionQuery.this;
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
      return new AllScorer(getSimilarity(searcher), reader, this);
    }

    public Explanation explain(IndexReader reader, int doc) throws IOException {
      return scorer(reader).explain(doc);
    }
  }

  protected class AllScorer extends Scorer {
    final IndexReader reader;
    final FunctionWeight weight;
    final int maxDoc;
    final float qWeight;
    int doc=-1;
    final DocValues vals;

    public AllScorer(Similarity similarity, IndexReader reader, FunctionWeight w) throws IOException {
      super(similarity);
      this.weight = w;
      this.qWeight = w.getValue();
      this.reader = reader;
      this.maxDoc = reader.maxDoc();
      vals = func.getValues(reader);
    }

    // instead of matching all docs, we could also embed a query.
    // the score could either ignore the subscore, or boost it.
    // Containment:  floatline(foo:myTerm, "myFloatField", 1.0, 0.0f)
    // Boost:        foo:myTerm^floatline("myFloatField",1.0,0.0f)
    public boolean next() throws IOException {
      for(;;) {
        ++doc;
        if (doc>=maxDoc) {
          return false;
        }
        if (reader.isDeleted(doc)) continue;
        // todo: maybe allow score() to throw a specific exception
        // and continue on to the next document if it is thrown...
        // that may be useful, but exceptions aren't really good
        // for flow control.
        return true;
      }
    }

    public int doc() {
      return doc;
    }

    public float score() throws IOException {
      return qWeight * vals.floatVal(doc);
    }

    public boolean skipTo(int target) throws IOException {
      doc=target-1;
      return next();
    }

    public Explanation explain(int doc) throws IOException {
      float sc = qWeight * vals.floatVal(doc);

      Explanation result = new ComplexExplanation
        (true, sc, "FunctionQuery(" + func + "), product of:");

      result.addDetail(vals.explain(doc));
      result.addDetail(new Explanation(getBoost(), "boost"));
      result.addDetail(new Explanation(weight.queryNorm,"queryNorm"));
      return result;
    }
  }


  protected Weight createWeight(Searcher searcher) {
    return new FunctionQuery.FunctionWeight(searcher);
  }


  /** Prints a user-readable version of this query. */
  public String toString(String field)
  {
    float boost = getBoost();
    return (boost!=1.0?"(":"") + func.toString()
            + (getBoost()==0 ? "" : ")^"+getBoost());
  }


  /** Returns true if <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (FunctionQuery.class != o.getClass()) return false;
    FunctionQuery other = (FunctionQuery)o;
    return this.getBoost() == other.getBoost()
            && this.func.equals(other.func);
  }

  /** Returns a hash code value for this object. */
  public int hashCode() {
    return func.hashCode() ^ Float.floatToIntBits(getBoost());
  }

}
