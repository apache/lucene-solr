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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;


/**
 * Returns a score for each document based on a ValueSource,
 * often some function of the value of a field.
 *
 * <b>Note: This API is experimental and may change in non backward-compatible ways in the future</b>
 *
 *
 */
public class FunctionQuery extends Query {
  final ValueSource func;

  /**
   * @param func defines the function to be used for scoring
   */
  public FunctionQuery(ValueSource func) {
    this.func=func;
  }

  /** @return The associated ValueSource */
  public ValueSource getValueSource() {
    return func;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    return this;
  }

  protected class FunctionWeight extends Weight {
    protected final IndexSearcher searcher;
    protected float queryNorm;
    protected float queryWeight;
    protected final Map context;

    public FunctionWeight(IndexSearcher searcher) throws IOException {
      super(FunctionQuery.this);
      this.searcher = searcher;
      this.context = ValueSource.newContext(searcher);
      func.createWeight(context, searcher);
    }

    @Override
    public void extractTerms(Set<Term> terms) {}

    @Override
    public float getValueForNormalization() throws IOException {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      this.queryNorm = norm * topLevelBoost;
      queryWeight *= this.queryNorm;
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      return new AllScorer(context, this, queryWeight);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      return ((AllScorer)scorer(context)).explain(doc);
    }
  }

  protected class AllScorer extends Scorer {
    final IndexReader reader;
    final FunctionWeight weight;
    final int maxDoc;
    final float qWeight;
    int doc=-1;
    final FunctionValues vals;

    public AllScorer(LeafReaderContext context, FunctionWeight w, float qWeight) throws IOException {
      super(w);
      this.weight = w;
      this.qWeight = qWeight;
      this.reader = context.reader();
      this.maxDoc = reader.maxDoc();
      vals = func.getValues(weight.context, context);
    }

    @Override
    public int docID() {
      return doc;
    }

    // instead of matching all docs, we could also embed a query.
    // the score could either ignore the subscore, or boost it.
    // Containment:  floatline(foo:myTerm, "myFloatField", 1.0, 0.0f)
    // Boost:        foo:myTerm^floatline("myFloatField",1.0,0.0f)
    @Override
    public int nextDoc() throws IOException {
      ++doc;
      if (doc>=maxDoc) {
        return doc=NO_MORE_DOCS;
      }
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    @Override
    public float score() throws IOException {
      float score = qWeight * vals.floatVal(doc);

      // Current Lucene priority queues can't handle NaN and -Infinity, so
      // map to -Float.MAX_VALUE. This conditional handles both -infinity
      // and NaN since comparisons with NaN are always false.
      return score>Float.NEGATIVE_INFINITY ? score : -Float.MAX_VALUE;
    }

    @Override
    public long cost() {
      return maxDoc;
    }

    @Override
    public int freq() throws IOException {
      return 1;
    }

    public Explanation explain(int doc) throws IOException {
      float sc = qWeight * vals.floatVal(doc);

      return Explanation.match(sc, "FunctionQuery(" + func + "), product of:",
          vals.explain(doc),
          Explanation.match(getBoost(), "boost"),
          Explanation.match(weight.queryNorm, "queryNorm"));
    }

  }


  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new FunctionQuery.FunctionWeight(searcher);
  }


  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field)
  {
    float boost = getBoost();
    return (boost!=1.0?"(":"") + func.toString()
            + (boost==1.0 ? "" : ")^"+boost);
  }


  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!FunctionQuery.class.isInstance(o)) return false;
    FunctionQuery other = (FunctionQuery)o;
    return super.equals(o)
            && this.func.equals(other.func);
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ func.hashCode();
  }
}
