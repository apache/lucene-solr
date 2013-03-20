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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Set;
import java.util.Map;


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

  @Override
  public void extractTerms(Set<Term> terms) {}

  protected class FunctionWeight extends Weight {
    protected final IndexSearcher searcher;
    protected float queryNorm;
    protected float queryWeight;
    protected final Map context;

    public FunctionWeight(IndexSearcher searcher) throws IOException {
      this.searcher = searcher;
      this.context = ValueSource.newContext(searcher);
      func.createWeight(context, searcher);
    }

    @Override
    public Query getQuery() {
      return FunctionQuery.this;
    }

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
    public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
        boolean topScorer, Bits acceptDocs) throws IOException {
      return new AllScorer(context, acceptDocs, this, queryWeight);
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      return ((AllScorer)scorer(context, true, true, context.reader().getLiveDocs())).explain(doc);
    }
  }

  protected class AllScorer extends Scorer {
    final IndexReader reader;
    final FunctionWeight weight;
    final int maxDoc;
    final float qWeight;
    int doc=-1;
    final FunctionValues vals;
    final Bits acceptDocs;

    public AllScorer(AtomicReaderContext context, Bits acceptDocs, FunctionWeight w, float qWeight) throws IOException {
      super(w);
      this.weight = w;
      this.qWeight = qWeight;
      this.reader = context.reader();
      this.maxDoc = reader.maxDoc();
      this.acceptDocs = acceptDocs;
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
      for(;;) {
        ++doc;
        if (doc>=maxDoc) {
          return doc=NO_MORE_DOCS;
        }
        if (acceptDocs != null && !acceptDocs.get(doc)) continue;
        return doc;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      // this will work even if target==NO_MORE_DOCS
      doc=target-1;
      return nextDoc();
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

      Explanation result = new ComplexExplanation
        (true, sc, "FunctionQuery(" + func + "), product of:");

      result.addDetail(vals.explain(doc));
      result.addDetail(new Explanation(getBoost(), "boost"));
      result.addDetail(new Explanation(weight.queryNorm,"queryNorm"));
      return result;
    }
  }


  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
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
    return this.getBoost() == other.getBoost()
            && this.func.equals(other.func);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    return func.hashCode()*31 + Float.floatToIntBits(getBoost());
  }

}
