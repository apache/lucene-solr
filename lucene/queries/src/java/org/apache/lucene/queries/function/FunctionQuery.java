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
package org.apache.lucene.queries.function;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;


/**
 * Returns a score for each document based on a ValueSource,
 * often some function of the value of a field.
 *
 * @see ValueSourceScorer
 * @lucene.experimental
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

  protected class FunctionWeight extends Weight {
    protected final IndexSearcher searcher;
    protected final float boost;
    protected final Map context;

    public FunctionWeight(IndexSearcher searcher, float boost) throws IOException {
      super(FunctionQuery.this);
      this.searcher = searcher;
      this.context = ValueSource.newContext(searcher);
      func.createWeight(context, searcher);
      this.boost = boost;
    }

    @Override
    public void extractTerms(Set<Term> terms) {}

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      return new AllScorer(context, this, boost);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
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
    final float boost;
    final DocIdSetIterator iterator;
    final FunctionValues vals;

    public AllScorer(LeafReaderContext context, FunctionWeight w, float boost) throws IOException {
      super(w);
      this.weight = w;
      this.boost = boost;
      this.reader = context.reader();
      this.maxDoc = reader.maxDoc();
      iterator = DocIdSetIterator.all(context.reader().maxDoc());
      vals = func.getValues(weight.context, context);
    }

    @Override
    public DocIdSetIterator iterator() {
      return iterator;
    }

    @Override
    public int docID() {
      return iterator.docID();
    }

    @Override
    public float score() throws IOException {
      float score = boost * vals.floatVal(docID());

      // Current Lucene priority queues can't handle NaN and -Infinity, so
      // map to -Float.MAX_VALUE. This conditional handles both -infinity
      // and NaN since comparisons with NaN are always false.
      return score>Float.NEGATIVE_INFINITY ? score : -Float.MAX_VALUE;
    }

    public Explanation explain(int doc) throws IOException {
      float sc = boost * vals.floatVal(doc);

      return Explanation.match(sc, "FunctionQuery(" + func + "), product of:",
          vals.explain(doc),
          Explanation.match(weight.boost, "boost"));
    }

  }


  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    return new FunctionQuery.FunctionWeight(searcher, boost);
  }


  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field)
  {
    return func.toString();
  }


  /** Returns true if <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           func.equals(((FunctionQuery) other).func);
  }

  @Override
  public int hashCode() {
    return classHash() ^ func.hashCode();
  }
}
