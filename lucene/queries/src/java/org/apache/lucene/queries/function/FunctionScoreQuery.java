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
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * A query that wraps another query, and uses a DoubleValuesSource to
 * replace or modify the wrapped query's score
 *
 * If the DoubleValuesSource doesn't return a value for a particular document,
 * then that document will be given a score of 0.
 */
public final class FunctionScoreQuery extends Query {

  private final Query in;
  private final DoubleValuesSource source;

  /**
   * Create a new FunctionScoreQuery
   * @param in      the query to wrap
   * @param source  a source of scores
   */
  public FunctionScoreQuery(Query in, DoubleValuesSource source) {
    this.in = in;
    this.source = source;
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    Weight inner = in.createWeight(searcher, needsScores && source.needsScores(), 1f);
    if (needsScores == false)
      return inner;
    return new FunctionScoreWeight(this, inner, source, boost);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = in.rewrite(reader);
    if (rewritten == in)
      return this;
    return new FunctionScoreQuery(rewritten, source);
  }

  @Override
  public String toString(String field) {
    return "FunctionScoreQuery(" + in.toString(field) + ", scored by " + source.toString() + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FunctionScoreQuery that = (FunctionScoreQuery) o;
    return Objects.equals(in, that.in) &&
        Objects.equals(source, that.source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(in, source);
  }

  private static class FunctionScoreWeight extends Weight {

    final Weight inner;
    final DoubleValuesSource valueSource;
    final float boost;

    FunctionScoreWeight(Query query, Weight inner, DoubleValuesSource valueSource, float boost) {
      super(query);
      this.inner = inner;
      this.valueSource = valueSource;
      this.boost = boost;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      this.inner.extractTerms(terms);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Scorer scorer = inner.scorer(context);
      if (scorer.iterator().advance(doc) != doc)
        return Explanation.noMatch("No match");
      Explanation scoreExplanation = inner.explain(context, doc);
      Explanation expl = valueSource.explain(context, doc, scoreExplanation);
      if (boost == 1f)
        return expl;
      return Explanation.match(expl.getValue() * boost, "product of:",
          Explanation.match(boost, "boost"), expl);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      Scorer in = inner.scorer(context);
      if (in == null)
        return null;
      DoubleValues scores = valueSource.getValues(context, DoubleValuesSource.fromScorer(in));
      return new FilterScorer(in) {
        @Override
        public float score() throws IOException {
          if (scores.advanceExact(docID()))
            return (float) (scores.doubleValue() * boost);
          else
            return 0;
        }
      };
    }
  }
}
