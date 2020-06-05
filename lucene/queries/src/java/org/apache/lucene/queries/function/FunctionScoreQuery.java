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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
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

  /**
   * @return the wrapped Query
   */
  public Query getWrappedQuery() {
    return in;
  }

  /**
   * @return the underlying value source
   */
  public DoubleValuesSource getSource() {
    return source;
  }

  /**
   * Returns a FunctionScoreQuery where the scores of a wrapped query are multiplied by
   * the value of a DoubleValuesSource.
   *
   * If the source has no value for a particular document, the score for that document
   * is preserved as-is.
   *
   * @param in    the query to boost
   * @param boost a {@link DoubleValuesSource} containing the boost values
   */
  public static FunctionScoreQuery boostByValue(Query in, DoubleValuesSource boost) {
    return new FunctionScoreQuery(in, new MultiplicativeBoostValuesSource(boost));
  }

  /**
   * Returns a FunctionScoreQuery where the scores of a wrapped query are multiplied by
   * a boost factor if the document being scored also matches a separate boosting query.
   *
   * Documents that do not match the boosting query have their scores preserved.
   *
   * This may be used to 'demote' documents that match the boosting query, by passing in
   * a boostValue between 0 and 1.
   *
   * @param in          the query to boost
   * @param boostMatch  the boosting query
   * @param boostValue  the amount to boost documents which match the boosting query
   */
  public static FunctionScoreQuery boostByQuery(Query in, Query boostMatch, float boostValue) {
    return new FunctionScoreQuery(in,
        new MultiplicativeBoostValuesSource(new QueryBoostValuesSource(DoubleValuesSource.fromQuery(boostMatch), boostValue)));
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    ScoreMode sm;
    if (scoreMode.needsScores() && source.needsScores()) {
      sm = ScoreMode.COMPLETE;
    } else {
      sm = ScoreMode.COMPLETE_NO_SCORES;
    }
    Weight inner = in.createWeight(searcher, sm, 1f);
    if (scoreMode.needsScores() == false)
      return inner;
    return new FunctionScoreWeight(this, inner, source.rewrite(searcher), boost);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewritten = in.rewrite(reader);
    if (rewritten == in)
      return this;
    return new FunctionScoreQuery(rewritten, source);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    in.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
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
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      return inner.matches(context, doc);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      Explanation scoreExplanation = inner.explain(context, doc);
      if (scoreExplanation.isMatch() == false) {
        return scoreExplanation;
      }

      Scorer scorer = inner.scorer(context);
      DoubleValues values = valueSource.getValues(context, DoubleValuesSource.fromScorer(scorer));
      int advanced = scorer.iterator().advance(doc);
      assert advanced == doc;

      double value;
      Explanation expl;
      if (values.advanceExact(doc)) {
        value = values.doubleValue();
        expl = valueSource.explain(context, doc, scoreExplanation);
        if (value < 0) {
          value = 0;
          expl = Explanation.match(0, "truncated score, max of:",
              Explanation.match(0f, "minimum score"), expl);
        } else if (Double.isNaN(value)) {
          value = 0;
          expl = Explanation.match(0, "score, computed as (score == NaN ? 0 : score) since NaN is an illegal score from:", expl);
        }
      } else {
        value = 0;
        expl = valueSource.explain(context, doc, scoreExplanation);
      }

      if (expl.isMatch() == false) {
        expl = Explanation.match(0f, "weight(" + getQuery().toString() + ") using default score of 0 because the function produced no value:", expl);
      } else if (boost != 1f) {
        expl = Explanation.match((float) (value * boost), "weight(" + getQuery().toString() + "), product of:",
            Explanation.match(boost, "boost"), expl);
      } else {
        expl = Explanation.match(expl.getValue(), "weight(" + getQuery().toString() + "), result of:", expl);
      }

      return expl;
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
          if (scores.advanceExact(docID())) {
            double factor = scores.doubleValue();
            if (factor >= 0) {
              return (float) (factor * boost);
            }
          }
          // default: missing value, negative value or NaN
          return 0;
        }
        @Override
        public float getMaxScore(int upTo) throws IOException {
          return Float.POSITIVE_INFINITY;
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return inner.isCacheable(ctx) && valueSource.isCacheable(ctx);
    }

  }

  static class MultiplicativeBoostValuesSource extends DoubleValuesSource {

    final DoubleValuesSource boost;

    private MultiplicativeBoostValuesSource(DoubleValuesSource boost) {
      this.boost = boost;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      DoubleValues in = DoubleValues.withDefault(boost.getValues(ctx, scores), 1);
      return new DoubleValues() {
        @Override
        public double doubleValue() throws IOException {
          return scores.doubleValue() * in.doubleValue();
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return in.advanceExact(doc);
        }
      };
    }

    @Override
    public boolean needsScores() {
      return true;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
      return new MultiplicativeBoostValuesSource(boost.rewrite(reader));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MultiplicativeBoostValuesSource that = (MultiplicativeBoostValuesSource) o;
      return Objects.equals(boost, that.boost);
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
      if (scoreExplanation.isMatch() == false) {
        return scoreExplanation;
      }
      Explanation boostExpl = boost.explain(ctx, docId, scoreExplanation);
      if (boostExpl.isMatch() == false) {
        return scoreExplanation;
      }
      return Explanation.match(scoreExplanation.getValue().doubleValue() * boostExpl.getValue().doubleValue(),
          "product of:", scoreExplanation, boostExpl);
    }

    @Override
    public int hashCode() {
      return Objects.hash(boost);
    }

    @Override
    public String toString() {
      return "boost(" + boost.toString() + ")";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return boost.isCacheable(ctx);
    }
  }

  private static class QueryBoostValuesSource extends DoubleValuesSource {

    private final DoubleValuesSource query;
    private final float boost;

    QueryBoostValuesSource(DoubleValuesSource query, float boost) {
      this.query = query;
      this.boost = boost;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      DoubleValues in = query.getValues(ctx, null);
      return DoubleValues.withDefault(new DoubleValues() {
        @Override
        public double doubleValue() {
          return boost;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
          return in.advanceExact(doc);
        }
      }, 1);
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
      return new QueryBoostValuesSource(query.rewrite(reader), boost);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      QueryBoostValuesSource that = (QueryBoostValuesSource) o;
      return Float.compare(that.boost, boost) == 0 &&
          Objects.equals(query, that.query);
    }

    @Override
    public int hashCode() {
      return Objects.hash(query, boost);
    }

    @Override
    public String toString() {
      return "queryboost(" + query + ")^" + boost;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return query.isCacheable(ctx);
    }

    @Override
    public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
      Explanation inner = query.explain(ctx, docId, scoreExplanation);
      if (inner.isMatch() == false) {
        return inner;
      }
      return Explanation.match(boost, "Matched boosting query " + query.toString());
    }
  }
}
