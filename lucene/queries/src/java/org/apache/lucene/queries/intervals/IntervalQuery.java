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

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterMatchesIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * A query that retrieves documents containing intervals returned from an
 * {@link IntervalsSource}
 *
 * Static constructor functions for various different sources can be found in the
 * {@link Intervals} class
 *
 * Scores for this query are computed as a function of the sloppy frequency of
 * intervals appearing in a particular document.  Sloppy frequency is calculated
 * from the number of matching intervals, and their width, with wider intervals
 * contributing lower values.  The scores can be adjusted with two optional
 * parameters:
 * <ul>
 *   <li>pivot - the sloppy frequency value at which the overall score of the
 *               document will equal 0.5.  The default value is 1</li>
 *   <li>exp   - higher values of this parameter make the function grow more slowly
 *               below the pivot and faster higher than the pivot.  The default value is 1</li>
 * </ul>
 *
 * Optimal values for both pivot and exp depend on the type of queries and corpus of
 * documents being queried.
 *
 * Scores are bounded to between 0 and 1.  For higher contributions, wrap the query
 * in a {@link org.apache.lucene.search.BoostQuery}
 */
public final class IntervalQuery extends Query {

  private final String field;
  private final IntervalsSource intervalsSource;
  private final IntervalScoreFunction scoreFunction;

  /**
   * Create a new IntervalQuery
   * @param field             the field to query
   * @param intervalsSource   an {@link IntervalsSource} to retrieve intervals from
   */
  public IntervalQuery(String field, IntervalsSource intervalsSource) {
    this(field, intervalsSource, IntervalScoreFunction.saturationFunction(1));
  }

  /**
   * Create a new IntervalQuery with a scoring pivot
   *
   * @param field             the field to query
   * @param intervalsSource   an {@link IntervalsSource} to retrieve intervals from
   * @param pivot             the sloppy frequency value at which the score will be 0.5, must be within (0, +Infinity)
   */
  public IntervalQuery(String field, IntervalsSource intervalsSource, float pivot) {
    this(field, intervalsSource, IntervalScoreFunction.saturationFunction(pivot));
  }

  /**
   * Create a new IntervalQuery with a scoring pivot and exponent
   * @param field             the field to query
   * @param intervalsSource   an {@link IntervalsSource} to retrieve intervals from
   * @param pivot             the sloppy frequency value at which the score will be 0.5, must be within (0, +Infinity)
   * @param exp               exponent, higher values make the function grow slower before 'pivot' and faster
   *                          after 'pivot', must be in (0, +Infinity)
   */
  public IntervalQuery(String field, IntervalsSource intervalsSource, float pivot, float exp) {
    this(field, intervalsSource, IntervalScoreFunction.sigmoidFunction(pivot, exp));
  }

  private IntervalQuery(String field, IntervalsSource intervalsSource, IntervalScoreFunction scoreFunction) {
    this.field = field;
    this.intervalsSource = intervalsSource;
    this.scoreFunction = scoreFunction;
  }

  /**
   * The field to query
   */
  public String getField() {
    return field;
  }

  @Override
  public String toString(String field) {
    return intervalsSource.toString();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return new IntervalWeight(this, boost, scoreMode);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      intervalsSource.visit(field, visitor);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IntervalQuery that = (IntervalQuery) o;
    return Objects.equals(field, that.field) &&
        Objects.equals(intervalsSource, that.intervalsSource);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, intervalsSource);
  }

  private class IntervalWeight extends Weight {

    final ScoreMode scoreMode;
    final float boost;

    public IntervalWeight(Query query, float boost, ScoreMode scoreMode) {
      super(query);
      this.scoreMode = scoreMode;
      this.boost = boost;
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      IntervalScorer scorer = (IntervalScorer) scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          float freq = scorer.freq();
          return scoreFunction.explain(intervalsSource.toString(), boost, freq);
        }
      }
      return Explanation.noMatch("no matching intervals");
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
      return MatchesUtils.forField(field, () -> {
        MatchesIterator mi = intervalsSource.matches(field, context, doc);
        if (mi == null) {
          return null;
        }
        return new FilterMatchesIterator(mi) {
          @Override
          public Query getQuery() {
            return new IntervalQuery(field, intervalsSource);
          }
        };
      });
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      IntervalIterator intervals = intervalsSource.intervals(field, context);
      if (intervals == null)
        return null;
      return new IntervalScorer(this, intervals, intervalsSource.minExtent(), boost, scoreFunction);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }

}
