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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.Similarity;

public class DifferenceIntervalQuery extends Query {

  private final Query minuend;
  private final Query subtrahend;
  private final DifferenceIntervalFunction function;
  private final String field;

  protected DifferenceIntervalQuery(String field, Query minuend, Query subtrahend, DifferenceIntervalFunction function) {
    this.minuend = minuend;
    this.subtrahend = subtrahend;
    this.function = function;
    this.field = field;
  }

  @Override
  public String toString(String field) {
    return function + "(" + minuend + ", " + subtrahend + ")";
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    Weight minuendWeight = searcher.createWeight(minuend, ScoreMode.COMPLETE_POSITIONS, 1);
    Weight subtrahendWeight = searcher.createWeight(subtrahend, ScoreMode.COMPLETE_POSITIONS, 1);
    return new IntervalDifferenceWeight(minuendWeight, subtrahendWeight, scoreMode,
        searcher.getSimilarity(), IntervalQuery.buildSimScorer(field, searcher, Collections.singletonList(minuendWeight), boost));
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewrittenMinuend = minuend.rewrite(reader);
    Query rewrittenSubtrahend = subtrahend.rewrite(reader);
    if (rewrittenMinuend != minuend || rewrittenSubtrahend != subtrahend) {
      return new DifferenceIntervalQuery(field, rewrittenMinuend, rewrittenSubtrahend, function);
    }
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DifferenceIntervalQuery that = (DifferenceIntervalQuery) o;
    return Objects.equals(minuend, that.minuend) &&
        Objects.equals(subtrahend, that.subtrahend) &&
        Objects.equals(function, that.function);
  }

  @Override
  public int hashCode() {
    return Objects.hash(minuend, subtrahend, function);
  }

  private class IntervalDifferenceWeight extends Weight {

    final Weight minuendWeight;
    final Weight subtrahendWeight;
    final ScoreMode scoreMode;
    final Similarity similarity;
    final Similarity.SimScorer simScorer;

    private IntervalDifferenceWeight(Weight minuendWeight, Weight subtrahendWeight, ScoreMode scoreMode,
                                     Similarity similarity, Similarity.SimScorer simScorer) {
      super(DifferenceIntervalQuery.this);
      this.minuendWeight = minuendWeight;
      this.subtrahendWeight = subtrahendWeight;
      this.scoreMode = scoreMode;
      this.similarity = similarity;
      this.simScorer = simScorer;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      this.minuendWeight.extractTerms(terms);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      IntervalScorer scorer = (IntervalScorer) scorer(context);
      if (scorer != null) {
        int newDoc = scorer.iterator().advance(doc);
        if (newDoc == doc) {
          return scorer.explain("weight("+getQuery()+" in "+doc+") [" + similarity.getClass().getSimpleName() + "]");
        }
      }
      return Explanation.noMatch("no matching intervals");
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      Scorer minuendScorer = minuendWeight.scorer(context);
      Scorer subtrahendScorer = subtrahendWeight.scorer(context);
      if (subtrahendScorer == null || minuendScorer == null)
        return minuendScorer;

      IntervalIterator minuendIt = minuendScorer.intervals(field);
      IntervalIterator subtrahendIt = subtrahendScorer.intervals(field);
      if (subtrahendIt == IntervalIterator.EMPTY || subtrahendIt == null)
        return minuendScorer;

      LeafSimScorer leafScorer = simScorer == null ? null
          : new LeafSimScorer(simScorer, context.reader(), scoreMode.needsScores(), Float.MAX_VALUE);

      return new IntervalScorer(this, field, minuendScorer.iterator(), function.apply(minuendIt, subtrahendIt), leafScorer){
        @Override
        public TwoPhaseIterator twoPhaseIterator() {
          return new TwoPhaseIterator(approximation) {
            @Override
            public boolean matches() throws IOException {
              if (subtrahendScorer.docID() < approximation.docID()) {
                subtrahendScorer.iterator().advance(approximation.docID());
              }
              return intervals.reset(approximation.docID()) && intervals.nextInterval() != Intervals.NO_MORE_INTERVALS;
            }

            @Override
            public float matchCost() {
              return 0;
            }
          };
        }
      };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return minuendWeight.isCacheable(ctx) && subtrahendWeight.isCacheable(ctx);
    }
  }
}
