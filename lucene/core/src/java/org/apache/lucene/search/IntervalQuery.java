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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.similarities.Similarity;

public final class IntervalQuery extends Query {

  private final String field;
  private final List<Query> subQueries;
  private final IntervalFunction iteratorFunction;

  protected IntervalQuery(String field, List<Query> subQueries, IntervalFunction iteratorFunction) {
    this(field, subQueries, null, iteratorFunction);
  }

  protected IntervalQuery(String field, List<Query> subQueries, Query subtrahend, IntervalFunction iteratorFunction) {
    this.field = field;
    this.subQueries = subQueries;
    this.iteratorFunction = iteratorFunction;
  }

  public String getField() {
    return field;
  }

  @Override
  public String toString(String field) {
    return iteratorFunction.toString() + subQueries.stream().map(Object::toString)
        .collect(Collectors.joining(",", "(", ")"));
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    List<Weight> subWeights = new ArrayList<>();
    for (Query q : subQueries) {
      subWeights.add(searcher.createWeight(q, ScoreMode.COMPLETE_POSITIONS, boost));
    }
    return new IntervalWeight(this, subWeights, scoreMode.needsScores() ? buildSimScorer(field, searcher, subWeights, boost) : null,
        searcher.getSimilarity(), scoreMode);
  }

  static Similarity.SimScorer buildSimScorer(String field, IndexSearcher searcher, List<Weight> subWeights, float boost) throws IOException {
    Set<Term> terms = new HashSet<>();
    for (Weight w : subWeights) {
      w.extractTerms(terms);
    }
    TermStatistics[] termStats = new TermStatistics[terms.size()];
    int termUpTo = 0;
    for (Term term : terms) {
      TermStatistics termStatistics = searcher.termStatistics(term, TermStates.build(searcher.readerContext, term, true));
      if (termStatistics != null) {
        termStats[termUpTo++] = termStatistics;
      }
    }
    CollectionStatistics collectionStats = searcher.collectionStatistics(field);
    return searcher.getSimilarity().scorer(boost, collectionStats, termStats);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IntervalQuery that = (IntervalQuery) o;
    return Objects.equals(field, that.field) &&
        Objects.equals(subQueries, that.subQueries) &&
        Objects.equals(iteratorFunction, that.iteratorFunction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, subQueries, iteratorFunction);
  }

  private class IntervalWeight extends Weight {

    final List<Weight> subWeights;
    final Similarity.SimScorer simScorer;
    final Similarity similarity;
    final ScoreMode scoreMode;

    public IntervalWeight(Query query, List<Weight> subWeights, Similarity.SimScorer simScorer, Similarity similarity, ScoreMode scoreMode) {
      super(query);
      this.subWeights = subWeights;
      this.simScorer = simScorer;
      this.similarity = similarity;
      this.scoreMode = scoreMode;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (Weight w : subWeights) {
        w.extractTerms(terms);
      }
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
      List<IntervalIterator> subIntervals = new ArrayList<>();
      List<DocIdSetIterator> disis = new ArrayList<>();
      for (Weight w : subWeights) {
        Scorer scorer = w.scorer(context);
        if (scorer == null)
          return null;
        disis.add(scorer.iterator());
        IntervalIterator it = scorer.intervals(field);
        if (it == null)
          return null;
        subIntervals.add(it);
      }
      IntervalIterator intervals = IntervalQuery.this.iteratorFunction.apply(subIntervals);
      LeafSimScorer leafScorer = simScorer == null ? null
          : new LeafSimScorer(simScorer, context.reader(), scoreMode.needsScores(), Float.MAX_VALUE);
      return new IntervalScorer(this, field, ConjunctionDISI.intersectIterators(disis), intervals, leafScorer);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      for (Weight w : subWeights) {
        if (w.isCacheable(ctx) == false)
          return false;
      }
      return true;
    }
  }

}
