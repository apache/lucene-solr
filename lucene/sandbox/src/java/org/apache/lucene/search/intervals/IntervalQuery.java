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

package org.apache.lucene.search.intervals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;

/**
 * A query that retrieves documents containing intervals returned from an
 * {@link IntervalsSource}
 *
 * Static constructor functions for various different sources can be found in the
 * {@link Intervals} class
 */
public final class IntervalQuery extends Query {

  private final String field;
  private final IntervalsSource intervalsSource;

  /**
   * Create a new IntervalQuery
   * @param field             the field to query
   * @param intervalsSource   an {@link IntervalsSource} to retrieve intervals from
   */
  public IntervalQuery(String field, IntervalsSource intervalsSource) {
    this.field = field;
    this.intervalsSource = intervalsSource;
  }

  public String getField() {
    return field;
  }

  @Override
  public String toString(String field) {
    return intervalsSource.toString();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    return new IntervalWeight(this, needsScores ? buildSimScorer(searcher, needsScores, boost) : null,
        searcher.getSimilarity(needsScores), needsScores);
  }

  private Similarity.SimWeight buildSimScorer(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    Set<Term> terms = new HashSet<>();
    intervalsSource.extractTerms(field, terms);
    TermStatistics[] termStats = new TermStatistics[terms.size()];
    int termUpTo = 0;
    for (Term term : terms) {
      TermStatistics termStatistics = searcher.termStatistics(term, TermContext.build(searcher.getTopReaderContext(), term));
      if (termStatistics != null) {
        termStats[termUpTo++] = termStatistics;
      }
    }
    if (termUpTo == 0) {
      return null;
    }
    CollectionStatistics collectionStats = searcher.collectionStatistics(field);
    return searcher.getSimilarity(needsScores).computeWeight(boost, collectionStats, Arrays.copyOf(termStats, termUpTo));
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

    final Similarity.SimWeight simWeight;
    final Similarity similarity;
    final boolean needsScores;

    public IntervalWeight(Query query, Similarity.SimWeight simWeight, Similarity similarity, boolean needsScores) {
      super(query);
      this.simWeight = simWeight;
      this.similarity = similarity;
      this.needsScores = needsScores;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      intervalsSource.extractTerms(field, terms);
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
      IntervalIterator intervals = intervalsSource.intervals(field, context);
      if (intervals == null)
        return null;
      Similarity.SimScorer leafScorer = simWeight == null ? null
          : similarity.simScorer(simWeight, context);
      return new IntervalScorer(this, intervals, leafScorer);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return true;
    }
  }

}
