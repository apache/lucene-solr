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
package org.apache.lucene.search.spans;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.ArrayList;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Explanation;

import org.apache.lucene.search.similarities.Similarity.SimScorer;

/** Wrapper class for scoring span queries via matching term occurrences.
 *
 * @lucene.experimental
 */
public class SpansTreeQuery extends Query {

  final SpanQuery spanQuery;
  final int TOP_LEVEL_SLOP = 0;

  /** Wrap a span query to score via its matching term occurrences.
   * <br>
   * For more details on scoring see {@link SpansTreeScorer#createSpansDocScorer}.
   *
   * @param spanQuery This can be any nested combination of
   *                  {@link org.apache.lucene.search.spans.SpanNearQuery},
   *                  {@link org.apache.lucene.search.spans.SpanOrQuery},
   *                  {@link org.apache.lucene.search.spans.SpanSynonymQuery},
   *                  {@link org.apache.lucene.search.spans.SpanTermQuery},
   *                  {@link org.apache.lucene.search.spans.SpanBoostQuery},
   *                  {@link org.apache.lucene.search.spans.SpanNotQuery},
   *                  {@link org.apache.lucene.search.spans.SpanFirstQuery},
   *                  {@link org.apache.lucene.search.spans.SpanContainingQuery} and
   *                  {@link org.apache.lucene.search.spans.SpanWithinQuery}.
   */
  public SpansTreeQuery(SpanQuery spanQuery) {
    this.spanQuery = Objects.requireNonNull(spanQuery);
  }

  /** Wrap the span (subqueries of a) query in a SpansTreeQuery.
   * <br>
   * A {@link SpanQuery} will be wrapped in a {@link SpansTreeQuery#SpansTreeQuery}.
   * For {@link BooleanQuery}, {@link DisjunctionMaxQuery} and {@link BoostQuery},
   * the subqueries/subquery will be wrapped recursively.
   * Otherwise the given query is returned.
   * <br>
   * No double wrapping will be done because
   * a {@link SpansTreeQuery} is not a {@link SpanQuery}.
   */
  public static Query wrap(Query query) {
    if (query instanceof SpanQuery) {
      return new SpansTreeQuery((SpanQuery)query);
    }
    if (query instanceof BooleanQuery) {
      return wrapBooleanQuery((BooleanQuery)query);
    }
    if (query instanceof DisjunctionMaxQuery) {
      return wrapDMQ((DisjunctionMaxQuery)query);
    }
    if (query instanceof BoostQuery) {
      Query subQuery = ((BoostQuery)query).getQuery();
      Query wrappedSubQuery = wrap(subQuery);
      if (wrappedSubQuery == subQuery) {
        return query;
      }
      float boost = ((BoostQuery)query).getBoost();
      return new BoostQuery(wrappedSubQuery, boost);
    }
    return query;
  }

  static BooleanQuery wrapBooleanQuery(BooleanQuery blq) {
    ArrayList<BooleanClause> wrappedClauses = new ArrayList<>();
    boolean wrapped = false;
    for (BooleanClause clause : blq.clauses()) {
      Query subQuery = clause.getQuery();
      Query wrappedSubQuery = wrap(subQuery);
      if (wrappedSubQuery != subQuery) {
        wrapped = true;
        wrappedClauses.add(new BooleanClause(wrappedSubQuery, clause.getOccur()));
      }
      else {
        wrappedClauses.add(clause);
      }
    }
    if (! wrapped) {
      return blq;
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (BooleanClause clause : wrappedClauses) {
      builder.add(clause);
    }
    return builder.build();
  }

  static DisjunctionMaxQuery wrapDMQ(DisjunctionMaxQuery dmq) {
    ArrayList<Query> wrappedDisjuncts = new ArrayList<>();
    boolean wrapped = false;
    for (Query disjunct : dmq.getDisjuncts()) {
      Query wrappedDisjunct = wrap(disjunct);
      if (wrappedDisjunct != disjunct) {
        wrapped = true;
        wrappedDisjuncts.add(wrappedDisjunct);
      }
      else {
        wrappedDisjuncts.add(disjunct);
      }
    }
    if (! wrapped) {
      return dmq;
    }
    float tbm = dmq.getTieBreakerMultiplier();
    return new DisjunctionMaxQuery(wrappedDisjuncts, tbm);
  }


  /** Wrap a given query by {@link #wrap(Query)} after it was rewritten.
   */
  public static Query wrapAfterRewrite(Query query) {
    return new Query() {
      @Override
      public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten =  query.rewrite(reader);
        Query wrapped = wrap(rewritten);
        return wrapped;
      }

      @Override
      public boolean equals(Object other) {
        return this == other;
      }

      @Override
      public int hashCode() {
        return query.hashCode() ^ SpansTreeQuery.class.hashCode();
      }

      @Override
      public String toString(String field) {
        return "SpansTreeQuery.wrapAfterRewrite: " + query.toString(field);
      }
    };
  }

  /** The wrapped SpanQuery */
  public SpanQuery getSpanQuery() { return spanQuery; }

  @Override
  public int hashCode() {
    return getClass().hashCode() - spanQuery.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(SpansTreeQuery other) {
    return spanQuery.equals(other.spanQuery);
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("SpansTreeQuery(");
    buffer.append(spanQuery.toString(field));
    buffer.append(")");
    return buffer.toString();
  }

  /** Return a weight for scoring by matching term occurrences.
   *  <br>{@link Weight#explain} is not supported on the result.
   */
  @Override
  public SpansTreeWeight createWeight(
                            IndexSearcher searcher,
                            boolean needsScores,
                            float boost)
  throws IOException
  {
    return new SpansTreeWeight(searcher, needsScores, boost);
  }

  public class SpansTreeWeight extends Weight {
    final SpanWeight spanWeight;

    public SpansTreeWeight(
              IndexSearcher searcher,
              boolean needsScores,
              float boost)
    throws IOException
    {
      super(SpansTreeQuery.this);
      this.spanWeight = spanQuery.createWeight(searcher, needsScores, boost);
    }

    /** Throws an UnsupportedOperationException. */
    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      spanWeight.extractTerms(terms);
    }

    /** Compute a minimal slop factor from the maximum possible slops that can occur
     * in a SpanQuery for nested SpanNearQueries and for nested SpanOrQueries with distance.
     * This supports the queries mentioned at {@link SpansTreeScorer#createSpansDocScorer}.
     * <p>
     * This uses the maximum slops from {@link SpanOrQuery#getMaxDistance()} and
     * {@link SpanNearQuery#getNonMatchSlop()}.
     * <p>
     * This assumes that slop factors are multiplied in
     * {@link ConjunctionNearSpansDocScorer#recordMatch} and in
     * {@link DisjunctionNearSpansDocScorer#recordMatch}
     */
    public double minSlopFactor(SpanQuery spanQuery, SimScorer simScorer, double slopFactor) {
      assert slopFactor >= 0;
      if (spanQuery instanceof SpanTermQuery) {
        return slopFactor;
      }
      if (spanQuery instanceof SpanSynonymQuery) {
        return slopFactor;
      }
      if (spanQuery instanceof SpanNotQuery) {
        return minSlopFactor(((SpanNotQuery)spanQuery).getInclude(), simScorer, slopFactor);
      }
      if (spanQuery instanceof SpanPositionCheckQuery) {
        return minSlopFactor(((SpanFirstQuery)spanQuery).getMatch(), simScorer, slopFactor);
      }
      if (spanQuery instanceof SpanContainingQuery) {
        return minSlopFactor(((SpanContainingQuery)spanQuery).getBig(), simScorer, slopFactor);
      }
      if (spanQuery instanceof SpanWithinQuery) {
        return minSlopFactor(((SpanWithinQuery)spanQuery).getLittle(), simScorer, slopFactor);
      }
      if (spanQuery instanceof SpanBoostQuery) {
        return minSlopFactor(((SpanBoostQuery)spanQuery).getQuery(), simScorer, slopFactor);
      }

      SpanQuery[] clauses = null;
      int maxAllowedSlop = -1;

      if (spanQuery instanceof SpanOrQuery) {
        SpanOrQuery spanOrQuery = (SpanOrQuery)spanQuery;
        clauses = spanOrQuery.getClauses();
        maxAllowedSlop = spanOrQuery.getMaxDistance();
        if (maxAllowedSlop == -1) {
          return minSlopFactorClauses(clauses, simScorer, slopFactor);
        }
      }
      else if (spanQuery instanceof SpanNearQuery) {
        SpanNearQuery spanNearQuery = (SpanNearQuery) spanQuery;
        clauses = spanNearQuery.getClauses();
        maxAllowedSlop = spanNearQuery.getNonMatchSlop();
      }

      if (clauses == null) {
        throw new IllegalArgumentException("Not implemented for SpanQuery class: "
                                            + spanQuery.getClass().getName());
      }

      assert maxAllowedSlop >= 0;
      double localSlopFactor = simScorer.computeSlopFactor(maxAllowedSlop);
      assert localSlopFactor >= 0;
      // assumed multiplication:
      return minSlopFactorClauses(clauses, simScorer, slopFactor * localSlopFactor);
    }

    /** Helper for {@link #minSlopFactor} */
    public double minSlopFactorClauses(SpanQuery[] clauses, SimScorer simScorer, double slopFactor) {
      assert slopFactor >= 0;
      assert clauses.length >= 1;
      double res = Double.MAX_VALUE;
      for (SpanQuery clause : clauses) {
        double minSlopFacClause = minSlopFactor(clause, simScorer, slopFactor);
        res = Double.min(res, minSlopFacClause);
      }
      return res;
    }

    /** Provide a SpansTreeScorer that has the result of {@link #minSlopFactor}
     * as the weight for non matching terms.
     */
    @Override
    public SpansTreeScorer scorer(LeafReaderContext context) throws IOException {
      final Spans spans = spanWeight.getSpans(context, SpanWeight.Postings.POSITIONS);
      if (spans == null) {
        return null;
      }
      SimScorer topLevelScorer = spanWeight.getSimScorer(context);
      double topLevelSlopFactor = topLevelScorer.computeSlopFactor(TOP_LEVEL_SLOP);
      double nonMatchWeight = minSlopFactor(spanQuery, topLevelScorer, topLevelSlopFactor);

      return new SpansTreeScorer(this, spans, topLevelSlopFactor, nonMatchWeight);
    }
  }
}
