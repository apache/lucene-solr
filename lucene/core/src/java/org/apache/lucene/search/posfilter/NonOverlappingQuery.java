package org.apache.lucene.search.posfilter;

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
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;

/**
 * A Query that matches documents containing an interval (the minuend) that
 * does not contain another interval (the subtrahend).
 *
 * As an example, given the following {@link org.apache.lucene.search.BooleanQuery}:
 * <pre>
 *   BooleanQuery bq = new BooleanQuery();
 *   bq.add(new TermQuery(new Term(field, "quick")), BooleanQuery.Occur.MUST);
 *   bq.add(new TermQuery(new Term(field, "fox")), BooleanQuery.Occur.MUST);
 * </pre>
 *
 * The document "the quick brown fox" will be matched by this query.  But
 * create a NonOverlappingQuery using this query as a minuend:
 * <pre>
 *   NonOverlappingQuery brq = new NonOverlappingQuery(bq, new TermQuery(new Term(field, "brown")));
 * </pre>
 *
 * This query will not match "the quick brown fox", because "brown" is found
 * within the interval of the boolean query for "quick" and "fox.  The query
 * will match "the quick fox is brown", because here "brown" is outside
 * the minuend's interval.
 *
 * N.B. Positions must be included in the index for this query to work
 *
 * Implements the Brouwerian operator as defined in <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics"</a>
 *
 * @lucene.experimental
 */
public final class NonOverlappingQuery extends PositionFilterQuery {

  private Query subtrahend;

  /**
   * Constructs a Query that matches documents containing intervals of the minuend
   * that are not subtended by the subtrahend
   * @param minuend the minuend Query
   * @param subtrahend the subtrahend Query
   */
  public NonOverlappingQuery(Query minuend, Query subtrahend) {
    super(minuend, new BrouwerianScorerFactory(subtrahend));
    this.subtrahend = subtrahend;
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    super.extractTerms(terms);
    subtrahend.extractTerms(terms);
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    Query rewrittenMinuend = innerQuery.rewrite(reader);
    Query rewrittenSubtrahend = subtrahend.rewrite(reader);
    if (rewrittenMinuend != innerQuery || rewrittenSubtrahend != subtrahend) {
      return new NonOverlappingQuery(rewrittenMinuend, rewrittenSubtrahend);
    }
    return this;
  }

  private static class BrouwerianScorerFactory implements ScorerFilterFactory {

    private final Query subtrahend;

    BrouwerianScorerFactory(Query subtrahend) {
      this.subtrahend = subtrahend;
    }

    @Override
    public Scorer scorer(Scorer filteredScorer, Similarity.SimScorer simScorer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
      return "NonOverlapping[" + subtrahend.toString() + "]/";
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new BrouwerianWeight(innerQuery.createWeight(searcher),
                                subtrahend.createWeight(searcher), searcher);
  }

  class BrouwerianWeight extends ScorerFilterWeight {

    private final Weight subtrahendWeight;

    public BrouwerianWeight(Weight minuendWeight, Weight subtrahendWeight, IndexSearcher searcher)
        throws IOException {
      super(minuendWeight, searcher);
      this.subtrahendWeight = subtrahendWeight;
    }

    @Override
    public Scorer scorer(LeafReaderContext context, int flags, Bits acceptDocs) throws IOException {
      return new BrouwerianScorer(innerWeight.scorer(context, flags, acceptDocs),
                                  subtrahendWeight.scorer(context, flags, acceptDocs),
                                  similarity.simScorer(stats, context));
    }
  }

  static class BrouwerianScorer extends PositionFilteredScorer {

    private final Scorer subtrahend;
    private Interval subtInterval = new Interval();
    private int subtPosition = -1;

    BrouwerianScorer(Scorer minuend, Scorer subtrahend, Similarity.SimScorer simScorer) {
      super(minuend, simScorer);
      this.subtrahend = subtrahend;
    }

    @Override
    protected void reset(int doc) throws IOException {
      super.reset(doc);
      if (this.subtrahend == null || this.subtrahend.docID() == NO_MORE_DOCS || this.subtrahend.advance(doc) != doc)
        subtPosition = NO_MORE_POSITIONS;
      else
        subtPosition = -1;
      this.subtInterval.reset();
    }

    @Override
    protected int doNextPosition() throws IOException {
      if (subtPosition == NO_MORE_POSITIONS) {
        int pos = child.nextPosition();
        if (pos != NO_MORE_POSITIONS)
          current.update(child);
        return pos;
      }
      while (child.nextPosition() != NO_MORE_POSITIONS) {
        current.update(child);
        while (subtInterval.lessThanExclusive(current) &&
                  (subtPosition = subtrahend.nextPosition()) != NO_MORE_POSITIONS) {
          subtInterval.update(subtrahend);
        }
        if (subtPosition == NO_MORE_POSITIONS || !current.overlaps(subtInterval))
          return current.begin;
      }
      return NO_MORE_POSITIONS;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((innerQuery == null) ? 0 : innerQuery.hashCode());
    result = prime * result
        + ((subtrahend == null) ? 0 : subtrahend.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    NonOverlappingQuery other = (NonOverlappingQuery) obj;
    if (innerQuery == null) {
      if (other.innerQuery != null) return false;
    } else if (!innerQuery.equals(other.innerQuery)) return false;
    if (subtrahend == null) {
      if (other.subtrahend != null) return false;
    } else if (!subtrahend.equals(other.subtrahend)) return false;
    return true;
  }

}