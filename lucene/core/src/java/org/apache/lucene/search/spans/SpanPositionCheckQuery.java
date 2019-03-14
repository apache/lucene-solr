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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.spans.FilterSpans.AcceptStatus;


/**
 * Base class for filtering a SpanQuery based on the position of a match.
 **/
public abstract class SpanPositionCheckQuery extends SpanQuery implements Cloneable {
  protected SpanQuery match;

  public SpanPositionCheckQuery(SpanQuery match) {
    this.match = Objects.requireNonNull(match);
  }

  /**
   * @return the SpanQuery whose matches are filtered.
   *
   * */
  public SpanQuery getMatch() { return match; }

  @Override
  public String getField() { return match.getField(); }

  /**
   * Implementing classes are required to return whether the current position is a match for the passed in
   * "match" {@link SpanQuery}.
   *
   * This is only called if the underlying last {@link Spans#nextStartPosition()} for the
   * match indicated a valid start position.
   *
   * @param spans The {@link Spans} instance, positioned at the spot to check
   *
   * @return whether the match is accepted, rejected, or rejected and should move to the next doc.
   *
   * @see Spans#nextDoc()
   *
   */
  protected abstract AcceptStatus acceptPosition(Spans spans) throws IOException;

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    SpanWeight matchWeight = match.createWeight(searcher, scoreMode, boost);
    return new SpanPositionCheckWeight(matchWeight, searcher, scoreMode.needsScores() ? getTermStates(matchWeight) : null, boost);
  }

  public class SpanPositionCheckWeight extends SpanWeight {

    final SpanWeight matchWeight;

    public SpanPositionCheckWeight(SpanWeight matchWeight, IndexSearcher searcher, Map<Term, TermStates> terms, float boost) throws IOException {
      super(SpanPositionCheckQuery.this, searcher, terms, boost);
      this.matchWeight = matchWeight;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      matchWeight.extractTerms(terms);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return matchWeight.isCacheable(ctx);
    }

    @Override
    public void extractTermStates(Map<Term, TermStates> contexts) {
      matchWeight.extractTermStates(contexts);
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings) throws IOException {
      Spans matchSpans = matchWeight.getSpans(context, requiredPostings);
      return (matchSpans == null) ? null : new FilterSpans(matchSpans) {
        @Override
        protected AcceptStatus accept(Spans candidate) throws IOException {
          return acceptPosition(candidate);
        }
      };
    }

  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    SpanQuery rewritten = (SpanQuery) match.rewrite(reader);
    if (rewritten != match) {
      try {
        SpanPositionCheckQuery clone = (SpanPositionCheckQuery) this.clone();
        clone.match = rewritten;
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new AssertionError(e);
      }
    }

    return super.rewrite(reader);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(getField())) {
      match.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }
  }

  /** Returns true iff <code>other</code> is equal to this. */
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           match.equals(((SpanPositionCheckQuery) other).match);
  }

  @Override
  public int hashCode() {
    return classHash() ^ match.hashCode();
  }
}
