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
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TwoPhaseIterator;

/** Removes matches which overlap with another SpanQuery or which are
 * within x tokens before or y tokens after another SpanQuery.
 */
public final class SpanNotQuery extends SpanQuery {
  private SpanQuery include;
  private SpanQuery exclude;
  private final int pre;
  private final int post;

  /** Construct a SpanNotQuery matching spans from <code>include</code> which
   * have no overlap with spans from <code>exclude</code>.*/
  public SpanNotQuery(SpanQuery include, SpanQuery exclude) {
     this(include, exclude, 0, 0);
  }


  /** Construct a SpanNotQuery matching spans from <code>include</code> which
   * have no overlap with spans from <code>exclude</code> within
   * <code>dist</code> tokens of <code>include</code>. Inversely, a negative
   * <code>dist</code> value may be used to specify a certain amount of allowable
   * overlap. */
  public SpanNotQuery(SpanQuery include, SpanQuery exclude, int dist) {
     this(include, exclude, dist, dist);
  }

  /** Construct a SpanNotQuery matching spans from <code>include</code> which
   * have no overlap with spans from <code>exclude</code> within
   * <code>pre</code> tokens before or <code>post</code> tokens of
   * <code>include</code>. Inversely, negative values for <code>pre</code> and/or
   * <code>post</code> allow a certain amount of overlap to occur. */
  public SpanNotQuery(SpanQuery include, SpanQuery exclude, int pre, int post) {
    this.include = Objects.requireNonNull(include);
    this.exclude = Objects.requireNonNull(exclude);
    this.pre = pre;
    this.post = post;

    if (include.getField() != null && exclude.getField() != null && !include.getField().equals(exclude.getField()))
      throw new IllegalArgumentException("Clauses must have same field.");
  }

  /** Return the SpanQuery whose matches are filtered. */
  public SpanQuery getInclude() { return include; }

  /** Return the SpanQuery whose matches must not overlap those returned. */
  public SpanQuery getExclude() { return exclude; }

  @Override
  public String getField() { return include.getField(); }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanNot(");
    buffer.append(include.toString(field));
    buffer.append(", ");
    buffer.append(exclude.toString(field));
    buffer.append(", ");
    buffer.append(Integer.toString(pre));
    buffer.append(", ");
    buffer.append(Integer.toString(post));
    buffer.append(")");
    return buffer.toString();
  }


  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    SpanWeight includeWeight = include.createWeight(searcher, false, boost);
    SpanWeight excludeWeight = exclude.createWeight(searcher, false, boost);
    return new SpanNotWeight(searcher, needsScores ? getTermContexts(includeWeight, excludeWeight) : null,
                                  includeWeight, excludeWeight, boost);
  }

  public class SpanNotWeight extends SpanWeight {

    final SpanWeight includeWeight;
    final SpanWeight excludeWeight;

    public SpanNotWeight(IndexSearcher searcher, Map<Term, TermContext> terms,
                         SpanWeight includeWeight, SpanWeight excludeWeight, float boost) throws IOException {
      super(SpanNotQuery.this, searcher, terms, boost);
      this.includeWeight = includeWeight;
      this.excludeWeight = excludeWeight;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      includeWeight.extractTermContexts(contexts);
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings) throws IOException {
      Spans includeSpans = includeWeight.getSpans(context, requiredPostings);
      if (includeSpans == null) {
        return null;
      }

      Spans excludeSpans = excludeWeight.getSpans(context, requiredPostings);
      if (excludeSpans == null) {
        return includeSpans;
      }

      TwoPhaseIterator excludeTwoPhase = excludeSpans.asTwoPhaseIterator();
      DocIdSetIterator excludeApproximation = excludeTwoPhase == null ? null : excludeTwoPhase.approximation();

      return new FilterSpans(includeSpans) {
        // last document we have checked matches() against for the exclusion, and failed
        // when using approximations, so we don't call it again, and pass thru all inclusions.
        int lastApproxDoc = -1;
        boolean lastApproxResult = false;

        @Override
        protected AcceptStatus accept(Spans candidate) throws IOException {
          // TODO: this logic is ugly and sneaky, can we clean it up?
          int doc = candidate.docID();
          if (doc > excludeSpans.docID()) {
            // catch up 'exclude' to the current doc
            if (excludeTwoPhase != null) {
              if (excludeApproximation.advance(doc) == doc) {
                lastApproxDoc = doc;
                lastApproxResult = excludeTwoPhase.matches();
              }
            } else {
              excludeSpans.advance(doc);
            }
          } else if (excludeTwoPhase != null && doc == excludeSpans.docID() && doc != lastApproxDoc) {
            // excludeSpans already sitting on our candidate doc, but matches not called yet.
            lastApproxDoc = doc;
            lastApproxResult = excludeTwoPhase.matches();
          }

          if (doc != excludeSpans.docID() || (doc == lastApproxDoc && lastApproxResult == false)) {
            return AcceptStatus.YES;
          }

          if (excludeSpans.startPosition() == -1) { // init exclude start position if needed
            excludeSpans.nextStartPosition();
          }

          while (excludeSpans.endPosition() <= candidate.startPosition() - pre) {
            // exclude end position is before a possible exclusion
            if (excludeSpans.nextStartPosition() == NO_MORE_POSITIONS) {
              return AcceptStatus.YES; // no more exclude at current doc.
            }
          }

          // exclude end position far enough in current doc, check start position:
          if (excludeSpans.startPosition() - post >= candidate.endPosition()) {
            return AcceptStatus.YES;
          } else {
            return AcceptStatus.NO;
          }
        }
      };
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      includeWeight.extractTerms(terms);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return includeWeight.isCacheable(ctx) && excludeWeight.isCacheable(ctx);
    }

  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    SpanQuery rewrittenInclude = (SpanQuery) include.rewrite(reader);
    SpanQuery rewrittenExclude = (SpanQuery) exclude.rewrite(reader);
    if (rewrittenInclude != include || rewrittenExclude != exclude) {
      return new SpanNotQuery(rewrittenInclude, rewrittenExclude, pre, post);
    }
    return super.rewrite(reader);
  }
    /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  } 

  private boolean equalsTo(SpanNotQuery other) { 
    return include.equals(other.include) && 
           exclude.equals(other.exclude) && 
           pre == other.pre && 
           post == other.post;
  }

  @Override
  public int hashCode() {
    int h = classHash();
    h = Integer.rotateLeft(h, 1);
    h ^= include.hashCode();
    h = Integer.rotateLeft(h, 1);
    h ^= exclude.hashCode();
    h = Integer.rotateLeft(h, 1);
    h ^= pre;
    h = Integer.rotateLeft(h, 1);
    h ^= post;
    return h;
  }

}
