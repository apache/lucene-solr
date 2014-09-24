package org.apache.lucene.search.spans;

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/** Removes matches which overlap with another SpanQuery or 
 * within a x tokens before or y tokens after another SpanQuery. */
public class SpanNotQuery extends SpanQuery implements Cloneable {
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
   * <code>dist</code> tokens of <code>include</code>. */
  public SpanNotQuery(SpanQuery include, SpanQuery exclude, int dist) {
     this(include, exclude, dist, dist);
  }
  
  /** Construct a SpanNotQuery matching spans from <code>include</code> which
   * have no overlap with spans from <code>exclude</code> within 
   * <code>pre</code> tokens before or <code>post</code> tokens of <code>include</code>. */
  public SpanNotQuery(SpanQuery include, SpanQuery exclude, int pre, int post) {
    this.include = include;
    this.exclude = exclude;
    this.pre = (pre >=0) ? pre : 0;
    this.post = (post >= 0) ? post : 0;

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
  public void extractTerms(Set<Term> terms) { include.extractTerms(terms); }

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
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public SpanNotQuery clone() {
    SpanNotQuery spanNotQuery = new SpanNotQuery((SpanQuery)include.clone(),
          (SpanQuery) exclude.clone(), pre, post);
    spanNotQuery.setBoost(getBoost());
    return  spanNotQuery;
  }

  @Override
  public Spans getSpans(final LeafReaderContext context, final Bits acceptDocs, final Map<Term,TermContext> termContexts) throws IOException {
    return new Spans() {
        private Spans includeSpans = include.getSpans(context, acceptDocs, termContexts);
        private boolean moreInclude = true;

        private Spans excludeSpans = exclude.getSpans(context, acceptDocs, termContexts);
        private boolean moreExclude = excludeSpans.next();

        @Override
        public boolean next() throws IOException {
          if (moreInclude)                        // move to next include
            moreInclude = includeSpans.next();

          while (moreInclude && moreExclude) {

            if (includeSpans.doc() > excludeSpans.doc()) // skip exclude
              moreExclude = excludeSpans.skipTo(includeSpans.doc());

            while (moreExclude                    // while exclude is before
                   && includeSpans.doc() == excludeSpans.doc()
                   && excludeSpans.end() <= includeSpans.start() - pre) {
              moreExclude = excludeSpans.next();  // increment exclude
            }

            if (!moreExclude                      // if no intersection
                || includeSpans.doc() != excludeSpans.doc()
                || includeSpans.end()+post <= excludeSpans.start())
              break;                              // we found a match

            moreInclude = includeSpans.next();    // intersected: keep scanning
          }
          return moreInclude;
        }

        @Override
        public boolean skipTo(int target) throws IOException {
          if (moreInclude)                        // skip include
            moreInclude = includeSpans.skipTo(target);

          if (!moreInclude)
            return false;

          if (moreExclude                         // skip exclude
              && includeSpans.doc() > excludeSpans.doc())
            moreExclude = excludeSpans.skipTo(includeSpans.doc());

          while (moreExclude                      // while exclude is before
                 && includeSpans.doc() == excludeSpans.doc()
                 && excludeSpans.end() <= includeSpans.start()-pre) {
            moreExclude = excludeSpans.next();    // increment exclude
          }

          if (!moreExclude                      // if no intersection
                || includeSpans.doc() != excludeSpans.doc()
                || includeSpans.end()+post <= excludeSpans.start())
            return true;                          // we found a match

          return next();                          // scan to next match
        }

        @Override
        public int doc() { return includeSpans.doc(); }
        @Override
        public int start() { return includeSpans.start(); }
        @Override
        public int end() { return includeSpans.end(); }

      // TODO: Remove warning after API has been finalized
      @Override
      public Collection<byte[]> getPayload() throws IOException {
        ArrayList<byte[]> result = null;
        if (includeSpans.isPayloadAvailable()) {
          result = new ArrayList<>(includeSpans.getPayload());
        }
        return result;
      }

      // TODO: Remove warning after API has been finalized
      @Override
      public boolean isPayloadAvailable() throws IOException {
        return includeSpans.isPayloadAvailable();
      }

      @Override
      public long cost() {
        return includeSpans.cost();
      }

      @Override
      public String toString() {
          return "spans(" + SpanNotQuery.this.toString() + ")";
        }

      };
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    SpanNotQuery clone = null;

    SpanQuery rewrittenInclude = (SpanQuery) include.rewrite(reader);
    if (rewrittenInclude != include) {
      clone = this.clone();
      clone.include = rewrittenInclude;
    }
    SpanQuery rewrittenExclude = (SpanQuery) exclude.rewrite(reader);
    if (rewrittenExclude != exclude) {
      if (clone == null) clone = this.clone();
      clone.exclude = rewrittenExclude;
    }

    if (clone != null) {
      return clone;                        // some clauses rewrote
    } else {
      return this;                         // no clauses rewrote
    }
  }

    /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    SpanNotQuery other = (SpanNotQuery)o;
    return this.include.equals(other.include)
            && this.exclude.equals(other.exclude)
            && this.pre == other.pre 
            && this.post == other.post;
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
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