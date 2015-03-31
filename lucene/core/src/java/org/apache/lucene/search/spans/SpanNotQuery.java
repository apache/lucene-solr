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
import java.util.Objects;

/** Removes matches which overlap with another SpanQuery or which are
 * within x tokens before or y tokens after another SpanQuery.
 */
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
    this.include = Objects.requireNonNull(include);
    this.exclude = Objects.requireNonNull(exclude);
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
    SpanNotQuery spanNotQuery = new SpanNotQuery((SpanQuery) include.clone(),
                                                                (SpanQuery) exclude.clone(), pre, post);
    spanNotQuery.setBoost(getBoost());
    return spanNotQuery;
  }

  @Override
  public Spans getSpans(final LeafReaderContext context, final Bits acceptDocs, final Map<Term,TermContext> termContexts) throws IOException {
    final Spans includeSpans = include.getSpans(context, acceptDocs, termContexts);
    if (includeSpans == null) {
      return null;
    }

    final Spans excludeSpans = exclude.getSpans(context, acceptDocs, termContexts);
    if (excludeSpans == null) {
      return includeSpans;
    }

    return new Spans() {
      private boolean moreInclude = true;
      private int includeStart = -1;
      private int includeEnd = -1;
      private boolean atFirstInCurrentDoc = false;

      private boolean moreExclude = excludeSpans.nextDoc() != NO_MORE_DOCS;
      private int excludeStart = moreExclude ? excludeSpans.nextStartPosition() : NO_MORE_POSITIONS;


      @Override
      public int nextDoc() throws IOException {
        if (moreInclude) {
          moreInclude = includeSpans.nextDoc() != NO_MORE_DOCS;
          if (moreInclude) {
            atFirstInCurrentDoc = true;
            includeStart = includeSpans.nextStartPosition();
            assert includeStart != NO_MORE_POSITIONS;
          }
        }
        toNextIncluded();
        int res = moreInclude ? includeSpans.docID() : NO_MORE_DOCS;
        return res;
      }

      private void toNextIncluded() throws IOException {
        while (moreInclude && moreExclude) {
          if (includeSpans.docID() > excludeSpans.docID()) {
            moreExclude = excludeSpans.advance(includeSpans.docID()) != NO_MORE_DOCS;
            if (moreExclude) {
              excludeStart = -1; // only use exclude positions at same doc
            }
          }
          if (excludeForwardInCurrentDocAndAtMatch()) {
            break; // at match.
          }

          // else intersected: keep scanning, to next doc if needed
          includeStart = includeSpans.nextStartPosition();
          if (includeStart == NO_MORE_POSITIONS) {
            moreInclude = includeSpans.nextDoc() != NO_MORE_DOCS;
            if (moreInclude) {
              atFirstInCurrentDoc = true;
              includeStart = includeSpans.nextStartPosition();
              assert includeStart != NO_MORE_POSITIONS;
            }
          }
        }
      }

      private boolean excludeForwardInCurrentDocAndAtMatch() throws IOException {
        assert moreInclude;
        assert includeStart != NO_MORE_POSITIONS;
        if (! moreExclude) {
          return true;
        }
        if (includeSpans.docID() != excludeSpans.docID()) {
          return true;
        }
        // at same doc
        if (excludeStart == -1) { // init exclude start position if needed
          excludeStart = excludeSpans.nextStartPosition();
          assert excludeStart != NO_MORE_POSITIONS;
        }
        while (excludeSpans.endPosition() <= includeStart - pre) {
          // exclude end position is before a possible exclusion
          excludeStart = excludeSpans.nextStartPosition();
          if (excludeStart == NO_MORE_POSITIONS) {
            return true; // no more exclude at current doc.
          }
        }
        // exclude end position far enough in current doc, check start position:
        boolean res = includeSpans.endPosition() + post <= excludeStart;
        return res;
      }

      @Override
      public int advance(int target) throws IOException {
        if (moreInclude) {
          assert target > includeSpans.docID() : "target="+target+", includeSpans.docID()="+includeSpans.docID();
          moreInclude = includeSpans.advance(target) != NO_MORE_DOCS;
          if (moreInclude) {
            atFirstInCurrentDoc = true;
            includeStart = includeSpans.nextStartPosition();
            assert includeStart != NO_MORE_POSITIONS;
          }
        }
        toNextIncluded();
        int res = moreInclude ? includeSpans.docID() : NO_MORE_DOCS;
        return res;
      }

      @Override
      public int docID() {
        int res = includeSpans.docID();
        return res;
      }

      @Override
      public int nextStartPosition() throws IOException {
        assert moreInclude;

        if (atFirstInCurrentDoc) {
          atFirstInCurrentDoc = false;
          assert includeStart != NO_MORE_POSITIONS;
          return includeStart;
        }

        includeStart = includeSpans.nextStartPosition();
        while ((includeStart != NO_MORE_POSITIONS)
            && (! excludeForwardInCurrentDocAndAtMatch()))
        {
          includeStart = includeSpans.nextStartPosition();
        }

        return includeStart;
      }

      @Override
      public int startPosition() {
        assert includeStart == includeSpans.startPosition();
        return atFirstInCurrentDoc ? -1 : includeStart;
      }

      @Override
      public int endPosition() {
        return atFirstInCurrentDoc ? -1 : includeSpans.endPosition();
      }

      @Override
      public Collection<byte[]> getPayload() throws IOException {
        ArrayList<byte[]> result = null;
        if (includeSpans.isPayloadAvailable()) {
          result = new ArrayList<>(includeSpans.getPayload());
        }
        return result;
      }

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
