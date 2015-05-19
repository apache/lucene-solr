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
import org.apache.lucene.search.spans.FilterSpans.AcceptStatus;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Objects;


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



  @Override
  public void extractTerms(Set<Term> terms) {
    match.extractTerms(terms);
  }

  /**
   * Implementing classes are required to return whether the current position is a match for the passed in
   * "match" {@link SpanQuery}.
   *
   * This is only called if the underlying last {@link Spans#nextStartPosition()} for the
   * match indicated a valid start position.
   *
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
  public Spans getSpans(final LeafReaderContext context, Bits acceptDocs, Map<Term,TermContext> termContexts, SpanCollector collector) throws IOException {
    Spans matchSpans = match.getSpans(context, acceptDocs, termContexts, collector);
    return (matchSpans == null) ? null : new FilterSpans(matchSpans) {
      @Override
      protected AcceptStatus accept(Spans candidate) throws IOException {
        return acceptPosition(candidate);
      }
    };
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    SpanPositionCheckQuery clone = null;

    SpanQuery rewritten = (SpanQuery) match.rewrite(reader);
    if (rewritten != match) {
      clone = (SpanPositionCheckQuery) this.clone();
      clone.match = rewritten;
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
    if (! super.equals(o)) {
      return false;
    }
    SpanPositionCheckQuery spcq = (SpanPositionCheckQuery) o;
    return match.equals(spcq.match);
  }

  @Override
  public int hashCode() {
    return match.hashCode() ^ super.hashCode();
  }
}
