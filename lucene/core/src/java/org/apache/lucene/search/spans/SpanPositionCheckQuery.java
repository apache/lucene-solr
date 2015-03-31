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
   * Return value for {@link SpanPositionCheckQuery#acceptPosition(Spans)}.
   */
  protected static enum AcceptStatus {
    /** Indicates the match should be accepted */
    YES,

    /** Indicates the match should be rejected */
    NO,

    /**
     * Indicates the match should be rejected, and the enumeration may continue
     * with the next document.
     */
    NO_MORE_IN_CURRENT_DOC
  };

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
  public Spans getSpans(final LeafReaderContext context, Bits acceptDocs, Map<Term,TermContext> termContexts) throws IOException {
    Spans matchSpans = match.getSpans(context, acceptDocs, termContexts);
    return (matchSpans == null) ? null : new PositionCheckSpans(matchSpans);
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

  protected class PositionCheckSpans extends FilterSpans {

    private boolean atFirstInCurrentDoc = false;
    private int startPos = -1;

    public PositionCheckSpans(Spans matchSpans) throws IOException {
      super(matchSpans);
    }

    @Override
    public int nextDoc() throws IOException {
      if (in.nextDoc() == NO_MORE_DOCS)
        return NO_MORE_DOCS;

      return toNextDocWithAllowedPosition();
    }

    @Override
    public int advance(int target) throws IOException {
      if (in.advance(target) == NO_MORE_DOCS)
        return NO_MORE_DOCS;

      return toNextDocWithAllowedPosition();
    }

    @SuppressWarnings("fallthrough")
    protected int toNextDocWithAllowedPosition() throws IOException {
      startPos = in.nextStartPosition();
      assert startPos != NO_MORE_POSITIONS;
      for (;;) {
        switch(acceptPosition(this)) {
          case YES:
            atFirstInCurrentDoc = true;
            return in.docID();
          case NO:
            startPos = in.nextStartPosition();
            if (startPos != NO_MORE_POSITIONS) {
              break;
            }
            // else fallthrough
          case NO_MORE_IN_CURRENT_DOC:
            if (in.nextDoc() == NO_MORE_DOCS) {
              startPos = -1;
              return NO_MORE_DOCS;
            }
            startPos = in.nextStartPosition();
            assert startPos != NO_MORE_POSITIONS : "no start position at doc="+in.docID();
            break;
        }
      }
    }

    @Override
    public int nextStartPosition() throws IOException {
      if (atFirstInCurrentDoc) {
        atFirstInCurrentDoc = false;
        return startPos;
      }

      for (;;) {
        startPos = in.nextStartPosition();
        if (startPos == NO_MORE_POSITIONS) {
          return NO_MORE_POSITIONS;
        }
        switch(acceptPosition(this)) {
          case YES:
            return startPos;
          case NO:
            break;
          case NO_MORE_IN_CURRENT_DOC:
            return startPos = NO_MORE_POSITIONS; // startPos ahead for the current doc.
        }
      }
    }

    @Override
    public int startPosition() {
      return atFirstInCurrentDoc ? -1 : startPos;
    }

    @Override
    public int endPosition() {
      return atFirstInCurrentDoc ? -1
            : (startPos != NO_MORE_POSITIONS) ? in.endPosition() : NO_MORE_POSITIONS;
    }

    @Override
    public String toString() {
      return "spans(" + SpanPositionCheckQuery.this.toString() + ")";
    }
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (getClass() != o.getClass()) return false;
    final SpanPositionCheckQuery spcq = (SpanPositionCheckQuery) o;
    return match.equals(spcq.match);
  }

  @Override
  public int hashCode() {
    return match.hashCode() ^ getClass().hashCode();
  }
}
