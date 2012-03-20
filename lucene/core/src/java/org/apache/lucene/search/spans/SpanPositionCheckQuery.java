package org.apache.lucene.search.spans;
/**
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


import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.TermContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;


/**
 * Base class for filtering a SpanQuery based on the position of a match.
 **/
public abstract class SpanPositionCheckQuery extends SpanQuery implements Cloneable {
  protected SpanQuery match;


  public SpanPositionCheckQuery(SpanQuery match) {
    this.match = match;
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

  /** Return value if the match should be accepted {@code YES}, rejected {@code NO},
   * or rejected and enumeration should advance to the next document {@code NO_AND_ADVANCE}.
   * @see #acceptPosition(Spans)
   */
  protected static enum AcceptStatus { YES, NO, NO_AND_ADVANCE };
  
  /**
   * Implementing classes are required to return whether the current position is a match for the passed in
   * "match" {@link org.apache.lucene.search.spans.SpanQuery}.
   *
   * This is only called if the underlying {@link org.apache.lucene.search.spans.Spans#next()} for the
   * match is successful
   *
   *
   * @param spans The {@link org.apache.lucene.search.spans.Spans} instance, positioned at the spot to check
   * @return whether the match is accepted, rejected, or rejected and should move to the next doc.
   *
   * @see org.apache.lucene.search.spans.Spans#next()
   *
   */
  protected abstract AcceptStatus acceptPosition(Spans spans) throws IOException;

  @Override
  public Spans getSpans(final AtomicReaderContext context, Bits acceptDocs, Map<Term,TermContext> termContexts) throws IOException {
    return new PositionCheckSpan(context, acceptDocs, termContexts);
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

  protected class PositionCheckSpan extends Spans {
    private Spans spans;

    public PositionCheckSpan(AtomicReaderContext context, Bits acceptDocs, Map<Term,TermContext> termContexts) throws IOException {
      spans = match.getSpans(context, acceptDocs, termContexts);
    }

    @Override
    public boolean next() throws IOException {
      if (!spans.next())
        return false;
      
      return doNext();
    }

    @Override
    public boolean skipTo(int target) throws IOException {
      if (!spans.skipTo(target))
        return false;

      return doNext();
    }
    
    protected boolean doNext() throws IOException {
      for (;;) {
        switch(acceptPosition(this)) {
          case YES: return true;
          case NO: 
            if (!spans.next()) 
              return false;
            break;
          case NO_AND_ADVANCE: 
            if (!spans.skipTo(spans.doc()+1)) 
              return false;
            break;
        }
      }
    }

    @Override
    public int doc() { return spans.doc(); }

    @Override
    public int start() { return spans.start(); }

    @Override
    public int end() { return spans.end(); }
    // TODO: Remove warning after API has been finalized

    @Override
    public Collection<byte[]> getPayload() throws IOException {
      ArrayList<byte[]> result = null;
      if (spans.isPayloadAvailable()) {
        result = new ArrayList<byte[]>(spans.getPayload());
      }
      return result;//TODO: any way to avoid the new construction?
    }
    // TODO: Remove warning after API has been finalized

    @Override
    public boolean isPayloadAvailable() {
      return spans.isPayloadAvailable();
    }

    @Override
    public String toString() {
        return "spans(" + SpanPositionCheckQuery.this.toString() + ")";
      }

  }
}