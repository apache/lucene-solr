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


import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;


/**
 *
 *
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

  /**
   * Implementing classes are required to return whether the current position is a match for the passed in
   * "match" {@link org.apache.lucene.search.spans.SpanQuery}.
   *
   * This is only called if the underlying {@link org.apache.lucene.search.spans.Spans#next()} for the
   * match is successful
   *
   *
   * @param spans The {@link org.apache.lucene.search.spans.Spans} instance, positioned at the spot to check
   * @return true if it is a match, else false.
   *
   * @see org.apache.lucene.search.spans.Spans#next()
   *
   */
  protected abstract boolean acceptPosition(Spans spans) throws IOException;

  /**
   * Implementing classes are required to return whether the position at the target is someplace that
   * can be skipped to.  For instance, the {@link org.apache.lucene.search.spans.SpanFirstQuery} returns
   * false if the target position is beyond the maximum position allowed or if {@link Spans#next()} is true.
   * <p/>
   * Note, this method is only called if the underlying match {@link org.apache.lucene.search.spans.SpanQuery} can
   * skip to the target.
   * <p/>
   * It is safe to assume that the passed in {@link org.apache.lucene.search.spans.Spans} object for the underlying {@link org.apache.lucene.search.spans.SpanQuery} is
   * positioned at the target.
   * <p/>
   * The default implementation is to return true if either {@link #acceptPosition(Spans)} or {@link org.apache.lucene.search.spans.Spans#next()} is true for the
   * passed in instance of Spans.
   *<p/>
   * @param spans The {@link org.apache.lucene.search.spans.Spans} to check
   * @return true if the instance can skip to this position
   *
   * @see Spans#skipTo(int)
   * @throws java.io.IOException if there is a low-level IO error
   */
  protected boolean acceptSkipTo(Spans spans) throws IOException{
    return acceptPosition(spans) || spans.next();
  }

  @Override
  public Spans getSpans(final IndexReader reader) throws IOException {
    return new PositionCheckSpan(reader);
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

    private final IndexReader reader;

    public PositionCheckSpan(IndexReader reader) throws IOException {
      this.reader = reader;
      spans = match.getSpans(reader);
    }

    @Override
    public boolean next() throws IOException {
      //TODO: optimize to skip ahead to start
      while (spans.next()) {                  // scan to next match
        if (acceptPosition(this))
          return true;
      }
      return false;
    }

    @Override
    public boolean skipTo(int target) throws IOException {
      if (!spans.skipTo(target))
        return false;

      return acceptSkipTo(this);

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