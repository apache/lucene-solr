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

import java.io.IOException;

import java.util.Collection;
import java.util.Set;
import java.util.ArrayList;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ToStringUtils;

/** Matches spans near the beginning of a field. */
public class SpanFirstQuery extends SpanQuery implements Cloneable {
  private SpanQuery match;
  private int end;

  /** Construct a SpanFirstQuery matching spans in <code>match</code> whose end
   * position is less than or equal to <code>end</code>. */
  public SpanFirstQuery(SpanQuery match, int end) {
    this.match = match;
    this.end = end;
  }

  /** Return the SpanQuery whose matches are filtered. */
  public SpanQuery getMatch() { return match; }

  /** Return the maximum end position permitted in a match. */
  public int getEnd() { return end; }

  public String getField() { return match.getField(); }

  /** Returns a collection of all terms matched by this query.
   * @deprecated use extractTerms instead
   * @see #extractTerms(Set)
   */
  public Collection getTerms() { return match.getTerms(); }

  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("spanFirst(");
    buffer.append(match.toString(field));
    buffer.append(", ");
    buffer.append(end);
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }
  
  public Object clone() {
    SpanFirstQuery spanFirstQuery = new SpanFirstQuery((SpanQuery) match.clone(), end);
    spanFirstQuery.setBoost(getBoost());
    return spanFirstQuery;
  }
  
  public void extractTerms(Set terms) {
	    match.extractTerms(terms);
  }

  public Spans getSpans(final IndexReader reader) throws IOException {
    return new Spans() {
        private Spans spans = match.getSpans(reader);

        public boolean next() throws IOException {
          while (spans.next()) {                  // scan to next match
            if (end() <= end)
              return true;
          }
          return false;
        }

        public boolean skipTo(int target) throws IOException {
          if (!spans.skipTo(target))
            return false;

          return spans.end() <= end || next();

        }

        public int doc() { return spans.doc(); }
        public int start() { return spans.start(); }
        public int end() { return spans.end(); }

      // TODO: Remove warning after API has been finalized
      public Collection/*<byte[]>*/ getPayload() throws IOException {
        ArrayList result = null;
        if (spans.isPayloadAvailable()) {
          result = new ArrayList(spans.getPayload());
        }
        return result;//TODO: any way to avoid the new construction?
      }

      // TODO: Remove warning after API has been finalized
     public boolean isPayloadAvailable() {
        return spans.isPayloadAvailable();
      }

      public String toString() {
          return "spans(" + SpanFirstQuery.this.toString() + ")";
        }

      };
  }

  public Query rewrite(IndexReader reader) throws IOException {
    SpanFirstQuery clone = null;

    SpanQuery rewritten = (SpanQuery) match.rewrite(reader);
    if (rewritten != match) {
      clone = (SpanFirstQuery) this.clone();
      clone.match = rewritten;
    }

    if (clone != null) {
      return clone;                        // some clauses rewrote
    } else {
      return this;                         // no clauses rewrote
    }
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SpanFirstQuery)) return false;

    SpanFirstQuery other = (SpanFirstQuery)o;
    return this.end == other.end
         && this.match.equals(other.match)
         && this.getBoost() == other.getBoost();
  }

  public int hashCode() {
    int h = match.hashCode();
    h ^= (h << 8) | (h >>> 25);  // reversible
    h ^= Float.floatToRawIntBits(getBoost()) ^ end;
    return h;
  }


}
