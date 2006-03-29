package org.apache.lucene.search.spans;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.ArrayList;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.util.ToStringUtils;

/** Matches spans containing a term. */
public class SpanTermQuery extends SpanQuery {
  private Term term;

  /** Construct a SpanTermQuery matching the named term's spans. */
  public SpanTermQuery(Term term) { this.term = term; }

  /** Return the term whose spans are matched. */
  public Term getTerm() { return term; }

  public String getField() { return term.field(); }
  
  /** Returns a collection of all terms matched by this query.
   * @deprecated use extractTerms instead
   * @see #extractTerms(Set)
   */
  public Collection getTerms() {
    Collection terms = new ArrayList();
    terms.add(term);
    return terms;
  }
  public void extractTerms(Set terms) {
	  terms.add(term);
  }

  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    if (term.field().equals(field))
      buffer.append(term.text());
    else
      buffer.append(term.toString());
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (!(o instanceof SpanTermQuery))
      return false;
    SpanTermQuery other = (SpanTermQuery)o;
    return (this.getBoost() == other.getBoost())
      && this.term.equals(other.term);
  }

  /** Returns a hash code value for this object.*/
  public int hashCode() {
    return Float.floatToIntBits(getBoost()) ^ term.hashCode() ^ 0xD23FE494;
  }

  public Spans getSpans(final IndexReader reader) throws IOException {
    return new Spans() {
        private TermPositions positions = reader.termPositions(term);

        private int doc = -1;
        private int freq;
        private int count;
        private int position;

        public boolean next() throws IOException {
          if (count == freq) {
            if (!positions.next()) {
              doc = Integer.MAX_VALUE;
              return false;
            }
            doc = positions.doc();
            freq = positions.freq();
            count = 0;
          }
          position = positions.nextPosition();
          count++;
          return true;
        }

        public boolean skipTo(int target) throws IOException {
          // are we already at the correct position?
          if (doc >= target) {
            return true;
          }

          if (!positions.skipTo(target)) {
            doc = Integer.MAX_VALUE;
            return false;
          }

          doc = positions.doc();
          freq = positions.freq();
          count = 0;

          position = positions.nextPosition();
          count++;

          return true;
        }

        public int doc() { return doc; }
        public int start() { return position; }
        public int end() { return position + 1; }

        public String toString() {
          return "spans(" + SpanTermQuery.this.toString() + ")@"+
            (doc==-1?"START":(doc==Integer.MAX_VALUE)?"END":doc+"-"+position);
        }

      };
  }

}
