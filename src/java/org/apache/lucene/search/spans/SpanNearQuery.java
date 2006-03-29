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
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;


import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ToStringUtils;

/** Matches spans which are near one another.  One can specify <i>slop</i>, the
 * maximum number of intervening unmatched positions, as well as whether
 * matches are required to be in-order. */
public class SpanNearQuery extends SpanQuery {
  private List clauses;
  private int slop;
  private boolean inOrder;

  private String field;

  /** Construct a SpanNearQuery.  Matches spans matching a span from each
   * clause, with up to <code>slop</code> total unmatched positions between
   * them.  * When <code>inOrder</code> is true, the spans from each clause
   * must be * ordered as in <code>clauses</code>. */
  public SpanNearQuery(SpanQuery[] clauses, int slop, boolean inOrder) {

    // copy clauses array into an ArrayList
    this.clauses = new ArrayList(clauses.length);
    for (int i = 0; i < clauses.length; i++) {
      SpanQuery clause = clauses[i];
      if (i == 0) {                               // check field
        field = clause.getField();
      } else if (!clause.getField().equals(field)) {
        throw new IllegalArgumentException("Clauses must have same field.");
      }
      this.clauses.add(clause);
    }

    this.slop = slop;
    this.inOrder = inOrder;
  }

  /** Return the clauses whose spans are matched. */
  public SpanQuery[] getClauses() {
    return (SpanQuery[])clauses.toArray(new SpanQuery[clauses.size()]);
  }

  /** Return the maximum number of intervening unmatched positions permitted.*/
  public int getSlop() { return slop; }

  /** Return true if matches are required to be in-order.*/
  public boolean isInOrder() { return inOrder; }

  public String getField() { return field; }
  
  /** Returns a collection of all terms matched by this query.
   * @deprecated use extractTerms instead
   * @see #extractTerms(Set)
   */
  public Collection getTerms() {
    Collection terms = new ArrayList();
    Iterator i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = (SpanQuery)i.next();
      terms.addAll(clause.getTerms());
    }
    return terms;
  }
  
  public void extractTerms(Set terms) {
	    Iterator i = clauses.iterator();
	    while (i.hasNext()) {
	      SpanQuery clause = (SpanQuery)i.next();
	      clause.extractTerms(terms);
	    }
  }  
  

  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("spanNear([");
    Iterator i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = (SpanQuery)i.next();
      buffer.append(clause.toString(field));
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("], ");
    buffer.append(slop);
    buffer.append(", ");
    buffer.append(inOrder);
    buffer.append(")");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  public Spans getSpans(final IndexReader reader) throws IOException {
    if (clauses.size() == 0)                      // optimize 0-clause case
      return new SpanOrQuery(getClauses()).getSpans(reader);

    if (clauses.size() == 1)                      // optimize 1-clause case
      return ((SpanQuery)clauses.get(0)).getSpans(reader);

    return new NearSpans(this, reader);
  }

  public Query rewrite(IndexReader reader) throws IOException {
    SpanNearQuery clone = null;
    for (int i = 0 ; i < clauses.size(); i++) {
      SpanQuery c = (SpanQuery)clauses.get(i);
      SpanQuery query = (SpanQuery) c.rewrite(reader);
      if (query != c) {                     // clause rewrote: must clone
        if (clone == null)
          clone = (SpanNearQuery) this.clone();
        clone.clauses.set(i,query);
      }
    }
    if (clone != null) {
      return clone;                        // some clauses rewrote
    } else {
      return this;                         // no clauses rewrote
    }
  }

  /** Returns true iff <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SpanNearQuery)) return false;

    final SpanNearQuery spanNearQuery = (SpanNearQuery) o;

    if (inOrder != spanNearQuery.inOrder) return false;
    if (slop != spanNearQuery.slop) return false;
    if (!clauses.equals(spanNearQuery.clauses)) return false;

    return getBoost() == spanNearQuery.getBoost();
  }

  public int hashCode() {
    int result;
    result = clauses.hashCode();
    // Mix bits before folding in things like boost, since it could cancel the
    // last element of clauses.  This particular mix also serves to
    // differentiate SpanNearQuery hashcodes from others.
    result ^= (result << 14) | (result >>> 19);  // reversible
    result += Float.floatToRawIntBits(getBoost());
    result += slop;
    result ^= (inOrder ? 0x99AFD3BD : 0);
    return result;
  }
}
