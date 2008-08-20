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

import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.search.Query;

/** Matches the union of its clauses.*/
public class SpanOrQuery extends SpanQuery {
  private List clauses;
  private String field;

  /** Construct a SpanOrQuery merging the provided clauses. */
  public SpanOrQuery(SpanQuery[] clauses) {

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
  }

  /** Return the clauses whose spans are matched. */
  public SpanQuery[] getClauses() {
    return (SpanQuery[])clauses.toArray(new SpanQuery[clauses.size()]);
  }

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

  public Query rewrite(IndexReader reader) throws IOException {
    SpanOrQuery clone = null;
    for (int i = 0 ; i < clauses.size(); i++) {
      SpanQuery c = (SpanQuery)clauses.get(i);
      SpanQuery query = (SpanQuery) c.rewrite(reader);
      if (query != c) {                     // clause rewrote: must clone
        if (clone == null)
          clone = (SpanOrQuery) this.clone();
        clone.clauses.set(i,query);
      }
    }
    if (clone != null) {
      return clone;                        // some clauses rewrote
    } else {
      return this;                         // no clauses rewrote
    }
  }

  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("spanOr([");
    Iterator i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = (SpanQuery)i.next();
      buffer.append(clause.toString(field));
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("])");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final SpanOrQuery that = (SpanOrQuery) o;

    if (!clauses.equals(that.clauses)) return false;
    if (!field.equals(that.field)) return false;

    return getBoost() == that.getBoost();
  }

  public int hashCode() {
    int h = clauses.hashCode();
    h ^= (h << 10) | (h >>> 23);
    h ^= Float.floatToRawIntBits(getBoost());
    return h;
  }


  private class SpanQueue extends PriorityQueue {
    public SpanQueue(int size) {
      initialize(size);
    }

    protected final boolean lessThan(Object o1, Object o2) {
      Spans spans1 = (Spans)o1;
      Spans spans2 = (Spans)o2;
      if (spans1.doc() == spans2.doc()) {
        if (spans1.start() == spans2.start()) {
          return spans1.end() < spans2.end();
        } else {
          return spans1.start() < spans2.start();
        }
      } else {
        return spans1.doc() < spans2.doc();
      }
    }
  }

  public PayloadSpans getPayloadSpans(final IndexReader reader) throws IOException {
    return (PayloadSpans)getSpans(reader);
  }

  public Spans getSpans(final IndexReader reader) throws IOException {
    if (clauses.size() == 1)                      // optimize 1-clause case
      return ((SpanQuery)clauses.get(0)).getPayloadSpans(reader);

    return new PayloadSpans() {
        private SpanQueue queue = null;

        private boolean initSpanQueue(int target) throws IOException {
          queue = new SpanQueue(clauses.size());
          Iterator i = clauses.iterator();
          while (i.hasNext()) {
            PayloadSpans spans = ((SpanQuery)i.next()).getPayloadSpans(reader);
            if (   ((target == -1) && spans.next())
                || ((target != -1) && spans.skipTo(target))) {
              queue.put(spans);
            }
          }
          return queue.size() != 0;
        }

        public boolean next() throws IOException {
          if (queue == null) {
            return initSpanQueue(-1);
          }

          if (queue.size() == 0) { // all done
            return false;
          }

          if (top().next()) { // move to next
            queue.adjustTop();
            return true;
          }

          queue.pop();  // exhausted a clause
          return queue.size() != 0;
        }

        private PayloadSpans top() { return (PayloadSpans)queue.top(); }

        public boolean skipTo(int target) throws IOException {
          if (queue == null) {
            return initSpanQueue(target);
          }

          while (queue.size() != 0 && top().doc() < target) {
            if (top().skipTo(target)) {
              queue.adjustTop();
            } else {
              queue.pop();
            }
          }

          return queue.size() != 0;
        }

        public int doc() { return top().doc(); }
        public int start() { return top().start(); }
        public int end() { return top().end(); }

      // TODO: Remove warning after API has been finalized
      public Collection/*<byte[]>*/ getPayload() throws IOException {
        ArrayList result = null;
        PayloadSpans theTop = top();
        if (theTop != null && theTop.isPayloadAvailable()) {
          result = new ArrayList(theTop.getPayload());
        }
        return result;
      }

      // TODO: Remove warning after API has been finalized
     public boolean isPayloadAvailable() {
        PayloadSpans top = top();
        return top != null && top.isPayloadAvailable();
      }

      public String toString() {
          return "spans("+SpanOrQuery.this+")@"+
            ((queue == null)?"START"
             :(queue.size()>0?(doc()+":"+start()+"-"+end()):"END"));
        }

      };
  }

}
