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

import java.io.IOException;

import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.search.Query;

/** Matches the union of its clauses.*/
public class SpanOrQuery extends SpanQuery implements Cloneable {
  private List<SpanQuery> clauses;
  private String field;

  /** Construct a SpanOrQuery merging the provided clauses. */
  public SpanOrQuery(SpanQuery... clauses) {

    // copy clauses array into an ArrayList
    this.clauses = new ArrayList<SpanQuery>(clauses.length);
    for (int i = 0; i < clauses.length; i++) {
      addClause(clauses[i]);
    }
  }

  /** Adds a clause to this query */
  public final void addClause(SpanQuery clause) {
    if (field == null) {
      field = clause.getField();
    } else if (!clause.getField().equals(field)) {
      throw new IllegalArgumentException("Clauses must have same field.");
    }
    this.clauses.add(clause);
  }
  
  /** Return the clauses whose spans are matched. */
  public SpanQuery[] getClauses() {
    return clauses.toArray(new SpanQuery[clauses.size()]);
  }

  @Override
  public String getField() { return field; }

  @Override
  public void extractTerms(Set<Term> terms) {
    for(final SpanQuery clause: clauses) {
      clause.extractTerms(terms);
    }
  }
  
  @Override
  public SpanOrQuery clone() {
    int sz = clauses.size();
    SpanQuery[] newClauses = new SpanQuery[sz];

    for (int i = 0; i < sz; i++) {
      newClauses[i] = (SpanQuery) clauses.get(i).clone();
    }
    SpanOrQuery soq = new SpanOrQuery(newClauses);
    soq.setBoost(getBoost());
    return soq;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    SpanOrQuery clone = null;
    for (int i = 0 ; i < clauses.size(); i++) {
      SpanQuery c = clauses.get(i);
      SpanQuery query = (SpanQuery) c.rewrite(reader);
      if (query != c) {                     // clause rewrote: must clone
        if (clone == null)
          clone = this.clone();
        clone.clauses.set(i,query);
      }
    }
    if (clone != null) {
      return clone;                        // some clauses rewrote
    } else {
      return this;                         // no clauses rewrote
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanOr([");
    Iterator<SpanQuery> i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = i.next();
      buffer.append(clause.toString(field));
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("])");
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final SpanOrQuery that = (SpanOrQuery) o;

    if (!clauses.equals(that.clauses)) return false;
    if (!clauses.isEmpty() && !field.equals(that.field)) return false;

    return getBoost() == that.getBoost();
  }

  @Override
  public int hashCode() {
    int h = clauses.hashCode();
    h ^= (h << 10) | (h >>> 23);
    h ^= Float.floatToRawIntBits(getBoost());
    return h;
  }


  private class SpanQueue extends PriorityQueue<Spans> {
    public SpanQueue(int size) {
      super(size);
    }

    @Override
    protected final boolean lessThan(Spans spans1, Spans spans2) {
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

  @Override
  public Spans getSpans(final AtomicReaderContext context, final Bits acceptDocs, final Map<Term,TermContext> termContexts) throws IOException {
    if (clauses.size() == 1)                      // optimize 1-clause case
      return (clauses.get(0)).getSpans(context, acceptDocs, termContexts);

    return new Spans() {
        private SpanQueue queue = null;
        private long cost;

        private boolean initSpanQueue(int target) throws IOException {
          queue = new SpanQueue(clauses.size());
          Iterator<SpanQuery> i = clauses.iterator();
          while (i.hasNext()) {
            Spans spans = i.next().getSpans(context, acceptDocs, termContexts);
            cost += spans.cost();
            if (   ((target == -1) && spans.next())
                || ((target != -1) && spans.skipTo(target))) {
              queue.add(spans);
            }
          }
          return queue.size() != 0;
        }

        @Override
        public boolean next() throws IOException {
          if (queue == null) {
            return initSpanQueue(-1);
          }

          if (queue.size() == 0) { // all done
            return false;
          }

          if (top().next()) { // move to next
            queue.updateTop();
            return true;
          }

          queue.pop();  // exhausted a clause
          return queue.size() != 0;
        }

        private Spans top() { return queue.top(); }

        @Override
        public boolean skipTo(int target) throws IOException {
          if (queue == null) {
            return initSpanQueue(target);
          }
  
          boolean skipCalled = false;
          while (queue.size() != 0 && top().doc() < target) {
            if (top().skipTo(target)) {
              queue.updateTop();
            } else {
              queue.pop();
            }
            skipCalled = true;
          }
  
          if (skipCalled) {
            return queue.size() != 0;
          }
          return next();
        }

        @Override
        public int doc() { return top().doc(); }
        @Override
        public int start() { return top().start(); }
        @Override
        public int end() { return top().end(); }

      @Override
      public Collection<byte[]> getPayload() throws IOException {
        ArrayList<byte[]> result = null;
        Spans theTop = top();
        if (theTop != null && theTop.isPayloadAvailable()) {
          result = new ArrayList<byte[]>(theTop.getPayload());
        }
        return result;
      }

      @Override
      public boolean isPayloadAvailable() throws IOException {
        Spans top = top();
        return top != null && top.isPayloadAvailable();
      }

      @Override
      public String toString() {
          return "spans("+SpanOrQuery.this+")@"+
            ((queue == null)?"START"
             :(queue.size()>0?(doc()+":"+start()+"-"+end()):"END"));
        }

      @Override
      public long cost() {
        return cost;
      }
      
      };
  }

}
