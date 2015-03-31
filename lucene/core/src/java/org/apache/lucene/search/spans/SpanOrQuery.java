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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.search.Query;

/** Matches the union of its clauses.
 */
public class SpanOrQuery extends SpanQuery implements Cloneable {
  private List<SpanQuery> clauses;
  private String field;

  /** Construct a SpanOrQuery merging the provided clauses.
   * All clauses must have the same field. 
   */
  public SpanOrQuery(SpanQuery... clauses) {
    this.clauses = new ArrayList<>(clauses.length);
    for (SpanQuery seq : clauses) {
      addClause(seq);
    }
  }

  /** Adds a clause to this query */
  public final void addClause(SpanQuery clause) {
    if (field == null) {
      field = clause.getField();
    } else if (clause.getField() != null && !clause.getField().equals(field)) {
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
      if (spans1.docID() == spans2.docID()) {
        if (spans1.startPosition() == spans2.startPosition()) {
          return spans1.endPosition() < spans2.endPosition();
        } else {
          return spans1.startPosition() < spans2.startPosition();
        }
      } else {
        return spans1.docID() < spans2.docID();
      }
    }
  }

  @Override
  public Spans getSpans(final LeafReaderContext context, final Bits acceptDocs, final Map<Term,TermContext> termContexts)
  throws IOException {

    final ArrayList<Spans> subSpans = new ArrayList<>(clauses.size());

    for (SpanQuery seq : clauses) {
      Spans subSpan = seq.getSpans(context, acceptDocs, termContexts);
      if (subSpan != null) {
        subSpans.add(subSpan);
      }
    }

    if (subSpans.size() == 0) {
      return null;
    } else if (subSpans.size() == 1) {
      return subSpans.get(0);
    }

    final SpanQueue queue = new SpanQueue(clauses.size());
    for (Spans spans : subSpans) {
      queue.add(spans);
    }

    return new Spans() {

      @Override
      public int nextDoc() throws IOException {
        if (queue.size() == 0) { // all done
          return NO_MORE_DOCS;
        }

        int currentDoc = top().docID();

        if (currentDoc == -1) { // initially
          return advance(0);
        }

        do {
          if (top().nextDoc() != NO_MORE_DOCS) { // move top to next doc
            queue.updateTop();
          } else {
            queue.pop(); // exhausted a clause
            if (queue.size() == 0) {
              return NO_MORE_DOCS;
            }
          }
          // assert queue.size() > 0;
          int doc = top().docID();
          if (doc > currentDoc) {
            return doc;
          }
        } while (true);
      }

      private Spans top() {
        return queue.top();
      }

      @Override
      public int advance(int target) throws IOException {

        while ((queue.size() > 0) && (top().docID() < target)) {
          if (top().advance(target) != NO_MORE_DOCS) {
            queue.updateTop();
          } else {
            queue.pop();
          }
        }

        return (queue.size() > 0) ? top().docID() : NO_MORE_DOCS;
      }

      @Override
      public int docID() {
        return (queue == null) ? -1
              : (queue.size() > 0) ? top().docID()
              : NO_MORE_DOCS;
      }

      @Override
      public int nextStartPosition() throws IOException {
        top().nextStartPosition();
        queue.updateTop();
        int startPos = top().startPosition();
        while (startPos == -1) { // initially at this doc
          top().nextStartPosition();
          queue.updateTop();
          startPos = top().startPosition();
        }
        return startPos;
      }

      @Override
      public int startPosition() {
        return top().startPosition();
      }

      @Override
      public int endPosition() {
        return top().endPosition();
      }

      @Override
      public Collection<byte[]> getPayload() throws IOException {
        ArrayList<byte[]> result = null;
        Spans theTop = top();
        if (theTop != null && theTop.isPayloadAvailable()) {
          result = new ArrayList<>(theTop.getPayload());
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
             :(queue.size()>0?(docID()+": "+top().startPosition()+" - "+top().endPosition()):"END"));
      }

      private long cost = -1;

      @Override
      public long cost() {
        if (cost == -1) {
          cost = 0;
          for (Spans spans : subSpans) {
            cost += spans.cost();
          }
        }
        return cost;
      }

    };
  }

}
