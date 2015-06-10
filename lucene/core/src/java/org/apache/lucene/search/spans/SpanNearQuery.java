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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Matches spans which are near one another.  One can specify <i>slop</i>, the
 * maximum number of intervening unmatched positions, as well as whether
 * matches are required to be in-order.
 */
public class SpanNearQuery extends SpanQuery implements Cloneable {
  protected List<SpanQuery> clauses;
  protected int slop;
  protected boolean inOrder;

  protected String field;
  private boolean collectPayloads;

  /** Construct a SpanNearQuery.  Matches spans matching a span from each
   * clause, with up to <code>slop</code> total unmatched positions between
   * them.
   * <br>When <code>inOrder</code> is true, the spans from each clause
   * must be in the same order as in <code>clauses</code> and must be non-overlapping.
   * <br>When <code>inOrder</code> is false, the spans from each clause
   * need not be ordered and may overlap.
   * @param clauses the clauses to find near each other, in the same field, at least 2.
   * @param slop The slop value
   * @param inOrder true if order is important
   */
  public SpanNearQuery(SpanQuery[] clauses, int slop, boolean inOrder) {
    this(clauses, slop, inOrder, true);
  }

  public SpanNearQuery(SpanQuery[] clausesIn, int slop, boolean inOrder, boolean collectPayloads) {
    this.clauses = new ArrayList<>(clausesIn.length);
    for (SpanQuery clause : clausesIn) {
      if (this.field == null) {                               // check field
        this.field = clause.getField();
      } else if (clause.getField() != null && !clause.getField().equals(field)) {
        throw new IllegalArgumentException("Clauses must have same field.");
      }
      this.clauses.add(clause);
    }
    this.collectPayloads = collectPayloads;
    this.slop = slop;
    this.inOrder = inOrder;
  }

  /** Return the clauses whose spans are matched. */
  public SpanQuery[] getClauses() {
    return clauses.toArray(new SpanQuery[clauses.size()]);
  }

  /** Return the maximum number of intervening unmatched positions permitted.*/
  public int getSlop() { return slop; }

  /** Return true if matches are required to be in-order.*/
  public boolean isInOrder() { return inOrder; }

  @Override
  public String getField() { return field; }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanNear([");
    Iterator<SpanQuery> i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = i.next();
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

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, SpanCollectorFactory factory) throws IOException {
    List<SpanWeight> subWeights = new ArrayList<>();
    for (SpanQuery q : clauses) {
      subWeights.add(q.createWeight(searcher, false, factory));
    }
    return new SpanNearWeight(subWeights, searcher, needsScores ? getTermContexts(subWeights) : null, factory);
  }

  public class SpanNearWeight extends SpanWeight {

    final List<SpanWeight> subWeights;

    public SpanNearWeight(List<SpanWeight> subWeights, IndexSearcher searcher, Map<Term, TermContext> terms, SpanCollectorFactory factory) throws IOException {
      super(SpanNearQuery.this, searcher, terms, factory);
      this.subWeights = subWeights;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      for (SpanWeight w : subWeights) {
        w.extractTermContexts(contexts);
      }
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Bits acceptDocs, SpanCollector collector) throws IOException {

      Terms terms = context.reader().terms(field);
      if (terms == null) {
        return null; // field does not exist
      }

      ArrayList<Spans> subSpans = new ArrayList<>(clauses.size());
      SpanCollector subSpanCollector = inOrder ? collector.bufferedCollector() : collector;
      for (SpanWeight w : subWeights) {
        Spans subSpan = w.getSpans(context, acceptDocs, subSpanCollector);
        if (subSpan != null) {
          subSpans.add(subSpan);
        } else {
          return null; // all required
        }
      }

      // all NearSpans require at least two subSpans
      return (!inOrder) ? new NearSpansUnordered(SpanNearQuery.this, subSpans)
          : new NearSpansOrdered(SpanNearQuery.this, subSpans);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (SpanWeight w : subWeights) {
        w.extractTerms(terms);
      }
    }
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    SpanNearQuery clone = null;
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
      return clone; // some clauses rewrote
    } else {
      return this; // no clauses rewrote
    }
  }

  @Override
  public SpanNearQuery clone() {
    int sz = clauses.size();
    SpanQuery[] newClauses = new SpanQuery[sz];

    for (int i = 0; i < sz; i++) {
      newClauses[i] = (SpanQuery) clauses.get(i).clone();
    }
    SpanNearQuery spanNearQuery = new SpanNearQuery(newClauses, slop, inOrder);
    spanNearQuery.setBoost(getBoost());
    return spanNearQuery;
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (! super.equals(o)) {
      return false;
    }
    final SpanNearQuery spanNearQuery = (SpanNearQuery) o;

    return (inOrder == spanNearQuery.inOrder)
        && (slop == spanNearQuery.slop)
        && (collectPayloads == spanNearQuery.collectPayloads)
        && clauses.equals(spanNearQuery.clauses);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result ^= clauses.hashCode();
    result += slop;
    int fac = 1 + (inOrder ? 8 : 4) + (collectPayloads ? 2 : 0);
    return fac * result;
  }
}
