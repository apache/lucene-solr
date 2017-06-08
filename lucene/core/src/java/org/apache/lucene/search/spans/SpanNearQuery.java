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
package org.apache.lucene.search.spans;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/** Matches spans which are near one another.  One can specify <i>slop</i>, the
 * maximum number of intervening unmatched positions, as well as whether
 * matches are required to be in-order.
 */
public class SpanNearQuery extends SpanQuery implements Cloneable {

  /**
   * A builder for SpanNearQueries
   */
  public static class Builder {
    private final boolean ordered;
    private final String field;
    private final List<SpanQuery> clauses = new LinkedList<>();
    private int slop;
    private int nonMatchSlop = -1;

    /**
     * Construct a new builder
     * @param field the field to search in
     * @param ordered whether or not clauses must be in-order to match
     */
    public Builder(String field, boolean ordered) {
      this.field = field;
      this.ordered = ordered;
    }

    /**
     * Add a new clause
     */
    public Builder addClause(SpanQuery clause) {
      if (Objects.equals(clause.getField(), field) == false)
        throw new IllegalArgumentException("Cannot add clause " + clause + " to SpanNearQuery for field " + field);
      this.clauses.add(clause);
      return this;
    }

    /**
     * Add a gap after the previous clause of a defined width
     */
    public Builder addGap(int width) {
      if (!ordered)
        throw new IllegalArgumentException("Gaps can only be added to ordered near queries");
      this.clauses.add(new SpanGapQuery(field, width));
      return this;
    }

    /**
     * Set the slop for this query
     */
    public Builder setSlop(int slop) {
      this.slop = slop;
      return this;
    }

    /**
     * Set the non match slop for this query
     */
    public Builder setNonMatchSlop(int nonMatchSlop) {
      this.nonMatchSlop = nonMatchSlop;
      return this;
    }

    /**
     * Build the query
     */
    public SpanNearQuery build() {
      return (nonMatchSlop == -1)
       ? new SpanNearQuery(clauses.toArray(new SpanQuery[clauses.size()]), slop, ordered)
       : new SpanNearQuery(clauses.toArray(new SpanQuery[clauses.size()]), slop, ordered, nonMatchSlop);
    }

  }

  /**
   * Returns a {@link Builder} for an ordered query on a particular field
   */
  public static Builder newOrderedNearQuery(String field) {
    return new Builder(field, true);
  }

  /**
   * Returns a {@link Builder} for an unordered query on a particular field
   */
  public static Builder newUnorderedNearQuery(String field) {
    return new Builder(field, false);
  }

  protected List<SpanQuery> clauses;
  protected int slop;
  protected boolean inOrder;
  protected int nonMatchSlop;

  protected String field;

  /**
   * Construct a SpanNearQuery.
   * See {@link SpanNearQuery#SpanNearQuery(SpanQuery[], int, boolean, int)}
   * for the first three parameters.
   * This will use <code>Integer.MAX_VALUE-1</code> for the non matching slop.
   */
  public SpanNearQuery(SpanQuery[] clausesIn, int slop, boolean inOrder) {
    // Integer.MAX_VALUE causes overflow in sloppyFreq which adds 1.
    this(clausesIn, slop, inOrder, Integer.MAX_VALUE-1);
  }

  /** Construct a SpanNearQuery.  Matches spans matching a span from each
   * clause, with up to <code>slop</code> total unmatched positions between
   * them.
   * <br>When <code>inOrder</code> is true, the spans from each clause
   * must be in the same order as in <code>clauses</code> and must be non-overlapping.
   * <br>When <code>inOrder</code> is false, the spans from each clause
   * need not be ordered and may overlap.
   * @param clausesIn the clauses to find near each other, in the same field, at least 2.
   * @param slop The allowed slop. This should be non negative and at most Integer.Max_VALUE-1.
   * @param inOrder true if order is important
   * @param nonMatchSlop
   *   The distance for determining the slop factor to be used for non matching
   *   occurrences. This is used for scoring by {@link SpansTreeQuery}, and it
   *   should not be smaller than <code>slop</code>.
   *   <br>
   *   Smaller values of <code>nonMatchSlop</code> will increase the
   *   score contribution of non matching occurrences
   *   via {@link org.apache.lucene.search.similarities.Similarity.SimScorer#computeSlopFactor}.
   *   <br>
   *   Smaller values may lead to a scoring inconsistency between two span near queries
   *   that only differ in the allowed slop.
   *   For example consider query A with a smaller allowed slop and query B with a larger one.
   *   For query B there can be more matches, and these should increase the score of B
   *   when compared to the score of A.
   *   For each extra match at B, the non matching score for query A should be lower than
   *   the matching score for query B.
   *   <br>
   *   To have consistent scoring between two such queries, choose
   *   a non matching scoring distance that is larger than the largest allowed distance,
   *   and provide that to both queries.
   */
  public SpanNearQuery(SpanQuery[] clausesIn, int slop, boolean inOrder, int nonMatchSlop) {
    this.clauses = new ArrayList<>(clausesIn.length);
    for (SpanQuery clause : clausesIn) {
      if (this.field == null) {                               // check field
        this.field = clause.getField();
      } else if (clause.getField() != null && !clause.getField().equals(field)) {
        throw new IllegalArgumentException("Clauses must have same field.");
      }
      this.clauses.add(clause);
    }
    if (nonMatchSlop != -1) {
      if (nonMatchSlop < slop) {
        throw new IllegalArgumentException("nonMatchSlop < slop: " + nonMatchSlop + " < " + slop);
      }
    }
    this.inOrder = inOrder;
    this.slop = slop;
    this.nonMatchSlop = nonMatchSlop;
  }

  /** Return the clauses whose spans are matched. */
  public SpanQuery[] getClauses() {
    return clauses.toArray(new SpanQuery[clauses.size()]);
  }

  /** Return the maximum number of intervening unmatched positions permitted.*/
  public int getSlop() { return slop; }

  /** Return true if matches are required to be in-order.*/
  public boolean isInOrder() { return inOrder; }

  /** Return the slop used for scoring non matching occurrences. */
  public int getNonMatchSlop() { return nonMatchSlop; }

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
    buffer.append(", ");
    buffer.append(nonMatchSlop);
    buffer.append(")");
    return buffer.toString();
  }

  @Override
  public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    List<SpanWeight> subWeights = new ArrayList<>();
    for (SpanQuery q : clauses) {
      subWeights.add(q.createWeight(searcher, needsScores, boost));
    }
    return new SpanNearWeight(subWeights, searcher, needsScores ? getTermContexts(subWeights) : null, boost);
  }

  public class SpanNearWeight extends SpanWeight {

    final List<SpanWeight> subWeights;

    public SpanNearWeight(List<SpanWeight> subWeights, IndexSearcher searcher, Map<Term, TermContext> terms, float boost) throws IOException {
      super(SpanNearQuery.this, searcher, terms, boost);
      this.subWeights = subWeights;
    }

    @Override
    public void extractTermContexts(Map<Term, TermContext> contexts) {
      for (SpanWeight w : subWeights) {
        w.extractTermContexts(contexts);
      }
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings) throws IOException {

      Terms terms = context.reader().terms(field);
      if (terms == null) {
        return null; // field does not exist
      }

      ArrayList<Spans> subSpans = new ArrayList<>(clauses.size());
      for (SpanWeight w : subWeights) {
        Spans subSpan = w.getSpans(context, requiredPostings);
        if (subSpan != null) {
          subSpans.add(subSpan);
        } else {
          return null; // all required
        }
      }

      // all NearSpans require at least two subSpans
      return (!inOrder) ? new NearSpansUnordered(slop, subSpans, getSimScorer(context))
          : new NearSpansOrdered(slop, subSpans, getSimScorer(context));
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
    boolean actuallyRewritten = false;
    List<SpanQuery> rewrittenClauses = new ArrayList<>();
    for (int i = 0 ; i < clauses.size(); i++) {
      SpanQuery c = clauses.get(i);
      SpanQuery query = (SpanQuery) c.rewrite(reader);
      actuallyRewritten |= query != c;
      rewrittenClauses.add(query);
    }
    if (actuallyRewritten) {
      try {
        SpanNearQuery rewritten = (SpanNearQuery) clone();
        rewritten.clauses = rewrittenClauses;
        return rewritten;
      } catch (CloneNotSupportedException e) {
        throw new AssertionError(e);
      }
    }
    return super.rewrite(reader);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }

  private boolean equalsTo(SpanNearQuery other) {
    return inOrder == other.inOrder &&
           slop == other.slop &&
           nonMatchSlop == other.nonMatchSlop &&
           clauses.equals(other.clauses);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result ^= clauses.hashCode();
    result += slop;
    result ^= 4 * nonMatchSlop;
    int fac = 1 + (inOrder ? 8 : 4);
    return fac * result;
  }

  private static class SpanGapQuery extends SpanQuery {

    private final String field;
    private final int width;

    public SpanGapQuery(String field, int width) {
      this.field = field;
      this.width = width;
    }

    @Override
    public String getField() {
      return field;
    }

    @Override
    public String toString(String field) {
      return "SpanGap(" + field + ":" + width + ")";
    }

    @Override
    public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
      return new SpanGapWeight(searcher, boost);
    }

    private class SpanGapWeight extends SpanWeight {

      SpanGapWeight(IndexSearcher searcher, float boost) throws IOException {
        super(SpanGapQuery.this, searcher, null, boost);
      }

      @Override
      public void extractTermContexts(Map<Term, TermContext> contexts) {

      }

      @Override
      public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) throws IOException {
        return new GapSpans(width);
      }

      @Override
      public void extractTerms(Set<Term> terms) {

      }
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(SpanGapQuery other) {
      return width == other.width &&
             field.equals(other.field);
    }

    @Override
    public int hashCode() {
      int result = classHash();
      result -= 7 * width;
      return result * 15 - field.hashCode();
    }

  }

  static class GapSpans extends Spans {

    int doc = -1;
    int pos = -1;
    final int width;

    GapSpans(int width) {
      this.width = width;
    }

    @Override
    public int nextStartPosition() throws IOException {
      return ++pos;
    }

    public int skipToPosition(int position) throws IOException {
      return pos = position;
    }

    @Override
    public int startPosition() {
      return pos;
    }

    @Override
    public int endPosition() {
      return pos + width;
    }

    @Override
    public int width() {
      return width;
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {

    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      pos = -1;
      return ++doc;
    }

    @Override
    public int advance(int target) throws IOException {
      pos = -1;
      return doc = target;
    }

    @Override
    public long cost() {
      return 0;
    }

    @Override
    public float positionsCost() {
      return 0;
    }
  }

}
