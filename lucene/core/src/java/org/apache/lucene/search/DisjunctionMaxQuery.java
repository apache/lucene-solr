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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * A query that generates the union of documents produced by its subqueries, and that scores each document with the maximum
 * score for that document as produced by any subquery, plus a tie breaking increment for any additional matching subqueries.
 * This is useful when searching for a word in multiple fields with different boost factors (so that the fields cannot be
 * combined equivalently into a single search field).  We want the primary score to be the one associated with the highest boost,
 * not the sum of the field scores (as BooleanQuery would give).
 * If the query is "albino elephant" this ensures that "albino" matching one field and "elephant" matching
 * another gets a higher score than "albino" matching both fields.
 * To get this result, use both BooleanQuery and DisjunctionMaxQuery:  for each term a DisjunctionMaxQuery searches for it in
 * each field, while the set of these DisjunctionMaxQuery's is combined into a BooleanQuery.
 * The tie breaker capability allows results that include the same term in multiple fields to be judged better than results that
 * include this term in only the best of those multiple fields, without confusing this with the better case of two different terms
 * in the multiple fields.
 */
public final class DisjunctionMaxQuery extends Query implements Iterable<Query> {

  /* The subqueries */
  private final Query[] disjuncts;

  /* Multiple of the non-max disjunct scores added into our final score.  Non-zero values support tie-breaking. */
  private final float tieBreakerMultiplier;

  /**
   * Creates a new DisjunctionMaxQuery
   * @param disjuncts a {@code Collection<Query>} of all the disjuncts to add
   * @param tieBreakerMultiplier  the score of each non-maximum disjunct for a document is multiplied by this weight
   *        and added into the final score.  If non-zero, the value should be small, on the order of 0.1, which says that
   *        10 occurrences of word in a lower-scored field that is also in a higher scored field is just as good as a unique
   *        word in the lower scored field (i.e., one that is not in any higher scored field.
   */
  public DisjunctionMaxQuery(Collection<Query> disjuncts, float tieBreakerMultiplier) {
    Objects.requireNonNull(disjuncts, "Collection of Querys must not be null");
    this.tieBreakerMultiplier = tieBreakerMultiplier;
    this.disjuncts = disjuncts.toArray(new Query[disjuncts.size()]);
  }

  /** @return An {@code Iterator<Query>} over the disjuncts */
  @Override
  public Iterator<Query> iterator() {
    return getDisjuncts().iterator();
  }
  
  /**
   * @return the disjuncts.
   */
  public List<Query> getDisjuncts() {
    return Collections.unmodifiableList(Arrays.asList(disjuncts));
  }

  /**
   * @return tie breaker value for multiple matches.
   */
  public float getTieBreakerMultiplier() {
    return tieBreakerMultiplier;
  }

  /**
   * Expert: the Weight for DisjunctionMaxQuery, used to
   * normalize, score and explain these queries.
   *
   * <p>NOTE: this API and implementation is subject to
   * change suddenly in the next release.</p>
   */
  protected class DisjunctionMaxWeight extends Weight {

    /** The Weights for our subqueries, in 1-1 correspondence with disjuncts */
    protected final ArrayList<Weight> weights = new ArrayList<>();  // The Weight's for our subqueries, in 1-1 correspondence with disjuncts
    private final boolean needsScores;

    /** Construct the Weight for this Query searched by searcher.  Recursively construct subquery weights. */
    public DisjunctionMaxWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
      super(DisjunctionMaxQuery.this);
      for (Query disjunctQuery : disjuncts) {
        weights.add(searcher.createWeight(disjunctQuery, needsScores, boost));
      }
      this.needsScores = needsScores;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (Weight weight : weights) {
        weight.extractTerms(terms);
      }
    }

    /** Create the scorer used to score our associated DisjunctionMaxQuery */
    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      List<Scorer> scorers = new ArrayList<>();
      for (Weight w : weights) {
        // we will advance() subscorers
        Scorer subScorer = w.scorer(context);
        if (subScorer != null) {
          scorers.add(subScorer);
        }
      }
      if (scorers.isEmpty()) {
        // no sub-scorers had any documents
        return null;
      } else if (scorers.size() == 1) {
        // only one sub-scorer in this segment
        return scorers.get(0);
      } else {
        return new DisjunctionMaxScorer(this, tieBreakerMultiplier, scorers, needsScores);
      }
    }

    /** Explain the score we computed for doc */
    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      boolean match = false;
      float max = Float.NEGATIVE_INFINITY, sum = 0.0f;
      List<Explanation> subs = new ArrayList<>();
      for (Weight wt : weights) {
        Explanation e = wt.explain(context, doc);
        if (e.isMatch()) {
          match = true;
          subs.add(e);
          sum += e.getValue();
          max = Math.max(max, e.getValue());
        }
      }
      if (match) {
        final float score = max + (sum - max) * tieBreakerMultiplier;
        final String desc = tieBreakerMultiplier == 0.0f ? "max of:" : "max plus " + tieBreakerMultiplier + " times others of:";
        return Explanation.match(score, desc, subs);
      } else {
        return Explanation.noMatch("No matching clause");
      }
    }
    
  }  // end of DisjunctionMaxWeight inner class

  /** Create the Weight used to score us */
  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
    return new DisjunctionMaxWeight(searcher, needsScores, boost);
  }

  /** Optimize our representation and our subqueries representations
   * @param reader the IndexReader we query
   * @return an optimized copy of us (which may not be a copy if there is nothing to optimize) */
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (disjuncts.length == 1) {
      return disjuncts[0];
    }

    boolean actuallyRewritten = false;
    List<Query> rewrittenDisjuncts = new ArrayList<>();
    for (Query sub : disjuncts) {
      Query rewrittenSub = sub.rewrite(reader);
      actuallyRewritten |= rewrittenSub != sub;
      rewrittenDisjuncts.add(rewrittenSub);
    }

    if (actuallyRewritten) {
      return new DisjunctionMaxQuery(rewrittenDisjuncts, tieBreakerMultiplier);
    }

    return super.rewrite(reader);
  }

  /** Prettyprint us.
   * @param field the field to which we are applied
   * @return a string that shows what we do, of the form "(disjunct1 | disjunct2 | ... | disjunctn)^boost"
   */
  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("(");
    for (int i = 0 ; i < disjuncts.length; i++) {
      Query subquery = disjuncts[i];
      if (subquery instanceof BooleanQuery) {   // wrap sub-bools in parens
        buffer.append("(");
        buffer.append(subquery.toString(field));
        buffer.append(")");
      }
      else buffer.append(subquery.toString(field));
      if (i != disjuncts.length-1) buffer.append(" | ");
    }
    buffer.append(")");
    if (tieBreakerMultiplier != 0.0f) {
      buffer.append("~");
      buffer.append(tieBreakerMultiplier);
    }
    return buffer.toString();
  }

  /** Return true iff we represent the same query as o
   * @param other another object
   * @return true iff o is a DisjunctionMaxQuery with the same boost and the same subqueries, in the same order, as us
   */
  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(DisjunctionMaxQuery other) {
    return tieBreakerMultiplier == other.tieBreakerMultiplier && 
           Arrays.equals(disjuncts, other.disjuncts);
  }

  /** Compute a hash code for hashing us
   * @return the hash code
   */
  @Override
  public int hashCode() {
    int h = classHash();
    h = 31 * h + Float.floatToIntBits(tieBreakerMultiplier);
    h = 31 * h + Arrays.hashCode(disjuncts);
    return h;
  }


}
