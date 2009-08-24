package org.apache.lucene.search;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.index.IndexReader;

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
public class DisjunctionMaxQuery extends Query {

  /* The subqueries */
  private ArrayList disjuncts = new ArrayList();

  /* Multiple of the non-max disjunct scores added into our final score.  Non-zero values support tie-breaking. */
  private float tieBreakerMultiplier = 0.0f;

  /** Creates a new empty DisjunctionMaxQuery.  Use add() to add the subqueries.
   * @param tieBreakerMultiplier the score of each non-maximum disjunct for a document is multiplied by this weight
   *        and added into the final score.  If non-zero, the value should be small, on the order of 0.1, which says that
   *        10 occurrences of word in a lower-scored field that is also in a higher scored field is just as good as a unique
   *        word in the lower scored field (i.e., one that is not in any higher scored field.
   */
  public DisjunctionMaxQuery(float tieBreakerMultiplier) {
    this.tieBreakerMultiplier = tieBreakerMultiplier;
  }

  /**
   * Creates a new DisjunctionMaxQuery
   * @param disjuncts a Collection<Query> of all the disjuncts to add
   * @param tieBreakerMultiplier   the weight to give to each matching non-maximum disjunct
   */
  public DisjunctionMaxQuery(Collection disjuncts, float tieBreakerMultiplier) {
    this.tieBreakerMultiplier = tieBreakerMultiplier;
    add(disjuncts);
  }

  /** Add a subquery to this disjunction
   * @param query the disjunct added
   */
  public void add(Query query) {
    disjuncts.add(query);
  }

  /** Add a collection of disjuncts to this disjunction
   * via Iterable<Query>
   */
  public void add(Collection disjuncts) {
    this.disjuncts.addAll(disjuncts);
  }

  /** An Iterator<Query> over the disjuncts */
  public Iterator iterator() {
    return disjuncts.iterator();
  }

  /**
   * Expert: the Weight for DisjunctionMaxQuery, used to
   * normalize, score and explain these queries.
   *
   * <p>NOTE: this API and implementation is subject to
   * change suddenly in the next release.</p>
   */
  protected class DisjunctionMaxWeight extends Weight {
    /** The Similarity implementation. */
    protected Similarity similarity;

    /** The Weights for our subqueries, in 1-1 correspondence with disjuncts */
    protected ArrayList weights = new ArrayList();  // The Weight's for our subqueries, in 1-1 correspondence with disjuncts

    /* Construct the Weight for this Query searched by searcher.  Recursively construct subquery weights. */
    public DisjunctionMaxWeight(Searcher searcher) throws IOException {
      this.similarity = searcher.getSimilarity();
      for (Iterator iter = disjuncts.iterator(); iter.hasNext();) {
        weights.add(((Query) iter.next()).createWeight(searcher));
      }
    }

    /* Return our associated DisjunctionMaxQuery */
    public Query getQuery() { return DisjunctionMaxQuery.this; }

    /* Return our boost */
    public float getValue() { return getBoost(); }

    /* Compute the sub of squared weights of us applied to our subqueries.  Used for normalization. */
    public float sumOfSquaredWeights() throws IOException {
      float max = 0.0f, sum = 0.0f;
      for (Iterator iter = weights.iterator(); iter.hasNext();) {
        float sub = ((Weight) iter.next()).sumOfSquaredWeights();
        sum += sub;
        max = Math.max(max, sub);
        
      }
      float boost = getBoost();
      return (((sum - max) * tieBreakerMultiplier * tieBreakerMultiplier) + max) * boost * boost;
    }

    /* Apply the computed normalization factor to our subqueries */
    public void normalize(float norm) {
      norm *= getBoost();  // Incorporate our boost
      for (Iterator iter = weights.iterator(); iter.hasNext();) {
        ((Weight) iter.next()).normalize(norm);
      }
    }

    /* Create the scorer used to score our associated DisjunctionMaxQuery */
    public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder,
        boolean topScorer) throws IOException {
      Scorer[] scorers = new Scorer[weights.size()];
      int idx = 0;
      for (Iterator iter = weights.iterator(); iter.hasNext();) {
        Weight w = (Weight) iter.next();
        Scorer subScorer = w.scorer(reader, true, false);
        if (subScorer != null && subScorer.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          scorers[idx++] = subScorer;
        }
      }
      if (idx == 0) return null; // all scorers did not have documents
      DisjunctionMaxScorer result = new DisjunctionMaxScorer(tieBreakerMultiplier, similarity, scorers, idx);
      return result;
    }

    /* Explain the score we computed for doc */
    public Explanation explain(IndexReader reader, int doc) throws IOException {
      if (disjuncts.size() == 1) return ((Weight) weights.get(0)).explain(reader,doc);
      ComplexExplanation result = new ComplexExplanation();
      float max = 0.0f, sum = 0.0f;
      result.setDescription(tieBreakerMultiplier == 0.0f ? "max of:" : "max plus " + tieBreakerMultiplier + " times others of:");
      for (Iterator iter = weights.iterator(); iter.hasNext();) {
        Explanation e = ((Weight) iter.next()).explain(reader, doc);
        if (e.isMatch()) {
          result.setMatch(Boolean.TRUE);
          result.addDetail(e);
          sum += e.getValue();
          max = Math.max(max, e.getValue());
        }
      }
      result.setValue(max + (sum - max) * tieBreakerMultiplier);
      return result;
    }
    
  }  // end of DisjunctionMaxWeight inner class

  /* Create the Weight used to score us */
  public Weight createWeight(Searcher searcher) throws IOException {
    return new DisjunctionMaxWeight(searcher);
  }

  /** Optimize our representation and our subqueries representations
   * @param reader the IndexReader we query
   * @return an optimized copy of us (which may not be a copy if there is nothing to optimize) */
  public Query rewrite(IndexReader reader) throws IOException {
    int numDisjunctions = disjuncts.size();
    if (numDisjunctions == 1) {
      Query singleton = (Query) disjuncts.get(0);
      Query result = singleton.rewrite(reader);
      if (getBoost() != 1.0f) {
        if (result == singleton) result = (Query)result.clone();
        result.setBoost(getBoost() * result.getBoost());
      }
      return result;
    }
    DisjunctionMaxQuery clone = null;
    for (int i = 0 ; i < numDisjunctions; i++) {
      Query clause = (Query) disjuncts.get(i);
      Query rewrite = clause.rewrite(reader);
      if (rewrite != clause) {
        if (clone == null) clone = (DisjunctionMaxQuery)this.clone();
        clone.disjuncts.set(i, rewrite);
      }
    }
    if (clone != null) return clone;
    else return this;
  }

  /** Create a shallow copy of us -- used in rewriting if necessary
   * @return a copy of us (but reuse, don't copy, our subqueries) */
  public Object clone() {
    DisjunctionMaxQuery clone = (DisjunctionMaxQuery)super.clone();
    clone.disjuncts = (ArrayList)this.disjuncts.clone();
    return clone;
  }

  // inherit javadoc
  public void extractTerms(Set terms) {
    for (Iterator iter = disjuncts.iterator(); iter.hasNext();) {
      ((Query) iter.next()).extractTerms(terms);
    }
  }

  /** Prettyprint us.
   * @param field the field to which we are applied
   * @return a string that shows what we do, of the form "(disjunct1 | disjunct2 | ... | disjunctn)^boost"
   */
  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("(");
    int numDisjunctions = disjuncts.size();
    for (int i = 0 ; i < numDisjunctions; i++) {
      Query subquery = (Query) disjuncts.get(i);
      if (subquery instanceof BooleanQuery) {   // wrap sub-bools in parens
        buffer.append("(");
        buffer.append(subquery.toString(field));
        buffer.append(")");
      }
      else buffer.append(subquery.toString(field));
      if (i != numDisjunctions-1) buffer.append(" | ");
    }
    buffer.append(")");
    if (tieBreakerMultiplier != 0.0f) {
      buffer.append("~");
      buffer.append(tieBreakerMultiplier);
    }
    if (getBoost() != 1.0) {
      buffer.append("^");
      buffer.append(getBoost());
    }
    return buffer.toString();
  }

  /** Return true iff we represent the same query as o
   * @param o another object
   * @return true iff o is a DisjunctionMaxQuery with the same boost and the same subqueries, in the same order, as us
   */
  public boolean equals(Object o) {
    if (! (o instanceof DisjunctionMaxQuery) ) return false;
    DisjunctionMaxQuery other = (DisjunctionMaxQuery)o;
    return this.getBoost() == other.getBoost()
            && this.tieBreakerMultiplier == other.tieBreakerMultiplier
            && this.disjuncts.equals(other.disjuncts);
  }

  /** Compute a hash code for hashing us
   * @return the hash code
   */
  public int hashCode() {
    return Float.floatToIntBits(getBoost())
            + Float.floatToIntBits(tieBreakerMultiplier)
            + disjuncts.hashCode();
  }

}
