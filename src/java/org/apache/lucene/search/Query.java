package org.apache.lucene.search;

/**
 * Copyright 2002-2004 The Apache Software Foundation
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

import java.util.HashSet;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;

/** The abstract base class for queries.
    <p>Instantiable subclasses are:
    <ul>
    <li> {@link TermQuery}
    <li> {@link MultiTermQuery}
    <li> {@link BooleanQuery}
    <li> {@link WildcardQuery}
    <li> {@link PhraseQuery}
    <li> {@link PrefixQuery}
    <li> {@link PhrasePrefixQuery}
    <li> {@link FuzzyQuery}
    <li> {@link RangeQuery}
    <li> {@link org.apache.lucene.search.spans.SpanQuery}
    </ul>
    <p>A parser for queries is contained in:
    <ul>
    <li>{@link org.apache.lucene.queryParser.QueryParser QueryParser}
    </ul>
*/
public abstract class Query implements java.io.Serializable, Cloneable {
  private float boost = 1.0f;                     // query boost factor

  /** Sets the boost for this query clause to <code>b</code>.  Documents
   * matching this clause will (in addition to the normal weightings) have
   * their score multiplied by <code>b</code>.
   */
  public void setBoost(float b) { boost = b; }

  /** Gets the boost for this clause.  Documents matching
   * this clause will (in addition to the normal weightings) have their score
   * multiplied by <code>b</code>.   The boost is 1.0 by default.
   */
  public float getBoost() { return boost; }

  /** Prints a query to a string, with <code>field</code> as the default field
   * for terms.  <p>The representation used is one that is supposed to be readable
   * by {@link org.apache.lucene.queryParser.QueryParser QueryParser}. However,
   * there are the following limitations:
   * <ul>
   *  <li>If the query was created by the parser, the printed
   *  representation may not be exactly what was parsed. For example,
   *  characters that need to be escaped will be represented without
   *  the required backslash.</li>
   * <li>Some of the more complicated queries (e.g. span queries)
   *  don't have a representation that can be parsed by QueryParser.</li>
   * </ul>
   */
  public abstract String toString(String field);

  /** Prints a query to a string. */
  public String toString() {
    return toString("");
  }

  /** Expert: Constructs an appropriate Weight implementation for this query.
   *
   * <p>Only implemented by primitive queries, which re-write to themselves.
   */
  protected Weight createWeight(Searcher searcher) {
    throw new UnsupportedOperationException();
  }

  /** Expert: Constructs an initializes a Weight for a top-level query. */
  public Weight weight(Searcher searcher)
    throws IOException {
    Query query = searcher.rewrite(this);
    Weight weight = query.createWeight(searcher);
    float sum = weight.sumOfSquaredWeights();
    float norm = getSimilarity(searcher).queryNorm(sum);
    weight.normalize(norm);
    return weight;
  }

  /** Expert: called to re-write queries into primitive queries. */
  public Query rewrite(IndexReader reader) throws IOException {
    return this;
  }

  /** Expert: called when re-writing queries under MultiSearcher.
   *
   * <p>Only implemented by derived queries, with no
   * {@link #createWeight(Searcher)} implementatation.
   */
  public Query combine(Query[] queries) {
    throw new UnsupportedOperationException();
  }


  /** Expert: merges the clauses of a set of BooleanQuery's into a single
   * BooleanQuery.
   *
   *<p>A utility for use by {@link #combine(Query[])} implementations.
   */
  public static Query mergeBooleanQueries(Query[] queries) {
    HashSet allClauses = new HashSet();
    for (int i = 0; i < queries.length; i++) {
      BooleanClause[] clauses = ((BooleanQuery)queries[i]).getClauses();
      for (int j = 0; j < clauses.length; j++) {
        allClauses.add(clauses[j]);
      }
    }

    BooleanQuery result = new BooleanQuery();
    Iterator i = allClauses.iterator();
    while (i.hasNext()) {
      result.add((BooleanClause)i.next());
    }
    return result;
  }

  /** Expert: Returns the Similarity implementation to be used for this query.
   * Subclasses may override this method to specify their own Similarity
   * implementation, perhaps one that delegates through that of the Searcher.
   * By default the Searcher's Similarity implementation is returned.*/
  public Similarity getSimilarity(Searcher searcher) {
    return searcher.getSimilarity();
  }

  /** Returns a clone of this query. */
  public Object clone() {
    try {
      return (Query)super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Clone not supported: " + e.getMessage());
    }
  }
}
