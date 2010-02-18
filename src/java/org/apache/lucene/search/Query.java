package org.apache.lucene.search;

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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
    <li> {@link MultiPhraseQuery}
    <li> {@link FuzzyQuery}
    <li> {@link TermRangeQuery}
    <li> {@link NumericRangeQuery}
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

  /** Prints a query to a string, with <code>field</code> assumed to be the 
   * default field and omitted.
   * <p>The representation used is one that is supposed to be readable
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

  /**
   * Expert: Constructs an appropriate Weight implementation for this query.
   * 
   * <p>
   * Only implemented by primitive queries, which re-write to themselves.
   */
  public Weight createWeight(Searcher searcher) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Expert: Constructs and initializes a Weight for a top-level query.
   */
  public Weight weight(Searcher searcher) throws IOException {
    Query query = searcher.rewrite(this);
    Weight weight = query.createWeight(searcher);
    float sum = weight.sumOfSquaredWeights();
    float norm = getSimilarity(searcher).queryNorm(sum);
    if (Float.isInfinite(norm) || Float.isNaN(norm))
      norm = 1.0f;
    weight.normalize(norm);
    return weight;
  }
  

  /** Expert: called to re-write queries into primitive queries. For example,
   * a PrefixQuery will be rewritten into a BooleanQuery that consists
   * of TermQuerys.
   */
  public Query rewrite(IndexReader reader) throws IOException {
    return this;
  }
  

  /** Expert: called when re-writing queries under MultiSearcher.
   *
   * Create a single query suitable for use by all subsearchers (in 1-1
   * correspondence with queries). This is an optimization of the OR of
   * all queries. We handle the common optimization cases of equal
   * queries and overlapping clauses of boolean OR queries (as generated
   * by MultiTermQuery.rewrite()).
   * Be careful overriding this method as queries[0] determines which
   * method will be called and is not necessarily of the same type as
   * the other queries.
  */
  public Query combine(Query[] queries) {
    HashSet uniques = new HashSet();
    for (int i = 0; i < queries.length; i++) {
      Query query = queries[i];
      BooleanClause[] clauses = null;
      // check if we can split the query into clauses
      boolean splittable = (query instanceof BooleanQuery);
      if(splittable){
        BooleanQuery bq = (BooleanQuery) query;
        splittable = bq.isCoordDisabled();
        clauses = bq.getClauses();
        for (int j = 0; splittable && j < clauses.length; j++) {
          splittable = (clauses[j].getOccur() == BooleanClause.Occur.SHOULD);
        }
      }
      if(splittable){
        for (int j = 0; j < clauses.length; j++) {
          uniques.add(clauses[j].getQuery());
        }
      } else {
        uniques.add(query);
      }
    }
    // optimization: if we have just one query, just return it
    if(uniques.size() == 1){
        return (Query)uniques.iterator().next();
    }
    Iterator it = uniques.iterator();
    BooleanQuery result = new BooleanQuery(true);
    while (it.hasNext())
      result.add((Query) it.next(), BooleanClause.Occur.SHOULD);
    return result;
  }
  

  /**
   * Expert: adds all terms occurring in this query to the terms set. Only
   * works if this query is in its {@link #rewrite rewritten} form.
   * 
   * @throws UnsupportedOperationException if this query is not yet rewritten
   */
  public void extractTerms(Set terms) {
    // needs to be implemented by query subclasses
    throw new UnsupportedOperationException();
  }
  


  /** Expert: merges the clauses of a set of BooleanQuery's into a single
   * BooleanQuery.
   *
   *<p>A utility for use by {@link #combine(Query[])} implementations.
   */
  public static Query mergeBooleanQueries(BooleanQuery[] queries) {
    HashSet allClauses = new HashSet();
    for (int i = 0; i < queries.length; i++) {
      BooleanClause[] clauses = queries[i].getClauses();
      for (int j = 0; j < clauses.length; j++) {
        allClauses.add(clauses[j]);
      }
    }

    boolean coordDisabled =
      queries.length==0? false : queries[0].isCoordDisabled();
    BooleanQuery result = new BooleanQuery(coordDisabled);
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
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Clone not supported: " + e.getMessage());
    }
  }

  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Float.floatToIntBits(boost);
    return result;
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Query other = (Query) obj;
    if (Float.floatToIntBits(boost) != Float.floatToIntBits(other.boost))
      return false;
    return true;
  }
}
