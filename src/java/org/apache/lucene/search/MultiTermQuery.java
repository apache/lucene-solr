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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.ToStringUtils;
import org.apache.lucene.queryParser.QueryParser; // for javadoc

/**
 * An abstract {@link Query} that matches documents
 * containing a subset of terms provided by a {@link
 * FilteredTermEnum} enumeration.
 *
 * <p>This query cannot be used directly; you must subclass
 * it and define {@link #getEnum} to provide a {@link
 * FilteredTermEnum} that iterates through the terms to be
 * matched.
 *
 * <p><b>NOTE</b>: if {@link #setConstantScoreRewrite} is
 * false, you may encounter a {@link
 * BooleanQuery.TooManyClauses} exception during searching,
 * which happens when the number of terms to be searched
 * exceeds {@link BooleanQuery#getMaxClauseCount()}.
 * Setting {@link #setConstantScoreRewrite} to false
 * prevents this.
 *
 * Note that {@link QueryParser} by default produces
 * MultiTermQueries with {@link #setConstantScoreRewrite}
 * true.
 */
public abstract class MultiTermQuery extends Query {
  /* @deprecated move to sub class */
  protected Term term;
  protected boolean constantScoreRewrite = false;
  transient int numberOfTerms = 0;

  /** Constructs a query for terms matching <code>term</code>. */
  public MultiTermQuery(Term term) {
    this.term = term;
  }

  /**
   * Constructs a query matching terms that cannot be represented with a single
   * Term.
   */
  public MultiTermQuery() {
  }

  /**
   * Returns the pattern term.
   * @deprecated check sub class for possible term access - getTerm does not
   * make sense for all MultiTermQuerys and will be removed.
   */
  public Term getTerm() {
    return term;
  }

  /** Construct the enumeration to be used, expanding the pattern term. */
  protected abstract FilteredTermEnum getEnum(IndexReader reader)
      throws IOException;

  /**
   * Expert: Return the number of unique terms visited during execution of the query.
   * If there are many of them, you may consider using another query type
   * or optimize your total term count in index.
   * <p>This method is not thread safe, be sure to only call it when no query is running!
   * If you re-use the same query instance for another
   * search, be sure to first reset the term counter
   * with {@link #clearTotalNumberOfTerms}.
   * <p>On optimized indexes / no MultiReaders, you get the correct number of
   * unique terms for the whole index. Use this number to compare different queries.
   * For non-optimized indexes this number can also be achived in
   * non-constant-score mode. In constant-score mode you get the total number of
   * terms seeked for all segments / sub-readers.
   * @see #clearTotalNumberOfTerms
   */
  public int getTotalNumberOfTerms() {
    return numberOfTerms;
  }
  
  /**
   * Expert: Resets the counting of unique terms.
   * Do this before executing the query/filter.
   * @see #getTotalNumberOfTerms
   */
  public void clearTotalNumberOfTerms() {
    numberOfTerms = 0;
  }
  
  protected Filter getFilter() {
    return new MultiTermQueryWrapperFilter(this);
  }

  public Query rewrite(IndexReader reader) throws IOException {
    if (!constantScoreRewrite) {
      FilteredTermEnum enumerator = getEnum(reader);
      BooleanQuery query = new BooleanQuery(true);
      try {
        do {
          Term t = enumerator.term();
          if (t != null) {
            numberOfTerms++;
            TermQuery tq = new TermQuery(t); // found a match
            tq.setBoost(getBoost() * enumerator.difference()); // set the boost
            query.add(tq, BooleanClause.Occur.SHOULD); // add to query
          }
        } while (enumerator.next());
      } finally {
        enumerator.close();
      }
      return query;
    } else {
      Query query = new ConstantScoreQuery(getFilter());
      query.setBoost(getBoost());
      return query;
    }
  }


  /* Prints a user-readable version of this query.
   * Implemented for back compat in case MultiTermQuery
   * subclasses do no implement.
   */
  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    if (term != null) {
      if (!term.field().equals(field)) {
        buffer.append(term.field());
        buffer.append(":");
      }
      buffer.append(term.text());
    } else {
      buffer.append("termPattern:unknown");
    }
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  /**
   * @see #setConstantScoreRewrite
   */
  public boolean getConstantScoreRewrite() {
    return constantScoreRewrite;
  }

  /**
   * This method determines what method is used during searching:
   * <ul>
   *
   *   <li> When constantScoreRewrite is <code>false</code>
   *   (the default), the query is rewritten to {@link
   *   BooleanQuery} with one clause for each term in the
   *   range.  If the the number of terms in the range
   *   exceeds {@link BooleanQuery#getMaxClauseCount()}, a
   *   {@link BooleanQuery.TooManyClauses} exception will be
   *   thrown during searching.  This mode may also give
   *   worse performance when the number of terms is large,
   *   and/or the number of matching documents is large.
   *
   *   <li> When constantScoreRewrite is <code>true</code>,
   *   the query is first rewritten to a filter.  Matching
   *   documents will identical scores, equal to this
   *   query's boost.
   * </ul>
   */
  public void setConstantScoreRewrite(boolean constantScoreRewrite) {
    this.constantScoreRewrite = constantScoreRewrite;
  }

  //@Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Float.floatToIntBits(getBoost());
    result = prime * result + (constantScoreRewrite ? 1231 : 1237);
    return result;
  }

  //@Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MultiTermQuery other = (MultiTermQuery) obj;
    if (Float.floatToIntBits(getBoost()) != Float.floatToIntBits(other.getBoost()))
      return false;
    if (constantScoreRewrite != other.constantScoreRewrite)
      return false;
    return true;
  }
 
}
