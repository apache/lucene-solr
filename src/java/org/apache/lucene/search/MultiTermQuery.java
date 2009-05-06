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

/**
 * A {@link Query} that matches documents containing a subset of terms provided
 * by a {@link FilteredTermEnum} enumeration.
 * <P>
 * <code>MultiTermQuery</code> is not designed to be used by itself. <BR>
 * The reason being that it is not intialized with a {@link FilteredTermEnum}
 * enumeration. A {@link FilteredTermEnum} enumeration needs to be provided.
 * <P>
 * For example, {@link WildcardQuery} and {@link FuzzyQuery} extend
 * <code>MultiTermQuery</code> to provide {@link WildcardTermEnum} and
 * {@link FuzzyTermEnum}, respectively.
 * 
 * The pattern Term may be null. A query that uses a null pattern Term should
 * override equals and hashcode.
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

  public boolean getConstantScoreRewrite() {
    return constantScoreRewrite;
  }

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
