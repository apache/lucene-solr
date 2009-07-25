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

import java.text.Collator;
import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * A Query that matches documents within an exclusive range of terms.
 *
 * <p>This query matches the documents looking for terms that fall into the
 * supplied range according to {@link Term#compareTo(Term)}. It is not intended
 * for numerical ranges, use {@link NumericRangeQuery} instead.
 *
 * <p>This query uses {@linkplain
 * MultiTermQuery#SCORING_BOOLEAN_QUERY_REWRITE}.  If you
 * want to change this, use the new {@link TermRangeQuery}
 * instead.
 *
 * @deprecated Use {@link TermRangeQuery} for term ranges or
 * {@link NumericRangeQuery} for numeric ranges instead.
 * This class will be removed in Lucene 3.0.
 */
public class RangeQuery extends Query {
  private final TermRangeQuery delegate;

  /** Constructs a query selecting all terms greater than
   * <code>lowerTerm</code> but less than <code>upperTerm</code>.
   * There must be at least one term and either term may be null,
   * in which case there is no bound on that side, but if there are
   * two terms, both terms <b>must</b> be for the same field.
   *
   * @param lowerTerm The Term at the lower end of the range
   * @param upperTerm The Term at the upper end of the range
   * @param inclusive If true, both <code>lowerTerm</code> and
   *  <code>upperTerm</code> will themselves be included in the range.
   */
  public RangeQuery(Term lowerTerm, Term upperTerm, boolean inclusive) {
    this(lowerTerm, upperTerm, inclusive, null);
  }

  /** Constructs a query selecting all terms greater than
   * <code>lowerTerm</code> but less than <code>upperTerm</code>.
   * There must be at least one term and either term may be null,
   * in which case there is no bound on that side, but if there are
   * two terms, both terms <b>must</b> be for the same field.
   * <p>
   * If <code>collator</code> is not null, it will be used to decide whether
   * index terms are within the given range, rather than using the Unicode code
   * point order in which index terms are stored.
   * <p>
   * <strong>WARNING:</strong> Using this constructor and supplying a non-null
   * value in the <code>collator</code> parameter will cause every single 
   * index Term in the Field referenced by lowerTerm and/or upperTerm to be
   * examined.  Depending on the number of index Terms in this Field, the 
   * operation could be very slow.
   *
   * @param lowerTerm The Term at the lower end of the range
   * @param upperTerm The Term at the upper end of the range
   * @param inclusive If true, both <code>lowerTerm</code> and
   *  <code>upperTerm</code> will themselves be included in the range.
   * @param collator The collator to use to collate index Terms, to determine
   *  their membership in the range bounded by <code>lowerTerm</code> and
   *  <code>upperTerm</code>.
   */
  public RangeQuery(Term lowerTerm, Term upperTerm, boolean inclusive, Collator collator) {
    if (lowerTerm == null && upperTerm == null)
      throw new IllegalArgumentException("At least one term must be non-null");
    if (lowerTerm != null && upperTerm != null && lowerTerm.field() != upperTerm.field())
      throw new IllegalArgumentException("Both terms must have the same field");
      
    delegate = new TermRangeQuery(
      (lowerTerm == null) ? upperTerm.field() : lowerTerm.field(), 
      (lowerTerm == null) ? null : lowerTerm.text(), 
      (upperTerm == null) ? null : upperTerm.text(), 
      inclusive, inclusive,
      collator
    );
    delegate.setRewriteMethod(TermRangeQuery.SCORING_BOOLEAN_QUERY_REWRITE);
  }
  
  public void setBoost(float b) {
    super.setBoost(b);
    delegate.setBoost(b);
  }

  public Query rewrite(IndexReader reader) throws IOException {
    return delegate.rewrite(reader);
  }

  /** Returns the field name for this query */
  public String getField() {
    return delegate.getField();
  }

  /** Returns the lower term of this range query. */
  public Term getLowerTerm() {
    final String term = delegate.getLowerTerm();
    return (term == null) ? null : new Term(getField(), term);
  }

  /** Returns the upper term of this range query. */
  public Term getUpperTerm() {
    final String term = delegate.getUpperTerm();
    return (term == null) ? null : new Term(getField(), term);
  }

  /** Returns <code>true</code> if the range query is inclusive */
  public boolean isInclusive() {
    return delegate.includesLower() && delegate.includesUpper();
  }

  /** Returns the collator used to determine range inclusion, if any. */
  public Collator getCollator() {
    return delegate.getCollator();
  }

  /** Prints a user-readable version of this query. */
  public String toString(String field) {
    return delegate.toString(field);
  }

  /** Returns true iff <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RangeQuery)) return false;

    final RangeQuery other = (RangeQuery) o;
    return this.delegate.equals(other.delegate);
  }

  /** Returns a hash code value for this object.*/
  public int hashCode() {
    return delegate.hashCode();
  }
}
