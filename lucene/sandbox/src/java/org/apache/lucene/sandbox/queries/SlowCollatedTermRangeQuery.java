package org.apache.lucene.sandbox.queries;

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
import java.text.Collator;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MultiTermQuery; // javadoc
import org.apache.lucene.search.NumericRangeQuery; // javadoc
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.ToStringUtils;

/**
 * A Query that matches documents within an range of terms.
 *
 * <p>This query matches the documents looking for terms that fall into the
 * supplied range according to {@link
 * String#compareTo(String)}, unless a <code>Collator</code> is provided. It is not intended
 * for numerical ranges; use {@link NumericRangeQuery} instead.
 *
 * <p>This query uses the {@link
 * MultiTermQuery#CONSTANT_SCORE_AUTO_REWRITE_DEFAULT}
 * rewrite method.
 * @deprecated Index collation keys with CollationKeyAnalyzer or ICUCollationKeyAnalyzer instead.
 * This class will be removed in Lucene 5.0
 */
@Deprecated
public class SlowCollatedTermRangeQuery extends MultiTermQuery {
  private String lowerTerm;
  private String upperTerm;
  private boolean includeLower;
  private boolean includeUpper;
  private Collator collator;

  /** Constructs a query selecting all terms greater/equal than
   * <code>lowerTerm</code> but less/equal than <code>upperTerm</code>.
   * <p>
   * If an endpoint is null, it is said 
   * to be "open". Either or both endpoints may be open.  Open endpoints may not 
   * be exclusive (you can't select all but the first or last term without 
   * explicitly specifying the term to exclude.)
   * <p>
   *
   * @param lowerTerm The Term text at the lower end of the range
   * @param upperTerm The Term text at the upper end of the range
   * @param includeLower
   *          If true, the <code>lowerTerm</code> is
   *          included in the range.
   * @param includeUpper
   *          If true, the <code>upperTerm</code> is
   *          included in the range.
   * @param collator The collator to use to collate index Terms, to determine
   *  their membership in the range bounded by <code>lowerTerm</code> and
   *  <code>upperTerm</code>.
   */
  public SlowCollatedTermRangeQuery(String field, String lowerTerm, String upperTerm, 
      boolean includeLower, boolean includeUpper,  Collator collator) {
    super(field);
    this.lowerTerm = lowerTerm;
    this.upperTerm = upperTerm;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
    this.collator = collator;
  }

  /** Returns the lower value of this range query */
  public String getLowerTerm() { return lowerTerm; }

  /** Returns the upper value of this range query */
  public String getUpperTerm() { return upperTerm; }
  
  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesLower() { return includeLower; }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesUpper() { return includeUpper; }

  /** Returns the collator used to determine range inclusion */
  public Collator getCollator() { return collator; }
  
  @Override
  protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
    if (lowerTerm != null && upperTerm != null && collator.compare(lowerTerm, upperTerm) > 0) {
      return TermsEnum.EMPTY;
    }
    
    TermsEnum tenum = terms.iterator(null);

    if (lowerTerm == null && upperTerm == null) {
      return tenum;
    }
    return new SlowCollatedTermRangeTermsEnum(tenum,
        lowerTerm, upperTerm, includeLower, includeUpper, collator);
  }

  /** @deprecated Use {@link #getField()} instead. */
  @Deprecated
  public String field() {
    return getField();
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
      StringBuilder buffer = new StringBuilder();
      if (!getField().equals(field)) {
          buffer.append(getField());
          buffer.append(":");
      }
      buffer.append(includeLower ? '[' : '{');
      buffer.append(lowerTerm != null ? lowerTerm : "*");
      buffer.append(" TO ");
      buffer.append(upperTerm != null ? upperTerm : "*");
      buffer.append(includeUpper ? ']' : '}');
      buffer.append(ToStringUtils.boost(getBoost()));
      return buffer.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((collator == null) ? 0 : collator.hashCode());
    result = prime * result + (includeLower ? 1231 : 1237);
    result = prime * result + (includeUpper ? 1231 : 1237);
    result = prime * result + ((lowerTerm == null) ? 0 : lowerTerm.hashCode());
    result = prime * result + ((upperTerm == null) ? 0 : upperTerm.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    SlowCollatedTermRangeQuery other = (SlowCollatedTermRangeQuery) obj;
    if (collator == null) {
      if (other.collator != null)
        return false;
    } else if (!collator.equals(other.collator))
      return false;
    if (includeLower != other.includeLower)
      return false;
    if (includeUpper != other.includeUpper)
      return false;
    if (lowerTerm == null) {
      if (other.lowerTerm != null)
        return false;
    } else if (!lowerTerm.equals(other.lowerTerm))
      return false;
    if (upperTerm == null) {
      if (other.upperTerm != null)
        return false;
    } else if (!upperTerm.equals(other.upperTerm))
      return false;
    return true;
  }
}
