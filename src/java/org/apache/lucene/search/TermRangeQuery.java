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
import java.text.Collator;

import org.apache.lucene.index.IndexReader;
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
 * @since 2.9
 */

public class TermRangeQuery extends MultiTermQuery {
  private String lowerTerm;
  private String upperTerm;
  private Collator collator;
  private String field;
  private boolean includeLower;
  private boolean includeUpper;


  /**
   * Constructs a query selecting all terms greater/equal than <code>lowerTerm</code>
   * but less/equal than <code>upperTerm</code>. 
   * 
   * <p>
   * If an endpoint is null, it is said 
   * to be "open". Either or both endpoints may be open.  Open endpoints may not 
   * be exclusive (you can't select all but the first or last term without 
   * explicitly specifying the term to exclude.)
   * 
   * @param field The field that holds both lower and upper terms.
   * @param lowerTerm
   *          The term text at the lower end of the range
   * @param upperTerm
   *          The term text at the upper end of the range
   * @param includeLower
   *          If true, the <code>lowerTerm</code> is
   *          included in the range.
   * @param includeUpper
   *          If true, the <code>upperTerm</code> is
   *          included in the range.
   */
  public TermRangeQuery(String field, String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
    this(field, lowerTerm, upperTerm, includeLower, includeUpper, null);
  }

  /** Constructs a query selecting all terms greater/equal than
   * <code>lowerTerm</code> but less/equal than <code>upperTerm</code>.
   * <p>
   * If an endpoint is null, it is said 
   * to be "open". Either or both endpoints may be open.  Open endpoints may not 
   * be exclusive (you can't select all but the first or last term without 
   * explicitly specifying the term to exclude.)
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
  public TermRangeQuery(String field, String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper,
                    Collator collator) {
    this.field = field;
    this.lowerTerm = lowerTerm;
    this.upperTerm = upperTerm;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
    this.collator = collator;
  }

  /** Returns the field name for this query */
  public String getField() { return field; }
  
  /** Returns the lower value of this range query */
  public String getLowerTerm() { return lowerTerm; }

  /** Returns the upper value of this range query */
  public String getUpperTerm() { return upperTerm; }
  
  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesLower() { return includeLower; }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesUpper() { return includeUpper; }

  /** Returns the collator used to determine range inclusion, if any. */
  public Collator getCollator() { return collator; }
  
  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    return new TermRangeTermEnum(reader, field, lowerTerm,
        upperTerm, includeLower, includeUpper, collator);
  }

  /** Prints a user-readable version of this query. */
  public String toString(String field) {
      StringBuffer buffer = new StringBuffer();
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

  //@Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((collator == null) ? 0 : collator.hashCode());
    result = prime * result + ((field == null) ? 0 : field.hashCode());
    result = prime * result + (includeLower ? 1231 : 1237);
    result = prime * result + (includeUpper ? 1231 : 1237);
    result = prime * result + ((lowerTerm == null) ? 0 : lowerTerm.hashCode());
    result = prime * result + ((upperTerm == null) ? 0 : upperTerm.hashCode());
    return result;
  }

  //@Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    TermRangeQuery other = (TermRangeQuery) obj;
    if (collator == null) {
      if (other.collator != null)
        return false;
    } else if (!collator.equals(other.collator))
      return false;
    if (field == null) {
      if (other.field != null)
        return false;
    } else if (!field.equals(other.field))
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
