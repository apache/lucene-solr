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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexReader;

/**
 * A Query that matches documents within an exclusive range. A RangeQuery
 * is built by QueryParser for input like <code>[010 TO 120]</code> but only if the QueryParser has 
 * the useOldRangeQuery property set to true. The QueryParser default behaviour is to use
 * the newer ConstantScore mode. This is generally preferable because:
 * <ul>
 *  <li>It is faster than the standard RangeQuery mode</li>
 *  <li>Unlike the RangeQuery mode, it does not cause a BooleanQuery.TooManyClauses exception if the range of values is large</li>
 *  <li>Unlike the RangeQuery mode, it does not influence scoring based on the scarcity of individual terms that may match</li>
 * </ul>
 * 
 *
 * @version $Id$
 */
public class RangeQuery extends MultiTermQuery {
  private Term lowerTerm;
  private Term upperTerm;
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
  public RangeQuery(String field, String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
    init(new Term(field, lowerTerm), new Term(field, upperTerm), includeLower, includeUpper, null);
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
  public RangeQuery(String field, String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper,
                    Collator collator) {
    init(new Term(field, lowerTerm), new Term(field,upperTerm), includeLower, includeUpper, collator);
  }

  /** @deprecated Please use {@link #RangeQuery(String,
   *  String, String, boolean, boolean, Collator)} instead */
  public RangeQuery(Term lowerTerm, Term upperTerm, boolean inclusive,
                    Collator collator) {
    init(lowerTerm, upperTerm, inclusive, inclusive, collator);
  }
  
  /** @deprecated Please use {@link #RangeQuery(String,
   *  String, String, boolean, boolean)} instead */
  public RangeQuery(Term lowerTerm, Term upperTerm, boolean inclusive) {
    init(lowerTerm, upperTerm, inclusive, inclusive, null);
  }

  private void init(Term lowerTerm, Term upperTerm, boolean includeLower, boolean includeUpper, Collator collator) {
    if (lowerTerm == null && upperTerm == null)
      throw new IllegalArgumentException("At least one term must be non-null");
    if (lowerTerm != null && upperTerm != null && lowerTerm.field() != upperTerm.field())
      throw new IllegalArgumentException("Both terms must be for the same field");

    if (lowerTerm == null)
      this.field = upperTerm.field();
    else
      this.field = lowerTerm.field();
    this.lowerTerm = lowerTerm;
    this.upperTerm = upperTerm;
    this.includeLower = includeLower;
    this.includeUpper = includeUpper;
    this.collator = collator;
  }
  
  /** Returns the field name for this query */
  public String getField() {
    return field;
  }

  /** Returns the lower term of this range query.
   *  @deprecated Use {@link #getLowerTermText} instead. */
  public Term getLowerTerm() { return lowerTerm; }

  /** Returns the upper term of this range query.
   *  @deprecated Use {@link #getUpperTermText} instead. */
  public Term getUpperTerm() { return upperTerm; }
  
  /** Returns the lower value of this range query */
  public String getLowerTermText() { return lowerTerm == null ? null : lowerTerm.text(); }

  /** Returns the upper value of this range query */
  public String getUpperTermText() { return upperTerm == null ? null : upperTerm.text(); }
  
  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesLower() { return includeLower; }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesUpper() { return includeUpper; }

  /** Returns <code>true</code> if the range query is inclusive 
   *  @deprecated Use {@link #includesLower}, {@link #includesUpper}  instead. 
   */
  public boolean isInclusive() { return includeUpper && includeLower; }

  /** Returns the collator used to determine range inclusion, if any. */
  public Collator getCollator() { return collator; }
  
  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    //TODO: when the deprecated 'Term' constructors are removed we can remove these null checks
    return new RangeTermEnum(reader, collator, getField(), lowerTerm == null ? null : lowerTerm.text(),
        upperTerm == null ? null : upperTerm.text(), includeLower, includeUpper);
  }

  /** Prints a user-readable version of this query. */
  public String toString(String field) {
      StringBuffer buffer = new StringBuffer();
      if (!getField().equals(field)) {
          buffer.append(getField());
          buffer.append(":");
      }
      buffer.append(includeLower ? '[' : '{');
      buffer.append(lowerTerm != null ? lowerTerm.text() : "*");
      buffer.append(" TO ");
      buffer.append(upperTerm != null ? upperTerm.text() : "*");
      buffer.append(includeUpper ? ']' : '}');
      if (getBoost() != 1.0f) {
          buffer.append("^");
          buffer.append(Float.toString(getBoost()));
      }
      return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof RangeQuery)) return false;
    RangeQuery other = (RangeQuery) o;

    if (this.field != other.field  // interned comparison
        || this.includeLower != other.includeLower
        || this.includeUpper != other.includeUpper
        || (this.collator != null && ! this.collator.equals(other.collator))
       ) { return false; }
    String lowerVal = this.lowerTerm == null ? null : lowerTerm.text();
    String upperVal = this.upperTerm == null ? null : upperTerm.text();
    String olowerText = other.lowerTerm == null ? null : other.lowerTerm.text();
    String oupperText = other.upperTerm == null ? null : other.upperTerm.text();
    if (lowerVal != null ? !lowerVal.equals(olowerText) : olowerText != null) return false;
    if (upperVal != null ? !upperVal.equals(oupperText) : oupperText != null) return false;
    return this.getBoost() == other.getBoost();
  }

  /** Returns a hash code value for this object.*/
  public int hashCode() {
    int h = Float.floatToIntBits(getBoost()) ^ field.hashCode();
    String lowerVal = this.lowerTerm == null ? null : lowerTerm.text();
    String upperVal = this.upperTerm == null ? null : upperTerm.text();
    // hashCode of "" is 0, so don't use that for null...
    h ^= lowerVal != null ? lowerVal.hashCode() : 0x965a965a;
    // don't just XOR upperVal with out mixing either it or h, as it will cancel
    // out lowerVal if they are equal.
    h ^= (h << 17) | (h >>> 16);  // a reversible (one to one) 32 bit mapping mix
    h ^= (upperVal != null ? (upperVal.hashCode()) : 0x5a695a69);
    h ^= (includeLower ? 0x665599aa : 0)
       ^ (includeUpper ? 0x99aa5566 : 0);
    h ^= collator != null ? collator.hashCode() : 0;
    return h;
  }
}
