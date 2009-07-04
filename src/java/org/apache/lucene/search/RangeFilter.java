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

/**
 * A Filter that restricts search results to a range of values in a given
 * field.
 *
 * <p>This filter matches the documents looking for terms that fall into the
 * supplied range according to {@link String#compareTo(String)}. It is not intended
 * for numerical ranges, use {@link NumericRangeFilter} instead.
 *
 * <p>If you construct a large number of range filters with different ranges but on the 
 * same field, {@link FieldCacheRangeFilter} may have significantly better performance. 
 *
 * @deprecated Use {@link TermRangeFilter} for term ranges or
 * {@link NumericRangeFilter} for numeric ranges instead.
 * This class will be removed in Lucene 3.0.
 */
public class RangeFilter extends MultiTermQueryWrapperFilter {
    
  /**
   * @param fieldName The field this range applies to
   * @param lowerTerm The lower bound on this range
   * @param upperTerm The upper bound on this range
   * @param includeLower Does this range include the lower bound?
   * @param includeUpper Does this range include the upper bound?
   * @throws IllegalArgumentException if both terms are null or if
   *  lowerTerm is null and includeLower is true (similar for upperTerm
   *  and includeUpper)
   */
  public RangeFilter(String fieldName, String lowerTerm, String upperTerm,
                     boolean includeLower, boolean includeUpper) {
      super(new TermRangeQuery(fieldName, lowerTerm, upperTerm, includeLower, includeUpper));
  }

  /**
   * <strong>WARNING:</strong> Using this constructor and supplying a non-null
   * value in the <code>collator</code> parameter will cause every single 
   * index Term in the Field referenced by lowerTerm and/or upperTerm to be
   * examined.  Depending on the number of index Terms in this Field, the 
   * operation could be very slow.
   *
   * @param lowerTerm The lower bound on this range
   * @param upperTerm The upper bound on this range
   * @param includeLower Does this range include the lower bound?
   * @param includeUpper Does this range include the upper bound?
   * @param collator The collator to use when determining range inclusion; set
   *  to null to use Unicode code point ordering instead of collation.
   * @throws IllegalArgumentException if both terms are null or if
   *  lowerTerm is null and includeLower is true (similar for upperTerm
   *  and includeUpper)
   */
  public RangeFilter(String fieldName, String lowerTerm, String upperTerm,
                     boolean includeLower, boolean includeUpper,
                     Collator collator) {
      super(new TermRangeQuery(fieldName, lowerTerm, upperTerm, includeLower, includeUpper, collator));
  }

  /**
   * Constructs a filter for field <code>fieldName</code> matching
   * less than or equal to <code>upperTerm</code>.
   */
  public static RangeFilter Less(String fieldName, String upperTerm) {
      return new RangeFilter(fieldName, null, upperTerm, false, true);
  }

  /**
   * Constructs a filter for field <code>fieldName</code> matching
   * greater than or equal to <code>lowerTerm</code>.
   */
  public static RangeFilter More(String fieldName, String lowerTerm) {
      return new RangeFilter(fieldName, lowerTerm, null, true, false);
  }
}
