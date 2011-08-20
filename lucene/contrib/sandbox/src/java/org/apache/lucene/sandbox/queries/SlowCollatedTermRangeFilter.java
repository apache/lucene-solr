package org.apache.lucene.sandbox.queries;

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

import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.NumericRangeFilter; // javadoc
import org.apache.lucene.search.FieldCacheRangeFilter; // javadoc

/**
 * A Filter that restricts search results to a range of term
 * values in a given field.
 *
 * <p>This filter matches the documents looking for terms that fall into the
 * supplied range according to {@link
 * String#compareTo(String)}, unless a <code>Collator</code> is provided. It is not intended
 * for numerical ranges; use {@link NumericRangeFilter} instead.
 *
 * <p>If you construct a large number of range filters with different ranges but on the 
 * same field, {@link FieldCacheRangeFilter} may have significantly better performance. 
 * @deprecated Index collation keys with CollationKeyAnalyzer or ICUCollationKeyAnalyzer instead.
 * This class will be removed in Lucene 5.0
 */
@Deprecated
public class SlowCollatedTermRangeFilter extends MultiTermQueryWrapperFilter<SlowCollatedTermRangeQuery> {
  /**
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
  public SlowCollatedTermRangeFilter(String fieldName, String lowerTerm, String upperTerm,
                     boolean includeLower, boolean includeUpper,
                     Collator collator) {
      super(new SlowCollatedTermRangeQuery(fieldName, lowerTerm, upperTerm, includeLower, includeUpper, collator));
  }
  
  /** Returns the lower value of this range filter */
  public String getLowerTerm() { return query.getLowerTerm(); }

  /** Returns the upper value of this range filter */
  public String getUpperTerm() { return query.getUpperTerm(); }
  
  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesLower() { return query.includesLower(); }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesUpper() { return query.includesUpper(); }

  /** Returns the collator used to determine range inclusion, if any. */
  public Collator getCollator() { return query.getCollator(); }
}
