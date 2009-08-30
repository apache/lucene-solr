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

import org.apache.lucene.analysis.NumericTokenStream; // for javadocs
import org.apache.lucene.document.NumericField; // for javadocs
import org.apache.lucene.util.NumericUtils; // for javadocs

/**
 * A {@link Filter} that only accepts numeric values within
 * a specified range. To use this, you must first index the
 * numeric values using {@link NumericField} (expert: {@link
 * NumericTokenStream}).
 *
 * <p>You create a new NumericRangeFilter with the static
 * factory methods, eg:
 *
 * <pre>
 * Filter f = NumericRangeFilter.newFloatRange("weight",
 *                                             new Float(0.3f), new Float(0.10f),
 *                                             true, true);
 * </pre>
 *
 * accepts all documents whose float valued "weight" field
 * ranges from 0.3 to 0.10, inclusive.
 * See {@link NumericRangeQuery} for details on how Lucene
 * indexes and searches numeric valued fields.
 *
 * <p><font color="red"><b>NOTE:</b> This API is experimental and
 * might change in incompatible ways in the next
 * release.</font>
 *
 * @since 2.9
 **/
public final class NumericRangeFilter extends MultiTermQueryWrapperFilter {

  private NumericRangeFilter(final NumericRangeQuery query) {
    super(query);
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that filters a <code>long</code>
   * range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter newLongRange(final String field, final int precisionStep,
    Long min, Long max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter(
      NumericRangeQuery.newLongRange(field, precisionStep, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that queries a <code>long</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (4).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter newLongRange(final String field,
    Long min, Long max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter(
      NumericRangeQuery.newLongRange(field, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that filters a <code>int</code>
   * range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter newIntRange(final String field, final int precisionStep,
    Integer min, Integer max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter(
      NumericRangeQuery.newIntRange(field, precisionStep, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that queries a <code>int</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (4).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter newIntRange(final String field,
    Integer min, Integer max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter(
      NumericRangeQuery.newIntRange(field, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that filters a <code>double</code>
   * range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter newDoubleRange(final String field, final int precisionStep,
    Double min, Double max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter(
      NumericRangeQuery.newDoubleRange(field, precisionStep, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that queries a <code>double</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (4).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter newDoubleRange(final String field,
    Double min, Double max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter(
      NumericRangeQuery.newDoubleRange(field, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that filters a <code>float</code>
   * range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter newFloatRange(final String field, final int precisionStep,
    Float min, Float max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter(
      NumericRangeQuery.newFloatRange(field, precisionStep, min, max, minInclusive, maxInclusive)
    );
  }

  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that queries a <code>float</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (4).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter newFloatRange(final String field,
    Float min, Float max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter(
      NumericRangeQuery.newFloatRange(field, min, max, minInclusive, maxInclusive)
    );
  }
  
  /** Returns the field name for this filter */
  public String getField() { return ((NumericRangeQuery)query).getField(); }

  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesMin() { return ((NumericRangeQuery)query).includesMin(); }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesMax() { return ((NumericRangeQuery)query).includesMax(); }

  /** Returns the lower value of this range filter */
  public Number getMin() { return ((NumericRangeQuery)query).getMin(); }

  /** Returns the upper value of this range filter */
  public Number getMax() { return ((NumericRangeQuery)query).getMax(); }
  
}
