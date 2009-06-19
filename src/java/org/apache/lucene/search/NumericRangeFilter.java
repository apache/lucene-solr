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

/**
 * Implementation of a {@link Filter} that implements <em>trie-based</em> range filtering
 * for numeric values. For more information about the algorithm look into the docs of
 * {@link NumericRangeQuery}.
 *
 * <p>This filter depends on a specific structure of terms in the index that can only be created
 * by indexing using {@link NumericTokenStream}.
 *
 * <p><b>Please note:</b> This class has no constructor, you can create filters depending on the data type
 * by using the static factories {@linkplain #newLongRange NumericRangeFilter.newLongRange()},
 * {@linkplain #newIntRange NumericRangeFilter.newIntRange()}, {@linkplain #newDoubleRange NumericRangeFilter.newDoubleRange()},
 * and {@linkplain #newFloatRange NumericRangeFilter.newFloatRange()}, e.g.:
 * <pre>
 * Filter f = NumericRangeFilter.newFloatRange(field, <a href="NumericRangeQuery.html#precisionStepDesc">precisionStep</a>,
 *                                             new Float(0.3f), new Float(0.10f),
 *                                             true, true);
 * </pre>
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
