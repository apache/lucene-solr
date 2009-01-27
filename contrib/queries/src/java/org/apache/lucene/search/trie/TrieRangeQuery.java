package org.apache.lucene.search.trie;

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

import java.util.Date;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.util.ToStringUtils;

/**
 * A Lucene {@link Query} that implements a trie-based range query.
 * This query depends on a specific structure of terms in the index that can only be created
 * by {@link TrieUtils} methods.
 * <p>This class wraps a {@link TrieRangeFilter}.
 * @see TrieRangeFilter
 */
public final class TrieRangeQuery extends ConstantScoreQuery {

  /**
   * Universal constructor (expert use only): Uses already trie-converted min/max values.
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
   * bound should be included or excluded from the range.
   */
  public TrieRangeQuery(final String field, final String min, final String max,
    final boolean minInclusive, final boolean maxInclusive, final TrieUtils variant
  ) {
    super(new TrieRangeFilter(field,min,max,minInclusive,maxInclusive,variant));
  }

  /**
   * A trie query using the supplied field with range bounds in numeric form (double).
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
   * bound should be included or excluded from the range.
   */
  public TrieRangeQuery(final String field, final Double min, final Double max,
    final boolean minInclusive, final boolean maxInclusive, final TrieUtils variant
  ) {
    super(new TrieRangeFilter(field,min,max,minInclusive,maxInclusive,variant));
  }

  /**
   * A trie query using the supplied field with range bounds in date/time form.
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
   * bound should be included or excluded from the range.
   */
  public TrieRangeQuery(final String field, final Date min, final Date max,
    final boolean minInclusive, final boolean maxInclusive, final TrieUtils variant
  ) {
    super(new TrieRangeFilter(field,min,max,minInclusive,maxInclusive,variant));
  }

  /**
   * A trie query using the supplied field with range bounds in integer form (long).
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * With <code>minInclusive</code> and <code>maxInclusive</code> can be choosen, if the corresponding
   * bound should be included or excluded from the range.
   */
  public TrieRangeQuery(final String field, final Long min, final Long max,
    final boolean minInclusive, final boolean maxInclusive, final TrieUtils variant
  ) {
    super(new TrieRangeFilter(field,min,max,minInclusive,maxInclusive,variant));
  }

  /**
   * EXPERT: Return the number of terms visited during the last execution of the query.
   * This may be used for performance comparisons of different trie variants and their effectiveness.
   * When using this method be sure to query an one-segment index (optimized one) to get correct results.
   * This method is not thread safe, be sure to only call it when no query is running!
   * @throws IllegalStateException if query was not yet executed.
   */
  public int getLastNumberOfTerms() {
    return ((TrieRangeFilter) filter).getLastNumberOfTerms();
  }

  //@Override
  public String toString(final String field) {
    // return a more convenient representation of this query than ConstantScoreQuery does:
    return ((TrieRangeFilter) filter).toString(field)+ToStringUtils.boost(getBoost());
  }

  /**
   * Two instances are equal if they have the same trie-encoded range bounds, same field, same boost, and same variant.
   * If one of the instances uses an exclusive lower bound, it is equal to a range with inclusive bound,
   * when the inclusive lower bound is equal to the decremented exclusive lower bound.
   * The same applys for the upper bound in other direction.
   */
  //@Override
  public final boolean equals(final Object o) {
    if (!(o instanceof TrieRangeQuery)) return false;
    return super.equals(o);
  }

  //@Override
  public final int hashCode() {
    // make hashCode a little bit different:
    return super.hashCode()^0x1756fa55;
  }

}
