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
   * <p>This constructor uses the trie variant returned by {@link TrieUtils#getDefaultTrieVariant()}.
   */
  public TrieRangeQuery(final String field, final String min, final String max) {
    super(new TrieRangeFilter(field,min,max));
  }

  /**
   * Universal constructor (expert use only): Uses already trie-converted min/max values.
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   */
  public TrieRangeQuery(final String field, final String min, final String max, final TrieUtils variant) {
    super(new TrieRangeFilter(field,min,max,variant));
  }

  /**
   * A trie query using the supplied field with range bounds in numeric form (double).
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * <p>This constructor uses the trie variant returned by {@link TrieUtils#getDefaultTrieVariant()}.
   */
  public TrieRangeQuery(final String field, final Double min, final Double max) {
    super(new TrieRangeFilter(field,min,max));
  }

  /**
   * A trie query using the supplied field with range bounds in numeric form (double).
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   */
  public TrieRangeQuery(final String field, final Double min, final Double max, final TrieUtils variant) {
    super(new TrieRangeFilter(field,min,max,variant));
  }

  /**
   * A trie query using the supplied field with range bounds in date/time form.
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * <p>This constructor uses the trie variant returned by {@link TrieUtils#getDefaultTrieVariant()}.
   */
  public TrieRangeQuery(final String field, final Date min, final Date max) {
    super(new TrieRangeFilter(field,min,max));
  }

  /**
   * A trie query using the supplied field with range bounds in date/time form.
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   */
  public TrieRangeQuery(final String field, final Date min, final Date max, final TrieUtils variant) {
    super(new TrieRangeFilter(field,min,max,variant));
  }

  /**
   * A trie query using the supplied field with range bounds in integer form (long).
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   * <p>This constructor uses the trie variant returned by {@link TrieUtils#getDefaultTrieVariant()}.
   */
  public TrieRangeQuery(final String field, final Long min, final Long max) {
    super(new TrieRangeFilter(field,min,max));
  }

  /**
   * A trie query using the supplied field with range bounds in integer form (long).
   * You can set <code>min</code> or <code>max</code> (but not both) to <code>null</code> to leave one bound open.
   */
  public TrieRangeQuery(final String field, final Long min, final Long max, final TrieUtils variant) {
    super(new TrieRangeFilter(field,min,max,variant));
  }

  /**
   * EXPERT: Return the number of terms visited during the last execution of the query.
   * This may be used for performance comparisons of different trie variants and their effectiveness.
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
