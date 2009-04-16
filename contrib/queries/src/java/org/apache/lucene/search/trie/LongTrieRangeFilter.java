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

import org.apache.lucene.search.Filter; // for javadocs
import org.apache.lucene.search.MultiTermQueryWrapperFilter;

/**
 * Implementation of a Lucene {@link Filter} that implements trie-based range filtering for longs/doubles.
 * This filter depends on a specific structure of terms in the index that can only be created
 * by indexing via {@link LongTrieTokenStream} methods.
 * For more information, how the algorithm works, see the {@linkplain org.apache.lucene.search.trie package description}.
 */
public class LongTrieRangeFilter extends MultiTermQueryWrapperFilter {

  /**
   * A trie filter for matching trie coded values using the given field name and
   * the default helper field.
   * <code>precisionStep</code> must me equal or a multiple of the <code>precisionStep</code>
   * used for indexing the values.
   * You can leave the bounds open, by supplying <code>null</code> for <code>min</code> and/or
   * <code>max</code>. Inclusive/exclusive bounds can also be supplied.
   * To filter double values use the converter {@link TrieUtils#doubleToSortableLong}.
   */
  public LongTrieRangeFilter(final String field, final int precisionStep,
    final Long min, final Long max, final boolean minInclusive, final boolean maxInclusive
  ) {
    super(new LongTrieRangeQuery(field,precisionStep,min,max,minInclusive,maxInclusive));
  }

  /** Returns the field name for this filter */
  public String getField() { return ((LongTrieRangeQuery)query).getField(); }

  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesMin() { return ((LongTrieRangeQuery)query).includesMin(); }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesMax() { return ((LongTrieRangeQuery)query).includesMax(); }

  /** Returns the lower value of this range filter */
  public Long getMin() { return ((LongTrieRangeQuery)query).getMin(); }

  /** Returns the upper value of this range filter */
  public Long getMax() { return ((LongTrieRangeQuery)query).getMax(); }
  
}
