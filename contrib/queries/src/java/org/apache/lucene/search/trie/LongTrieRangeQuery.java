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

import org.apache.lucene.search.Query;

/**
 * Implementation of a Lucene {@link Query} that implements trie-based range querying for longs/doubles.
 * This query depends on a specific structure of terms in the index that can only be created
 * by indexing via {@link LongTrieTokenStream} methods.
 * <p>The query is in constant score mode per default. With precision steps of &le;4, this
 * query can be run in conventional boolean rewrite mode without changing the max clause count.
 * For more information, how the algorithm works, see the {@linkplain org.apache.lucene.search.trie package description}.
 */
public class LongTrieRangeQuery extends AbstractTrieRangeQuery {

  /**
   * A trie query for matching trie coded values using the given field name and
   * the default helper field.
   * <code>precisionStep</code> must me equal or a multiple of the <code>precisionStep</code>
   * used for indexing the values.
   * You can leave the bounds open, by supplying <code>null</code> for <code>min</code> and/or
   * <code>max</code>. Inclusive/exclusive bounds can also be supplied.
   * To query double values use the converter {@link TrieUtils#doubleToSortableLong}.
   */
  public LongTrieRangeQuery(final String field, final int precisionStep,
    final Long min, final Long max, final boolean minInclusive, final boolean maxInclusive
  ) {
    super(field,precisionStep,min,max,minInclusive,maxInclusive);
  }

  //@Override
  void passRanges(TrieRangeTermEnum enumerator) {
    // calculate the upper and lower bounds respecting the inclusive and null values.
    long minBound=(this.min==null) ? Long.MIN_VALUE : (
      minInclusive ? this.min.longValue() : (this.min.longValue()+1L)
    );
    long maxBound=(this.max==null) ? Long.MAX_VALUE : (
      maxInclusive ? this.max.longValue() : (this.max.longValue()-1L)
    );
    
    TrieUtils.splitLongRange(enumerator.getLongRangeBuilder(), precisionStep, minBound, maxBound);
  }

  /** Returns the lower value of this range query */
  public Long getMin() { return (Long)min; }

  /** Returns the upper value of this range query */
  public Long getMax() { return (Long)max; }
  
}
