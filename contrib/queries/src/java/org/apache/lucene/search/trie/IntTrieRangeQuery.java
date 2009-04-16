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
 * Implementation of a Lucene {@link Query} that implements trie-based range querying for ints/floats.
 * This query depends on a specific structure of terms in the index that can only be created
 * by indexing via {@link IntTrieTokenStream} methods.
 * <p>The query is in constant score mode per default. With precision steps of &le;4, this
 * query can be run in conventional boolean rewrite mode without changing the max clause count.
 * For more information, how the algorithm works, see the {@linkplain org.apache.lucene.search.trie package description}.
 */
public class IntTrieRangeQuery extends AbstractTrieRangeQuery {

  /**
   * A trie query for matching trie coded values using the given field name and
   * the default helper field.
   * <code>precisionStep</code> must me equal or a multiple of the <code>precisionStep</code>
   * used for indexing the values.
   * You can leave the bounds open, by supplying <code>null</code> for <code>min</code> and/or
   * <code>max</code>. Inclusive/exclusive bounds can also be supplied.
   * To query float values use the converter {@link TrieUtils#floatToSortableInt}.
   */
  public IntTrieRangeQuery(final String field, final int precisionStep,
    final Integer min, final Integer max, final boolean minInclusive, final boolean maxInclusive
  ) {
    super(field,precisionStep,min,max,minInclusive,maxInclusive);
  }

  //@Override
  void passRanges(TrieRangeTermEnum enumerator) {
    // calculate the upper and lower bounds respecting the inclusive and null values.
    int minBound=(this.min==null) ? Integer.MIN_VALUE : (
      minInclusive ? this.min.intValue() : (this.min.intValue()+1)
    );
    int maxBound=(this.max==null) ? Integer.MAX_VALUE : (
      maxInclusive ? this.max.intValue() : (this.max.intValue()-1)
    );
    
    TrieUtils.splitIntRange(enumerator.getIntRangeBuilder(), precisionStep, minBound, maxBound);
  }

  /** Returns the lower value of this range query */
  public Integer getMin() { return (Integer)min; }

  /** Returns the upper value of this range query */
  public Integer getMax() { return (Integer)max; }
  
}
