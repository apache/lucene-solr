package org.apache.lucene.util;

/*
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

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet.FixedBitSetIterator;

/**
 * Implementation of the {@link DocIdSet} interface on top of a {@link FixedBitSet}.
 * @lucene.internal
 */
public class FixedBitDocIdSet extends DocIdSet {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SparseFixedBitDocIdSet.class);

  private final FixedBitSet set;
  private final long cost;

  /**
   * Wrap the given {@link FixedBitSet} as a {@link DocIdSet}. The provided
   * {@link FixedBitSet} should not be modified after having wrapped as a
   * {@link DocIdSet}.
   */
  public FixedBitDocIdSet(FixedBitSet set, long cost) {
    this.set = set;
    this.cost = cost;
  }

  /**
   * Same as {@link #FixedBitDocIdSet(FixedBitSet, long)} but uses the set
   * {@link FixedBitSet#cardinality() cardinality} as a cost.
   */
  public FixedBitDocIdSet(FixedBitSet set) {
    this(set, set.cardinality());
  }

  @Override
  public DocIdSetIterator iterator() {
    return new FixedBitSetIterator(set, cost);
  }

  @Override
  public FixedBitSet bits() {
    return set;
  }

  /** This DocIdSet implementation is cacheable. */
  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + set.ramBytesUsed();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(set=" + set + ",cost=" + cost + ")";
  }

}
