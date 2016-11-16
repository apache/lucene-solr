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
package org.apache.lucene.index;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;

/** Accumulator for documents that have a value for a field. This is optimized
 *  for the case that all documents have a value. */
final class DocsWithFieldSet extends DocIdSet {

  private static long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DocsWithFieldSet.class);

  private FixedBitSet set;
  private int cost = 0;
  private int lastDocId = -1;

  void add(int docID) {
    if (docID <= lastDocId) {
      throw new IllegalArgumentException("Out of order doc ids: last=" + lastDocId + ", next=" + docID);
    }
    if (set != null) {
      set = FixedBitSet.ensureCapacity(set, docID);
      set.set(docID);
    } else if (docID != cost) {
      // migrate to a sparse encoding using a bit set
      set = new FixedBitSet(docID + 1);
      set.set(0, cost);
      set.set(docID);
    }
    lastDocId = docID;
    cost++;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + (set == null ? 0 : set.ramBytesUsed());
  }

  @Override
  public DocIdSetIterator iterator() {
    return set != null ? new BitSetIterator(set, cost) : DocIdSetIterator.all(cost);
  }

}
