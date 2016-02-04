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
package org.apache.lucene.util;


import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;

public class TestSparseFixedBitSet extends BaseBitSetTestCase<SparseFixedBitSet> {

  @Override
  public SparseFixedBitSet copyOf(BitSet bs, int length) throws IOException {
    final SparseFixedBitSet set = new SparseFixedBitSet(length);
    for (int doc = bs.nextSetBit(0); doc != DocIdSetIterator.NO_MORE_DOCS; doc = doc + 1 >= length ? DocIdSetIterator.NO_MORE_DOCS : bs.nextSetBit(doc + 1)) {
      set.set(doc);
    }
    return set;
  }

  @Override
  protected void assertEquals(BitSet set1, SparseFixedBitSet set2, int maxDoc) {
    super.assertEquals(set1, set2, maxDoc);
    // check invariants of the sparse set
    int nonZeroLongCount = 0;
    for (int i = 0; i < set2.indices.length; ++i) {
      final int n = Long.bitCount(set2.indices[i]);
      if (n != 0) {
        nonZeroLongCount += n;
        for (int j = n; j < set2.bits[i].length; ++j) {
          assertEquals(0, set2.bits[i][j]);
        }
      }
    }
    assertEquals(nonZeroLongCount, set2.nonZeroLongCount);
  }

  public void testApproximateCardinality() {
    final SparseFixedBitSet set = new SparseFixedBitSet(10000);
    final int first = random().nextInt(1000);
    final int interval = 200 + random().nextInt(1000);
    for (int i = first; i < set.length(); i += interval) {
      set.set(i);
    }
    assertEquals(set.cardinality(), set.approximateCardinality(), 20);
  }

  public void testApproximateCardinalityOnDenseSet() {
    // this tests that things work as expected in approximateCardinality when
    // all longs are different than 0, in which case we divide by zero
    final int numDocs = TestUtil.nextInt(random(), 1, 10000);
    final SparseFixedBitSet set = new SparseFixedBitSet(numDocs);
    for (int i = 0; i < set.length(); ++i) {
      set.set(i);
    }
    assertEquals(numDocs, set.approximateCardinality());
  }
}
