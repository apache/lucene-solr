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

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

public class TestSparseFixedBitSet extends BaseDocIdSetTestCase<SparseFixedBitSet> {

  @Override
  public SparseFixedBitSet copyOf(BitSet bs, int length) throws IOException {
    final SparseFixedBitSet set = new SparseFixedBitSet(length);
    // SparseFixedBitSet can be sensitive to the order of insertion so
    // randomize insertion a bit
    List<Integer> buffer = new ArrayList<>();
    for (int doc = bs.nextSetBit(0); doc != -1; doc = bs.nextSetBit(doc + 1)) {
      buffer.add(doc);
      if (buffer.size() >= 100000) {
        Collections.shuffle(buffer, random());
        for (int i : buffer) {
          set.set(i);
        }
        buffer.clear();
      }
    }
    Collections.shuffle(buffer, random());
    for (int i : buffer) {
      set.set(i);
    }
    return set;
  }

  @Override
  public void assertEquals(int numBits, BitSet ds1, SparseFixedBitSet ds2) throws IOException {
    for (int i = 0; i < numBits; ++i) {
      assertEquals(ds1.get(i), ds2.get(i));
    }
    assertEquals(ds1.cardinality(), ds2.cardinality());
    super.assertEquals(numBits, ds1, ds2);
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
    final int numDocs = 70;//TestUtil.nextInt(random(), 1, 10000);
    final SparseFixedBitSet set = new SparseFixedBitSet(numDocs);
    for (int i = 0; i < set.length(); ++i) {
      set.set(i);
    }
    assertEquals(numDocs, set.approximateCardinality());
  }

}
