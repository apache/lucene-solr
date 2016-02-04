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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

public class TestSparseFixedBitDocIdSet extends BaseDocIdSetTestCase<BitDocIdSet> {

  @Override
  public BitDocIdSet copyOf(BitSet bs, int length) throws IOException {
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
    return new BitDocIdSet(set, set.approximateCardinality());
  }

  @Override
  public void assertEquals(int numBits, BitSet ds1, BitDocIdSet ds2) throws IOException {
    for (int i = 0; i < numBits; ++i) {
      assertEquals(ds1.get(i), ds2.bits().get(i));
    }
    assertEquals(ds1.cardinality(), ds2.bits().cardinality());
    super.assertEquals(numBits, ds1, ds2);
  }

}
