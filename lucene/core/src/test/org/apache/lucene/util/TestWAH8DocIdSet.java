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
import java.util.List;

public class TestWAH8DocIdSet extends BaseDocIdSetTestCase<WAH8DocIdSet> {

  @Override
  public WAH8DocIdSet copyOf(BitSet bs, int length) throws IOException {
    final int indexInterval = TestUtil.nextInt(random(), 8, 256);
    final WAH8DocIdSet.Builder builder = new WAH8DocIdSet.Builder().setIndexInterval(indexInterval);
    for (int i = bs.nextSetBit(0); i != -1; i = bs.nextSetBit(i + 1)) {
      builder.add(i);
    }
    return builder.build();
  }

  @Override
  public void assertEquals(int numBits, BitSet ds1, WAH8DocIdSet ds2)
      throws IOException {
    super.assertEquals(numBits, ds1, ds2);
    assertEquals(ds1.cardinality(), ds2.cardinality());
  }

  public void testUnion() throws IOException {
    final int numBits = TestUtil.nextInt(random(), 100, 1 << 20);
    final int numDocIdSets = TestUtil.nextInt(random(), 0, 4);
    final List<BitSet> fixedSets = new ArrayList<>(numDocIdSets);
    for (int i = 0; i < numDocIdSets; ++i) {
      fixedSets.add(randomSet(numBits, random().nextFloat() / 16));
    }
    final List<WAH8DocIdSet> compressedSets = new ArrayList<>(numDocIdSets);
    for (BitSet set : fixedSets) {
      compressedSets.add(copyOf(set, numBits));
    }

    final WAH8DocIdSet union = WAH8DocIdSet.union(compressedSets);
    final BitSet expected = new BitSet(numBits);
    for (BitSet set : fixedSets) {
      for (int doc = set.nextSetBit(0); doc != -1; doc = set.nextSetBit(doc + 1)) {
        expected.set(doc);
      }
    }
    assertEquals(numBits, expected, union);
  }

  public void testIntersection() throws IOException {
    final int numBits = TestUtil.nextInt(random(), 100, 1 << 20);
    final int numDocIdSets = TestUtil.nextInt(random(), 1, 4);
    final List<BitSet> fixedSets = new ArrayList<>(numDocIdSets);
    for (int i = 0; i < numDocIdSets; ++i) {
      fixedSets.add(randomSet(numBits, random().nextFloat()));
    }
    final List<WAH8DocIdSet> compressedSets = new ArrayList<>(numDocIdSets);
    for (BitSet set : fixedSets) {
      compressedSets.add(copyOf(set, numBits));
    }

    final WAH8DocIdSet union = WAH8DocIdSet.intersect(compressedSets);
    final BitSet expected = new BitSet(numBits);
    expected.set(0, expected.size());
    for (BitSet set : fixedSets) {
      for (int previousDoc = -1, doc = set.nextSetBit(0); ; previousDoc = doc, doc = set.nextSetBit(doc + 1)) {
        if (doc == -1) {
          expected.clear(previousDoc + 1, set.size());
          break;
        } else {
          expected.clear(previousDoc + 1, doc);
        }
      }
    }
    assertEquals(numBits, expected, union);
  }

}
