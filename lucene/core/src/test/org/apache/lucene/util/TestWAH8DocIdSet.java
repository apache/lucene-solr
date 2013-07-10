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
import java.util.List;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

public class TestWAH8DocIdSet extends LuceneTestCase {

  private static FixedBitSet randomSet(int numBits, int numBitsSet) {
    assert numBitsSet <= numBits;
    final FixedBitSet set = new FixedBitSet(numBits);
    if (numBitsSet == numBits) {
      set.set(0, set.length());
    } else {
      for (int i = 0; i < numBitsSet; ++i) {
        while (true) {
          final int o = random().nextInt(numBits);
          if (!set.get(o)) {
            set.set(o);
            break;
          }
        }
      }
    }
    return set;
  }

  private static FixedBitSet randomSet(int numBits, float percentSet) {
    return randomSet(numBits, (int) (percentSet * numBits));
  }

  public void testAgainstFixedBitSet() throws IOException {
    final int numBits = _TestUtil.nextInt(random(), 100, 1 << 20);
    for (float percentSet : new float[] {0f, 0.0001f, random().nextFloat() / 2, 0.9f, 1f}) {
      final FixedBitSet set = randomSet(numBits, percentSet);
      final WAH8DocIdSet copy = WAH8DocIdSet.copyOf(set.iterator());
      assertEquals(numBits, set, copy);
    }
  }

  public void assertEquals(int numBits, FixedBitSet ds1, WAH8DocIdSet ds2) throws IOException {
    assertEquals(ds1.cardinality(), ds2.cardinality());

    // nextDoc
    DocIdSetIterator it1 = ds1.iterator();
    DocIdSetIterator it2 = ds2.iterator();
    assertEquals(it1.docID(), it2.docID());
    for (int doc = it1.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it1.nextDoc()) {
      assertEquals(doc, it2.nextDoc());
      assertEquals(it1.docID(), it2.docID());
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, it2.nextDoc());
    assertEquals(it1.docID(), it2.docID());

    // nextDoc / advance
    it1 = ds1.iterator();
    it2 = ds2.iterator();
    for (int doc = -1; doc != DocIdSetIterator.NO_MORE_DOCS;) {
      if (random().nextBoolean()) {
        doc = it1.nextDoc();
        assertEquals(doc, it2.nextDoc());
        assertEquals(it1.docID(), it2.docID());
      } else {
        final int target = doc + 1 + random().nextInt(random().nextBoolean() ? 64 : numBits / 64);
        doc = it1.advance(target);
        assertEquals(doc, it2.advance(target));
        assertEquals(it1.docID(), it2.docID());
      }
    }
  }

  public void testUnion() throws IOException {
    final int numBits = _TestUtil.nextInt(random(), 100, 1 << 20);
    final int numDocIdSets = _TestUtil.nextInt(random(), 0, 4);
    final List<FixedBitSet> fixedSets = new ArrayList<FixedBitSet>(numDocIdSets);
    for (int i = 0; i < numDocIdSets; ++i) {
      fixedSets.add(randomSet(numBits, random().nextFloat() / 16));
    }
    final List<WAH8DocIdSet> compressedSets = new ArrayList<WAH8DocIdSet>(numDocIdSets);
    for (FixedBitSet set : fixedSets) {
      compressedSets.add(WAH8DocIdSet.copyOf(set.iterator()));
    }

    final WAH8DocIdSet union = WAH8DocIdSet.union(compressedSets);
    final FixedBitSet expected = new FixedBitSet(numBits);
    for (DocIdSet set : fixedSets) {
      final DocIdSetIterator it = set.iterator();
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        expected.set(doc);
      }
    }
    assertEquals(numBits, expected, union);
  }

  public void testIntersection() throws IOException {
    final int numBits = _TestUtil.nextInt(random(), 100, 1 << 20);
    final int numDocIdSets = _TestUtil.nextInt(random(), 1, 4);
    final List<FixedBitSet> fixedSets = new ArrayList<FixedBitSet>(numDocIdSets);
    for (int i = 0; i < numDocIdSets; ++i) {
      fixedSets.add(randomSet(numBits, random().nextFloat()));
    }
    final List<WAH8DocIdSet> compressedSets = new ArrayList<WAH8DocIdSet>(numDocIdSets);
    for (FixedBitSet set : fixedSets) {
      compressedSets.add(WAH8DocIdSet.copyOf(set.iterator()));
    }

    final WAH8DocIdSet union = WAH8DocIdSet.intersect(compressedSets);
    final FixedBitSet expected = new FixedBitSet(numBits);
    expected.set(0, expected.length());
    for (DocIdSet set : fixedSets) {
      final DocIdSetIterator it = set.iterator();
      int lastDoc = -1;
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        expected.clear(lastDoc + 1, doc);
        lastDoc = doc;
      }
      if (lastDoc + 1 < expected.length()) {
        expected.clear(lastDoc + 1, expected.length());
      }
    }
    assertEquals(numBits, expected, union);
  }

}
