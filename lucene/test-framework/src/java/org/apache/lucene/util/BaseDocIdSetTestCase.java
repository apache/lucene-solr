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
import java.util.BitSet;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

/** Base test class for {@link DocIdSet}s. */
public abstract class BaseDocIdSetTestCase<T extends DocIdSet> extends LuceneTestCase {

  /** Create a copy of the given {@link BitSet} which has <code>length</code> bits. */
  public abstract T copyOf(BitSet bs, int length) throws IOException;

  /** Create a random set which has <code>numBitsSet</code> of its <code>numBits</code> bits set. */
  protected static BitSet randomSet(int numBits, int numBitsSet) {
    assert numBitsSet <= numBits;
    final BitSet set = new BitSet(numBits);
    if (numBitsSet == numBits) {
      set.set(0, numBits);
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

  /** Same as {@link #randomSet(int, int)} but given a load factor. */
  protected static BitSet randomSet(int numBits, float percentSet) {
    return randomSet(numBits, (int) (percentSet * numBits));
  }

  /** Test length=0. */
  public void testNoBit() throws IOException {
    final BitSet bs = new BitSet(1);
    final T copy = copyOf(bs, 0);
    assertEquals(0, bs, copy);
  }

  /** Test length=1. */
  public void test1Bit() throws IOException {
    final BitSet bs = new BitSet(1);
    if (random().nextBoolean()) {
      bs.set(0);
    }
    final T copy = copyOf(bs, 1);
    assertEquals(1, bs, copy);
  }

  /** Test length=2. */
  public void test2Bits() throws IOException {
    final BitSet bs = new BitSet(2);
    if (random().nextBoolean()) {
      bs.set(0);
    }
    if (random().nextBoolean()) {
      bs.set(1);
    }
    final T copy = copyOf(bs, 2);
    assertEquals(2, bs, copy);
  }

  /** Compare the content of the set against a {@link BitSet}. */
  public void testAgainstBitSet() throws IOException {
    final int numBits = _TestUtil.nextInt(random(), 100, 1 << 20);
    // test various random sets with various load factors
    for (float percentSet : new float[] {0f, 0.0001f, random().nextFloat() / 2, 0.9f, 1f}) {
      final BitSet set = randomSet(numBits, percentSet);
      final T copy = copyOf(set, numBits);
      assertEquals(numBits, set, copy);
    }
    // test one doc
    BitSet set = new BitSet(numBits);
    set.set(0); // 0 first
    T copy = copyOf(set, numBits);
    assertEquals(numBits, set, copy);
    set.clear(0);
    set.set(random().nextInt(numBits));
    copy = copyOf(set, numBits); // then random index
    assertEquals(numBits, set, copy);
    // test regular increments
    for (int inc = 2; inc < 1000; inc += _TestUtil.nextInt(random(), 1, 100)) {
      set = new BitSet(numBits);
      for (int d = random().nextInt(10); d < numBits; d += inc) {
        set.set(d);
      }
      copy = copyOf(set, numBits);
      assertEquals(numBits, set, copy);
    }
  }

  /** Assert that the content of the {@link DocIdSet} is the same as the content of the {@link BitSet}. */
  public void assertEquals(int numBits, BitSet ds1, T ds2) throws IOException {
    // nextDoc
    DocIdSetIterator it2 = ds2.iterator();
    if (it2 == null) {
      assertEquals(-1, ds1.nextSetBit(0));
    } else {
      assertEquals(-1, it2.docID());
      for (int doc = ds1.nextSetBit(0); doc != -1; doc = ds1.nextSetBit(doc + 1)) {
        assertEquals(doc, it2.nextDoc());
        assertEquals(doc, it2.docID());
      }
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, it2.nextDoc());
      assertEquals(DocIdSetIterator.NO_MORE_DOCS, it2.docID());
    }

    // nextDoc / advance
    it2 = ds2.iterator();
    if (it2 == null) {
      assertEquals(-1, ds1.nextSetBit(0));
    } else {
      for (int doc = -1; doc != DocIdSetIterator.NO_MORE_DOCS;) {
        if (random().nextBoolean()) {
          doc = ds1.nextSetBit(doc + 1);
          if (doc == -1) {
            doc = DocIdSetIterator.NO_MORE_DOCS;
          }
          assertEquals(doc, it2.nextDoc());
          assertEquals(doc, it2.docID());
        } else {
          final int target = doc + 1 + random().nextInt(random().nextBoolean() ? 64 : Math.max(numBits / 8, 1));
          doc = ds1.nextSetBit(target);
          if (doc == -1) {
            doc = DocIdSetIterator.NO_MORE_DOCS;
          }
          assertEquals(doc, it2.advance(target));
          assertEquals(doc, it2.docID());
        }
      }
    }

    // bits()
    final Bits bits = ds2.bits();
    if (bits != null) {
      // test consistency between bits and iterator
      it2 = ds2.iterator();
      for (int previousDoc = -1, doc = it2.nextDoc(); ; previousDoc = doc, doc = it2.nextDoc()) {
        final int max = doc == DocIdSetIterator.NO_MORE_DOCS ? bits.length() : doc;
        for (int i = previousDoc + 1; i < max; ++i) {
          assertEquals(false, bits.get(i));
        }
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        assertEquals(true, bits.get(doc));
      }
    }
  }

}
