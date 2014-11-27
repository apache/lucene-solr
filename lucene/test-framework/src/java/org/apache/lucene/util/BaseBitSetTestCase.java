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
import java.util.Collections;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.junit.Ignore;

/**
 * Base test case for BitSets.
 */
@Ignore
public abstract class BaseBitSetTestCase<T extends BitSet> extends LuceneTestCase {

  /** Create a copy of the given {@link BitSet} which has <code>length</code> bits. */
  public abstract T copyOf(BitSet bs, int length) throws IOException;

  /** Create a random set which has <code>numBitsSet</code> of its <code>numBits</code> bits set. */
  static java.util.BitSet randomSet(int numBits, int numBitsSet) {
    assert numBitsSet <= numBits;
    final java.util.BitSet set = new java.util.BitSet(numBits);
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
  static java.util.BitSet randomSet(int numBits, float percentSet) {
    return randomSet(numBits, (int) (percentSet * numBits));
  }

  protected void assertEquals(BitSet set1, T set2, int maxDoc) {
    for (int i = 0; i < maxDoc; ++i) {
      assertEquals("Different at " + i, set1.get(i), set2.get(i));
    }
  }

  /** Test the {@link BitSet#cardinality()} method. */
  public void testCardinality() throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      BitSet set1 = new JavaUtilBitSet(randomSet(numBits, percentSet), numBits);
      T set2 = copyOf(set1, numBits);
      assertEquals(set1.cardinality(), set2.cardinality());
    }
  }

  /** Test {@link BitSet#prevSetBit(int)}. */
  public void testPrevSetBit() throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      BitSet set1 = new JavaUtilBitSet(randomSet(numBits, percentSet), numBits);
      T set2 = copyOf(set1, numBits);
      for (int i = 0; i < numBits; ++i) {
        assertEquals(Integer.toString(i), set1.prevSetBit(i), set2.prevSetBit(i));
      }
    }
  }

  /** Test {@link BitSet#nextSetBit(int)}. */
  public void testNextSetBit() throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      BitSet set1 = new JavaUtilBitSet(randomSet(numBits, percentSet), numBits);
      T set2 = copyOf(set1, numBits);
      for (int i = 0; i < numBits; ++i) {
        assertEquals(set1.nextSetBit(i), set2.nextSetBit(i));
      }
    }
  }

  /** Test the {@link BitSet#set} method. */
  public void testSet() throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    BitSet set1 = new JavaUtilBitSet(randomSet(numBits, 0), numBits);
    T set2 = copyOf(set1, numBits);
    final int iters = 10000 + random().nextInt(10000);
    for (int i = 0; i < iters; ++i) {
      final int index = random().nextInt(numBits);
      set1.set(index);
      set2.set(index);
    }
    assertEquals(set1, set2, numBits);
  }

  /** Test the {@link BitSet#clear(int)} method. */
  public void testClear() throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      BitSet set1 = new JavaUtilBitSet(randomSet(numBits, percentSet), numBits);
      T set2 = copyOf(set1, numBits);
      final int iters = 1 + random().nextInt(numBits * 2);
      for (int i = 0; i < iters; ++i) {
        final int index = random().nextInt(numBits);
        set1.clear(index);
        set2.clear(index);
      }
      assertEquals(set1, set2, numBits);
    }
  }

  /** Test the {@link BitSet#clear(int,int)} method. */
  public void testClearRange() throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    for (float percentSet : new float[] {0, 0.01f, 0.1f, 0.5f, 0.9f, 0.99f, 1f}) {
      BitSet set1 = new JavaUtilBitSet(randomSet(numBits, percentSet), numBits);
      T set2 = copyOf(set1, numBits);
      final int iters = 1 + random().nextInt(100);
      for (int i = 0; i < iters; ++i) {
        final int from = random().nextInt(numBits);
        final int to = random().nextInt(numBits + 1);
        set1.clear(from, to);
        set2.clear(from, to);
        assertEquals(set1, set2, numBits);
      }
    }
  }

  private DocIdSet randomCopy(BitSet set, int numBits) throws IOException {
    switch (random().nextInt(5)) {
      case 0:
        return new BitDocIdSet(set, set.cardinality());
      case 1:
        return new BitDocIdSet(copyOf(set, numBits), set.cardinality());
      case 2:
        final RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(numBits);
        for (int i = set.nextSetBit(0); i != DocIdSetIterator.NO_MORE_DOCS; i = i + 1 >= numBits ? DocIdSetIterator.NO_MORE_DOCS : set.nextSetBit(i + 1)) {
          builder.add(i);
        }
        return builder.build();
      case 3:
        FixedBitSet fbs = new FixedBitSet(numBits);
        fbs.or(new BitSetIterator(set, 0));
        return new BitDocIdSet(fbs);
      case 4:
        SparseFixedBitSet sfbs = new SparseFixedBitSet(numBits);
        sfbs.or(new BitSetIterator(set, 0));
        return new BitDocIdSet(sfbs);
      default:
        fail();
        return null;
    }
  }

  /** Test the {@link BitSet#and}, {@link BitSet#or} and {@link BitSet#andNot} methods. */
  public void testBulkOperations() throws IOException {
    final int numBits = 1 + random().nextInt(100000);
    BitSet set1 = new JavaUtilBitSet(randomSet(numBits, 0), numBits);
    T set2 = copyOf(set1, numBits);
    final int iters = TEST_NIGHTLY ? 50 + random().nextInt(50) : 10 + random().nextInt(10);
    for (int i = 0; i < iters; ++i) {
      // make extreme percents more likely
      float percentSet2 = rarely() ? 0 : (float) Math.pow(random().nextDouble(), 2);
      if (random().nextBoolean()) {
        percentSet2 = 1 - percentSet2;
      }
      BitSet bulkSet = new JavaUtilBitSet(randomSet(numBits, percentSet2), numBits);
      // operations are sometimes specialized based on the impl, so randomize the impl
      final DocIdSet bulkSetCopy = randomCopy(bulkSet, numBits);
      // now randomize the operation
      if (bulkSetCopy.iterator() == null) {
        continue;
      }
      DocIdSetIterator it1 = bulkSetCopy.iterator();
      DocIdSetIterator it2 = bulkSetCopy.iterator();
      switch (random().nextInt(3)) {
        case 0:
          set1.or(it1);
          set2.or(it2);
          break;
        case 1:
          set1.and(it1);
          set2.and(it2);
          break;
        default:
          set1.andNot(it1);
          set2.andNot(it2);
          break;
      }
      assertEquals(set1, set2, numBits);
    }
  }

  private static class JavaUtilBitSet extends BitSet {

    private final java.util.BitSet bitSet;
    private final int numBits;

    JavaUtilBitSet(java.util.BitSet bitSet, int numBits) {
      this.bitSet = bitSet;
      this.numBits = numBits;
    }

    @Override
    public void clear(int index) {
      bitSet.clear(index);
    }

    @Override
    public boolean get(int index) {
      return bitSet.get(index);
    }

    @Override
    public int length() {
      return numBits;
    }

    @Override
    public long ramBytesUsed() {
      return -1;
    }

    @Override
    public Iterable<? extends Accountable> getChildResources() {
      return Collections.emptyList();
    }

    @Override
    public void set(int i) {
      bitSet.set(i);
    }

    @Override
    public void clear(int startIndex, int endIndex) {
      if (startIndex >= endIndex) {
        return;
      }
      bitSet.clear(startIndex, endIndex);
    }

    @Override
    public int cardinality() {
      return bitSet.cardinality();
    }

    @Override
    public int prevSetBit(int index) {
      return bitSet.previousSetBit(index);
    }

    @Override
    public int nextSetBit(int i) {
      int next = bitSet.nextSetBit(i);
      if (next == -1) {
        next = DocIdSetIterator.NO_MORE_DOCS;
      }
      return next;
    }

  }

}
