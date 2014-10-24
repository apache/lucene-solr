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

import org.apache.lucene.search.DocIdSetIterator;

/**
 * A bit set that only stores longs that have at least one bit which is set.
 * The way it works is that the space of bits is divided into blocks of
 * 4096 bits, which is 64 longs. Then for each block, we have:<ul>
 * <li>a long[] which stores the non-zero longs for that block</li>
 * <li>a long so that bit <tt>i</tt> being set means that the <code>i-th</code>
 *     long of the block is non-null, and its offset in the array of longs is
 *     the number of one bits on the right of the <code>i-th</code> bit.</li></ul>
 *
 * @lucene.internal
 */
public class SparseFixedBitSet extends BitSet implements Bits, Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SparseFixedBitSet.class);
  private static final long SINGLE_ELEMENT_ARRAY_BYTES_USED = RamUsageEstimator.sizeOf(new long[1]);
  private static final int MASK_4096 = (1 << 12) - 1;

  private static int blockCount(int length) {
    int blockCount = length >>> 12;
    if ((blockCount << 12) < length) {
      ++blockCount;
    }
    assert (blockCount << 12) >= length;
    return blockCount;
  }

  final long[] indices;
  final long[][] bits;
  final int length;
  int nonZeroLongCount;
  long ramBytesUsed;

  /** Create a {@link SparseFixedBitSet} that can contain bits between
   *  <code>0</code> included and <code>length</code> excluded. */
  public SparseFixedBitSet(int length) {
    if (length < 1) {
      throw new IllegalArgumentException("length needs to be >= 1");
    }
    this.length = length;
    final int blockCount = blockCount(length);
    indices = new long[blockCount];
    bits = new long[blockCount][];
    ramBytesUsed = BASE_RAM_BYTES_USED
        + RamUsageEstimator.shallowSizeOf(indices)
        + RamUsageEstimator.shallowSizeOf(bits);
  }

  @Override
  public int length() {
    return length;
  }

  private boolean consistent(int index) {
    assert index >= 0 && index < length : "index=" + index + ",length=" + length;
    return true;
  }

  @Override
  public int cardinality() {
    int cardinality = 0;
    for (long[] bitArray : bits) {
      if (bitArray != null) {
        for (long bits : bitArray) {
          cardinality += Long.bitCount(bits);
        }
      }
    }
    return cardinality;
  }

  @Override
  public int approximateCardinality() {
    // we are assuming that bits are uniformly set and use the linear counting
    // algorithm to estimate the number of bits that are set based on the number
    // of longs that are different from zero
    final int totalLongs = (length + 63) >>> 6; // total number of longs in the space
    assert totalLongs >= nonZeroLongCount;
    final int zeroLongs = totalLongs - nonZeroLongCount; // number of longs that are zeros
    // No need to guard against division by zero, it will return +Infinity and things will work as expected
    final long estimate = Math.round(totalLongs * Math.log((double) totalLongs / zeroLongs));
    return (int) Math.min(length, estimate);
  }

  @Override
  public boolean get(int i) {
    assert consistent(i);
    final int i4096 = i >>> 12;
    final long index = indices[i4096];
    final int i64 = i >>> 6;
    // first check the index, if the i64-th bit is not set, then i is not set
    // note: this relies on the fact that shifts are mod 64 in java
    if ((index & (1L << i64)) == 0) {
      return false;
    }

    // if it is set, then we count the number of bits that are set on the right
    // of i64, and that gives us the index of the long that stores the bits we
    // are interested in
    final long bits = this.bits[i4096][Long.bitCount(index & ((1L << i64) - 1))];
    return (bits & (1L << i)) != 0;
  }

  private static int oversize(int s) {
    int newSize = s + (s >>> 1);
    if (newSize > 50) {
      newSize = 64;
    }
    return newSize;
  }

  /**
   * Set the bit at index <tt>i</tt>.
   */
  public void set(int i) {
    assert consistent(i);
    final int i4096 = i >>> 12;
    final long index = indices[i4096];
    final int i64 = i >>> 6;
    if ((index & (1L << i64)) != 0) {
      // in that case the sub 64-bits block we are interested in already exists,
      // we just need to set a bit in an existing long: the number of ones on
      // the right of i64 gives us the index of the long we need to update
      bits[i4096][Long.bitCount(index & ((1L << i64) - 1))] |= 1L << i; // shifts are mod 64 in java
    } else if (index == 0) {
      // if the index is 0, it means that we just found a block of 4096 bits
      // that has no bit that is set yet. So let's initialize a new block:
      insertBlock(i4096, i64, i);
    } else {
      // in that case we found a block of 4096 bits that has some values, but
      // the sub-block of 64 bits that we are interested in has no value yet,
      // so we need to insert a new long
      insertLong(i4096, i64, i, index);
    }
  }
  
  private void insertBlock(int i4096, int i64, int i) {
    indices[i4096] = 1L << i64; // shifts are mod 64 in java
    assert bits[i4096] == null;
    bits[i4096] = new long[] { 1L << i }; // shifts are mod 64 in java
    ++nonZeroLongCount;
    ramBytesUsed += SINGLE_ELEMENT_ARRAY_BYTES_USED;
  }

  private void insertLong(int i4096, int i64, int i, long index) {
    indices[i4096] |= 1L << i64; // shifts are mod 64 in java
    // we count the number of bits that are set on the right of i64
    // this gives us the index at which to perform the insertion
    final int o = Long.bitCount(index & ((1L << i64) - 1));
    final long[] bitArray = bits[i4096];
    if (bitArray[bitArray.length - 1] == 0) {
      // since we only store non-zero longs, if the last value is 0, it means
      // that we alreay have extra space, make use of it
      System.arraycopy(bitArray, o, bitArray, o + 1, bitArray.length - o - 1);
      bitArray[o] = 1L << i;
    } else {
      // we don't have extra space so we need to resize to insert the new long
      final int newSize = oversize(bitArray.length + 1);
      final long[] newBitArray = new long[newSize];
      System.arraycopy(bitArray, 0, newBitArray, 0, o);
      newBitArray[o] = 1L << i;
      System.arraycopy(bitArray, o, newBitArray, o + 1, bitArray.length - o);
      bits[i4096] = newBitArray;
      ramBytesUsed += (newSize - bitArray.length) * RamUsageEstimator.NUM_BYTES_LONG;
    }
    ++nonZeroLongCount;
  }

  /**
   * Clear the bit at index <tt>i</tt>.
   */
  public void clear(int i) {
    assert consistent(i);
    final int i4096 = i >>> 12;
    final int i64 = i >>> 6;
    clearWithinLong(i4096, i64, ~(1L << i));
  }

  private void clearWithinLong(int i4096, int i64, long mask) {
    final long index = indices[i4096];
    if ((index & (1L << i64)) != 0) {
      // offset of the long bits we are interested in in the array
      final int o = Long.bitCount(index & ((1L << i64) - 1));
      long bits = this.bits[i4096][o] & mask;
      if (bits == 0) {
        removeLong(i4096, i64, index, o);
      } else {
        this.bits[i4096][o] = bits;
      }
    }
  }

  private void removeLong(int i4096, int i64, long index, int o) {
    index &= ~(1L << i64);
    indices[i4096] = index;
    if (index == 0) {
      // release memory, there is nothing in this block anymore
      this.bits[i4096] = null;
    } else {
      final int length = Long.bitCount(index);
      final long[] bitArray = bits[i4096];
      System.arraycopy(bitArray, o + 1, bitArray, o, length - o);
      bitArray[length] = 0L;
    }
  }

  @Override
  public void clear(int from, int to) {
    assert from >= 0;
    assert to <= length;
    if (from >= to) {
      return;
    }
    final int firstBlock = from >>> 12;
    final int lastBlock = (to - 1) >>> 12;
    if (firstBlock == lastBlock) {
      clearWithinBlock(firstBlock, from & MASK_4096, (to - 1) & MASK_4096);
    } else {
      clearWithinBlock(firstBlock, from & MASK_4096, MASK_4096);
      for (int i = firstBlock + 1; i < lastBlock; ++i) {
        indices[i] = 0;
        bits[i] = null;
      }
      clearWithinBlock(lastBlock, 0, (to - 1) & MASK_4096);
    }
  }

  // create a long that has bits set to one between from and to
  private static long mask(int from, int to) {
    return ((1L << (to - from) << 1) - 1) << from;
  }

  private void clearWithinBlock(int i4096, int from, int to) {
    int firstLong = from >>> 6;
    int lastLong = to >>> 6;

    if (firstLong == lastLong) {
      clearWithinLong(i4096, firstLong, ~mask(from, to));
    } else {
      assert firstLong < lastLong;
      clearWithinLong(i4096, lastLong, ~mask(0, to));
      for (int i = lastLong - 1; i >= firstLong + 1; --i) {
        clearWithinLong(i4096, i, 0L);
      }
      clearWithinLong(i4096, firstLong, ~mask(from, 63));
    }
  }

  /** Return the first document that occurs on or after the provided block index. */
  private int firstDoc(int i4096) {
    long index = 0;
    while (i4096 < indices.length) {
      index = indices[i4096];
      if (index != 0) {
        final int i64 = Long.numberOfTrailingZeros(index);
        return (i4096 << 12) | (i64 << 6) | Long.numberOfTrailingZeros(bits[i4096][0]);
      }
      i4096 += 1;
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  @Override
  public int nextSetBit(int i) {
    assert i < length;
    final int i4096 = i >>> 12;
    final long index = indices[i4096];
    int i64 = i >>> 6;
    long indexBits = index >>> i64;
    if (indexBits == 0) {
      // if the index is zero, it means that there is no value in the
      // current block, so return the first document of the next block
      // or
      // if neither the i64-th bit or any other bit on its left is set then
      // it means that there are no more documents in this block, go to the
      // next one
      return firstDoc(i4096 + 1);
    } else {
      // We know we still have some 64-bits blocks that have bits set, let's
      // advance to the next one by skipping trailing zeros of the index
      int i1 = i & 0x3F;
      int trailingZeros = Long.numberOfTrailingZeros(indexBits);
      if (trailingZeros != 0) {
        // no bits in the current long, go to the next one
        i64 += trailingZeros;
        i1 = 0;
      }

      // So now we are on a sub 64-bits block that has values
      assert (index & (1L << i64)) != 0;
      // we count the number of ones on the left of i64 to figure out the
      // index of the long that contains the bits we are interested in
      int longIndex = Long.bitCount(index & ((1L << i64) - 1)); // shifts are mod 64 in java
      final long[] longArray = bits[i4096];
      assert longArray[longIndex] != 0;
      long bits = longArray[longIndex] >>> i1; // shifts are mod 64 in java
      if (bits != 0L) {
        // hurray, we found some non-zero bits, this gives us the next document:
        i1 += Long.numberOfTrailingZeros(bits);
        return (i4096 << 12) | ((i64 & 0x3F) << 6) | i1;
      }

      // otherwise it means that although we were on a sub-64 block that contains
      // documents, all documents of this sub-block have already been consumed
      // so two cases:
      indexBits = index >>> i64 >>> 1; // we don't shift by (i64+1) otherwise we might shift by a multiple of 64 which is a no-op
      if (indexBits == 0) {
        // Case 1: this was the last long of the block of 4096 bits, then go
        // to the next block
        return firstDoc(i4096 + 1);
      }
      // Case 2: go to the next sub 64-bits block in the current block of 4096 bits
      // by skipping trailing zeros of the index
      trailingZeros = Long.numberOfTrailingZeros(indexBits);
      i64 += 1 + trailingZeros;
      bits = longArray[longIndex + 1];
      assert bits != 0;
      i1 = Long.numberOfTrailingZeros(bits);
      return (i4096 << 12) | ((i64 & 0x3F) << 6) | i1;
    }
  }

  @Override
  public void or(DocIdSetIterator it) throws IOException {
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      set(doc);
    }
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Iterable<? extends Accountable> getChildResources() {
    return Collections.emptyList();
  }
}
