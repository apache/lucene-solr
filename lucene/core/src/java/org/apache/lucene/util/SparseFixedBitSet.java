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

import org.apache.lucene.search.DocIdSet;
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
public class SparseFixedBitSet extends DocIdSet implements Bits {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SparseFixedBitSet.class);
  private static final long SINGLE_ELEMENT_ARRAY_BYTES_USED = RamUsageEstimator.sizeOf(new long[1]);

  private static int blockCount(int length) {
    int blockCount = length >>> 12;
    if ((blockCount << 12) < length) {
      ++blockCount;
    }
    assert (blockCount << 12) >= length;
    return blockCount;
  }

  private static class Block {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Block.class) + RamUsageEstimator.sizeOf(new long[1]);

    long index;
    long[] bits;

    Block(int bit) {
      this.index = 1L << (bit >>> 6);
      this.bits = new long[] {1L << bit};
    }
  }

  final Block[] blocks;
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
    blocks = new Block[blockCount];
    ramBytesUsed = BASE_RAM_BYTES_USED
        + RamUsageEstimator.shallowSizeOf(blocks);
  }

  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public int length() {
    return length;
  }

  @Override
  public Bits bits() throws IOException {
    return this;
  }

  private boolean consistent(int index) {
    assert index >= 0 && index < length : "index=" + index + ",length=" + length;
    return true;
  }

  /**
   * Compute the cardinality of this set.
   * NOTE: this operation runs in linear time.
   */
  public int cardinality() {
    int cardinality = 0;
    for (Block block : blocks) {
      if (block != null) {
        final int numLongs = Long.bitCount(block.index);
        for (int i = 0; i < numLongs; ++i) {
          cardinality += Long.bitCount(block.bits[i]);
        }
      }
    }
    return cardinality;
  }

  /**
   * Return an approximation of the cardinality of this set, assuming that bits
   * are uniformly distributed. This operation runs in constant time.
   */
  public int approximateCardinality() {
    // this is basically the linear counting algorithm
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
    final Block block = blocks[i4096];
    final int i64 = i >>> 6;
    // first check the index, if the i64-th bit is not set, then i is not set
    // note: this relies on the fact that shifts are mod 64 in java
    if (block == null || (block.index & (1L << i64)) == 0) {
      return false;
    }

    // if it is set, then we count the number of bits that are set on the right
    // of i64, and that gives us the index of the long that stores the bits we
    // are interested in
    final long bits = block.bits[Long.bitCount(block.index & ((1L << i64) - 1))];
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
    final Block block = blocks[i4096];
    final int i64 = i >>> 6;
    if (block == null) {
      blocks[i4096] = new Block(i);
      ++nonZeroLongCount;
      ramBytesUsed += Block.BASE_RAM_BYTES_USED;
    } else if ((block.index & (1L << i64)) != 0) {
      // in that case the sub 64-bits block we are interested in already exists,
      // we just need to set a bit in an existing long: the number of ones on
      // the right of i64 gives us the index of the long we need to update
      block.bits[Long.bitCount(block.index & ((1L << i64) - 1))] |= 1L << i; // shifts are mod 64 in java
    } else {
      // in that case we found a block of 4096 bits that has some values, but
      // the sub-block of 64 bits that we are interested in has no value yet,
      // so we need to insert a new long
      insertLong(block, i64, i);
    }
  }

  private void insertLong(Block block, int i64, int i) {
    block.index |= 1L << i64; // shifts are mod 64 in java
    // we count the number of bits that are set on the right of i64
    // this gives us the index at which to perform the insertion
    final int o = Long.bitCount(block.index & ((1L << i64) - 1));
    if (block.bits[block.bits.length - 1] == 0) {
      // since we only store non-zero longs, if the last value is 0, it means
      // that we alreay have extra space, make use of it
      System.arraycopy(block.bits, o, block.bits, o + 1, block.bits.length - o - 1);
      block.bits[o] = 1L << i;
    } else {
      // we don't have extra space so we need to resize to insert the new long
      final int newSize = oversize(block.bits.length + 1);
      final long[] newBitArray = new long[newSize];
      System.arraycopy(block.bits, 0, newBitArray, 0, o);
      newBitArray[o] = 1L << i;
      System.arraycopy(block.bits, o, newBitArray, o + 1, block.bits.length - o);
      block.bits = newBitArray;
      ramBytesUsed += (newSize - block.bits.length) * RamUsageEstimator.NUM_BYTES_LONG;
    }
    ++nonZeroLongCount;
  }

  /**
   * Add the documents contained in the provided {@link DocIdSetIterator} to
   * this bit set.
   */
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
  public DocIdSetIterator iterator() throws IOException {
    return new Iterator();
  }

  class Iterator extends DocIdSetIterator {

    private int doc = -1;
    private int cost = -1;

    @Override
    public int docID() {
      return doc;
    }

    /** Return the first document that occurs on or after the provided block index. */
    private int firstDoc(int i4096) {
      Block block = null;
      while (i4096 < blocks.length) {
        block = blocks[i4096];
        if (block != null) {
          final int i64 = Long.numberOfTrailingZeros(block.index);
          return doc = (i4096 << 12) | (i64 << 6) | Long.numberOfTrailingZeros(block.bits[0]);
        }
        i4096 += 1;
      }
      return doc = NO_MORE_DOCS;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      final int i4096 = target >>> 12;
      if (i4096 >= blocks.length) {
        return doc = NO_MORE_DOCS;
      }
      final Block block = blocks[i4096];
      if (block == null) {
        return firstDoc(i4096 + 1);
      }
      int i64 = target >>> 6;
      long indexBits = block.index >>> i64;
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
        int i1 = target & 0x3F;
        int trailingZeros = Long.numberOfTrailingZeros(indexBits);
        if (trailingZeros != 0) {
          // no bits in the current long, go to the next one
          i64 += trailingZeros;
          i1 = 0;
        }

        // So now we are on a sub 64-bits block that has values
        assert (block.index & (1L << i64)) != 0;
        // we count the number of ones on the left of i64 to figure out the
        // index of the long that contains the bits we are interested in
        int longIndex = Long.bitCount(block.index & ((1L << i64) - 1)); // shifts are mod 64 in java
        assert block.bits[longIndex] != 0;
        long bits = block.bits[longIndex] >>> i1; // shifts are mod 64 in java
        if (bits != 0L) {
          // hurray, we found some non-zero bits, this gives us the next document:
          i1 += Long.numberOfTrailingZeros(bits);
          return doc = (i4096 << 12) | ((i64 & 0x3F) << 6) | i1;
        }

        // otherwise it means that although we were on a sub-64 block that contains
        // documents, all documents of this sub-block have already been consumed
        // so two cases:
        indexBits = block.index >>> i64 >>> 1; // we don't shift by (i64+1) otherwise we might shift by a multiple of 64 which is a no-op
        if (indexBits == 0) {
          // Case 1: this was the last long of the block of 4096 bits, then go
          // to the next block
          return firstDoc(i4096 + 1);
        }
        // Case 2: go to the next sub 64-bits block in the current block of 4096 bits
        // by skipping trailing zeros of the index
        trailingZeros = Long.numberOfTrailingZeros(indexBits);
        i64 += 1 + trailingZeros;
        bits = block.bits[longIndex + 1];
        assert bits != 0;
        i1 = Long.numberOfTrailingZeros(bits);
        return doc = (i4096 << 12) | ((i64 & 0x3F) << 6) | i1;
      }
    }

    @Override
    public long cost() {
      // although constant-time, approximateCardinality is a bit expensive so
      // we cache it to avoid performance traps eg. when sorting iterators by
      // cost
      if (cost < 0) {
        cost = approximateCardinality();
      }
      assert cost >= 0;
      return cost;
    }

  }

}
