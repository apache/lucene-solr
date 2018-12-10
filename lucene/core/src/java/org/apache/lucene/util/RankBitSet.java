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
import java.util.Collection;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * Wrapper for OpenBitSet which creates and exposes a rank cache. The rank-cache scales to 2 billion bits.
 *
 * The rankCache has a long for every 2048 bits and thus has an overhead of 3.17% relative to the given bit set.
 * Performance is O(1):
 * 1 lookup in cache,
 * a maximum of 3 sums,
 * a maximum of 8 Long.bitCounts.
 *
 * Creation performance is equivalent to a full count of all set bits in the bit set O(n).
 *
 * Note: {@link #buildRankCache()} must be called once after the bit set has been created or changed and before
 * calling {@link #rank(long)}.
 *
 * The design is based heavily on the article
 * Space-Efficient, High-Performance Rank and Select Structures on Uncompressed Bit Sequences
 * by Dong Zhou, David G. Andersen, Michael Kaminsky, Carnegie Mellon University, Intel Labs
 * http://www.cs.cmu.edu/~dga/papers/zhou-sea2013.pdf
 */
// Note: If the total number of set bits is <= 65535, a faster rank cache would be a short for every 512 bits
// This is not available in the current implementation.
// Extending the rank beyond 2 billion bits could be done by dividing the bitmap into blocks of 2b bits and
// introducing yet another table with a rank-origo for each 2b-block
public class RankBitSet extends BitSet {
  public static final int  LOWER_BITS = 32; // Must be capable of addressing full Java array
  public static final long LOWER_MASK = ~(~1L << (LOWER_BITS-1));
  public static final int LOWER_OVER_BITS = 11;
  public static final long LOWER_OVER_MASK = ~(~1L << (LOWER_OVER_BITS-1));
  public static final int LOWER_OVER_SIZE = 2048; // Overflow bits handled by a lower block

  public static final int BASIC_BITS = 10; // Needs to hold counts from 0-512 (513-1023 are never used)
  public static final long BASIC_MASK = ~(~1L << (BASIC_BITS-1));
  public static final int BASIC_OVER_BITS = 9;
  public static final long BASIC_OVER_MASK = ~(~1L << (BASIC_OVER_BITS-1));
  public static final int BASIC_OVER_SIZE = 512; // Overflow bits handled by a basic block
  public static final int BASIC_WORDS = BASIC_OVER_SIZE /Long.SIZE; // word == long
  /**
   * Each entry is made up of 1 long<br/>
   * Bits 63-32: 32 bit first-level absolute index.<br/>
   * Bits 30+31 are unused. These could be used to signal all-set or all-unset for the block to spare a few cycles?
   * Bits 29-0: 3 * 10 bit (0-1023) second-level relative index. Only numbers 0-512 are used.
   */
  private long[] rankCache = null;
  private final FixedBitSet inner;
  private final int wlen; // Number of words (longs with FixedBitSet) in inner

  public RankBitSet(int numBits) {
    this(new FixedBitSet(numBits));
  }

  public RankBitSet(FixedBitSet inner) {
    this.inner = inner;
    wlen = inner.getBits().length;
  }

  /**
   * Must be called after bits has changed and before {@link #rank} is called.
   */
  public void buildRankCache() {
    rankCache = new long[(length() >>> LOWER_OVER_BITS)+1];
    long total = 0;
    int lower = 0 ;
    while (lower * LOWER_OVER_SIZE < length()) { // Full lower block processing
      final int origoWordIndex = (lower * LOWER_OVER_SIZE) >>> 6;
    // TODO: Some conditionals could be spared by checking once if all basic blocks are within size
      final long basic1 = origoWordIndex + BASIC_WORDS <= wlen ?
          BitUtil.pop_array(inner.getBits(), origoWordIndex, BASIC_WORDS) : 0;
      final long basic2 =  origoWordIndex + BASIC_WORDS*2 <= wlen ?
          BitUtil.pop_array(inner.getBits(), origoWordIndex + BASIC_WORDS, BASIC_WORDS) : 0;
      final long basic3 =  origoWordIndex + BASIC_WORDS*3 <= wlen ?
          BitUtil.pop_array(inner.getBits(), origoWordIndex + BASIC_WORDS *2, BASIC_WORDS) : 0;
      final long basic4 =  origoWordIndex + BASIC_WORDS*4 <= wlen ?
          BitUtil.pop_array(inner.getBits(), origoWordIndex + BASIC_WORDS *3, BASIC_WORDS) : 0;
      rankCache[lower] = total << (Long.SIZE-LOWER_BITS) |
           basic1 << (BASIC_BITS *2) |
           basic2 << BASIC_BITS |
           basic3;
      total += basic1 + basic2 + basic3 + basic4;
      lower++;
    }
  }

  /**
   * Get the rank (number of set bits up to right before the index) for the given index in O(1).
   * @param index offset in the originating bit set.
   * @return the rank for the index.
   */
  public int rank(long index) {
    final long cache = rankCache[((int) (index >>> LOWER_OVER_BITS))];
    // lower cache (absolute)
    long rank = cache >>> (Long.SIZE-LOWER_BITS);
    int startBitIndex = (int) (index & ~LOWER_OVER_MASK);
    // basic blocks (relative)
    if (startBitIndex < index-BASIC_OVER_SIZE) {
      rank += (cache >>> (BASIC_BITS*2)) & BASIC_MASK;
      startBitIndex += BASIC_OVER_SIZE;
      if (startBitIndex < index-BASIC_OVER_SIZE) {
        rank += (cache >>> BASIC_BITS) & BASIC_MASK;
        startBitIndex += BASIC_OVER_SIZE;
        if (startBitIndex < index-BASIC_OVER_SIZE) {
          rank += cache & BASIC_MASK;
          startBitIndex += BASIC_OVER_SIZE;
        }
      }
    }
    // long.bitcount (relative)
    while(startBitIndex < index-Long.SIZE) {
      rank += Long.bitCount(inner.getBits()[startBitIndex >>> 6]);
      startBitIndex += Long.SIZE;
    }
    // Single bits (relative)
    if (startBitIndex < index) {
/*      System.out.println(String.format(Locale.ENGLISH,
          "startBitIndex=%d, index=%d, getBits()[startBitIndex>>>6=%d]=%s, index-startBitIndex=%d, mask=%s",
          startBitIndex, index, startBitIndex>>>6, Long.toBinaryString(getBits()[startBitIndex>>>6]),
          index-startBitIndex, Long.toBinaryString(~(~1L << (index-startBitIndex-1)))));*/
      rank += Long.bitCount(inner.getBits()[startBitIndex >>> 6] & ~(~1L << (index-startBitIndex-1)));
    }
//    for (int i = startBitIndex ; i < index ; i++) {
//      rank += fastGet(i) ? 1 : 0;
//    }
    return (int) rank;
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_OBJECT_REF*2 +
        Integer.BYTES + Long.BYTES) +
        inner.ramBytesUsed() +
        (rankCache == null ? 0 :
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + Long.BYTES*rankCache.length);
  }

  /* Delegations to inner bit set below */

  public static FixedBitSet ensureCapacity(FixedBitSet bits, int numBits) {
    return FixedBitSet.ensureCapacity(bits, numBits);
  }

  public static int bits2words(int numBits) {
    return FixedBitSet.bits2words(numBits);
  }

  public static long intersectionCount(FixedBitSet a, FixedBitSet b) {
    return FixedBitSet.intersectionCount(a, b);
  }

  public static long unionCount(FixedBitSet a, FixedBitSet b) {
    return FixedBitSet.unionCount(a, b);
  }

  public static long andNotCount(FixedBitSet a, FixedBitSet b) {
    return FixedBitSet.andNotCount(a, b);
  }

  @Override
  public int length() {
    return inner.length();
  }

  public long[] getBits() {
    return inner.getBits();
  }

  @Override
  public int cardinality() {
    return inner.cardinality();
  }

  @Override
  public boolean get(int index) {
    return inner.get(index);
  }

  @Override
  public void set(int index) {
    inner.set(index);
  }

  public boolean getAndSet(int index) {
    return inner.getAndSet(index);
  }

  @Override
  public void clear(int index) {
    inner.clear(index);
  }

  public boolean getAndClear(int index) {
    return inner.getAndClear(index);
  }

  @Override
  public int nextSetBit(int index) {
    return inner.nextSetBit(index);
  }

  @Override
  public int prevSetBit(int index) {
    return inner.prevSetBit(index);
  }

  @Override
  public void or(DocIdSetIterator iter) throws IOException {
    inner.or(iter);
  }

  public void or(FixedBitSet other) {
    inner.or(other);
  }

  public void xor(FixedBitSet other) {
    inner.xor(other);
  }

  public void xor(DocIdSetIterator iter) throws IOException {
    inner.xor(iter);
  }

  public boolean intersects(FixedBitSet other) {
    return inner.intersects(other);
  }

  public void and(FixedBitSet other) {
    inner.and(other);
  }

  public void andNot(FixedBitSet other) {
    inner.andNot(other);
  }

  public boolean scanIsEmpty() {
    return inner.scanIsEmpty();
  }

  public void flip(int startIndex, int endIndex) {
    inner.flip(startIndex, endIndex);
  }

  public void flip(int index) {
    inner.flip(index);
  }

  public void set(int startIndex, int endIndex) {
    inner.set(startIndex, endIndex);
  }

  @Override
  public void clear(int startIndex, int endIndex) {
    inner.clear(startIndex, endIndex);
  }

  @Override
  public int hashCode() {
    return inner.hashCode();
  }

  public static FixedBitSet copyOf(Bits bits) {
    return FixedBitSet.copyOf(bits);
  }

  public Bits asReadOnlyBits() {
    return inner.asReadOnlyBits();
  }

  public static BitSet of(DocIdSetIterator it, int maxDoc) throws IOException {
    return BitSet.of(it, maxDoc);
  }

  @Override
  public int approximateCardinality() {
    return inner.approximateCardinality();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return inner.getChildResources();
  }
}
