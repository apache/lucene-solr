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
package org.apache.lucene.util; // from org.apache.solr.util rev 555343

/**  A variety of high efficiency bit twiddling routines.
 * @lucene.internal
 */
public final class BitUtil {

  private BitUtil() {} // no instance

  // The pop methods used to rely on bit-manipulation tricks for speed but it
  // turns out that it is faster to use the Long.bitCount method (which is an
  // intrinsic since Java 6u18) in a naive loop, see LUCENE-2221

  /** Returns the number of set bits in an array of longs. */
  public static long pop_array(long[] arr, int wordOffset, int numWords) {
    long popCount = 0;
    for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
      popCount += Long.bitCount(arr[i]);
    }
    return popCount;
  }

  /** Returns the popcount or cardinality of the two sets after an intersection.
   *  Neither array is modified. */
  public static long pop_intersect(long[] arr1, long[] arr2, int wordOffset, int numWords) {
    long popCount = 0;
    for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
      popCount += Long.bitCount(arr1[i] & arr2[i]);
    }
    return popCount;
  }

   /** Returns the popcount or cardinality of the union of two sets.
    *  Neither array is modified. */
   public static long pop_union(long[] arr1, long[] arr2, int wordOffset, int numWords) {
     long popCount = 0;
     for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
       popCount += Long.bitCount(arr1[i] | arr2[i]);
     }
     return popCount;
   }

  /** Returns the popcount or cardinality of {@code A & ~B}.
   *  Neither array is modified. */
  public static long pop_andnot(long[] arr1, long[] arr2, int wordOffset, int numWords) {
    long popCount = 0;
    for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
      popCount += Long.bitCount(arr1[i] & ~arr2[i]);
    }
    return popCount;
  }

  /** Returns the popcount or cardinality of A ^ B
    * Neither array is modified. */
  public static long pop_xor(long[] arr1, long[] arr2, int wordOffset, int numWords) {
    long popCount = 0;
    for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
      popCount += Long.bitCount(arr1[i] ^ arr2[i]);
    }
    return popCount;
  }

  /** returns the next highest power of two, or the current value if it's already a power of two or zero*/
  public static int nextHighestPowerOfTwo(int v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
  }

  /** returns the next highest power of two, or the current value if it's already a power of two or zero*/
   public static long nextHighestPowerOfTwo(long v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
  }

  // magic numbers for bit interleaving
  private static final long MAGIC0 = 0x5555555555555555L;
  private static final long MAGIC1 = 0x3333333333333333L;
  private static final long MAGIC2 = 0x0F0F0F0F0F0F0F0FL;
  private static final long MAGIC3 = 0x00FF00FF00FF00FFL;
  private static final long MAGIC4 = 0x0000FFFF0000FFFFL;
  private static final long MAGIC5 = 0x00000000FFFFFFFFL;
  private static final long MAGIC6 = 0xAAAAAAAAAAAAAAAAL;

  // shift values for bit interleaving
  private static final long SHIFT0 = 1;
  private static final long SHIFT1 = 2;
  private static final long SHIFT2 = 4;
  private static final long SHIFT3 = 8;
  private static final long SHIFT4 = 16;

  /**
   * Interleaves the first 32 bits of each long value
   *
   * Adapted from: http://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
   */
  public static long interleave(int even, int odd) {
    long v1 = 0x00000000FFFFFFFFL & even;
    long v2 = 0x00000000FFFFFFFFL & odd;
    v1 = (v1 | (v1 << SHIFT4)) & MAGIC4;
    v1 = (v1 | (v1 << SHIFT3)) & MAGIC3;
    v1 = (v1 | (v1 << SHIFT2)) & MAGIC2;
    v1 = (v1 | (v1 << SHIFT1)) & MAGIC1;
    v1 = (v1 | (v1 << SHIFT0)) & MAGIC0;
    v2 = (v2 | (v2 << SHIFT4)) & MAGIC4;
    v2 = (v2 | (v2 << SHIFT3)) & MAGIC3;
    v2 = (v2 | (v2 << SHIFT2)) & MAGIC2;
    v2 = (v2 | (v2 << SHIFT1)) & MAGIC1;
    v2 = (v2 | (v2 << SHIFT0)) & MAGIC0;

    return (v2<<1) | v1;
  }

  /**
   * Extract just the even-bits value as a long from the bit-interleaved value
   */
  public static long deinterleave(long b) {
    b &= MAGIC0;
    b = (b ^ (b >>> SHIFT0)) & MAGIC1;
    b = (b ^ (b >>> SHIFT1)) & MAGIC2;
    b = (b ^ (b >>> SHIFT2)) & MAGIC3;
    b = (b ^ (b >>> SHIFT3)) & MAGIC4;
    b = (b ^ (b >>> SHIFT4)) & MAGIC5;
    return b;
  }

  /**
   * flip flops odd with even bits
   */
  public static final long flipFlop(final long b) {
    return ((b & MAGIC6) >>> 1) | ((b & MAGIC0) << 1 );
  }

   /** Same as {@link #zigZagEncode(long)} but on integers. */
   public static int zigZagEncode(int i) {
     return (i >> 31) ^ (i << 1);
   }

   /**
    * <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">Zig-zag</a>
    * encode the provided long. Assuming the input is a signed long whose
    * absolute value can be stored on <tt>n</tt> bits, the returned value will
    * be an unsigned long that can be stored on <tt>n+1</tt> bits.
    */
   public static long zigZagEncode(long l) {
     return (l >> 63) ^ (l << 1);
   }

   /** Decode an int previously encoded with {@link #zigZagEncode(int)}. */
   public static int zigZagDecode(int i) {
     return ((i >>> 1) ^ -(i & 1));
   }

   /** Decode a long previously encoded with {@link #zigZagEncode(long)}. */
   public static long zigZagDecode(long l) {
     return ((l >>> 1) ^ -(l & 1));
   }

  /**
   * Returns whether the bit at given zero-based index is set.
   * <br>Example: bitIndex 66 means the third bit on the right of the second long.
   *
   * @param bits     The bits stored in an array of long for efficiency.
   * @param numLongs The number of longs in {@code bits} to consider.
   * @param bitIndex The bit zero-based index. It must be greater than or equal to 0,
   *                 and strictly less than {@code numLongs * Long.SIZE}.
   */
  public static boolean isBitSet(long[] bits, int numLongs, int bitIndex) {
    assert numLongs >= 0 && numLongs <= bits.length && bitIndex >= 0 && bitIndex < numLongs * Long.SIZE
        : "bitIndex=" + bitIndex + " numLongs=" + numLongs + " bits.length=" + bits.length;
    return (bits[bitIndex / Long.SIZE] & (1L << bitIndex)) != 0; // Shifts are mod 64.
  }

  /**
   * Counts all bits set in the provided longs.
   *
   * @param bits     The bits stored in an array of long for efficiency.
   * @param numLongs The number of longs in {@code bits} to consider.
   */
  public static int countBits(long[] bits, int numLongs) {
    assert numLongs >= 0 && numLongs <= bits.length
        : "numLongs=" + numLongs + " bits.length=" + bits.length;
    int bitCount = 0;
    for (int i = 0; i < numLongs; i++) {
      bitCount += Long.bitCount(bits[i]);
    }
    return bitCount;
  }

  /**
   * Counts the bits set up to the given bit zero-based index, exclusive.
   * <br>In other words, how many 1s there are up to the bit at the given index excluded.
   * <br>Example: bitIndex 66 means the third bit on the right of the second long.
   *
   * @param bits     The bits stored in an array of long for efficiency.
   * @param numLongs The number of longs in {@code bits} to consider.
   * @param bitIndex The bit zero-based index, exclusive. It must be greater than or equal to 0,
   *                 and less than or equal to {@code numLongs * Long.SIZE}.
   */
  public static int countBitsUpTo(long[] bits, int numLongs, int bitIndex) {
    assert numLongs >= 0 && numLongs <= bits.length && bitIndex >= 0 && bitIndex <= numLongs * Long.SIZE
        : "bitIndex=" + bitIndex + " numLongs=" + numLongs + " bits.length=" + bits.length;
    int bitCount = 0;
    int lastLong = bitIndex / Long.SIZE;
    for (int i = 0; i < lastLong; i++) {
      // Count the bits set for all plain longs.
      bitCount += Long.bitCount(bits[i]);
    }
    if (lastLong < numLongs) {
      // Prepare a mask with 1s on the right up to bitIndex exclusive.
      long mask = (1L << bitIndex) - 1L; // Shifts are mod 64.
      // Count the bits set only within the mask part, so up to bitIndex exclusive.
      bitCount += Long.bitCount(bits[lastLong] & mask);
    }
    return bitCount;
  }

  /**
   * Returns the index of the next bit set following the given bit zero-based index.
   * <br>For example with bits 100011:
   * the next bit set after index=-1 is at index=0;
   * the next bit set after index=0 is at index=1;
   * the next bit set after index=1 is at index=5;
   * there is no next bit set after index=5.
   *
   * @param bits     The bits stored in an array of long for efficiency.
   * @param numLongs The number of longs in {@code bits} to consider.
   * @param bitIndex The bit zero-based index. It must be greater than or equal to -1,
   *                 and strictly less than {@code numLongs * Long.SIZE}.
   * @return The zero-based index of the next bit set after the provided {@code bitIndex};
   * or -1 if none.
   */
  public static int nextBitSet(long[] bits, int numLongs, int bitIndex) {
    assert numLongs >= 0 && numLongs <= bits.length && bitIndex >= -1 && bitIndex < numLongs * Long.SIZE
        : "bitIndex=" + bitIndex + " numLongs=" + numLongs + " bits.length=" + bits.length;
    int longIndex = bitIndex / Long.SIZE;
    // Prepare a mask with 1s on the left down to bitIndex exclusive.
    long mask = -(1L << (bitIndex + 1)); // Shifts are mod 64.
    long l = mask == -1 && bitIndex != -1 ? 0 : bits[longIndex] & mask;
    while (l == 0) {
      if (++longIndex == numLongs) {
        return -1;
      }
      l = bits[longIndex];
    }
    return Long.numberOfTrailingZeros(l) + longIndex * 64;
  }

  /**
   * Returns the index of the previous bit set preceding the given bit zero-based index.
   * <br>For example with bits 100011:
   * there is no previous bit set before index=0.
   * the previous bit set before index=1 is at index=0;
   * the previous bit set before index=5 is at index=1;
   * the previous bit set before index=64 is at index=5;
   *
   * @param bits     The bits stored in an array of long for efficiency.
   * @param numLongs The number of longs in {@code bits} to consider.
   * @param bitIndex The bit zero-based index. It must be greater than or equal to 0,
   *                 and less than or equal to {@code numLongs * Long.SIZE}.
   * @return The zero-based index of the previous bit set before the provided {@code bitIndex};
   * or -1 if none.
   */
  public static int previousBitSet(long[] bits, int numLongs, int bitIndex) {
    assert numLongs >= 0 && numLongs <= bits.length && bitIndex >= 0 && bitIndex <= numLongs * Long.SIZE
        : "bitIndex=" + bitIndex + " numLongs=" + numLongs + " bits.length=" + bits.length;
    int longIndex = bitIndex / Long.SIZE;
    long l;
    if (longIndex == numLongs) {
      l = 0;
    } else {
      // Prepare a mask with 1s on the right up to bitIndex exclusive.
      long mask = (1L << bitIndex) - 1L; // Shifts are mod 64.
      l = bits[longIndex] & mask;
    }
    while (l == 0) {
      if (longIndex-- == 0) {
        return -1;
      }
      l = bits[longIndex];
    }
    return 63 - Long.numberOfLeadingZeros(l) + longIndex * 64;
  }
}
