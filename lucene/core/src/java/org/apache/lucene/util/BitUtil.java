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


  /** Select a 1-bit from a long. See also LUCENE-6040.
   * @return The index of the r-th 1 bit in x. This bit must exist.
   */
  public static int select(long x, int r) {
    long s = x - ((x & 0xAAAAAAAAAAAAAAAAL) >>> 1); // pairwise bitsums
    s = (s & 0x3333333333333333L) + ((s >>> 2) & 0x3333333333333333L); // nibblewise bitsums
    s = ((s + (s >>> 4)) & 0x0F0F0F0F0F0F0F0FL) * L8_L; // bytewise bitsums, cumulative

    int b = (Long.numberOfTrailingZeros((s + psOverflow[r-1]) & (L8_L << 7)) >> 3) << 3; // bit position of byte with r-th 1 bit.
    long l = r - (((s << 8) >>> b) & 0xFFL); // bit rank in byte at b

    // Select bit l from byte (x >>> b):
    int selectIndex = (int) (((x >>> b) & 0xFFL) | ((l-1) << 8));
    int res = b + select256[selectIndex];
    return res;
  }

  private final static long L8_L = 0x0101010101010101L;

  private static final long[] psOverflow = new long[64];
  static {
    for (int s = 1; s <= 64; s++) {
      psOverflow[s-1] = (128-s) * L8_L;
    }
  }

  private static final byte[] select256 = new byte[8 * 256];
  static {
    for (int b = 0; b <= 0xFF; b++) {
      for (int s = 1; s <= 8; s++) {
        int byteIndex = b | ((s-1) << 8);
        int bitIndex = selectNaive(b, s);
        if (bitIndex < 0) {
          bitIndex = 127; // positive as byte
        }
        assert bitIndex >= 0;
        assert ((byte) bitIndex) >= 0; // non negative as byte, no need to mask the sign
        select256[byteIndex] = (byte) bitIndex;
      }
    }
  }

  /**
   * Naive implementation of {@link #select(long,int)}, using {@link Long#numberOfTrailingZeros} repetitively.
   * Works relatively fast for low ranks.
   * @return The index of the r-th 1 bit in x, or -1 if no such bit exists.
   */
  public static int selectNaive(long x, int r) {
    assert r >= 1;
    int s = -1;
    while ((x != 0L) && (r > 0)) {
      int ntz = Long.numberOfTrailingZeros(x);
      x >>>= (ntz + 1);
      s += (ntz + 1);
      r -= 1;
    }
    int res = (r > 0) ? -1 : s;
    return res;
  }
}
