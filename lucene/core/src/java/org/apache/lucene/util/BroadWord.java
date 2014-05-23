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

/**
 * Methods and constants inspired by the article
 * "Broadword Implementation of Rank/Select Queries" by Sebastiano Vigna, January 30, 2012:
 * <ul>
 * <li>algorithm 1: {@link #bitCount(long)}, count of set bits in a <code>long</code>
 * <li>algorithm 2: {@link #select(long, int)}, selection of a set bit in a <code>long</code>,
 * <li>bytewise signed smaller &lt;<sub><small>8</small></sub> operator: {@link #smallerUpTo7_8(long,long)}.
 * <li>shortwise signed smaller &lt;<sub><small>16</small></sub> operator: {@link #smallerUpto15_16(long,long)}.
 * <li>some of the Lk and Hk constants that are used by the above:
 * L8 {@link #L8_L}, H8 {@link #H8_L}, L9 {@link #L9_L}, L16 {@link #L16_L}and H16 {@link #H8_L}.
 * </ul>
 * @lucene.internal
 */
public final class BroadWord {

// TBD: test smaller8 and smaller16 separately.
  private BroadWord() {} // no instance

  /** Bit count of a long.
   * Only here to compare the implementation with {@link #select(long,int)},
   * normally {@link Long#bitCount} is preferable.
   * @return The total number of 1 bits in x.
   */
  static int bitCount(long x) {
    // Step 0 leaves in each pair of bits the number of ones originally contained in that pair:
    x = x - ((x & 0xAAAAAAAAAAAAAAAAL) >>> 1);
    // Step 1, idem for each nibble:
    x = (x & 0x3333333333333333L) + ((x >>> 2) & 0x3333333333333333L);
    // Step 2, idem for each byte:
    x = (x + (x >>> 4)) & 0x0F0F0F0F0F0F0F0FL;
    // Multiply to sum them all into the high byte, and return the high byte:
    return (int) ((x * L8_L) >>> 56);
  }

  /** Select a 1-bit from a long.
   * @return The index of the r-th 1 bit in x, or if no such bit exists, 72.
   */
  public static int select(long x, int r) {
    long s = x - ((x & 0xAAAAAAAAAAAAAAAAL) >>> 1); // Step 0, pairwise bitsums

    // Correct a small mistake in algorithm 2:
    // Use s instead of x the second time in right shift 2, compare to Algorithm 1 in rank9 above.
    s = (s & 0x3333333333333333L) + ((s >>> 2) & 0x3333333333333333L); // Step 1, nibblewise bitsums

    s = ((s + (s >>> 4)) & 0x0F0F0F0F0F0F0F0FL) * L8_L; // Step 2, bytewise bitsums

    long b = ((smallerUpTo7_8(s, (r * L8_L)) >>> 7) * L8_L) >>> 53; // & (~7L); // Step 3, side ways addition for byte number times 8

    long l = r - (((s << 8) >>> b) & 0xFFL); // Step 4, byte wise rank, subtract the rank with byte at b-8, or zero for b=0;
    assert 0L <= l : l;
    //assert l < 8 : l; //fails when bit r is not available.

    // Select bit l from byte (x >>> b):
    long spr = (((x >>> b) & 0xFFL) * L8_L) & L9_L; // spread the 8 bits of the byte at b over the long at L9 positions

    // long spr_bigger8_zero = smaller8(0L, spr); // inlined smaller8 with 0L argument:
    // FIXME: replace by biggerequal8_one formula from article page 6, line 9. four operators instead of five here.
    long spr_bigger8_zero = ( ( H8_L - (spr & (~H8_L)) ) ^ (~spr) ) & H8_L;
    s = (spr_bigger8_zero >>> 7) * L8_L; // Step 5, sideways byte add the 8 bits towards the high byte

    int res = (int) (b + (((smallerUpTo7_8(s, (l * L8_L)) >>> 7) * L8_L) >>> 56)); // Step 6
    return res;
  }

  /** A signed bytewise smaller &lt;<sub><small>8</small></sub> operator, for operands 0L<= x, y <=0x7L.
   * This uses the following numbers of basic long operations: 1 or, 2 and, 2 xor, 1 minus, 1 not.
   * @return A long with bits set in the {@link #H8_L} positions corresponding to each input signed byte pair that compares smaller.
   */
  public static long smallerUpTo7_8(long x, long y) {
    // See section 4, page 5, line 14 of the Vigna article:
    return ( ( (x | H8_L) - (y & (~H8_L)) ) ^ x ^ ~y) & H8_L;
  }

  /** An unsigned bytewise smaller &lt;<sub><small>8</small></sub> operator.
   * This uses the following numbers of basic long operations: 3 or, 2 and, 2 xor, 1 minus, 1 not.
   * @return A long with bits set in the {@link #H8_L} positions corresponding to each input unsigned byte pair that compares smaller.
   */
  public static long smalleru_8(long x, long y) {
    // See section 4, 8th line from the bottom of the page 5, of the Vigna article:
    return ( ( ( (x | H8_L) - (y & ~H8_L) ) | x ^ y) ^ (x | ~y) ) & H8_L;
  }

  /** An unsigned bytewise not equals 0 operator.
   * This uses the following numbers of basic long operations: 2 or, 1 and, 1 minus.
   * @return A long with bits set in the {@link #H8_L} positions corresponding to each unsigned byte that does not equal 0.
   */
  public static long notEquals0_8(long x) {
    // See section 4, line 6-8 on page 6, of the Vigna article:
    return (((x | H8_L) - L8_L) | x) & H8_L;
  }

  /** A bytewise smaller &lt;<sub><small>16</small></sub> operator.
   * This uses the following numbers of basic long operations: 1 or, 2 and, 2 xor, 1 minus, 1 not.
   * @return A long with bits set in the {@link #H16_L} positions corresponding to each input signed short pair that compares smaller.
   */
  public static long smallerUpto15_16(long x, long y) {
    return ( ( (x | H16_L) - (y & (~H16_L)) ) ^ x ^ ~y) & H16_L;
  }

  /** Lk denotes the constant whose ones are in position 0, k, 2k, . . .
   *  These contain the low bit of each group of k bits.
   *  The suffix _L indicates the long implementation.
   */
  public final static long L8_L = 0x0101010101010101L;
  public final static long L9_L = 0x8040201008040201L;
  public final static long L16_L = 0x0001000100010001L;

  /** Hk = Lk << (k-1) .
   *  These contain the high bit of each group of k bits.
   *  The suffix _L indicates the long implementation.
   */
  public final static long H8_L = L8_L << 7;
  public final static long H16_L = L16_L << 15;

  /**
   * Naive implementation of {@link #select(long,int)}, using {@link Long#numberOfTrailingZeros} repetitively.
   * Works relatively fast for low ranks.
   * @return The index of the r-th 1 bit in x, or if no such bit exists, 72.
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
    int res = (r > 0) ? 72 : s;
    return res;
  }

}
