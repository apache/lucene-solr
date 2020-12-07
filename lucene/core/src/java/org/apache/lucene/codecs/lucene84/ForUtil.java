// This file has been automatically generated, DO NOT EDIT

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
package org.apache.lucene.codecs.lucene84;

import java.io.IOException;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

// Inspired from https://fulmicoton.com/posts/bitpacking/
// Encodes multiple integers in a long to get SIMD-like speedups.
// If bitsPerValue <= 8 then we pack 8 ints per long
// else if bitsPerValue <= 16 we pack 4 ints per long
// else we pack 2 ints per long
final class ForUtil {

  static final int BLOCK_SIZE = 128;
  private static final int BLOCK_SIZE_LOG2 = 7;

  private static long expandMask32(long mask32) {
    return mask32 | (mask32 << 32);
  }

  private static long expandMask16(long mask16) {
    return expandMask32(mask16 | (mask16 << 16));
  }

  private static long expandMask8(long mask8) {
    return expandMask16(mask8 | (mask8 << 8));
  }

  private static long mask32(int bitsPerValue) {
    return expandMask32((1L << bitsPerValue) - 1);
  }

  private static long mask16(int bitsPerValue) {
    return expandMask16((1L << bitsPerValue) - 1);
  }

  private static long mask8(int bitsPerValue) {
    return expandMask8((1L << bitsPerValue) - 1);
  }

  private static void expand8(long[] arr) {
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 56) & 0xFFL;
      arr[16+i] = (l >>> 48) & 0xFFL;
      arr[32+i] = (l >>> 40) & 0xFFL;
      arr[48+i] = (l >>> 32) & 0xFFL;
      arr[64+i] = (l >>> 24) & 0xFFL;
      arr[80+i] = (l >>> 16) & 0xFFL;
      arr[96+i] = (l >>> 8) & 0xFFL;
      arr[112+i] = l & 0xFFL;
    }
  }

  private static void expand8To32(long[] arr) {
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 24) & 0x000000FF000000FFL;
      arr[16+i] = (l >>> 16) & 0x000000FF000000FFL;
      arr[32+i] = (l >>> 8) & 0x000000FF000000FFL;
      arr[48+i] = l & 0x000000FF000000FFL;
    }
  }

  private static void collapse8(long[] arr) {
    for (int i = 0; i < 16; ++i) {
      arr[i] = (arr[i] << 56) | (arr[16+i] << 48) | (arr[32+i] << 40) | (arr[48+i] << 32) | (arr[64+i] << 24) | (arr[80+i] << 16) | (arr[96+i] << 8) | arr[112+i];
    }
  }

  private static void expand16(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 48) & 0xFFFFL;
      arr[32+i] = (l >>> 32) & 0xFFFFL;
      arr[64+i] = (l >>> 16) & 0xFFFFL;
      arr[96+i] = l & 0xFFFFL;
    }
  }

  private static void expand16To32(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 16) & 0x0000FFFF0000FFFFL;
      arr[32+i] = l & 0x0000FFFF0000FFFFL;
    }
  }

  private static void collapse16(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      arr[i] = (arr[i] << 48) | (arr[32+i] << 32) | (arr[64+i] << 16) | arr[96+i];
    }
  }

  private static void expand32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      arr[i] = l >>> 32;
      arr[64 + i] = l & 0xFFFFFFFFL;
    }
  }

  private static void collapse32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      arr[i] = (arr[i] << 32) | arr[64+i];
    }
  }

  private static void prefixSum8(long[] arr, long base) {
    expand8To32(arr);
    prefixSum32(arr, base);
  }

  private static void prefixSum16(long[] arr, long base) {
    // We need to move to the next primitive size to avoid overflows
    expand16To32(arr);
    prefixSum32(arr, base);
  }

  private static void prefixSum32(long[] arr, long base) {
    arr[0] += base << 32;
    innerPrefixSum32(arr);
    expand32(arr);
    final long l = arr[BLOCK_SIZE/2-1];
    for (int i = BLOCK_SIZE/2; i < BLOCK_SIZE; ++i) {
      arr[i] += l;
    }
  }

  // For some reason unrolling seems to help
  private static void innerPrefixSum32(long[] arr) {
    arr[1] += arr[0];
    arr[2] += arr[1];
    arr[3] += arr[2];
    arr[4] += arr[3];
    arr[5] += arr[4];
    arr[6] += arr[5];
    arr[7] += arr[6];
    arr[8] += arr[7];
    arr[9] += arr[8];
    arr[10] += arr[9];
    arr[11] += arr[10];
    arr[12] += arr[11];
    arr[13] += arr[12];
    arr[14] += arr[13];
    arr[15] += arr[14];
    arr[16] += arr[15];
    arr[17] += arr[16];
    arr[18] += arr[17];
    arr[19] += arr[18];
    arr[20] += arr[19];
    arr[21] += arr[20];
    arr[22] += arr[21];
    arr[23] += arr[22];
    arr[24] += arr[23];
    arr[25] += arr[24];
    arr[26] += arr[25];
    arr[27] += arr[26];
    arr[28] += arr[27];
    arr[29] += arr[28];
    arr[30] += arr[29];
    arr[31] += arr[30];
    arr[32] += arr[31];
    arr[33] += arr[32];
    arr[34] += arr[33];
    arr[35] += arr[34];
    arr[36] += arr[35];
    arr[37] += arr[36];
    arr[38] += arr[37];
    arr[39] += arr[38];
    arr[40] += arr[39];
    arr[41] += arr[40];
    arr[42] += arr[41];
    arr[43] += arr[42];
    arr[44] += arr[43];
    arr[45] += arr[44];
    arr[46] += arr[45];
    arr[47] += arr[46];
    arr[48] += arr[47];
    arr[49] += arr[48];
    arr[50] += arr[49];
    arr[51] += arr[50];
    arr[52] += arr[51];
    arr[53] += arr[52];
    arr[54] += arr[53];
    arr[55] += arr[54];
    arr[56] += arr[55];
    arr[57] += arr[56];
    arr[58] += arr[57];
    arr[59] += arr[58];
    arr[60] += arr[59];
    arr[61] += arr[60];
    arr[62] += arr[61];
    arr[63] += arr[62];
  }

  private final long[] tmp = new long[BLOCK_SIZE/2];

  /**
   * Encode 128 integers from {@code longs} into {@code out}.
   */
  void encode(long[] longs, int bitsPerValue, DataOutput out) throws IOException {
    final int nextPrimitive;
    final int numLongs;
    if (bitsPerValue <= 8) {
      nextPrimitive = 8;
      numLongs = BLOCK_SIZE / 8;
      collapse8(longs);
    } else if (bitsPerValue <= 16) {
      nextPrimitive = 16;
      numLongs = BLOCK_SIZE / 4;
      collapse16(longs);
    } else {
      nextPrimitive = 32;
      numLongs = BLOCK_SIZE / 2;
      collapse32(longs);
    }

    final int numLongsPerShift = bitsPerValue * 2;
    int idx = 0;
    int shift = nextPrimitive - bitsPerValue;
    for (int i = 0; i < numLongsPerShift; ++i) {
      tmp[i] = longs[idx++] << shift;
    }
    for (shift = shift - bitsPerValue; shift >= 0; shift -= bitsPerValue) {
      for (int i = 0; i < numLongsPerShift; ++i) {
        tmp[i] |= longs[idx++] << shift;
      }
    }

    final int remainingBitsPerLong = shift + bitsPerValue;
    final long maskRemainingBitsPerLong;
    if (nextPrimitive == 8) {
      maskRemainingBitsPerLong = MASKS8[remainingBitsPerLong];
    } else if (nextPrimitive == 16) {
      maskRemainingBitsPerLong = MASKS16[remainingBitsPerLong];
    } else {
      maskRemainingBitsPerLong = MASKS32[remainingBitsPerLong];
    }

    int tmpIdx = 0;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < numLongs) {
      if (remainingBitsPerValue >= remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        tmp[tmpIdx++] |= (longs[idx] >>> remainingBitsPerValue) & maskRemainingBitsPerLong;
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        final long mask1, mask2;
        if (nextPrimitive == 8) {
          mask1 = MASKS8[remainingBitsPerValue];
          mask2 = MASKS8[remainingBitsPerLong - remainingBitsPerValue];
        } else if (nextPrimitive == 16) {
          mask1 = MASKS16[remainingBitsPerValue];
          mask2 = MASKS16[remainingBitsPerLong - remainingBitsPerValue];
        } else {
          mask1 = MASKS32[remainingBitsPerValue];
          mask2 = MASKS32[remainingBitsPerLong - remainingBitsPerValue];
        }
        tmp[tmpIdx] |= (longs[idx++] & mask1) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        tmp[tmpIdx++] |= (longs[idx] >>> remainingBitsPerValue) & mask2;
      }
    }

    for (int i = 0; i < numLongsPerShift; ++i) {
      // Java longs are big endian and we want to read little endian longs, so we need to reverse bytes
      long l = Long.reverseBytes(tmp[i]);
      out.writeLong(l);
    }
  }

  /**
   * Number of bytes required to encode 128 integers of {@code bitsPerValue} bits per value.
   */
  int numBytes(int bitsPerValue) throws IOException {
    return bitsPerValue << (BLOCK_SIZE_LOG2 - 3);
  }

  private static void decodeSlow(int bitsPerValue, DataInput in, long[] tmp, long[] longs) throws IOException {
    final int numLongs = bitsPerValue << 1;
    in.readLELongs(tmp, 0, numLongs);
    final long mask = MASKS32[bitsPerValue];
    int longsIdx = 0;
    int shift = 32 - bitsPerValue;
    for (; shift >= 0; shift -= bitsPerValue) {
      shiftLongs(tmp, numLongs, longs, longsIdx, shift, mask);
      longsIdx += numLongs;
    }
    final int remainingBitsPerLong = shift + bitsPerValue;
    final long mask32RemainingBitsPerLong = MASKS32[remainingBitsPerLong];
    int tmpIdx = 0;
    int remainingBits = remainingBitsPerLong;
    for (; longsIdx < BLOCK_SIZE / 2; ++longsIdx) {
      int b = bitsPerValue - remainingBits;
      long l = (tmp[tmpIdx++] & MASKS32[remainingBits]) << b;
      while (b >= remainingBitsPerLong) {
        b -= remainingBitsPerLong;
        l |= (tmp[tmpIdx++] & mask32RemainingBitsPerLong) << b;
      }
      if (b > 0) {
        l |= (tmp[tmpIdx] >>> (remainingBitsPerLong-b)) & MASKS32[b];
        remainingBits = remainingBitsPerLong - b;
      } else {
        remainingBits = remainingBitsPerLong;
      }
      longs[longsIdx] = l;
    }
  }

  /**
   * The pattern that this shiftLongs method applies is recognized by the C2
   * compiler, which generates SIMD instructions for it in order to shift
   * multiple longs at once.
   */
  private static void shiftLongs(long[] a, int count, long[] b, int bi, int shift, long mask) {
    for (int i = 0; i < count; ++i) {
      b[bi+i] = (a[i] >>> shift) & mask;
    }
  }

  private static final long[] MASKS8 = new long[8];
  private static final long[] MASKS16 = new long[16];
  private static final long[] MASKS32 = new long[32];
  static {
    for (int i = 0; i < 8; ++i) {
      MASKS8[i] = mask8(i);
    }
    for (int i = 0; i < 16; ++i) {
      MASKS16[i] = mask16(i);
    }
    for (int i = 0; i < 32; ++i) {
      MASKS32[i] = mask32(i);
    }
  }

  /**
   * Decode 128 integers into {@code longs}.
   */
  void decode(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    switch (bitsPerValue) {
    case 1:
      decode1(in, tmp, longs);
      expand8(longs);
      break;
    case 2:
      decode2(in, tmp, longs);
      expand8(longs);
      break;
    case 3:
      decode3(in, tmp, longs);
      expand8(longs);
      break;
    case 4:
      decode4(in, tmp, longs);
      expand8(longs);
      break;
    case 5:
      decode5(in, tmp, longs);
      expand8(longs);
      break;
    case 6:
      decode6(in, tmp, longs);
      expand8(longs);
      break;
    case 7:
      decode7(in, tmp, longs);
      expand8(longs);
      break;
    case 8:
      decode8(in, tmp, longs);
      expand8(longs);
      break;
    case 9:
      decode9(in, tmp, longs);
      expand16(longs);
      break;
    case 10:
      decode10(in, tmp, longs);
      expand16(longs);
      break;
    case 11:
      decode11(in, tmp, longs);
      expand16(longs);
      break;
    case 12:
      decode12(in, tmp, longs);
      expand16(longs);
      break;
    case 13:
      decode13(in, tmp, longs);
      expand16(longs);
      break;
    case 14:
      decode14(in, tmp, longs);
      expand16(longs);
      break;
    case 15:
      decode15(in, tmp, longs);
      expand16(longs);
      break;
    case 16:
      decode16(in, tmp, longs);
      expand16(longs);
      break;
    case 17:
      decode17(in, tmp, longs);
      expand32(longs);
      break;
    case 18:
      decode18(in, tmp, longs);
      expand32(longs);
      break;
    case 19:
      decode19(in, tmp, longs);
      expand32(longs);
      break;
    case 20:
      decode20(in, tmp, longs);
      expand32(longs);
      break;
    case 21:
      decode21(in, tmp, longs);
      expand32(longs);
      break;
    case 22:
      decode22(in, tmp, longs);
      expand32(longs);
      break;
    case 23:
      decode23(in, tmp, longs);
      expand32(longs);
      break;
    case 24:
      decode24(in, tmp, longs);
      expand32(longs);
      break;
    default:
      decodeSlow(bitsPerValue, in, tmp, longs);
      expand32(longs);
      break;
    }
  }

  /**
   * Delta-decode 128 integers into {@code longs}.
   */
  void decodeAndPrefixSum(int bitsPerValue, DataInput in, long base, long[] longs) throws IOException {
    switch (bitsPerValue) {
    case 1:
      decode1(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 2:
      decode2(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 3:
      decode3(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 4:
      decode4(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 5:
      decode5(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 6:
      decode6(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 7:
      decode7(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 8:
      decode8(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 9:
      decode9(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 10:
      decode10(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 11:
      decode11(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 12:
      decode12(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 13:
      decode13(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 14:
      decode14(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 15:
      decode15(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 16:
      decode16(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 17:
      decode17(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 18:
      decode18(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 19:
      decode19(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 20:
      decode20(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 21:
      decode21(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 22:
      decode22(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 23:
      decode23(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 24:
      decode24(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    default:
      decodeSlow(bitsPerValue, in, tmp, longs);
      prefixSum32(longs, base);
      break;
    }
  }

  private static void decode1(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 2);
    shiftLongs(tmp, 2, longs, 0, 7, MASKS8[1]);
    shiftLongs(tmp, 2, longs, 2, 6, MASKS8[1]);
    shiftLongs(tmp, 2, longs, 4, 5, MASKS8[1]);
    shiftLongs(tmp, 2, longs, 6, 4, MASKS8[1]);
    shiftLongs(tmp, 2, longs, 8, 3, MASKS8[1]);
    shiftLongs(tmp, 2, longs, 10, 2, MASKS8[1]);
    shiftLongs(tmp, 2, longs, 12, 1, MASKS8[1]);
    shiftLongs(tmp, 2, longs, 14, 0, MASKS8[1]);
  }

  private static void decode2(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 4);
    shiftLongs(tmp, 4, longs, 0, 6, MASKS8[2]);
    shiftLongs(tmp, 4, longs, 4, 4, MASKS8[2]);
    shiftLongs(tmp, 4, longs, 8, 2, MASKS8[2]);
    shiftLongs(tmp, 4, longs, 12, 0, MASKS8[2]);
  }

  private static void decode3(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 6);
    shiftLongs(tmp, 6, longs, 0, 5, MASKS8[3]);
    shiftLongs(tmp, 6, longs, 6, 2, MASKS8[3]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 12; iter < 2; ++iter, tmpIdx += 3, longsIdx += 2) {
      long l0 = (tmp[tmpIdx+0] & MASKS8[2]) << 1;
      l0 |= (tmp[tmpIdx+1] >>> 1) & MASKS8[1];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASKS8[1]) << 2;
      l1 |= (tmp[tmpIdx+2] & MASKS8[2]) << 0;
      longs[longsIdx+1] = l1;
    }
  }

  private static void decode4(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 8);
    shiftLongs(tmp, 8, longs, 0, 4, MASKS8[4]);
    shiftLongs(tmp, 8, longs, 8, 0, MASKS8[4]);
  }

  private static void decode5(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 10);
    shiftLongs(tmp, 10, longs, 0, 3, MASKS8[5]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 10; iter < 2; ++iter, tmpIdx += 5, longsIdx += 3) {
      long l0 = (tmp[tmpIdx+0] & MASKS8[3]) << 2;
      l0 |= (tmp[tmpIdx+1] >>> 1) & MASKS8[2];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASKS8[1]) << 4;
      l1 |= (tmp[tmpIdx+2] & MASKS8[3]) << 1;
      l1 |= (tmp[tmpIdx+3] >>> 2) & MASKS8[1];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+3] & MASKS8[2]) << 3;
      l2 |= (tmp[tmpIdx+4] & MASKS8[3]) << 0;
      longs[longsIdx+2] = l2;
    }
  }

  private static void decode6(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 12);
    shiftLongs(tmp, 12, longs, 0, 2, MASKS8[6]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 12; iter < 4; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASKS8[2]) << 4;
      l0 |= (tmp[tmpIdx+1] & MASKS8[2]) << 2;
      l0 |= (tmp[tmpIdx+2] & MASKS8[2]) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  private static void decode7(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 14);
    shiftLongs(tmp, 14, longs, 0, 1, MASKS8[7]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 14; iter < 2; ++iter, tmpIdx += 7, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASKS8[1]) << 6;
      l0 |= (tmp[tmpIdx+1] & MASKS8[1]) << 5;
      l0 |= (tmp[tmpIdx+2] & MASKS8[1]) << 4;
      l0 |= (tmp[tmpIdx+3] & MASKS8[1]) << 3;
      l0 |= (tmp[tmpIdx+4] & MASKS8[1]) << 2;
      l0 |= (tmp[tmpIdx+5] & MASKS8[1]) << 1;
      l0 |= (tmp[tmpIdx+6] & MASKS8[1]) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  private static void decode8(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(longs, 0, 16);
  }

  private static void decode9(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 18);
    shiftLongs(tmp, 18, longs, 0, 7, MASKS16[9]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 18; iter < 2; ++iter, tmpIdx += 9, longsIdx += 7) {
      long l0 = (tmp[tmpIdx+0] & MASKS16[7]) << 2;
      l0 |= (tmp[tmpIdx+1] >>> 5) & MASKS16[2];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASKS16[5]) << 4;
      l1 |= (tmp[tmpIdx+2] >>> 3) & MASKS16[4];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+2] & MASKS16[3]) << 6;
      l2 |= (tmp[tmpIdx+3] >>> 1) & MASKS16[6];
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+3] & MASKS16[1]) << 8;
      l3 |= (tmp[tmpIdx+4] & MASKS16[7]) << 1;
      l3 |= (tmp[tmpIdx+5] >>> 6) & MASKS16[1];
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+5] & MASKS16[6]) << 3;
      l4 |= (tmp[tmpIdx+6] >>> 4) & MASKS16[3];
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+6] & MASKS16[4]) << 5;
      l5 |= (tmp[tmpIdx+7] >>> 2) & MASKS16[5];
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+7] & MASKS16[2]) << 7;
      l6 |= (tmp[tmpIdx+8] & MASKS16[7]) << 0;
      longs[longsIdx+6] = l6;
    }
  }

  private static void decode10(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 20);
    shiftLongs(tmp, 20, longs, 0, 6, MASKS16[10]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 20; iter < 4; ++iter, tmpIdx += 5, longsIdx += 3) {
      long l0 = (tmp[tmpIdx+0] & MASKS16[6]) << 4;
      l0 |= (tmp[tmpIdx+1] >>> 2) & MASKS16[4];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASKS16[2]) << 8;
      l1 |= (tmp[tmpIdx+2] & MASKS16[6]) << 2;
      l1 |= (tmp[tmpIdx+3] >>> 4) & MASKS16[2];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+3] & MASKS16[4]) << 6;
      l2 |= (tmp[tmpIdx+4] & MASKS16[6]) << 0;
      longs[longsIdx+2] = l2;
    }
  }

  private static void decode11(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 22);
    shiftLongs(tmp, 22, longs, 0, 5, MASKS16[11]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 22; iter < 2; ++iter, tmpIdx += 11, longsIdx += 5) {
      long l0 = (tmp[tmpIdx+0] & MASKS16[5]) << 6;
      l0 |= (tmp[tmpIdx+1] & MASKS16[5]) << 1;
      l0 |= (tmp[tmpIdx+2] >>> 4) & MASKS16[1];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+2] & MASKS16[4]) << 7;
      l1 |= (tmp[tmpIdx+3] & MASKS16[5]) << 2;
      l1 |= (tmp[tmpIdx+4] >>> 3) & MASKS16[2];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+4] & MASKS16[3]) << 8;
      l2 |= (tmp[tmpIdx+5] & MASKS16[5]) << 3;
      l2 |= (tmp[tmpIdx+6] >>> 2) & MASKS16[3];
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+6] & MASKS16[2]) << 9;
      l3 |= (tmp[tmpIdx+7] & MASKS16[5]) << 4;
      l3 |= (tmp[tmpIdx+8] >>> 1) & MASKS16[4];
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+8] & MASKS16[1]) << 10;
      l4 |= (tmp[tmpIdx+9] & MASKS16[5]) << 5;
      l4 |= (tmp[tmpIdx+10] & MASKS16[5]) << 0;
      longs[longsIdx+4] = l4;
    }
  }

  private static void decode12(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 24);
    shiftLongs(tmp, 24, longs, 0, 4, MASKS16[12]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 24; iter < 8; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASKS16[4]) << 8;
      l0 |= (tmp[tmpIdx+1] & MASKS16[4]) << 4;
      l0 |= (tmp[tmpIdx+2] & MASKS16[4]) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  private static void decode13(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 26);
    shiftLongs(tmp, 26, longs, 0, 3, MASKS16[13]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 26; iter < 2; ++iter, tmpIdx += 13, longsIdx += 3) {
      long l0 = (tmp[tmpIdx+0] & MASKS16[3]) << 10;
      l0 |= (tmp[tmpIdx+1] & MASKS16[3]) << 7;
      l0 |= (tmp[tmpIdx+2] & MASKS16[3]) << 4;
      l0 |= (tmp[tmpIdx+3] & MASKS16[3]) << 1;
      l0 |= (tmp[tmpIdx+4] >>> 2) & MASKS16[1];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+4] & MASKS16[2]) << 11;
      l1 |= (tmp[tmpIdx+5] & MASKS16[3]) << 8;
      l1 |= (tmp[tmpIdx+6] & MASKS16[3]) << 5;
      l1 |= (tmp[tmpIdx+7] & MASKS16[3]) << 2;
      l1 |= (tmp[tmpIdx+8] >>> 1) & MASKS16[2];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+8] & MASKS16[1]) << 12;
      l2 |= (tmp[tmpIdx+9] & MASKS16[3]) << 9;
      l2 |= (tmp[tmpIdx+10] & MASKS16[3]) << 6;
      l2 |= (tmp[tmpIdx+11] & MASKS16[3]) << 3;
      l2 |= (tmp[tmpIdx+12] & MASKS16[3]) << 0;
      longs[longsIdx+2] = l2;
    }
  }

  private static void decode14(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 28);
    shiftLongs(tmp, 28, longs, 0, 2, MASKS16[14]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 28; iter < 4; ++iter, tmpIdx += 7, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASKS16[2]) << 12;
      l0 |= (tmp[tmpIdx+1] & MASKS16[2]) << 10;
      l0 |= (tmp[tmpIdx+2] & MASKS16[2]) << 8;
      l0 |= (tmp[tmpIdx+3] & MASKS16[2]) << 6;
      l0 |= (tmp[tmpIdx+4] & MASKS16[2]) << 4;
      l0 |= (tmp[tmpIdx+5] & MASKS16[2]) << 2;
      l0 |= (tmp[tmpIdx+6] & MASKS16[2]) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  private static void decode15(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 30);
    shiftLongs(tmp, 30, longs, 0, 1, MASKS16[15]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 30; iter < 2; ++iter, tmpIdx += 15, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASKS16[1]) << 14;
      l0 |= (tmp[tmpIdx+1] & MASKS16[1]) << 13;
      l0 |= (tmp[tmpIdx+2] & MASKS16[1]) << 12;
      l0 |= (tmp[tmpIdx+3] & MASKS16[1]) << 11;
      l0 |= (tmp[tmpIdx+4] & MASKS16[1]) << 10;
      l0 |= (tmp[tmpIdx+5] & MASKS16[1]) << 9;
      l0 |= (tmp[tmpIdx+6] & MASKS16[1]) << 8;
      l0 |= (tmp[tmpIdx+7] & MASKS16[1]) << 7;
      l0 |= (tmp[tmpIdx+8] & MASKS16[1]) << 6;
      l0 |= (tmp[tmpIdx+9] & MASKS16[1]) << 5;
      l0 |= (tmp[tmpIdx+10] & MASKS16[1]) << 4;
      l0 |= (tmp[tmpIdx+11] & MASKS16[1]) << 3;
      l0 |= (tmp[tmpIdx+12] & MASKS16[1]) << 2;
      l0 |= (tmp[tmpIdx+13] & MASKS16[1]) << 1;
      l0 |= (tmp[tmpIdx+14] & MASKS16[1]) << 0;
      longs[longsIdx+0] = l0;
    }
  }

  private static void decode16(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(longs, 0, 32);
  }

  private static void decode17(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 34);
    shiftLongs(tmp, 34, longs, 0, 15, MASKS32[17]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 34; iter < 2; ++iter, tmpIdx += 17, longsIdx += 15) {
      long l0 = (tmp[tmpIdx+0] & MASKS32[15]) << 2;
      l0 |= (tmp[tmpIdx+1] >>> 13) & MASKS32[2];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASKS32[13]) << 4;
      l1 |= (tmp[tmpIdx+2] >>> 11) & MASKS32[4];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+2] & MASKS32[11]) << 6;
      l2 |= (tmp[tmpIdx+3] >>> 9) & MASKS32[6];
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+3] & MASKS32[9]) << 8;
      l3 |= (tmp[tmpIdx+4] >>> 7) & MASKS32[8];
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+4] & MASKS32[7]) << 10;
      l4 |= (tmp[tmpIdx+5] >>> 5) & MASKS32[10];
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+5] & MASKS32[5]) << 12;
      l5 |= (tmp[tmpIdx+6] >>> 3) & MASKS32[12];
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+6] & MASKS32[3]) << 14;
      l6 |= (tmp[tmpIdx+7] >>> 1) & MASKS32[14];
      longs[longsIdx+6] = l6;
      long l7 = (tmp[tmpIdx+7] & MASKS32[1]) << 16;
      l7 |= (tmp[tmpIdx+8] & MASKS32[15]) << 1;
      l7 |= (tmp[tmpIdx+9] >>> 14) & MASKS32[1];
      longs[longsIdx+7] = l7;
      long l8 = (tmp[tmpIdx+9] & MASKS32[14]) << 3;
      l8 |= (tmp[tmpIdx+10] >>> 12) & MASKS32[3];
      longs[longsIdx+8] = l8;
      long l9 = (tmp[tmpIdx+10] & MASKS32[12]) << 5;
      l9 |= (tmp[tmpIdx+11] >>> 10) & MASKS32[5];
      longs[longsIdx+9] = l9;
      long l10 = (tmp[tmpIdx+11] & MASKS32[10]) << 7;
      l10 |= (tmp[tmpIdx+12] >>> 8) & MASKS32[7];
      longs[longsIdx+10] = l10;
      long l11 = (tmp[tmpIdx+12] & MASKS32[8]) << 9;
      l11 |= (tmp[tmpIdx+13] >>> 6) & MASKS32[9];
      longs[longsIdx+11] = l11;
      long l12 = (tmp[tmpIdx+13] & MASKS32[6]) << 11;
      l12 |= (tmp[tmpIdx+14] >>> 4) & MASKS32[11];
      longs[longsIdx+12] = l12;
      long l13 = (tmp[tmpIdx+14] & MASKS32[4]) << 13;
      l13 |= (tmp[tmpIdx+15] >>> 2) & MASKS32[13];
      longs[longsIdx+13] = l13;
      long l14 = (tmp[tmpIdx+15] & MASKS32[2]) << 15;
      l14 |= (tmp[tmpIdx+16] & MASKS32[15]) << 0;
      longs[longsIdx+14] = l14;
    }
  }

  private static void decode18(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 36);
    shiftLongs(tmp, 36, longs, 0, 14, MASKS32[18]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 36; iter < 4; ++iter, tmpIdx += 9, longsIdx += 7) {
      long l0 = (tmp[tmpIdx+0] & MASKS32[14]) << 4;
      l0 |= (tmp[tmpIdx+1] >>> 10) & MASKS32[4];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASKS32[10]) << 8;
      l1 |= (tmp[tmpIdx+2] >>> 6) & MASKS32[8];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+2] & MASKS32[6]) << 12;
      l2 |= (tmp[tmpIdx+3] >>> 2) & MASKS32[12];
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+3] & MASKS32[2]) << 16;
      l3 |= (tmp[tmpIdx+4] & MASKS32[14]) << 2;
      l3 |= (tmp[tmpIdx+5] >>> 12) & MASKS32[2];
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+5] & MASKS32[12]) << 6;
      l4 |= (tmp[tmpIdx+6] >>> 8) & MASKS32[6];
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+6] & MASKS32[8]) << 10;
      l5 |= (tmp[tmpIdx+7] >>> 4) & MASKS32[10];
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+7] & MASKS32[4]) << 14;
      l6 |= (tmp[tmpIdx+8] & MASKS32[14]) << 0;
      longs[longsIdx+6] = l6;
    }
  }

  private static void decode19(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 38);
    shiftLongs(tmp, 38, longs, 0, 13, MASKS32[19]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 38; iter < 2; ++iter, tmpIdx += 19, longsIdx += 13) {
      long l0 = (tmp[tmpIdx+0] & MASKS32[13]) << 6;
      l0 |= (tmp[tmpIdx+1] >>> 7) & MASKS32[6];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASKS32[7]) << 12;
      l1 |= (tmp[tmpIdx+2] >>> 1) & MASKS32[12];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+2] & MASKS32[1]) << 18;
      l2 |= (tmp[tmpIdx+3] & MASKS32[13]) << 5;
      l2 |= (tmp[tmpIdx+4] >>> 8) & MASKS32[5];
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+4] & MASKS32[8]) << 11;
      l3 |= (tmp[tmpIdx+5] >>> 2) & MASKS32[11];
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+5] & MASKS32[2]) << 17;
      l4 |= (tmp[tmpIdx+6] & MASKS32[13]) << 4;
      l4 |= (tmp[tmpIdx+7] >>> 9) & MASKS32[4];
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+7] & MASKS32[9]) << 10;
      l5 |= (tmp[tmpIdx+8] >>> 3) & MASKS32[10];
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+8] & MASKS32[3]) << 16;
      l6 |= (tmp[tmpIdx+9] & MASKS32[13]) << 3;
      l6 |= (tmp[tmpIdx+10] >>> 10) & MASKS32[3];
      longs[longsIdx+6] = l6;
      long l7 = (tmp[tmpIdx+10] & MASKS32[10]) << 9;
      l7 |= (tmp[tmpIdx+11] >>> 4) & MASKS32[9];
      longs[longsIdx+7] = l7;
      long l8 = (tmp[tmpIdx+11] & MASKS32[4]) << 15;
      l8 |= (tmp[tmpIdx+12] & MASKS32[13]) << 2;
      l8 |= (tmp[tmpIdx+13] >>> 11) & MASKS32[2];
      longs[longsIdx+8] = l8;
      long l9 = (tmp[tmpIdx+13] & MASKS32[11]) << 8;
      l9 |= (tmp[tmpIdx+14] >>> 5) & MASKS32[8];
      longs[longsIdx+9] = l9;
      long l10 = (tmp[tmpIdx+14] & MASKS32[5]) << 14;
      l10 |= (tmp[tmpIdx+15] & MASKS32[13]) << 1;
      l10 |= (tmp[tmpIdx+16] >>> 12) & MASKS32[1];
      longs[longsIdx+10] = l10;
      long l11 = (tmp[tmpIdx+16] & MASKS32[12]) << 7;
      l11 |= (tmp[tmpIdx+17] >>> 6) & MASKS32[7];
      longs[longsIdx+11] = l11;
      long l12 = (tmp[tmpIdx+17] & MASKS32[6]) << 13;
      l12 |= (tmp[tmpIdx+18] & MASKS32[13]) << 0;
      longs[longsIdx+12] = l12;
    }
  }

  private static void decode20(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 40);
    shiftLongs(tmp, 40, longs, 0, 12, MASKS32[20]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 40; iter < 8; ++iter, tmpIdx += 5, longsIdx += 3) {
      long l0 = (tmp[tmpIdx+0] & MASKS32[12]) << 8;
      l0 |= (tmp[tmpIdx+1] >>> 4) & MASKS32[8];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASKS32[4]) << 16;
      l1 |= (tmp[tmpIdx+2] & MASKS32[12]) << 4;
      l1 |= (tmp[tmpIdx+3] >>> 8) & MASKS32[4];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+3] & MASKS32[8]) << 12;
      l2 |= (tmp[tmpIdx+4] & MASKS32[12]) << 0;
      longs[longsIdx+2] = l2;
    }
  }

  private static void decode21(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 42);
    shiftLongs(tmp, 42, longs, 0, 11, MASKS32[21]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 42; iter < 2; ++iter, tmpIdx += 21, longsIdx += 11) {
      long l0 = (tmp[tmpIdx+0] & MASKS32[11]) << 10;
      l0 |= (tmp[tmpIdx+1] >>> 1) & MASKS32[10];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+1] & MASKS32[1]) << 20;
      l1 |= (tmp[tmpIdx+2] & MASKS32[11]) << 9;
      l1 |= (tmp[tmpIdx+3] >>> 2) & MASKS32[9];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+3] & MASKS32[2]) << 19;
      l2 |= (tmp[tmpIdx+4] & MASKS32[11]) << 8;
      l2 |= (tmp[tmpIdx+5] >>> 3) & MASKS32[8];
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+5] & MASKS32[3]) << 18;
      l3 |= (tmp[tmpIdx+6] & MASKS32[11]) << 7;
      l3 |= (tmp[tmpIdx+7] >>> 4) & MASKS32[7];
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+7] & MASKS32[4]) << 17;
      l4 |= (tmp[tmpIdx+8] & MASKS32[11]) << 6;
      l4 |= (tmp[tmpIdx+9] >>> 5) & MASKS32[6];
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+9] & MASKS32[5]) << 16;
      l5 |= (tmp[tmpIdx+10] & MASKS32[11]) << 5;
      l5 |= (tmp[tmpIdx+11] >>> 6) & MASKS32[5];
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+11] & MASKS32[6]) << 15;
      l6 |= (tmp[tmpIdx+12] & MASKS32[11]) << 4;
      l6 |= (tmp[tmpIdx+13] >>> 7) & MASKS32[4];
      longs[longsIdx+6] = l6;
      long l7 = (tmp[tmpIdx+13] & MASKS32[7]) << 14;
      l7 |= (tmp[tmpIdx+14] & MASKS32[11]) << 3;
      l7 |= (tmp[tmpIdx+15] >>> 8) & MASKS32[3];
      longs[longsIdx+7] = l7;
      long l8 = (tmp[tmpIdx+15] & MASKS32[8]) << 13;
      l8 |= (tmp[tmpIdx+16] & MASKS32[11]) << 2;
      l8 |= (tmp[tmpIdx+17] >>> 9) & MASKS32[2];
      longs[longsIdx+8] = l8;
      long l9 = (tmp[tmpIdx+17] & MASKS32[9]) << 12;
      l9 |= (tmp[tmpIdx+18] & MASKS32[11]) << 1;
      l9 |= (tmp[tmpIdx+19] >>> 10) & MASKS32[1];
      longs[longsIdx+9] = l9;
      long l10 = (tmp[tmpIdx+19] & MASKS32[10]) << 11;
      l10 |= (tmp[tmpIdx+20] & MASKS32[11]) << 0;
      longs[longsIdx+10] = l10;
    }
  }

  private static void decode22(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 44);
    shiftLongs(tmp, 44, longs, 0, 10, MASKS32[22]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 44; iter < 4; ++iter, tmpIdx += 11, longsIdx += 5) {
      long l0 = (tmp[tmpIdx+0] & MASKS32[10]) << 12;
      l0 |= (tmp[tmpIdx+1] & MASKS32[10]) << 2;
      l0 |= (tmp[tmpIdx+2] >>> 8) & MASKS32[2];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+2] & MASKS32[8]) << 14;
      l1 |= (tmp[tmpIdx+3] & MASKS32[10]) << 4;
      l1 |= (tmp[tmpIdx+4] >>> 6) & MASKS32[4];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+4] & MASKS32[6]) << 16;
      l2 |= (tmp[tmpIdx+5] & MASKS32[10]) << 6;
      l2 |= (tmp[tmpIdx+6] >>> 4) & MASKS32[6];
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+6] & MASKS32[4]) << 18;
      l3 |= (tmp[tmpIdx+7] & MASKS32[10]) << 8;
      l3 |= (tmp[tmpIdx+8] >>> 2) & MASKS32[8];
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+8] & MASKS32[2]) << 20;
      l4 |= (tmp[tmpIdx+9] & MASKS32[10]) << 10;
      l4 |= (tmp[tmpIdx+10] & MASKS32[10]) << 0;
      longs[longsIdx+4] = l4;
    }
  }

  private static void decode23(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 46);
    shiftLongs(tmp, 46, longs, 0, 9, MASKS32[23]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 46; iter < 2; ++iter, tmpIdx += 23, longsIdx += 9) {
      long l0 = (tmp[tmpIdx+0] & MASKS32[9]) << 14;
      l0 |= (tmp[tmpIdx+1] & MASKS32[9]) << 5;
      l0 |= (tmp[tmpIdx+2] >>> 4) & MASKS32[5];
      longs[longsIdx+0] = l0;
      long l1 = (tmp[tmpIdx+2] & MASKS32[4]) << 19;
      l1 |= (tmp[tmpIdx+3] & MASKS32[9]) << 10;
      l1 |= (tmp[tmpIdx+4] & MASKS32[9]) << 1;
      l1 |= (tmp[tmpIdx+5] >>> 8) & MASKS32[1];
      longs[longsIdx+1] = l1;
      long l2 = (tmp[tmpIdx+5] & MASKS32[8]) << 15;
      l2 |= (tmp[tmpIdx+6] & MASKS32[9]) << 6;
      l2 |= (tmp[tmpIdx+7] >>> 3) & MASKS32[6];
      longs[longsIdx+2] = l2;
      long l3 = (tmp[tmpIdx+7] & MASKS32[3]) << 20;
      l3 |= (tmp[tmpIdx+8] & MASKS32[9]) << 11;
      l3 |= (tmp[tmpIdx+9] & MASKS32[9]) << 2;
      l3 |= (tmp[tmpIdx+10] >>> 7) & MASKS32[2];
      longs[longsIdx+3] = l3;
      long l4 = (tmp[tmpIdx+10] & MASKS32[7]) << 16;
      l4 |= (tmp[tmpIdx+11] & MASKS32[9]) << 7;
      l4 |= (tmp[tmpIdx+12] >>> 2) & MASKS32[7];
      longs[longsIdx+4] = l4;
      long l5 = (tmp[tmpIdx+12] & MASKS32[2]) << 21;
      l5 |= (tmp[tmpIdx+13] & MASKS32[9]) << 12;
      l5 |= (tmp[tmpIdx+14] & MASKS32[9]) << 3;
      l5 |= (tmp[tmpIdx+15] >>> 6) & MASKS32[3];
      longs[longsIdx+5] = l5;
      long l6 = (tmp[tmpIdx+15] & MASKS32[6]) << 17;
      l6 |= (tmp[tmpIdx+16] & MASKS32[9]) << 8;
      l6 |= (tmp[tmpIdx+17] >>> 1) & MASKS32[8];
      longs[longsIdx+6] = l6;
      long l7 = (tmp[tmpIdx+17] & MASKS32[1]) << 22;
      l7 |= (tmp[tmpIdx+18] & MASKS32[9]) << 13;
      l7 |= (tmp[tmpIdx+19] & MASKS32[9]) << 4;
      l7 |= (tmp[tmpIdx+20] >>> 5) & MASKS32[4];
      longs[longsIdx+7] = l7;
      long l8 = (tmp[tmpIdx+20] & MASKS32[5]) << 18;
      l8 |= (tmp[tmpIdx+21] & MASKS32[9]) << 9;
      l8 |= (tmp[tmpIdx+22] & MASKS32[9]) << 0;
      longs[longsIdx+8] = l8;
    }
  }

  private static void decode24(DataInput in, long[] tmp, long[] longs) throws IOException {
    in.readLELongs(tmp, 0, 48);
    shiftLongs(tmp, 48, longs, 0, 8, MASKS32[24]);
    for (int iter = 0, tmpIdx = 0, longsIdx = 48; iter < 16; ++iter, tmpIdx += 3, longsIdx += 1) {
      long l0 = (tmp[tmpIdx+0] & MASKS32[8]) << 16;
      l0 |= (tmp[tmpIdx+1] & MASKS32[8]) << 8;
      l0 |= (tmp[tmpIdx+2] & MASKS32[8]) << 0;
      longs[longsIdx+0] = l0;
    }
  }

}
