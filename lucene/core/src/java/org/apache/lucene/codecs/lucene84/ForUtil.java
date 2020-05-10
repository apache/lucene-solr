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
import org.apache.lucene.util.ForPrimitives;

/**
 * Utility class that uses {@link ForPrimitives} for encoding /decoding
 * blocks of 128 Integers.
 */
final class ForUtil {

  private final long[] tmp = new long[ForPrimitives.BLOCK_SIZE/2];

  /**
   * Encode 128 integers from {@code longs} into {@code out}.
   */
  void encode(long[] longs, int bitsPerValue, DataOutput out) throws IOException {
    ForPrimitives.encode(longs, bitsPerValue, out, tmp);
  }

  /**
   * Decode 128 integers into {@code longs}.
   */
  void decode(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    switch (bitsPerValue) {
    case 1:
      ForPrimitives.decode1(in, tmp, longs);
      expand8(longs);
      break;
    case 2:
      ForPrimitives.decode2(in, tmp, longs);
      expand8(longs);
      break;
    case 3:
      ForPrimitives.decode3(in, tmp, longs);
      expand8(longs);
      break;
    case 4:
      ForPrimitives.decode4(in, tmp, longs);
      expand8(longs);
      break;
    case 5:
      ForPrimitives.decode5(in, tmp, longs);
      expand8(longs);
      break;
    case 6:
      ForPrimitives.decode6(in, tmp, longs);
      expand8(longs);
      break;
    case 7:
      ForPrimitives.decode7(in, tmp, longs);
      expand8(longs);
      break;
    case 8:
      ForPrimitives.decode8(in, tmp, longs);
      expand8(longs);
      break;
    case 9:
      ForPrimitives.decode9(in, tmp, longs);
      expand16(longs);
      break;
    case 10:
      ForPrimitives.decode10(in, tmp, longs);
      expand16(longs);
      break;
    case 11:
      ForPrimitives.decode11(in, tmp, longs);
      expand16(longs);
      break;
    case 12:
      ForPrimitives.decode12(in, tmp, longs);
      expand16(longs);
      break;
    case 13:
      ForPrimitives.decode13(in, tmp, longs);
      expand16(longs);
      break;
    case 14:
      ForPrimitives.decode14(in, tmp, longs);
      expand16(longs);
      break;
    case 15:
      ForPrimitives.decode15(in, tmp, longs);
      expand16(longs);
      break;
    case 16:
      ForPrimitives.decode16(in, tmp, longs);
      expand16(longs);
      break;
    case 17:
      ForPrimitives.decode17(in, tmp, longs);
      expand32(longs);
      break;
    case 18:
      ForPrimitives.decode18(in, tmp, longs);
      expand32(longs);
      break;
    case 19:
      ForPrimitives.decode19(in, tmp, longs);
      expand32(longs);
      break;
    case 20:
      ForPrimitives.decode20(in, tmp, longs);
      expand32(longs);
      break;
    case 21:
      ForPrimitives.decode21(in, tmp, longs);
      expand32(longs);
      break;
    case 22:
      ForPrimitives.decode22(in, tmp, longs);
      expand32(longs);
      break;
    case 23:
      ForPrimitives.decode23(in, tmp, longs);
      expand32(longs);
      break;
    case 24:
      ForPrimitives.decode24(in, tmp, longs);
      expand32(longs);
      break;
    default:
      ForPrimitives.decodeSlow(bitsPerValue, in, tmp, longs);
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
      ForPrimitives.decode1(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 2:
      ForPrimitives.decode2(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 3:
      ForPrimitives.decode3(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 4:
      ForPrimitives.decode4(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 5:
      ForPrimitives.decode5(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 6:
      ForPrimitives.decode6(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 7:
      ForPrimitives.decode7(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 8:
      ForPrimitives.decode8(in, tmp, longs);
      prefixSum8(longs, base);
      break;
    case 9:
      ForPrimitives.decode9(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 10:
      ForPrimitives.decode10(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 11:
      ForPrimitives.decode11(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 12:
      ForPrimitives.decode12(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 13:
      ForPrimitives.decode13(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 14:
      ForPrimitives.decode14(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 15:
      ForPrimitives.decode15(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 16:
      ForPrimitives.decode16(in, tmp, longs);
      prefixSum16(longs, base);
      break;
    case 17:
      ForPrimitives.decode17(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 18:
      ForPrimitives.decode18(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 19:
      ForPrimitives.decode19(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 20:
      ForPrimitives.decode20(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 21:
      ForPrimitives.decode21(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 22:
      ForPrimitives.decode22(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 23:
      ForPrimitives.decode23(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    case 24:
      ForPrimitives.decode24(in, tmp, longs);
      prefixSum32(longs, base);
      break;
    default:
      ForPrimitives.decodeSlow(bitsPerValue, in, tmp, longs);
      prefixSum32(longs, base);
      break;
    }
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

  private static void expand32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      arr[i] = l >>> 32;
      arr[64 + i] = l & 0xFFFFFFFFL;
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
    final long l = arr[ForPrimitives.BLOCK_SIZE/2-1];
    for (int i = ForPrimitives.BLOCK_SIZE/2; i < ForPrimitives.BLOCK_SIZE; ++i) {
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
}
