package org.apache.lucene.util.pfor;
/**
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
 /* This code is generated, do not modify. See gendecompress.py */

import java.nio.IntBuffer;

final class ForDecompress {

  // nocommit: assess perf of this to see if specializing is really needed
  /*
  static void decodeAnyFrame(
        final IntBuffer compressedBuffer, int inputSize, int numFrameBits,
        int[] output) {

    assert numFrameBits > 0 : numFrameBits;
    assert numFrameBits <= 31 : numFrameBits;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int intValue1 = compressedBuffer.get();
    output[outputOffset] = intValue1 & mask;
    if (--inputSize == 0) return;
    int bitPos = numFrameBits;

    do {
      while (bitPos <= (32 - numFrameBits)) {
        // No mask needed when bitPos == (32 - numFrameBits), but prefer to avoid testing for this:
        output[++outputOffset] = (intValue1 >>> bitPos) & mask;
        if (--inputSize == 0) return;
        bitPos += numFrameBits;
      }
      
      int intValue2 = compressedBuffer.get();
      output[++outputOffset] = ( (bitPos == 32)
                                  ? intValue2
                                  : ((intValue1 >>> bitPos) | (intValue2 << (32 - bitPos)))
                               ) & mask;
        
      if (--inputSize == 0) return;
      
      intValue1 = intValue2;
      bitPos += numFrameBits - 32;
    } while (true);
  }
  */


  // NOTE: hardwired to blockSize == 128
  public static void decode1(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 1;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 1) & mask;
      output[2 + outputOffset] = (intValue0 >>> 2) & mask;
      output[3 + outputOffset] = (intValue0 >>> 3) & mask;
      output[4 + outputOffset] = (intValue0 >>> 4) & mask;
      output[5 + outputOffset] = (intValue0 >>> 5) & mask;
      output[6 + outputOffset] = (intValue0 >>> 6) & mask;
      output[7 + outputOffset] = (intValue0 >>> 7) & mask;
      output[8 + outputOffset] = (intValue0 >>> 8) & mask;
      output[9 + outputOffset] = (intValue0 >>> 9) & mask;
      output[10 + outputOffset] = (intValue0 >>> 10) & mask;
      output[11 + outputOffset] = (intValue0 >>> 11) & mask;
      output[12 + outputOffset] = (intValue0 >>> 12) & mask;
      output[13 + outputOffset] = (intValue0 >>> 13) & mask;
      output[14 + outputOffset] = (intValue0 >>> 14) & mask;
      output[15 + outputOffset] = (intValue0 >>> 15) & mask;
      output[16 + outputOffset] = (intValue0 >>> 16) & mask;
      output[17 + outputOffset] = (intValue0 >>> 17) & mask;
      output[18 + outputOffset] = (intValue0 >>> 18) & mask;
      output[19 + outputOffset] = (intValue0 >>> 19) & mask;
      output[20 + outputOffset] = (intValue0 >>> 20) & mask;
      output[21 + outputOffset] = (intValue0 >>> 21) & mask;
      output[22 + outputOffset] = (intValue0 >>> 22) & mask;
      output[23 + outputOffset] = (intValue0 >>> 23) & mask;
      output[24 + outputOffset] = (intValue0 >>> 24) & mask;
      output[25 + outputOffset] = (intValue0 >>> 25) & mask;
      output[26 + outputOffset] = (intValue0 >>> 26) & mask;
      output[27 + outputOffset] = (intValue0 >>> 27) & mask;
      output[28 + outputOffset] = (intValue0 >>> 28) & mask;
      output[29 + outputOffset] = (intValue0 >>> 29) & mask;
      output[30 + outputOffset] = (intValue0 >>> 30) & mask;
      output[31 + outputOffset] = intValue0 >>> 31;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode2(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 2;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 2) & mask;
      output[2 + outputOffset] = (intValue0 >>> 4) & mask;
      output[3 + outputOffset] = (intValue0 >>> 6) & mask;
      output[4 + outputOffset] = (intValue0 >>> 8) & mask;
      output[5 + outputOffset] = (intValue0 >>> 10) & mask;
      output[6 + outputOffset] = (intValue0 >>> 12) & mask;
      output[7 + outputOffset] = (intValue0 >>> 14) & mask;
      output[8 + outputOffset] = (intValue0 >>> 16) & mask;
      output[9 + outputOffset] = (intValue0 >>> 18) & mask;
      output[10 + outputOffset] = (intValue0 >>> 20) & mask;
      output[11 + outputOffset] = (intValue0 >>> 22) & mask;
      output[12 + outputOffset] = (intValue0 >>> 24) & mask;
      output[13 + outputOffset] = (intValue0 >>> 26) & mask;
      output[14 + outputOffset] = (intValue0 >>> 28) & mask;
      output[15 + outputOffset] = intValue0 >>> 30;
      output[16 + outputOffset] = intValue1 & mask;
      output[17 + outputOffset] = (intValue1 >>> 2) & mask;
      output[18 + outputOffset] = (intValue1 >>> 4) & mask;
      output[19 + outputOffset] = (intValue1 >>> 6) & mask;
      output[20 + outputOffset] = (intValue1 >>> 8) & mask;
      output[21 + outputOffset] = (intValue1 >>> 10) & mask;
      output[22 + outputOffset] = (intValue1 >>> 12) & mask;
      output[23 + outputOffset] = (intValue1 >>> 14) & mask;
      output[24 + outputOffset] = (intValue1 >>> 16) & mask;
      output[25 + outputOffset] = (intValue1 >>> 18) & mask;
      output[26 + outputOffset] = (intValue1 >>> 20) & mask;
      output[27 + outputOffset] = (intValue1 >>> 22) & mask;
      output[28 + outputOffset] = (intValue1 >>> 24) & mask;
      output[29 + outputOffset] = (intValue1 >>> 26) & mask;
      output[30 + outputOffset] = (intValue1 >>> 28) & mask;
      output[31 + outputOffset] = intValue1 >>> 30;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode3(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 3;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 3) & mask;
      output[2 + outputOffset] = (intValue0 >>> 6) & mask;
      output[3 + outputOffset] = (intValue0 >>> 9) & mask;
      output[4 + outputOffset] = (intValue0 >>> 12) & mask;
      output[5 + outputOffset] = (intValue0 >>> 15) & mask;
      output[6 + outputOffset] = (intValue0 >>> 18) & mask;
      output[7 + outputOffset] = (intValue0 >>> 21) & mask;
      output[8 + outputOffset] = (intValue0 >>> 24) & mask;
      output[9 + outputOffset] = (intValue0 >>> 27) & mask;
      output[10 + outputOffset] = ((intValue0 >>> 30) | (intValue1 << 2)) & mask;
      output[11 + outputOffset] = (intValue1 >>> 1) & mask;
      output[12 + outputOffset] = (intValue1 >>> 4) & mask;
      output[13 + outputOffset] = (intValue1 >>> 7) & mask;
      output[14 + outputOffset] = (intValue1 >>> 10) & mask;
      output[15 + outputOffset] = (intValue1 >>> 13) & mask;
      output[16 + outputOffset] = (intValue1 >>> 16) & mask;
      output[17 + outputOffset] = (intValue1 >>> 19) & mask;
      output[18 + outputOffset] = (intValue1 >>> 22) & mask;
      output[19 + outputOffset] = (intValue1 >>> 25) & mask;
      output[20 + outputOffset] = (intValue1 >>> 28) & mask;
      output[21 + outputOffset] = ((intValue1 >>> 31) | (intValue2 << 1)) & mask;
      output[22 + outputOffset] = (intValue2 >>> 2) & mask;
      output[23 + outputOffset] = (intValue2 >>> 5) & mask;
      output[24 + outputOffset] = (intValue2 >>> 8) & mask;
      output[25 + outputOffset] = (intValue2 >>> 11) & mask;
      output[26 + outputOffset] = (intValue2 >>> 14) & mask;
      output[27 + outputOffset] = (intValue2 >>> 17) & mask;
      output[28 + outputOffset] = (intValue2 >>> 20) & mask;
      output[29 + outputOffset] = (intValue2 >>> 23) & mask;
      output[30 + outputOffset] = (intValue2 >>> 26) & mask;
      output[31 + outputOffset] = intValue2 >>> 29;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode4(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 4;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 4) & mask;
      output[2 + outputOffset] = (intValue0 >>> 8) & mask;
      output[3 + outputOffset] = (intValue0 >>> 12) & mask;
      output[4 + outputOffset] = (intValue0 >>> 16) & mask;
      output[5 + outputOffset] = (intValue0 >>> 20) & mask;
      output[6 + outputOffset] = (intValue0 >>> 24) & mask;
      output[7 + outputOffset] = intValue0 >>> 28;
      output[8 + outputOffset] = intValue1 & mask;
      output[9 + outputOffset] = (intValue1 >>> 4) & mask;
      output[10 + outputOffset] = (intValue1 >>> 8) & mask;
      output[11 + outputOffset] = (intValue1 >>> 12) & mask;
      output[12 + outputOffset] = (intValue1 >>> 16) & mask;
      output[13 + outputOffset] = (intValue1 >>> 20) & mask;
      output[14 + outputOffset] = (intValue1 >>> 24) & mask;
      output[15 + outputOffset] = intValue1 >>> 28;
      output[16 + outputOffset] = intValue2 & mask;
      output[17 + outputOffset] = (intValue2 >>> 4) & mask;
      output[18 + outputOffset] = (intValue2 >>> 8) & mask;
      output[19 + outputOffset] = (intValue2 >>> 12) & mask;
      output[20 + outputOffset] = (intValue2 >>> 16) & mask;
      output[21 + outputOffset] = (intValue2 >>> 20) & mask;
      output[22 + outputOffset] = (intValue2 >>> 24) & mask;
      output[23 + outputOffset] = intValue2 >>> 28;
      output[24 + outputOffset] = intValue3 & mask;
      output[25 + outputOffset] = (intValue3 >>> 4) & mask;
      output[26 + outputOffset] = (intValue3 >>> 8) & mask;
      output[27 + outputOffset] = (intValue3 >>> 12) & mask;
      output[28 + outputOffset] = (intValue3 >>> 16) & mask;
      output[29 + outputOffset] = (intValue3 >>> 20) & mask;
      output[30 + outputOffset] = (intValue3 >>> 24) & mask;
      output[31 + outputOffset] = intValue3 >>> 28;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode5(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 5;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 5) & mask;
      output[2 + outputOffset] = (intValue0 >>> 10) & mask;
      output[3 + outputOffset] = (intValue0 >>> 15) & mask;
      output[4 + outputOffset] = (intValue0 >>> 20) & mask;
      output[5 + outputOffset] = (intValue0 >>> 25) & mask;
      output[6 + outputOffset] = ((intValue0 >>> 30) | (intValue1 << 2)) & mask;
      output[7 + outputOffset] = (intValue1 >>> 3) & mask;
      output[8 + outputOffset] = (intValue1 >>> 8) & mask;
      output[9 + outputOffset] = (intValue1 >>> 13) & mask;
      output[10 + outputOffset] = (intValue1 >>> 18) & mask;
      output[11 + outputOffset] = (intValue1 >>> 23) & mask;
      output[12 + outputOffset] = ((intValue1 >>> 28) | (intValue2 << 4)) & mask;
      output[13 + outputOffset] = (intValue2 >>> 1) & mask;
      output[14 + outputOffset] = (intValue2 >>> 6) & mask;
      output[15 + outputOffset] = (intValue2 >>> 11) & mask;
      output[16 + outputOffset] = (intValue2 >>> 16) & mask;
      output[17 + outputOffset] = (intValue2 >>> 21) & mask;
      output[18 + outputOffset] = (intValue2 >>> 26) & mask;
      output[19 + outputOffset] = ((intValue2 >>> 31) | (intValue3 << 1)) & mask;
      output[20 + outputOffset] = (intValue3 >>> 4) & mask;
      output[21 + outputOffset] = (intValue3 >>> 9) & mask;
      output[22 + outputOffset] = (intValue3 >>> 14) & mask;
      output[23 + outputOffset] = (intValue3 >>> 19) & mask;
      output[24 + outputOffset] = (intValue3 >>> 24) & mask;
      output[25 + outputOffset] = ((intValue3 >>> 29) | (intValue4 << 3)) & mask;
      output[26 + outputOffset] = (intValue4 >>> 2) & mask;
      output[27 + outputOffset] = (intValue4 >>> 7) & mask;
      output[28 + outputOffset] = (intValue4 >>> 12) & mask;
      output[29 + outputOffset] = (intValue4 >>> 17) & mask;
      output[30 + outputOffset] = (intValue4 >>> 22) & mask;
      output[31 + outputOffset] = intValue4 >>> 27;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode6(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 6;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 6) & mask;
      output[2 + outputOffset] = (intValue0 >>> 12) & mask;
      output[3 + outputOffset] = (intValue0 >>> 18) & mask;
      output[4 + outputOffset] = (intValue0 >>> 24) & mask;
      output[5 + outputOffset] = ((intValue0 >>> 30) | (intValue1 << 2)) & mask;
      output[6 + outputOffset] = (intValue1 >>> 4) & mask;
      output[7 + outputOffset] = (intValue1 >>> 10) & mask;
      output[8 + outputOffset] = (intValue1 >>> 16) & mask;
      output[9 + outputOffset] = (intValue1 >>> 22) & mask;
      output[10 + outputOffset] = ((intValue1 >>> 28) | (intValue2 << 4)) & mask;
      output[11 + outputOffset] = (intValue2 >>> 2) & mask;
      output[12 + outputOffset] = (intValue2 >>> 8) & mask;
      output[13 + outputOffset] = (intValue2 >>> 14) & mask;
      output[14 + outputOffset] = (intValue2 >>> 20) & mask;
      output[15 + outputOffset] = intValue2 >>> 26;
      output[16 + outputOffset] = intValue3 & mask;
      output[17 + outputOffset] = (intValue3 >>> 6) & mask;
      output[18 + outputOffset] = (intValue3 >>> 12) & mask;
      output[19 + outputOffset] = (intValue3 >>> 18) & mask;
      output[20 + outputOffset] = (intValue3 >>> 24) & mask;
      output[21 + outputOffset] = ((intValue3 >>> 30) | (intValue4 << 2)) & mask;
      output[22 + outputOffset] = (intValue4 >>> 4) & mask;
      output[23 + outputOffset] = (intValue4 >>> 10) & mask;
      output[24 + outputOffset] = (intValue4 >>> 16) & mask;
      output[25 + outputOffset] = (intValue4 >>> 22) & mask;
      output[26 + outputOffset] = ((intValue4 >>> 28) | (intValue5 << 4)) & mask;
      output[27 + outputOffset] = (intValue5 >>> 2) & mask;
      output[28 + outputOffset] = (intValue5 >>> 8) & mask;
      output[29 + outputOffset] = (intValue5 >>> 14) & mask;
      output[30 + outputOffset] = (intValue5 >>> 20) & mask;
      output[31 + outputOffset] = intValue5 >>> 26;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode7(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 7;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 7) & mask;
      output[2 + outputOffset] = (intValue0 >>> 14) & mask;
      output[3 + outputOffset] = (intValue0 >>> 21) & mask;
      output[4 + outputOffset] = ((intValue0 >>> 28) | (intValue1 << 4)) & mask;
      output[5 + outputOffset] = (intValue1 >>> 3) & mask;
      output[6 + outputOffset] = (intValue1 >>> 10) & mask;
      output[7 + outputOffset] = (intValue1 >>> 17) & mask;
      output[8 + outputOffset] = (intValue1 >>> 24) & mask;
      output[9 + outputOffset] = ((intValue1 >>> 31) | (intValue2 << 1)) & mask;
      output[10 + outputOffset] = (intValue2 >>> 6) & mask;
      output[11 + outputOffset] = (intValue2 >>> 13) & mask;
      output[12 + outputOffset] = (intValue2 >>> 20) & mask;
      output[13 + outputOffset] = ((intValue2 >>> 27) | (intValue3 << 5)) & mask;
      output[14 + outputOffset] = (intValue3 >>> 2) & mask;
      output[15 + outputOffset] = (intValue3 >>> 9) & mask;
      output[16 + outputOffset] = (intValue3 >>> 16) & mask;
      output[17 + outputOffset] = (intValue3 >>> 23) & mask;
      output[18 + outputOffset] = ((intValue3 >>> 30) | (intValue4 << 2)) & mask;
      output[19 + outputOffset] = (intValue4 >>> 5) & mask;
      output[20 + outputOffset] = (intValue4 >>> 12) & mask;
      output[21 + outputOffset] = (intValue4 >>> 19) & mask;
      output[22 + outputOffset] = ((intValue4 >>> 26) | (intValue5 << 6)) & mask;
      output[23 + outputOffset] = (intValue5 >>> 1) & mask;
      output[24 + outputOffset] = (intValue5 >>> 8) & mask;
      output[25 + outputOffset] = (intValue5 >>> 15) & mask;
      output[26 + outputOffset] = (intValue5 >>> 22) & mask;
      output[27 + outputOffset] = ((intValue5 >>> 29) | (intValue6 << 3)) & mask;
      output[28 + outputOffset] = (intValue6 >>> 4) & mask;
      output[29 + outputOffset] = (intValue6 >>> 11) & mask;
      output[30 + outputOffset] = (intValue6 >>> 18) & mask;
      output[31 + outputOffset] = intValue6 >>> 25;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode8(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 8;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 8) & mask;
      output[2 + outputOffset] = (intValue0 >>> 16) & mask;
      output[3 + outputOffset] = intValue0 >>> 24;
      output[4 + outputOffset] = intValue1 & mask;
      output[5 + outputOffset] = (intValue1 >>> 8) & mask;
      output[6 + outputOffset] = (intValue1 >>> 16) & mask;
      output[7 + outputOffset] = intValue1 >>> 24;
      output[8 + outputOffset] = intValue2 & mask;
      output[9 + outputOffset] = (intValue2 >>> 8) & mask;
      output[10 + outputOffset] = (intValue2 >>> 16) & mask;
      output[11 + outputOffset] = intValue2 >>> 24;
      output[12 + outputOffset] = intValue3 & mask;
      output[13 + outputOffset] = (intValue3 >>> 8) & mask;
      output[14 + outputOffset] = (intValue3 >>> 16) & mask;
      output[15 + outputOffset] = intValue3 >>> 24;
      output[16 + outputOffset] = intValue4 & mask;
      output[17 + outputOffset] = (intValue4 >>> 8) & mask;
      output[18 + outputOffset] = (intValue4 >>> 16) & mask;
      output[19 + outputOffset] = intValue4 >>> 24;
      output[20 + outputOffset] = intValue5 & mask;
      output[21 + outputOffset] = (intValue5 >>> 8) & mask;
      output[22 + outputOffset] = (intValue5 >>> 16) & mask;
      output[23 + outputOffset] = intValue5 >>> 24;
      output[24 + outputOffset] = intValue6 & mask;
      output[25 + outputOffset] = (intValue6 >>> 8) & mask;
      output[26 + outputOffset] = (intValue6 >>> 16) & mask;
      output[27 + outputOffset] = intValue6 >>> 24;
      output[28 + outputOffset] = intValue7 & mask;
      output[29 + outputOffset] = (intValue7 >>> 8) & mask;
      output[30 + outputOffset] = (intValue7 >>> 16) & mask;
      output[31 + outputOffset] = intValue7 >>> 24;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode9(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 9;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 9) & mask;
      output[2 + outputOffset] = (intValue0 >>> 18) & mask;
      output[3 + outputOffset] = ((intValue0 >>> 27) | (intValue1 << 5)) & mask;
      output[4 + outputOffset] = (intValue1 >>> 4) & mask;
      output[5 + outputOffset] = (intValue1 >>> 13) & mask;
      output[6 + outputOffset] = (intValue1 >>> 22) & mask;
      output[7 + outputOffset] = ((intValue1 >>> 31) | (intValue2 << 1)) & mask;
      output[8 + outputOffset] = (intValue2 >>> 8) & mask;
      output[9 + outputOffset] = (intValue2 >>> 17) & mask;
      output[10 + outputOffset] = ((intValue2 >>> 26) | (intValue3 << 6)) & mask;
      output[11 + outputOffset] = (intValue3 >>> 3) & mask;
      output[12 + outputOffset] = (intValue3 >>> 12) & mask;
      output[13 + outputOffset] = (intValue3 >>> 21) & mask;
      output[14 + outputOffset] = ((intValue3 >>> 30) | (intValue4 << 2)) & mask;
      output[15 + outputOffset] = (intValue4 >>> 7) & mask;
      output[16 + outputOffset] = (intValue4 >>> 16) & mask;
      output[17 + outputOffset] = ((intValue4 >>> 25) | (intValue5 << 7)) & mask;
      output[18 + outputOffset] = (intValue5 >>> 2) & mask;
      output[19 + outputOffset] = (intValue5 >>> 11) & mask;
      output[20 + outputOffset] = (intValue5 >>> 20) & mask;
      output[21 + outputOffset] = ((intValue5 >>> 29) | (intValue6 << 3)) & mask;
      output[22 + outputOffset] = (intValue6 >>> 6) & mask;
      output[23 + outputOffset] = (intValue6 >>> 15) & mask;
      output[24 + outputOffset] = ((intValue6 >>> 24) | (intValue7 << 8)) & mask;
      output[25 + outputOffset] = (intValue7 >>> 1) & mask;
      output[26 + outputOffset] = (intValue7 >>> 10) & mask;
      output[27 + outputOffset] = (intValue7 >>> 19) & mask;
      output[28 + outputOffset] = ((intValue7 >>> 28) | (intValue8 << 4)) & mask;
      output[29 + outputOffset] = (intValue8 >>> 5) & mask;
      output[30 + outputOffset] = (intValue8 >>> 14) & mask;
      output[31 + outputOffset] = intValue8 >>> 23;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode10(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 10;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 10) & mask;
      output[2 + outputOffset] = (intValue0 >>> 20) & mask;
      output[3 + outputOffset] = ((intValue0 >>> 30) | (intValue1 << 2)) & mask;
      output[4 + outputOffset] = (intValue1 >>> 8) & mask;
      output[5 + outputOffset] = (intValue1 >>> 18) & mask;
      output[6 + outputOffset] = ((intValue1 >>> 28) | (intValue2 << 4)) & mask;
      output[7 + outputOffset] = (intValue2 >>> 6) & mask;
      output[8 + outputOffset] = (intValue2 >>> 16) & mask;
      output[9 + outputOffset] = ((intValue2 >>> 26) | (intValue3 << 6)) & mask;
      output[10 + outputOffset] = (intValue3 >>> 4) & mask;
      output[11 + outputOffset] = (intValue3 >>> 14) & mask;
      output[12 + outputOffset] = ((intValue3 >>> 24) | (intValue4 << 8)) & mask;
      output[13 + outputOffset] = (intValue4 >>> 2) & mask;
      output[14 + outputOffset] = (intValue4 >>> 12) & mask;
      output[15 + outputOffset] = intValue4 >>> 22;
      output[16 + outputOffset] = intValue5 & mask;
      output[17 + outputOffset] = (intValue5 >>> 10) & mask;
      output[18 + outputOffset] = (intValue5 >>> 20) & mask;
      output[19 + outputOffset] = ((intValue5 >>> 30) | (intValue6 << 2)) & mask;
      output[20 + outputOffset] = (intValue6 >>> 8) & mask;
      output[21 + outputOffset] = (intValue6 >>> 18) & mask;
      output[22 + outputOffset] = ((intValue6 >>> 28) | (intValue7 << 4)) & mask;
      output[23 + outputOffset] = (intValue7 >>> 6) & mask;
      output[24 + outputOffset] = (intValue7 >>> 16) & mask;
      output[25 + outputOffset] = ((intValue7 >>> 26) | (intValue8 << 6)) & mask;
      output[26 + outputOffset] = (intValue8 >>> 4) & mask;
      output[27 + outputOffset] = (intValue8 >>> 14) & mask;
      output[28 + outputOffset] = ((intValue8 >>> 24) | (intValue9 << 8)) & mask;
      output[29 + outputOffset] = (intValue9 >>> 2) & mask;
      output[30 + outputOffset] = (intValue9 >>> 12) & mask;
      output[31 + outputOffset] = intValue9 >>> 22;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode11(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 11;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 11) & mask;
      output[2 + outputOffset] = ((intValue0 >>> 22) | (intValue1 << 10)) & mask;
      output[3 + outputOffset] = (intValue1 >>> 1) & mask;
      output[4 + outputOffset] = (intValue1 >>> 12) & mask;
      output[5 + outputOffset] = ((intValue1 >>> 23) | (intValue2 << 9)) & mask;
      output[6 + outputOffset] = (intValue2 >>> 2) & mask;
      output[7 + outputOffset] = (intValue2 >>> 13) & mask;
      output[8 + outputOffset] = ((intValue2 >>> 24) | (intValue3 << 8)) & mask;
      output[9 + outputOffset] = (intValue3 >>> 3) & mask;
      output[10 + outputOffset] = (intValue3 >>> 14) & mask;
      output[11 + outputOffset] = ((intValue3 >>> 25) | (intValue4 << 7)) & mask;
      output[12 + outputOffset] = (intValue4 >>> 4) & mask;
      output[13 + outputOffset] = (intValue4 >>> 15) & mask;
      output[14 + outputOffset] = ((intValue4 >>> 26) | (intValue5 << 6)) & mask;
      output[15 + outputOffset] = (intValue5 >>> 5) & mask;
      output[16 + outputOffset] = (intValue5 >>> 16) & mask;
      output[17 + outputOffset] = ((intValue5 >>> 27) | (intValue6 << 5)) & mask;
      output[18 + outputOffset] = (intValue6 >>> 6) & mask;
      output[19 + outputOffset] = (intValue6 >>> 17) & mask;
      output[20 + outputOffset] = ((intValue6 >>> 28) | (intValue7 << 4)) & mask;
      output[21 + outputOffset] = (intValue7 >>> 7) & mask;
      output[22 + outputOffset] = (intValue7 >>> 18) & mask;
      output[23 + outputOffset] = ((intValue7 >>> 29) | (intValue8 << 3)) & mask;
      output[24 + outputOffset] = (intValue8 >>> 8) & mask;
      output[25 + outputOffset] = (intValue8 >>> 19) & mask;
      output[26 + outputOffset] = ((intValue8 >>> 30) | (intValue9 << 2)) & mask;
      output[27 + outputOffset] = (intValue9 >>> 9) & mask;
      output[28 + outputOffset] = (intValue9 >>> 20) & mask;
      output[29 + outputOffset] = ((intValue9 >>> 31) | (intValue10 << 1)) & mask;
      output[30 + outputOffset] = (intValue10 >>> 10) & mask;
      output[31 + outputOffset] = intValue10 >>> 21;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode12(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 12;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 12) & mask;
      output[2 + outputOffset] = ((intValue0 >>> 24) | (intValue1 << 8)) & mask;
      output[3 + outputOffset] = (intValue1 >>> 4) & mask;
      output[4 + outputOffset] = (intValue1 >>> 16) & mask;
      output[5 + outputOffset] = ((intValue1 >>> 28) | (intValue2 << 4)) & mask;
      output[6 + outputOffset] = (intValue2 >>> 8) & mask;
      output[7 + outputOffset] = intValue2 >>> 20;
      output[8 + outputOffset] = intValue3 & mask;
      output[9 + outputOffset] = (intValue3 >>> 12) & mask;
      output[10 + outputOffset] = ((intValue3 >>> 24) | (intValue4 << 8)) & mask;
      output[11 + outputOffset] = (intValue4 >>> 4) & mask;
      output[12 + outputOffset] = (intValue4 >>> 16) & mask;
      output[13 + outputOffset] = ((intValue4 >>> 28) | (intValue5 << 4)) & mask;
      output[14 + outputOffset] = (intValue5 >>> 8) & mask;
      output[15 + outputOffset] = intValue5 >>> 20;
      output[16 + outputOffset] = intValue6 & mask;
      output[17 + outputOffset] = (intValue6 >>> 12) & mask;
      output[18 + outputOffset] = ((intValue6 >>> 24) | (intValue7 << 8)) & mask;
      output[19 + outputOffset] = (intValue7 >>> 4) & mask;
      output[20 + outputOffset] = (intValue7 >>> 16) & mask;
      output[21 + outputOffset] = ((intValue7 >>> 28) | (intValue8 << 4)) & mask;
      output[22 + outputOffset] = (intValue8 >>> 8) & mask;
      output[23 + outputOffset] = intValue8 >>> 20;
      output[24 + outputOffset] = intValue9 & mask;
      output[25 + outputOffset] = (intValue9 >>> 12) & mask;
      output[26 + outputOffset] = ((intValue9 >>> 24) | (intValue10 << 8)) & mask;
      output[27 + outputOffset] = (intValue10 >>> 4) & mask;
      output[28 + outputOffset] = (intValue10 >>> 16) & mask;
      output[29 + outputOffset] = ((intValue10 >>> 28) | (intValue11 << 4)) & mask;
      output[30 + outputOffset] = (intValue11 >>> 8) & mask;
      output[31 + outputOffset] = intValue11 >>> 20;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode13(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 13;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 13) & mask;
      output[2 + outputOffset] = ((intValue0 >>> 26) | (intValue1 << 6)) & mask;
      output[3 + outputOffset] = (intValue1 >>> 7) & mask;
      output[4 + outputOffset] = ((intValue1 >>> 20) | (intValue2 << 12)) & mask;
      output[5 + outputOffset] = (intValue2 >>> 1) & mask;
      output[6 + outputOffset] = (intValue2 >>> 14) & mask;
      output[7 + outputOffset] = ((intValue2 >>> 27) | (intValue3 << 5)) & mask;
      output[8 + outputOffset] = (intValue3 >>> 8) & mask;
      output[9 + outputOffset] = ((intValue3 >>> 21) | (intValue4 << 11)) & mask;
      output[10 + outputOffset] = (intValue4 >>> 2) & mask;
      output[11 + outputOffset] = (intValue4 >>> 15) & mask;
      output[12 + outputOffset] = ((intValue4 >>> 28) | (intValue5 << 4)) & mask;
      output[13 + outputOffset] = (intValue5 >>> 9) & mask;
      output[14 + outputOffset] = ((intValue5 >>> 22) | (intValue6 << 10)) & mask;
      output[15 + outputOffset] = (intValue6 >>> 3) & mask;
      output[16 + outputOffset] = (intValue6 >>> 16) & mask;
      output[17 + outputOffset] = ((intValue6 >>> 29) | (intValue7 << 3)) & mask;
      output[18 + outputOffset] = (intValue7 >>> 10) & mask;
      output[19 + outputOffset] = ((intValue7 >>> 23) | (intValue8 << 9)) & mask;
      output[20 + outputOffset] = (intValue8 >>> 4) & mask;
      output[21 + outputOffset] = (intValue8 >>> 17) & mask;
      output[22 + outputOffset] = ((intValue8 >>> 30) | (intValue9 << 2)) & mask;
      output[23 + outputOffset] = (intValue9 >>> 11) & mask;
      output[24 + outputOffset] = ((intValue9 >>> 24) | (intValue10 << 8)) & mask;
      output[25 + outputOffset] = (intValue10 >>> 5) & mask;
      output[26 + outputOffset] = (intValue10 >>> 18) & mask;
      output[27 + outputOffset] = ((intValue10 >>> 31) | (intValue11 << 1)) & mask;
      output[28 + outputOffset] = (intValue11 >>> 12) & mask;
      output[29 + outputOffset] = ((intValue11 >>> 25) | (intValue12 << 7)) & mask;
      output[30 + outputOffset] = (intValue12 >>> 6) & mask;
      output[31 + outputOffset] = intValue12 >>> 19;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode14(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 14;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 14) & mask;
      output[2 + outputOffset] = ((intValue0 >>> 28) | (intValue1 << 4)) & mask;
      output[3 + outputOffset] = (intValue1 >>> 10) & mask;
      output[4 + outputOffset] = ((intValue1 >>> 24) | (intValue2 << 8)) & mask;
      output[5 + outputOffset] = (intValue2 >>> 6) & mask;
      output[6 + outputOffset] = ((intValue2 >>> 20) | (intValue3 << 12)) & mask;
      output[7 + outputOffset] = (intValue3 >>> 2) & mask;
      output[8 + outputOffset] = (intValue3 >>> 16) & mask;
      output[9 + outputOffset] = ((intValue3 >>> 30) | (intValue4 << 2)) & mask;
      output[10 + outputOffset] = (intValue4 >>> 12) & mask;
      output[11 + outputOffset] = ((intValue4 >>> 26) | (intValue5 << 6)) & mask;
      output[12 + outputOffset] = (intValue5 >>> 8) & mask;
      output[13 + outputOffset] = ((intValue5 >>> 22) | (intValue6 << 10)) & mask;
      output[14 + outputOffset] = (intValue6 >>> 4) & mask;
      output[15 + outputOffset] = intValue6 >>> 18;
      output[16 + outputOffset] = intValue7 & mask;
      output[17 + outputOffset] = (intValue7 >>> 14) & mask;
      output[18 + outputOffset] = ((intValue7 >>> 28) | (intValue8 << 4)) & mask;
      output[19 + outputOffset] = (intValue8 >>> 10) & mask;
      output[20 + outputOffset] = ((intValue8 >>> 24) | (intValue9 << 8)) & mask;
      output[21 + outputOffset] = (intValue9 >>> 6) & mask;
      output[22 + outputOffset] = ((intValue9 >>> 20) | (intValue10 << 12)) & mask;
      output[23 + outputOffset] = (intValue10 >>> 2) & mask;
      output[24 + outputOffset] = (intValue10 >>> 16) & mask;
      output[25 + outputOffset] = ((intValue10 >>> 30) | (intValue11 << 2)) & mask;
      output[26 + outputOffset] = (intValue11 >>> 12) & mask;
      output[27 + outputOffset] = ((intValue11 >>> 26) | (intValue12 << 6)) & mask;
      output[28 + outputOffset] = (intValue12 >>> 8) & mask;
      output[29 + outputOffset] = ((intValue12 >>> 22) | (intValue13 << 10)) & mask;
      output[30 + outputOffset] = (intValue13 >>> 4) & mask;
      output[31 + outputOffset] = intValue13 >>> 18;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode15(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 15;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = (intValue0 >>> 15) & mask;
      output[2 + outputOffset] = ((intValue0 >>> 30) | (intValue1 << 2)) & mask;
      output[3 + outputOffset] = (intValue1 >>> 13) & mask;
      output[4 + outputOffset] = ((intValue1 >>> 28) | (intValue2 << 4)) & mask;
      output[5 + outputOffset] = (intValue2 >>> 11) & mask;
      output[6 + outputOffset] = ((intValue2 >>> 26) | (intValue3 << 6)) & mask;
      output[7 + outputOffset] = (intValue3 >>> 9) & mask;
      output[8 + outputOffset] = ((intValue3 >>> 24) | (intValue4 << 8)) & mask;
      output[9 + outputOffset] = (intValue4 >>> 7) & mask;
      output[10 + outputOffset] = ((intValue4 >>> 22) | (intValue5 << 10)) & mask;
      output[11 + outputOffset] = (intValue5 >>> 5) & mask;
      output[12 + outputOffset] = ((intValue5 >>> 20) | (intValue6 << 12)) & mask;
      output[13 + outputOffset] = (intValue6 >>> 3) & mask;
      output[14 + outputOffset] = ((intValue6 >>> 18) | (intValue7 << 14)) & mask;
      output[15 + outputOffset] = (intValue7 >>> 1) & mask;
      output[16 + outputOffset] = (intValue7 >>> 16) & mask;
      output[17 + outputOffset] = ((intValue7 >>> 31) | (intValue8 << 1)) & mask;
      output[18 + outputOffset] = (intValue8 >>> 14) & mask;
      output[19 + outputOffset] = ((intValue8 >>> 29) | (intValue9 << 3)) & mask;
      output[20 + outputOffset] = (intValue9 >>> 12) & mask;
      output[21 + outputOffset] = ((intValue9 >>> 27) | (intValue10 << 5)) & mask;
      output[22 + outputOffset] = (intValue10 >>> 10) & mask;
      output[23 + outputOffset] = ((intValue10 >>> 25) | (intValue11 << 7)) & mask;
      output[24 + outputOffset] = (intValue11 >>> 8) & mask;
      output[25 + outputOffset] = ((intValue11 >>> 23) | (intValue12 << 9)) & mask;
      output[26 + outputOffset] = (intValue12 >>> 6) & mask;
      output[27 + outputOffset] = ((intValue12 >>> 21) | (intValue13 << 11)) & mask;
      output[28 + outputOffset] = (intValue13 >>> 4) & mask;
      output[29 + outputOffset] = ((intValue13 >>> 19) | (intValue14 << 13)) & mask;
      output[30 + outputOffset] = (intValue14 >>> 2) & mask;
      output[31 + outputOffset] = intValue14 >>> 17;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode16(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 16;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = intValue0 >>> 16;
      output[2 + outputOffset] = intValue1 & mask;
      output[3 + outputOffset] = intValue1 >>> 16;
      output[4 + outputOffset] = intValue2 & mask;
      output[5 + outputOffset] = intValue2 >>> 16;
      output[6 + outputOffset] = intValue3 & mask;
      output[7 + outputOffset] = intValue3 >>> 16;
      output[8 + outputOffset] = intValue4 & mask;
      output[9 + outputOffset] = intValue4 >>> 16;
      output[10 + outputOffset] = intValue5 & mask;
      output[11 + outputOffset] = intValue5 >>> 16;
      output[12 + outputOffset] = intValue6 & mask;
      output[13 + outputOffset] = intValue6 >>> 16;
      output[14 + outputOffset] = intValue7 & mask;
      output[15 + outputOffset] = intValue7 >>> 16;
      output[16 + outputOffset] = intValue8 & mask;
      output[17 + outputOffset] = intValue8 >>> 16;
      output[18 + outputOffset] = intValue9 & mask;
      output[19 + outputOffset] = intValue9 >>> 16;
      output[20 + outputOffset] = intValue10 & mask;
      output[21 + outputOffset] = intValue10 >>> 16;
      output[22 + outputOffset] = intValue11 & mask;
      output[23 + outputOffset] = intValue11 >>> 16;
      output[24 + outputOffset] = intValue12 & mask;
      output[25 + outputOffset] = intValue12 >>> 16;
      output[26 + outputOffset] = intValue13 & mask;
      output[27 + outputOffset] = intValue13 >>> 16;
      output[28 + outputOffset] = intValue14 & mask;
      output[29 + outputOffset] = intValue14 >>> 16;
      output[30 + outputOffset] = intValue15 & mask;
      output[31 + outputOffset] = intValue15 >>> 16;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode17(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 17;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 17) | (intValue1 << 15)) & mask;
      output[2 + outputOffset] = (intValue1 >>> 2) & mask;
      output[3 + outputOffset] = ((intValue1 >>> 19) | (intValue2 << 13)) & mask;
      output[4 + outputOffset] = (intValue2 >>> 4) & mask;
      output[5 + outputOffset] = ((intValue2 >>> 21) | (intValue3 << 11)) & mask;
      output[6 + outputOffset] = (intValue3 >>> 6) & mask;
      output[7 + outputOffset] = ((intValue3 >>> 23) | (intValue4 << 9)) & mask;
      output[8 + outputOffset] = (intValue4 >>> 8) & mask;
      output[9 + outputOffset] = ((intValue4 >>> 25) | (intValue5 << 7)) & mask;
      output[10 + outputOffset] = (intValue5 >>> 10) & mask;
      output[11 + outputOffset] = ((intValue5 >>> 27) | (intValue6 << 5)) & mask;
      output[12 + outputOffset] = (intValue6 >>> 12) & mask;
      output[13 + outputOffset] = ((intValue6 >>> 29) | (intValue7 << 3)) & mask;
      output[14 + outputOffset] = (intValue7 >>> 14) & mask;
      output[15 + outputOffset] = ((intValue7 >>> 31) | (intValue8 << 1)) & mask;
      output[16 + outputOffset] = ((intValue8 >>> 16) | (intValue9 << 16)) & mask;
      output[17 + outputOffset] = (intValue9 >>> 1) & mask;
      output[18 + outputOffset] = ((intValue9 >>> 18) | (intValue10 << 14)) & mask;
      output[19 + outputOffset] = (intValue10 >>> 3) & mask;
      output[20 + outputOffset] = ((intValue10 >>> 20) | (intValue11 << 12)) & mask;
      output[21 + outputOffset] = (intValue11 >>> 5) & mask;
      output[22 + outputOffset] = ((intValue11 >>> 22) | (intValue12 << 10)) & mask;
      output[23 + outputOffset] = (intValue12 >>> 7) & mask;
      output[24 + outputOffset] = ((intValue12 >>> 24) | (intValue13 << 8)) & mask;
      output[25 + outputOffset] = (intValue13 >>> 9) & mask;
      output[26 + outputOffset] = ((intValue13 >>> 26) | (intValue14 << 6)) & mask;
      output[27 + outputOffset] = (intValue14 >>> 11) & mask;
      output[28 + outputOffset] = ((intValue14 >>> 28) | (intValue15 << 4)) & mask;
      output[29 + outputOffset] = (intValue15 >>> 13) & mask;
      output[30 + outputOffset] = ((intValue15 >>> 30) | (intValue16 << 2)) & mask;
      output[31 + outputOffset] = intValue16 >>> 15;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode18(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 18;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 18) | (intValue1 << 14)) & mask;
      output[2 + outputOffset] = (intValue1 >>> 4) & mask;
      output[3 + outputOffset] = ((intValue1 >>> 22) | (intValue2 << 10)) & mask;
      output[4 + outputOffset] = (intValue2 >>> 8) & mask;
      output[5 + outputOffset] = ((intValue2 >>> 26) | (intValue3 << 6)) & mask;
      output[6 + outputOffset] = (intValue3 >>> 12) & mask;
      output[7 + outputOffset] = ((intValue3 >>> 30) | (intValue4 << 2)) & mask;
      output[8 + outputOffset] = ((intValue4 >>> 16) | (intValue5 << 16)) & mask;
      output[9 + outputOffset] = (intValue5 >>> 2) & mask;
      output[10 + outputOffset] = ((intValue5 >>> 20) | (intValue6 << 12)) & mask;
      output[11 + outputOffset] = (intValue6 >>> 6) & mask;
      output[12 + outputOffset] = ((intValue6 >>> 24) | (intValue7 << 8)) & mask;
      output[13 + outputOffset] = (intValue7 >>> 10) & mask;
      output[14 + outputOffset] = ((intValue7 >>> 28) | (intValue8 << 4)) & mask;
      output[15 + outputOffset] = intValue8 >>> 14;
      output[16 + outputOffset] = intValue9 & mask;
      output[17 + outputOffset] = ((intValue9 >>> 18) | (intValue10 << 14)) & mask;
      output[18 + outputOffset] = (intValue10 >>> 4) & mask;
      output[19 + outputOffset] = ((intValue10 >>> 22) | (intValue11 << 10)) & mask;
      output[20 + outputOffset] = (intValue11 >>> 8) & mask;
      output[21 + outputOffset] = ((intValue11 >>> 26) | (intValue12 << 6)) & mask;
      output[22 + outputOffset] = (intValue12 >>> 12) & mask;
      output[23 + outputOffset] = ((intValue12 >>> 30) | (intValue13 << 2)) & mask;
      output[24 + outputOffset] = ((intValue13 >>> 16) | (intValue14 << 16)) & mask;
      output[25 + outputOffset] = (intValue14 >>> 2) & mask;
      output[26 + outputOffset] = ((intValue14 >>> 20) | (intValue15 << 12)) & mask;
      output[27 + outputOffset] = (intValue15 >>> 6) & mask;
      output[28 + outputOffset] = ((intValue15 >>> 24) | (intValue16 << 8)) & mask;
      output[29 + outputOffset] = (intValue16 >>> 10) & mask;
      output[30 + outputOffset] = ((intValue16 >>> 28) | (intValue17 << 4)) & mask;
      output[31 + outputOffset] = intValue17 >>> 14;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode19(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 19;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 19) | (intValue1 << 13)) & mask;
      output[2 + outputOffset] = (intValue1 >>> 6) & mask;
      output[3 + outputOffset] = ((intValue1 >>> 25) | (intValue2 << 7)) & mask;
      output[4 + outputOffset] = (intValue2 >>> 12) & mask;
      output[5 + outputOffset] = ((intValue2 >>> 31) | (intValue3 << 1)) & mask;
      output[6 + outputOffset] = ((intValue3 >>> 18) | (intValue4 << 14)) & mask;
      output[7 + outputOffset] = (intValue4 >>> 5) & mask;
      output[8 + outputOffset] = ((intValue4 >>> 24) | (intValue5 << 8)) & mask;
      output[9 + outputOffset] = (intValue5 >>> 11) & mask;
      output[10 + outputOffset] = ((intValue5 >>> 30) | (intValue6 << 2)) & mask;
      output[11 + outputOffset] = ((intValue6 >>> 17) | (intValue7 << 15)) & mask;
      output[12 + outputOffset] = (intValue7 >>> 4) & mask;
      output[13 + outputOffset] = ((intValue7 >>> 23) | (intValue8 << 9)) & mask;
      output[14 + outputOffset] = (intValue8 >>> 10) & mask;
      output[15 + outputOffset] = ((intValue8 >>> 29) | (intValue9 << 3)) & mask;
      output[16 + outputOffset] = ((intValue9 >>> 16) | (intValue10 << 16)) & mask;
      output[17 + outputOffset] = (intValue10 >>> 3) & mask;
      output[18 + outputOffset] = ((intValue10 >>> 22) | (intValue11 << 10)) & mask;
      output[19 + outputOffset] = (intValue11 >>> 9) & mask;
      output[20 + outputOffset] = ((intValue11 >>> 28) | (intValue12 << 4)) & mask;
      output[21 + outputOffset] = ((intValue12 >>> 15) | (intValue13 << 17)) & mask;
      output[22 + outputOffset] = (intValue13 >>> 2) & mask;
      output[23 + outputOffset] = ((intValue13 >>> 21) | (intValue14 << 11)) & mask;
      output[24 + outputOffset] = (intValue14 >>> 8) & mask;
      output[25 + outputOffset] = ((intValue14 >>> 27) | (intValue15 << 5)) & mask;
      output[26 + outputOffset] = ((intValue15 >>> 14) | (intValue16 << 18)) & mask;
      output[27 + outputOffset] = (intValue16 >>> 1) & mask;
      output[28 + outputOffset] = ((intValue16 >>> 20) | (intValue17 << 12)) & mask;
      output[29 + outputOffset] = (intValue17 >>> 7) & mask;
      output[30 + outputOffset] = ((intValue17 >>> 26) | (intValue18 << 6)) & mask;
      output[31 + outputOffset] = intValue18 >>> 13;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode20(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 20;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 20) | (intValue1 << 12)) & mask;
      output[2 + outputOffset] = (intValue1 >>> 8) & mask;
      output[3 + outputOffset] = ((intValue1 >>> 28) | (intValue2 << 4)) & mask;
      output[4 + outputOffset] = ((intValue2 >>> 16) | (intValue3 << 16)) & mask;
      output[5 + outputOffset] = (intValue3 >>> 4) & mask;
      output[6 + outputOffset] = ((intValue3 >>> 24) | (intValue4 << 8)) & mask;
      output[7 + outputOffset] = intValue4 >>> 12;
      output[8 + outputOffset] = intValue5 & mask;
      output[9 + outputOffset] = ((intValue5 >>> 20) | (intValue6 << 12)) & mask;
      output[10 + outputOffset] = (intValue6 >>> 8) & mask;
      output[11 + outputOffset] = ((intValue6 >>> 28) | (intValue7 << 4)) & mask;
      output[12 + outputOffset] = ((intValue7 >>> 16) | (intValue8 << 16)) & mask;
      output[13 + outputOffset] = (intValue8 >>> 4) & mask;
      output[14 + outputOffset] = ((intValue8 >>> 24) | (intValue9 << 8)) & mask;
      output[15 + outputOffset] = intValue9 >>> 12;
      output[16 + outputOffset] = intValue10 & mask;
      output[17 + outputOffset] = ((intValue10 >>> 20) | (intValue11 << 12)) & mask;
      output[18 + outputOffset] = (intValue11 >>> 8) & mask;
      output[19 + outputOffset] = ((intValue11 >>> 28) | (intValue12 << 4)) & mask;
      output[20 + outputOffset] = ((intValue12 >>> 16) | (intValue13 << 16)) & mask;
      output[21 + outputOffset] = (intValue13 >>> 4) & mask;
      output[22 + outputOffset] = ((intValue13 >>> 24) | (intValue14 << 8)) & mask;
      output[23 + outputOffset] = intValue14 >>> 12;
      output[24 + outputOffset] = intValue15 & mask;
      output[25 + outputOffset] = ((intValue15 >>> 20) | (intValue16 << 12)) & mask;
      output[26 + outputOffset] = (intValue16 >>> 8) & mask;
      output[27 + outputOffset] = ((intValue16 >>> 28) | (intValue17 << 4)) & mask;
      output[28 + outputOffset] = ((intValue17 >>> 16) | (intValue18 << 16)) & mask;
      output[29 + outputOffset] = (intValue18 >>> 4) & mask;
      output[30 + outputOffset] = ((intValue18 >>> 24) | (intValue19 << 8)) & mask;
      output[31 + outputOffset] = intValue19 >>> 12;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode21(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 21;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 21) | (intValue1 << 11)) & mask;
      output[2 + outputOffset] = (intValue1 >>> 10) & mask;
      output[3 + outputOffset] = ((intValue1 >>> 31) | (intValue2 << 1)) & mask;
      output[4 + outputOffset] = ((intValue2 >>> 20) | (intValue3 << 12)) & mask;
      output[5 + outputOffset] = (intValue3 >>> 9) & mask;
      output[6 + outputOffset] = ((intValue3 >>> 30) | (intValue4 << 2)) & mask;
      output[7 + outputOffset] = ((intValue4 >>> 19) | (intValue5 << 13)) & mask;
      output[8 + outputOffset] = (intValue5 >>> 8) & mask;
      output[9 + outputOffset] = ((intValue5 >>> 29) | (intValue6 << 3)) & mask;
      output[10 + outputOffset] = ((intValue6 >>> 18) | (intValue7 << 14)) & mask;
      output[11 + outputOffset] = (intValue7 >>> 7) & mask;
      output[12 + outputOffset] = ((intValue7 >>> 28) | (intValue8 << 4)) & mask;
      output[13 + outputOffset] = ((intValue8 >>> 17) | (intValue9 << 15)) & mask;
      output[14 + outputOffset] = (intValue9 >>> 6) & mask;
      output[15 + outputOffset] = ((intValue9 >>> 27) | (intValue10 << 5)) & mask;
      output[16 + outputOffset] = ((intValue10 >>> 16) | (intValue11 << 16)) & mask;
      output[17 + outputOffset] = (intValue11 >>> 5) & mask;
      output[18 + outputOffset] = ((intValue11 >>> 26) | (intValue12 << 6)) & mask;
      output[19 + outputOffset] = ((intValue12 >>> 15) | (intValue13 << 17)) & mask;
      output[20 + outputOffset] = (intValue13 >>> 4) & mask;
      output[21 + outputOffset] = ((intValue13 >>> 25) | (intValue14 << 7)) & mask;
      output[22 + outputOffset] = ((intValue14 >>> 14) | (intValue15 << 18)) & mask;
      output[23 + outputOffset] = (intValue15 >>> 3) & mask;
      output[24 + outputOffset] = ((intValue15 >>> 24) | (intValue16 << 8)) & mask;
      output[25 + outputOffset] = ((intValue16 >>> 13) | (intValue17 << 19)) & mask;
      output[26 + outputOffset] = (intValue17 >>> 2) & mask;
      output[27 + outputOffset] = ((intValue17 >>> 23) | (intValue18 << 9)) & mask;
      output[28 + outputOffset] = ((intValue18 >>> 12) | (intValue19 << 20)) & mask;
      output[29 + outputOffset] = (intValue19 >>> 1) & mask;
      output[30 + outputOffset] = ((intValue19 >>> 22) | (intValue20 << 10)) & mask;
      output[31 + outputOffset] = intValue20 >>> 11;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode22(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 22;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      int intValue21 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 22) | (intValue1 << 10)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 12) | (intValue2 << 20)) & mask;
      output[3 + outputOffset] = (intValue2 >>> 2) & mask;
      output[4 + outputOffset] = ((intValue2 >>> 24) | (intValue3 << 8)) & mask;
      output[5 + outputOffset] = ((intValue3 >>> 14) | (intValue4 << 18)) & mask;
      output[6 + outputOffset] = (intValue4 >>> 4) & mask;
      output[7 + outputOffset] = ((intValue4 >>> 26) | (intValue5 << 6)) & mask;
      output[8 + outputOffset] = ((intValue5 >>> 16) | (intValue6 << 16)) & mask;
      output[9 + outputOffset] = (intValue6 >>> 6) & mask;
      output[10 + outputOffset] = ((intValue6 >>> 28) | (intValue7 << 4)) & mask;
      output[11 + outputOffset] = ((intValue7 >>> 18) | (intValue8 << 14)) & mask;
      output[12 + outputOffset] = (intValue8 >>> 8) & mask;
      output[13 + outputOffset] = ((intValue8 >>> 30) | (intValue9 << 2)) & mask;
      output[14 + outputOffset] = ((intValue9 >>> 20) | (intValue10 << 12)) & mask;
      output[15 + outputOffset] = intValue10 >>> 10;
      output[16 + outputOffset] = intValue11 & mask;
      output[17 + outputOffset] = ((intValue11 >>> 22) | (intValue12 << 10)) & mask;
      output[18 + outputOffset] = ((intValue12 >>> 12) | (intValue13 << 20)) & mask;
      output[19 + outputOffset] = (intValue13 >>> 2) & mask;
      output[20 + outputOffset] = ((intValue13 >>> 24) | (intValue14 << 8)) & mask;
      output[21 + outputOffset] = ((intValue14 >>> 14) | (intValue15 << 18)) & mask;
      output[22 + outputOffset] = (intValue15 >>> 4) & mask;
      output[23 + outputOffset] = ((intValue15 >>> 26) | (intValue16 << 6)) & mask;
      output[24 + outputOffset] = ((intValue16 >>> 16) | (intValue17 << 16)) & mask;
      output[25 + outputOffset] = (intValue17 >>> 6) & mask;
      output[26 + outputOffset] = ((intValue17 >>> 28) | (intValue18 << 4)) & mask;
      output[27 + outputOffset] = ((intValue18 >>> 18) | (intValue19 << 14)) & mask;
      output[28 + outputOffset] = (intValue19 >>> 8) & mask;
      output[29 + outputOffset] = ((intValue19 >>> 30) | (intValue20 << 2)) & mask;
      output[30 + outputOffset] = ((intValue20 >>> 20) | (intValue21 << 12)) & mask;
      output[31 + outputOffset] = intValue21 >>> 10;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode23(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 23;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      int intValue21 = compressedBuffer.get();
      int intValue22 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 23) | (intValue1 << 9)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 14) | (intValue2 << 18)) & mask;
      output[3 + outputOffset] = (intValue2 >>> 5) & mask;
      output[4 + outputOffset] = ((intValue2 >>> 28) | (intValue3 << 4)) & mask;
      output[5 + outputOffset] = ((intValue3 >>> 19) | (intValue4 << 13)) & mask;
      output[6 + outputOffset] = ((intValue4 >>> 10) | (intValue5 << 22)) & mask;
      output[7 + outputOffset] = (intValue5 >>> 1) & mask;
      output[8 + outputOffset] = ((intValue5 >>> 24) | (intValue6 << 8)) & mask;
      output[9 + outputOffset] = ((intValue6 >>> 15) | (intValue7 << 17)) & mask;
      output[10 + outputOffset] = (intValue7 >>> 6) & mask;
      output[11 + outputOffset] = ((intValue7 >>> 29) | (intValue8 << 3)) & mask;
      output[12 + outputOffset] = ((intValue8 >>> 20) | (intValue9 << 12)) & mask;
      output[13 + outputOffset] = ((intValue9 >>> 11) | (intValue10 << 21)) & mask;
      output[14 + outputOffset] = (intValue10 >>> 2) & mask;
      output[15 + outputOffset] = ((intValue10 >>> 25) | (intValue11 << 7)) & mask;
      output[16 + outputOffset] = ((intValue11 >>> 16) | (intValue12 << 16)) & mask;
      output[17 + outputOffset] = (intValue12 >>> 7) & mask;
      output[18 + outputOffset] = ((intValue12 >>> 30) | (intValue13 << 2)) & mask;
      output[19 + outputOffset] = ((intValue13 >>> 21) | (intValue14 << 11)) & mask;
      output[20 + outputOffset] = ((intValue14 >>> 12) | (intValue15 << 20)) & mask;
      output[21 + outputOffset] = (intValue15 >>> 3) & mask;
      output[22 + outputOffset] = ((intValue15 >>> 26) | (intValue16 << 6)) & mask;
      output[23 + outputOffset] = ((intValue16 >>> 17) | (intValue17 << 15)) & mask;
      output[24 + outputOffset] = (intValue17 >>> 8) & mask;
      output[25 + outputOffset] = ((intValue17 >>> 31) | (intValue18 << 1)) & mask;
      output[26 + outputOffset] = ((intValue18 >>> 22) | (intValue19 << 10)) & mask;
      output[27 + outputOffset] = ((intValue19 >>> 13) | (intValue20 << 19)) & mask;
      output[28 + outputOffset] = (intValue20 >>> 4) & mask;
      output[29 + outputOffset] = ((intValue20 >>> 27) | (intValue21 << 5)) & mask;
      output[30 + outputOffset] = ((intValue21 >>> 18) | (intValue22 << 14)) & mask;
      output[31 + outputOffset] = intValue22 >>> 9;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode24(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 24;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      int intValue21 = compressedBuffer.get();
      int intValue22 = compressedBuffer.get();
      int intValue23 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 24) | (intValue1 << 8)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 16) | (intValue2 << 16)) & mask;
      output[3 + outputOffset] = intValue2 >>> 8;
      output[4 + outputOffset] = intValue3 & mask;
      output[5 + outputOffset] = ((intValue3 >>> 24) | (intValue4 << 8)) & mask;
      output[6 + outputOffset] = ((intValue4 >>> 16) | (intValue5 << 16)) & mask;
      output[7 + outputOffset] = intValue5 >>> 8;
      output[8 + outputOffset] = intValue6 & mask;
      output[9 + outputOffset] = ((intValue6 >>> 24) | (intValue7 << 8)) & mask;
      output[10 + outputOffset] = ((intValue7 >>> 16) | (intValue8 << 16)) & mask;
      output[11 + outputOffset] = intValue8 >>> 8;
      output[12 + outputOffset] = intValue9 & mask;
      output[13 + outputOffset] = ((intValue9 >>> 24) | (intValue10 << 8)) & mask;
      output[14 + outputOffset] = ((intValue10 >>> 16) | (intValue11 << 16)) & mask;
      output[15 + outputOffset] = intValue11 >>> 8;
      output[16 + outputOffset] = intValue12 & mask;
      output[17 + outputOffset] = ((intValue12 >>> 24) | (intValue13 << 8)) & mask;
      output[18 + outputOffset] = ((intValue13 >>> 16) | (intValue14 << 16)) & mask;
      output[19 + outputOffset] = intValue14 >>> 8;
      output[20 + outputOffset] = intValue15 & mask;
      output[21 + outputOffset] = ((intValue15 >>> 24) | (intValue16 << 8)) & mask;
      output[22 + outputOffset] = ((intValue16 >>> 16) | (intValue17 << 16)) & mask;
      output[23 + outputOffset] = intValue17 >>> 8;
      output[24 + outputOffset] = intValue18 & mask;
      output[25 + outputOffset] = ((intValue18 >>> 24) | (intValue19 << 8)) & mask;
      output[26 + outputOffset] = ((intValue19 >>> 16) | (intValue20 << 16)) & mask;
      output[27 + outputOffset] = intValue20 >>> 8;
      output[28 + outputOffset] = intValue21 & mask;
      output[29 + outputOffset] = ((intValue21 >>> 24) | (intValue22 << 8)) & mask;
      output[30 + outputOffset] = ((intValue22 >>> 16) | (intValue23 << 16)) & mask;
      output[31 + outputOffset] = intValue23 >>> 8;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode25(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 25;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      int intValue21 = compressedBuffer.get();
      int intValue22 = compressedBuffer.get();
      int intValue23 = compressedBuffer.get();
      int intValue24 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 25) | (intValue1 << 7)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 18) | (intValue2 << 14)) & mask;
      output[3 + outputOffset] = ((intValue2 >>> 11) | (intValue3 << 21)) & mask;
      output[4 + outputOffset] = (intValue3 >>> 4) & mask;
      output[5 + outputOffset] = ((intValue3 >>> 29) | (intValue4 << 3)) & mask;
      output[6 + outputOffset] = ((intValue4 >>> 22) | (intValue5 << 10)) & mask;
      output[7 + outputOffset] = ((intValue5 >>> 15) | (intValue6 << 17)) & mask;
      output[8 + outputOffset] = ((intValue6 >>> 8) | (intValue7 << 24)) & mask;
      output[9 + outputOffset] = (intValue7 >>> 1) & mask;
      output[10 + outputOffset] = ((intValue7 >>> 26) | (intValue8 << 6)) & mask;
      output[11 + outputOffset] = ((intValue8 >>> 19) | (intValue9 << 13)) & mask;
      output[12 + outputOffset] = ((intValue9 >>> 12) | (intValue10 << 20)) & mask;
      output[13 + outputOffset] = (intValue10 >>> 5) & mask;
      output[14 + outputOffset] = ((intValue10 >>> 30) | (intValue11 << 2)) & mask;
      output[15 + outputOffset] = ((intValue11 >>> 23) | (intValue12 << 9)) & mask;
      output[16 + outputOffset] = ((intValue12 >>> 16) | (intValue13 << 16)) & mask;
      output[17 + outputOffset] = ((intValue13 >>> 9) | (intValue14 << 23)) & mask;
      output[18 + outputOffset] = (intValue14 >>> 2) & mask;
      output[19 + outputOffset] = ((intValue14 >>> 27) | (intValue15 << 5)) & mask;
      output[20 + outputOffset] = ((intValue15 >>> 20) | (intValue16 << 12)) & mask;
      output[21 + outputOffset] = ((intValue16 >>> 13) | (intValue17 << 19)) & mask;
      output[22 + outputOffset] = (intValue17 >>> 6) & mask;
      output[23 + outputOffset] = ((intValue17 >>> 31) | (intValue18 << 1)) & mask;
      output[24 + outputOffset] = ((intValue18 >>> 24) | (intValue19 << 8)) & mask;
      output[25 + outputOffset] = ((intValue19 >>> 17) | (intValue20 << 15)) & mask;
      output[26 + outputOffset] = ((intValue20 >>> 10) | (intValue21 << 22)) & mask;
      output[27 + outputOffset] = (intValue21 >>> 3) & mask;
      output[28 + outputOffset] = ((intValue21 >>> 28) | (intValue22 << 4)) & mask;
      output[29 + outputOffset] = ((intValue22 >>> 21) | (intValue23 << 11)) & mask;
      output[30 + outputOffset] = ((intValue23 >>> 14) | (intValue24 << 18)) & mask;
      output[31 + outputOffset] = intValue24 >>> 7;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode26(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 26;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      int intValue21 = compressedBuffer.get();
      int intValue22 = compressedBuffer.get();
      int intValue23 = compressedBuffer.get();
      int intValue24 = compressedBuffer.get();
      int intValue25 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 26) | (intValue1 << 6)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 20) | (intValue2 << 12)) & mask;
      output[3 + outputOffset] = ((intValue2 >>> 14) | (intValue3 << 18)) & mask;
      output[4 + outputOffset] = ((intValue3 >>> 8) | (intValue4 << 24)) & mask;
      output[5 + outputOffset] = (intValue4 >>> 2) & mask;
      output[6 + outputOffset] = ((intValue4 >>> 28) | (intValue5 << 4)) & mask;
      output[7 + outputOffset] = ((intValue5 >>> 22) | (intValue6 << 10)) & mask;
      output[8 + outputOffset] = ((intValue6 >>> 16) | (intValue7 << 16)) & mask;
      output[9 + outputOffset] = ((intValue7 >>> 10) | (intValue8 << 22)) & mask;
      output[10 + outputOffset] = (intValue8 >>> 4) & mask;
      output[11 + outputOffset] = ((intValue8 >>> 30) | (intValue9 << 2)) & mask;
      output[12 + outputOffset] = ((intValue9 >>> 24) | (intValue10 << 8)) & mask;
      output[13 + outputOffset] = ((intValue10 >>> 18) | (intValue11 << 14)) & mask;
      output[14 + outputOffset] = ((intValue11 >>> 12) | (intValue12 << 20)) & mask;
      output[15 + outputOffset] = intValue12 >>> 6;
      output[16 + outputOffset] = intValue13 & mask;
      output[17 + outputOffset] = ((intValue13 >>> 26) | (intValue14 << 6)) & mask;
      output[18 + outputOffset] = ((intValue14 >>> 20) | (intValue15 << 12)) & mask;
      output[19 + outputOffset] = ((intValue15 >>> 14) | (intValue16 << 18)) & mask;
      output[20 + outputOffset] = ((intValue16 >>> 8) | (intValue17 << 24)) & mask;
      output[21 + outputOffset] = (intValue17 >>> 2) & mask;
      output[22 + outputOffset] = ((intValue17 >>> 28) | (intValue18 << 4)) & mask;
      output[23 + outputOffset] = ((intValue18 >>> 22) | (intValue19 << 10)) & mask;
      output[24 + outputOffset] = ((intValue19 >>> 16) | (intValue20 << 16)) & mask;
      output[25 + outputOffset] = ((intValue20 >>> 10) | (intValue21 << 22)) & mask;
      output[26 + outputOffset] = (intValue21 >>> 4) & mask;
      output[27 + outputOffset] = ((intValue21 >>> 30) | (intValue22 << 2)) & mask;
      output[28 + outputOffset] = ((intValue22 >>> 24) | (intValue23 << 8)) & mask;
      output[29 + outputOffset] = ((intValue23 >>> 18) | (intValue24 << 14)) & mask;
      output[30 + outputOffset] = ((intValue24 >>> 12) | (intValue25 << 20)) & mask;
      output[31 + outputOffset] = intValue25 >>> 6;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode27(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 27;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      int intValue21 = compressedBuffer.get();
      int intValue22 = compressedBuffer.get();
      int intValue23 = compressedBuffer.get();
      int intValue24 = compressedBuffer.get();
      int intValue25 = compressedBuffer.get();
      int intValue26 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 27) | (intValue1 << 5)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 22) | (intValue2 << 10)) & mask;
      output[3 + outputOffset] = ((intValue2 >>> 17) | (intValue3 << 15)) & mask;
      output[4 + outputOffset] = ((intValue3 >>> 12) | (intValue4 << 20)) & mask;
      output[5 + outputOffset] = ((intValue4 >>> 7) | (intValue5 << 25)) & mask;
      output[6 + outputOffset] = (intValue5 >>> 2) & mask;
      output[7 + outputOffset] = ((intValue5 >>> 29) | (intValue6 << 3)) & mask;
      output[8 + outputOffset] = ((intValue6 >>> 24) | (intValue7 << 8)) & mask;
      output[9 + outputOffset] = ((intValue7 >>> 19) | (intValue8 << 13)) & mask;
      output[10 + outputOffset] = ((intValue8 >>> 14) | (intValue9 << 18)) & mask;
      output[11 + outputOffset] = ((intValue9 >>> 9) | (intValue10 << 23)) & mask;
      output[12 + outputOffset] = (intValue10 >>> 4) & mask;
      output[13 + outputOffset] = ((intValue10 >>> 31) | (intValue11 << 1)) & mask;
      output[14 + outputOffset] = ((intValue11 >>> 26) | (intValue12 << 6)) & mask;
      output[15 + outputOffset] = ((intValue12 >>> 21) | (intValue13 << 11)) & mask;
      output[16 + outputOffset] = ((intValue13 >>> 16) | (intValue14 << 16)) & mask;
      output[17 + outputOffset] = ((intValue14 >>> 11) | (intValue15 << 21)) & mask;
      output[18 + outputOffset] = ((intValue15 >>> 6) | (intValue16 << 26)) & mask;
      output[19 + outputOffset] = (intValue16 >>> 1) & mask;
      output[20 + outputOffset] = ((intValue16 >>> 28) | (intValue17 << 4)) & mask;
      output[21 + outputOffset] = ((intValue17 >>> 23) | (intValue18 << 9)) & mask;
      output[22 + outputOffset] = ((intValue18 >>> 18) | (intValue19 << 14)) & mask;
      output[23 + outputOffset] = ((intValue19 >>> 13) | (intValue20 << 19)) & mask;
      output[24 + outputOffset] = ((intValue20 >>> 8) | (intValue21 << 24)) & mask;
      output[25 + outputOffset] = (intValue21 >>> 3) & mask;
      output[26 + outputOffset] = ((intValue21 >>> 30) | (intValue22 << 2)) & mask;
      output[27 + outputOffset] = ((intValue22 >>> 25) | (intValue23 << 7)) & mask;
      output[28 + outputOffset] = ((intValue23 >>> 20) | (intValue24 << 12)) & mask;
      output[29 + outputOffset] = ((intValue24 >>> 15) | (intValue25 << 17)) & mask;
      output[30 + outputOffset] = ((intValue25 >>> 10) | (intValue26 << 22)) & mask;
      output[31 + outputOffset] = intValue26 >>> 5;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode28(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 28;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      int intValue21 = compressedBuffer.get();
      int intValue22 = compressedBuffer.get();
      int intValue23 = compressedBuffer.get();
      int intValue24 = compressedBuffer.get();
      int intValue25 = compressedBuffer.get();
      int intValue26 = compressedBuffer.get();
      int intValue27 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 28) | (intValue1 << 4)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 24) | (intValue2 << 8)) & mask;
      output[3 + outputOffset] = ((intValue2 >>> 20) | (intValue3 << 12)) & mask;
      output[4 + outputOffset] = ((intValue3 >>> 16) | (intValue4 << 16)) & mask;
      output[5 + outputOffset] = ((intValue4 >>> 12) | (intValue5 << 20)) & mask;
      output[6 + outputOffset] = ((intValue5 >>> 8) | (intValue6 << 24)) & mask;
      output[7 + outputOffset] = intValue6 >>> 4;
      output[8 + outputOffset] = intValue7 & mask;
      output[9 + outputOffset] = ((intValue7 >>> 28) | (intValue8 << 4)) & mask;
      output[10 + outputOffset] = ((intValue8 >>> 24) | (intValue9 << 8)) & mask;
      output[11 + outputOffset] = ((intValue9 >>> 20) | (intValue10 << 12)) & mask;
      output[12 + outputOffset] = ((intValue10 >>> 16) | (intValue11 << 16)) & mask;
      output[13 + outputOffset] = ((intValue11 >>> 12) | (intValue12 << 20)) & mask;
      output[14 + outputOffset] = ((intValue12 >>> 8) | (intValue13 << 24)) & mask;
      output[15 + outputOffset] = intValue13 >>> 4;
      output[16 + outputOffset] = intValue14 & mask;
      output[17 + outputOffset] = ((intValue14 >>> 28) | (intValue15 << 4)) & mask;
      output[18 + outputOffset] = ((intValue15 >>> 24) | (intValue16 << 8)) & mask;
      output[19 + outputOffset] = ((intValue16 >>> 20) | (intValue17 << 12)) & mask;
      output[20 + outputOffset] = ((intValue17 >>> 16) | (intValue18 << 16)) & mask;
      output[21 + outputOffset] = ((intValue18 >>> 12) | (intValue19 << 20)) & mask;
      output[22 + outputOffset] = ((intValue19 >>> 8) | (intValue20 << 24)) & mask;
      output[23 + outputOffset] = intValue20 >>> 4;
      output[24 + outputOffset] = intValue21 & mask;
      output[25 + outputOffset] = ((intValue21 >>> 28) | (intValue22 << 4)) & mask;
      output[26 + outputOffset] = ((intValue22 >>> 24) | (intValue23 << 8)) & mask;
      output[27 + outputOffset] = ((intValue23 >>> 20) | (intValue24 << 12)) & mask;
      output[28 + outputOffset] = ((intValue24 >>> 16) | (intValue25 << 16)) & mask;
      output[29 + outputOffset] = ((intValue25 >>> 12) | (intValue26 << 20)) & mask;
      output[30 + outputOffset] = ((intValue26 >>> 8) | (intValue27 << 24)) & mask;
      output[31 + outputOffset] = intValue27 >>> 4;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode29(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 29;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      int intValue21 = compressedBuffer.get();
      int intValue22 = compressedBuffer.get();
      int intValue23 = compressedBuffer.get();
      int intValue24 = compressedBuffer.get();
      int intValue25 = compressedBuffer.get();
      int intValue26 = compressedBuffer.get();
      int intValue27 = compressedBuffer.get();
      int intValue28 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 29) | (intValue1 << 3)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 26) | (intValue2 << 6)) & mask;
      output[3 + outputOffset] = ((intValue2 >>> 23) | (intValue3 << 9)) & mask;
      output[4 + outputOffset] = ((intValue3 >>> 20) | (intValue4 << 12)) & mask;
      output[5 + outputOffset] = ((intValue4 >>> 17) | (intValue5 << 15)) & mask;
      output[6 + outputOffset] = ((intValue5 >>> 14) | (intValue6 << 18)) & mask;
      output[7 + outputOffset] = ((intValue6 >>> 11) | (intValue7 << 21)) & mask;
      output[8 + outputOffset] = ((intValue7 >>> 8) | (intValue8 << 24)) & mask;
      output[9 + outputOffset] = ((intValue8 >>> 5) | (intValue9 << 27)) & mask;
      output[10 + outputOffset] = (intValue9 >>> 2) & mask;
      output[11 + outputOffset] = ((intValue9 >>> 31) | (intValue10 << 1)) & mask;
      output[12 + outputOffset] = ((intValue10 >>> 28) | (intValue11 << 4)) & mask;
      output[13 + outputOffset] = ((intValue11 >>> 25) | (intValue12 << 7)) & mask;
      output[14 + outputOffset] = ((intValue12 >>> 22) | (intValue13 << 10)) & mask;
      output[15 + outputOffset] = ((intValue13 >>> 19) | (intValue14 << 13)) & mask;
      output[16 + outputOffset] = ((intValue14 >>> 16) | (intValue15 << 16)) & mask;
      output[17 + outputOffset] = ((intValue15 >>> 13) | (intValue16 << 19)) & mask;
      output[18 + outputOffset] = ((intValue16 >>> 10) | (intValue17 << 22)) & mask;
      output[19 + outputOffset] = ((intValue17 >>> 7) | (intValue18 << 25)) & mask;
      output[20 + outputOffset] = ((intValue18 >>> 4) | (intValue19 << 28)) & mask;
      output[21 + outputOffset] = (intValue19 >>> 1) & mask;
      output[22 + outputOffset] = ((intValue19 >>> 30) | (intValue20 << 2)) & mask;
      output[23 + outputOffset] = ((intValue20 >>> 27) | (intValue21 << 5)) & mask;
      output[24 + outputOffset] = ((intValue21 >>> 24) | (intValue22 << 8)) & mask;
      output[25 + outputOffset] = ((intValue22 >>> 21) | (intValue23 << 11)) & mask;
      output[26 + outputOffset] = ((intValue23 >>> 18) | (intValue24 << 14)) & mask;
      output[27 + outputOffset] = ((intValue24 >>> 15) | (intValue25 << 17)) & mask;
      output[28 + outputOffset] = ((intValue25 >>> 12) | (intValue26 << 20)) & mask;
      output[29 + outputOffset] = ((intValue26 >>> 9) | (intValue27 << 23)) & mask;
      output[30 + outputOffset] = ((intValue27 >>> 6) | (intValue28 << 26)) & mask;
      output[31 + outputOffset] = intValue28 >>> 3;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode30(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 30;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      int intValue21 = compressedBuffer.get();
      int intValue22 = compressedBuffer.get();
      int intValue23 = compressedBuffer.get();
      int intValue24 = compressedBuffer.get();
      int intValue25 = compressedBuffer.get();
      int intValue26 = compressedBuffer.get();
      int intValue27 = compressedBuffer.get();
      int intValue28 = compressedBuffer.get();
      int intValue29 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 30) | (intValue1 << 2)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 28) | (intValue2 << 4)) & mask;
      output[3 + outputOffset] = ((intValue2 >>> 26) | (intValue3 << 6)) & mask;
      output[4 + outputOffset] = ((intValue3 >>> 24) | (intValue4 << 8)) & mask;
      output[5 + outputOffset] = ((intValue4 >>> 22) | (intValue5 << 10)) & mask;
      output[6 + outputOffset] = ((intValue5 >>> 20) | (intValue6 << 12)) & mask;
      output[7 + outputOffset] = ((intValue6 >>> 18) | (intValue7 << 14)) & mask;
      output[8 + outputOffset] = ((intValue7 >>> 16) | (intValue8 << 16)) & mask;
      output[9 + outputOffset] = ((intValue8 >>> 14) | (intValue9 << 18)) & mask;
      output[10 + outputOffset] = ((intValue9 >>> 12) | (intValue10 << 20)) & mask;
      output[11 + outputOffset] = ((intValue10 >>> 10) | (intValue11 << 22)) & mask;
      output[12 + outputOffset] = ((intValue11 >>> 8) | (intValue12 << 24)) & mask;
      output[13 + outputOffset] = ((intValue12 >>> 6) | (intValue13 << 26)) & mask;
      output[14 + outputOffset] = ((intValue13 >>> 4) | (intValue14 << 28)) & mask;
      output[15 + outputOffset] = intValue14 >>> 2;
      output[16 + outputOffset] = intValue15 & mask;
      output[17 + outputOffset] = ((intValue15 >>> 30) | (intValue16 << 2)) & mask;
      output[18 + outputOffset] = ((intValue16 >>> 28) | (intValue17 << 4)) & mask;
      output[19 + outputOffset] = ((intValue17 >>> 26) | (intValue18 << 6)) & mask;
      output[20 + outputOffset] = ((intValue18 >>> 24) | (intValue19 << 8)) & mask;
      output[21 + outputOffset] = ((intValue19 >>> 22) | (intValue20 << 10)) & mask;
      output[22 + outputOffset] = ((intValue20 >>> 20) | (intValue21 << 12)) & mask;
      output[23 + outputOffset] = ((intValue21 >>> 18) | (intValue22 << 14)) & mask;
      output[24 + outputOffset] = ((intValue22 >>> 16) | (intValue23 << 16)) & mask;
      output[25 + outputOffset] = ((intValue23 >>> 14) | (intValue24 << 18)) & mask;
      output[26 + outputOffset] = ((intValue24 >>> 12) | (intValue25 << 20)) & mask;
      output[27 + outputOffset] = ((intValue25 >>> 10) | (intValue26 << 22)) & mask;
      output[28 + outputOffset] = ((intValue26 >>> 8) | (intValue27 << 24)) & mask;
      output[29 + outputOffset] = ((intValue27 >>> 6) | (intValue28 << 26)) & mask;
      output[30 + outputOffset] = ((intValue28 >>> 4) | (intValue29 << 28)) & mask;
      output[31 + outputOffset] = intValue29 >>> 2;
      outputOffset += 32;
    }
  }

  // NOTE: hardwired to blockSize == 128
  public static void decode31(final IntBuffer compressedBuffer, final int[] output) {
    final int numFrameBits = 31;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int outputOffset = 0;
    for(int step=0;step<4;step++) {
      int intValue0 = compressedBuffer.get();
      int intValue1 = compressedBuffer.get();
      int intValue2 = compressedBuffer.get();
      int intValue3 = compressedBuffer.get();
      int intValue4 = compressedBuffer.get();
      int intValue5 = compressedBuffer.get();
      int intValue6 = compressedBuffer.get();
      int intValue7 = compressedBuffer.get();
      int intValue8 = compressedBuffer.get();
      int intValue9 = compressedBuffer.get();
      int intValue10 = compressedBuffer.get();
      int intValue11 = compressedBuffer.get();
      int intValue12 = compressedBuffer.get();
      int intValue13 = compressedBuffer.get();
      int intValue14 = compressedBuffer.get();
      int intValue15 = compressedBuffer.get();
      int intValue16 = compressedBuffer.get();
      int intValue17 = compressedBuffer.get();
      int intValue18 = compressedBuffer.get();
      int intValue19 = compressedBuffer.get();
      int intValue20 = compressedBuffer.get();
      int intValue21 = compressedBuffer.get();
      int intValue22 = compressedBuffer.get();
      int intValue23 = compressedBuffer.get();
      int intValue24 = compressedBuffer.get();
      int intValue25 = compressedBuffer.get();
      int intValue26 = compressedBuffer.get();
      int intValue27 = compressedBuffer.get();
      int intValue28 = compressedBuffer.get();
      int intValue29 = compressedBuffer.get();
      int intValue30 = compressedBuffer.get();
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 31) | (intValue1 << 1)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 30) | (intValue2 << 2)) & mask;
      output[3 + outputOffset] = ((intValue2 >>> 29) | (intValue3 << 3)) & mask;
      output[4 + outputOffset] = ((intValue3 >>> 28) | (intValue4 << 4)) & mask;
      output[5 + outputOffset] = ((intValue4 >>> 27) | (intValue5 << 5)) & mask;
      output[6 + outputOffset] = ((intValue5 >>> 26) | (intValue6 << 6)) & mask;
      output[7 + outputOffset] = ((intValue6 >>> 25) | (intValue7 << 7)) & mask;
      output[8 + outputOffset] = ((intValue7 >>> 24) | (intValue8 << 8)) & mask;
      output[9 + outputOffset] = ((intValue8 >>> 23) | (intValue9 << 9)) & mask;
      output[10 + outputOffset] = ((intValue9 >>> 22) | (intValue10 << 10)) & mask;
      output[11 + outputOffset] = ((intValue10 >>> 21) | (intValue11 << 11)) & mask;
      output[12 + outputOffset] = ((intValue11 >>> 20) | (intValue12 << 12)) & mask;
      output[13 + outputOffset] = ((intValue12 >>> 19) | (intValue13 << 13)) & mask;
      output[14 + outputOffset] = ((intValue13 >>> 18) | (intValue14 << 14)) & mask;
      output[15 + outputOffset] = ((intValue14 >>> 17) | (intValue15 << 15)) & mask;
      output[16 + outputOffset] = ((intValue15 >>> 16) | (intValue16 << 16)) & mask;
      output[17 + outputOffset] = ((intValue16 >>> 15) | (intValue17 << 17)) & mask;
      output[18 + outputOffset] = ((intValue17 >>> 14) | (intValue18 << 18)) & mask;
      output[19 + outputOffset] = ((intValue18 >>> 13) | (intValue19 << 19)) & mask;
      output[20 + outputOffset] = ((intValue19 >>> 12) | (intValue20 << 20)) & mask;
      output[21 + outputOffset] = ((intValue20 >>> 11) | (intValue21 << 21)) & mask;
      output[22 + outputOffset] = ((intValue21 >>> 10) | (intValue22 << 22)) & mask;
      output[23 + outputOffset] = ((intValue22 >>> 9) | (intValue23 << 23)) & mask;
      output[24 + outputOffset] = ((intValue23 >>> 8) | (intValue24 << 24)) & mask;
      output[25 + outputOffset] = ((intValue24 >>> 7) | (intValue25 << 25)) & mask;
      output[26 + outputOffset] = ((intValue25 >>> 6) | (intValue26 << 26)) & mask;
      output[27 + outputOffset] = ((intValue26 >>> 5) | (intValue27 << 27)) & mask;
      output[28 + outputOffset] = ((intValue27 >>> 4) | (intValue28 << 28)) & mask;
      output[29 + outputOffset] = ((intValue28 >>> 3) | (intValue29 << 29)) & mask;
      output[30 + outputOffset] = ((intValue29 >>> 2) | (intValue30 << 30)) & mask;
      output[31 + outputOffset] = intValue30 >>> 1;
      outputOffset += 32;
    }
  }
}
