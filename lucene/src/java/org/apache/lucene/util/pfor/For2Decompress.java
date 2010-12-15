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
/* This program is generated, do not modify. See gendecompress.py */

import java.nio.IntBuffer;
class For2Decompress extends ForDecompress {
  static final int numFrameBits = 2;
  static final int mask = (int) ((1L<<numFrameBits) - 1);

  static void decompressFrame(FrameOfRef frameOfRef) {
    int[] output = frameOfRef.unCompressedData;
    IntBuffer compressedBuffer = frameOfRef.compressedBuffer;
    int outputOffset = frameOfRef.offset;
    //int inputSize = frameOfRef.unComprSize;
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
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
