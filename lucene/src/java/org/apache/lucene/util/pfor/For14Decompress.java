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
class For14Decompress extends ForDecompress {
  static final int numFrameBits = 14;
  static final int mask = (int) ((1L<<numFrameBits) - 1);

  static void decompressFrame(FrameOfRef frameOfRef) {
    int[] output = frameOfRef.unCompressedData;
    IntBuffer compressedBuffer = frameOfRef.compressedBuffer;
    int outputOffset = frameOfRef.offset;
    //int inputSize = frameOfRef.unComprSize;
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
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
