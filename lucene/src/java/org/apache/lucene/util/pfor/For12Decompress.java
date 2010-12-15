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
class For12Decompress extends ForDecompress {
  static final int numFrameBits = 12;
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
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
