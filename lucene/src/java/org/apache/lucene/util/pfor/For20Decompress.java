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
class For20Decompress extends ForDecompress {
  static final int numFrameBits = 20;
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
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
