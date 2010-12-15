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
class For24Decompress extends ForDecompress {
  static final int numFrameBits = 24;
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
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
