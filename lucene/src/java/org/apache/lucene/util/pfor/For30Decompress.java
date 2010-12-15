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
class For30Decompress extends ForDecompress {
  static final int numFrameBits = 30;
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
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
