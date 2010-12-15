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
class For26Decompress extends ForDecompress {
  static final int numFrameBits = 26;
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
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
