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
class For29Decompress extends ForDecompress {
  static final int numFrameBits = 29;
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
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
