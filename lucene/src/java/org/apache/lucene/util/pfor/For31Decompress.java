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
class For31Decompress extends ForDecompress {
  static final int numFrameBits = 31;
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
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
