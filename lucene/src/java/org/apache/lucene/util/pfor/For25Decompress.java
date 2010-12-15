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
class For25Decompress extends ForDecompress {
  static final int numFrameBits = 25;
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
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
