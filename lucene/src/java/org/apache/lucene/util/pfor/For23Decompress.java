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
class For23Decompress extends ForDecompress {
  static final int numFrameBits = 23;
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
      output[0 + outputOffset] = intValue0 & mask;
      output[1 + outputOffset] = ((intValue0 >>> 23) | (intValue1 << 9)) & mask;
      output[2 + outputOffset] = ((intValue1 >>> 14) | (intValue2 << 18)) & mask;
      output[3 + outputOffset] = (intValue2 >>> 5) & mask;
      output[4 + outputOffset] = ((intValue2 >>> 28) | (intValue3 << 4)) & mask;
      output[5 + outputOffset] = ((intValue3 >>> 19) | (intValue4 << 13)) & mask;
      output[6 + outputOffset] = ((intValue4 >>> 10) | (intValue5 << 22)) & mask;
      output[7 + outputOffset] = (intValue5 >>> 1) & mask;
      output[8 + outputOffset] = ((intValue5 >>> 24) | (intValue6 << 8)) & mask;
      output[9 + outputOffset] = ((intValue6 >>> 15) | (intValue7 << 17)) & mask;
      output[10 + outputOffset] = (intValue7 >>> 6) & mask;
      output[11 + outputOffset] = ((intValue7 >>> 29) | (intValue8 << 3)) & mask;
      output[12 + outputOffset] = ((intValue8 >>> 20) | (intValue9 << 12)) & mask;
      output[13 + outputOffset] = ((intValue9 >>> 11) | (intValue10 << 21)) & mask;
      output[14 + outputOffset] = (intValue10 >>> 2) & mask;
      output[15 + outputOffset] = ((intValue10 >>> 25) | (intValue11 << 7)) & mask;
      output[16 + outputOffset] = ((intValue11 >>> 16) | (intValue12 << 16)) & mask;
      output[17 + outputOffset] = (intValue12 >>> 7) & mask;
      output[18 + outputOffset] = ((intValue12 >>> 30) | (intValue13 << 2)) & mask;
      output[19 + outputOffset] = ((intValue13 >>> 21) | (intValue14 << 11)) & mask;
      output[20 + outputOffset] = ((intValue14 >>> 12) | (intValue15 << 20)) & mask;
      output[21 + outputOffset] = (intValue15 >>> 3) & mask;
      output[22 + outputOffset] = ((intValue15 >>> 26) | (intValue16 << 6)) & mask;
      output[23 + outputOffset] = ((intValue16 >>> 17) | (intValue17 << 15)) & mask;
      output[24 + outputOffset] = (intValue17 >>> 8) & mask;
      output[25 + outputOffset] = ((intValue17 >>> 31) | (intValue18 << 1)) & mask;
      output[26 + outputOffset] = ((intValue18 >>> 22) | (intValue19 << 10)) & mask;
      output[27 + outputOffset] = ((intValue19 >>> 13) | (intValue20 << 19)) & mask;
      output[28 + outputOffset] = (intValue20 >>> 4) & mask;
      output[29 + outputOffset] = ((intValue20 >>> 27) | (intValue21 << 5)) & mask;
      output[30 + outputOffset] = ((intValue21 >>> 18) | (intValue22 << 14)) & mask;
      output[31 + outputOffset] = intValue22 >>> 9;
      // inputSize -= 32;
      outputOffset += 32;
    }
    
    //if (inputSize > 0) {
    //  decodeAnyFrame(compressedBuffer, bufIndex, inputSize, numFrameBits, output, outputOffset);
    //}
  }
}
