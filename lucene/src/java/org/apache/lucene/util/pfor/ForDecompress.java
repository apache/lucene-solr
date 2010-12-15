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

import java.nio.IntBuffer;

/** PFor frame decompression for any number of frame bits. */
class ForDecompress {

  static void decodeAnyFrame(
        IntBuffer intBuffer, int bufIndex, int inputSize, int numFrameBits,
        int[] output, int outputOffset) {

    assert numFrameBits > 0 : numFrameBits;
    assert numFrameBits <= 32 : numFrameBits;
    final int mask = (int) ((1L<<numFrameBits) - 1);
    int intValue1 = intBuffer.get(bufIndex);
    output[outputOffset] = intValue1 & mask;
    if (--inputSize == 0) return;
    int bitPos = numFrameBits;

    do {
      while (bitPos <= (32 - numFrameBits)) {
        // No mask needed when bitPos == (32 - numFrameBits), but prefer to avoid testing for this:
        output[++outputOffset] = (intValue1 >>> bitPos) & mask;
        if (--inputSize == 0) return;
        bitPos += numFrameBits;
      }
      
      int intValue2 = intBuffer.get(++bufIndex);
      output[++outputOffset] = ( (bitPos == 32)
                                  ? intValue2
                                  : ((intValue1 >>> bitPos) | (intValue2 << (32 - bitPos)))
                               ) & mask;
        
      if (--inputSize == 0) return;
      
      intValue1 = intValue2;
      bitPos += numFrameBits - 32;
    } while (true);
  }
}
