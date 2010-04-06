package org.apache.lucene.util.packed;

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

import org.apache.lucene.store.IndexInput;

import java.io.IOException;

class PackedReaderIterator implements PackedInts.ReaderIterator {
  private long pending;
  private int pendingBitsLeft;
  private final IndexInput in;
  private final int bitsPerValue;
  private final int valueCount;

  // masks[n-1] masks for bottom n bits
  private final long[] masks;

  public PackedReaderIterator(int bitsPerValue, int valueCount, IndexInput in)
    throws IOException {

    this.valueCount = valueCount;
    this.bitsPerValue = bitsPerValue;
    
    this.in = in;

    masks = new long[bitsPerValue];

    long v = 1;
    for (int i = 0; i < bitsPerValue; i++) {
      v *= 2;
      masks[i] = v - 1;
    }
  }

  public int getBitsPerValue() {
    return bitsPerValue;
  }

  public int size() {
    return valueCount;
  }

  public long next() throws IOException {
    if (pendingBitsLeft == 0) {
      pending = in.readLong();
      pendingBitsLeft = 64;
    }

    if (pendingBitsLeft >= bitsPerValue) {
      // not split
      final long result = (pending >> (pendingBitsLeft - bitsPerValue)) & masks[bitsPerValue-1];
      pendingBitsLeft -= bitsPerValue;
      return result;
    } else {
      // split
      final int bits1 = bitsPerValue - pendingBitsLeft;
      final long result1 = (pending & masks[pendingBitsLeft-1]) << bits1;
      pending = in.readLong();
      final long result2 = (pending >> (64 - bits1)) & masks[bits1-1];
      pendingBitsLeft = 64 + pendingBitsLeft - bitsPerValue;
      return result1 | result2;
    }
  }

  public void close() throws IOException {
    in.close();
  }
}
