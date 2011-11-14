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

/* Reads directly from disk on each get */
final class DirectReader implements PackedInts.Reader {
  private final IndexInput in;
  private final long startPointer;
  private final int bitsPerValue;
  private final int valueCount;

  private static final int BLOCK_BITS = Packed64.BLOCK_BITS;
  private static final int MOD_MASK = Packed64.MOD_MASK;

  // masks[n-1] masks for bottom n bits
  private final long[] masks;

  public DirectReader(int bitsPerValue, int valueCount, IndexInput in)
    throws IOException {
    this.valueCount = valueCount;
    this.bitsPerValue = bitsPerValue;
    this.in = in;

    long v = 1;
    masks = new long[bitsPerValue];
    for (int i = 0; i < bitsPerValue; i++) {
      v *= 2;
      masks[i] = v - 1;
    }

    startPointer = in.getFilePointer();
  }

  @Override
  public int getBitsPerValue() {
    return bitsPerValue;
  }

  @Override
  public int size() {
    return valueCount;
  }

  @Override
  public boolean hasArray() {
    return false;
  }

  @Override
  public Object getArray() {
    return null;
  }

  @Override
  public long get(int index) {
    final long majorBitPos = (long)index * bitsPerValue;
    final int elementPos = (int)(majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
    final int bitPos =     (int)(majorBitPos & MOD_MASK); // % BLOCK_SIZE);

    final long result;
    try {
      in.seek(startPointer + (elementPos << 3));
      final long l1 = in.readLong();
      final int bits1 = 64 - bitPos;
      if (bits1 >= bitsPerValue) { // not split
        result = l1 >> (bits1-bitsPerValue) & masks[bitsPerValue-1];
      } else {
        final int bits2 = bitsPerValue - bits1;
        final long result1 = (l1 & masks[bits1-1]) << bits2;
        final long l2 = in.readLong();
        final long result2 = l2 >> (64 - bits2) & masks[bits2-1];
        result = result1 | result2;
      }

      return result;
    } catch (IOException ioe) {
      throw new IllegalStateException("failed", ioe);
    }
  }
}
