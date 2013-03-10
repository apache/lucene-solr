package org.apache.lucene.util.packed;

/*
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
class DirectPackedReader extends PackedInts.ReaderImpl {
  private final IndexInput in;
  private final long startPointer;

  public DirectPackedReader(int bitsPerValue, int valueCount, IndexInput in) {
    super(valueCount, bitsPerValue);
    this.in = in;

    startPointer = in.getFilePointer();
  }

  @Override
  public long get(int index) {
    final long majorBitPos = (long)index * bitsPerValue;
    final long elementPos = majorBitPos >>> 3;
    try {
      in.seek(startPointer + elementPos);

      final byte b0 = in.readByte();
      final int bitPos = (int) (majorBitPos & 7);
      if (bitPos + bitsPerValue <= 8) {
        // special case: all bits are in the first byte
        return (b0 & ((1L << (8 - bitPos)) - 1)) >>> (8 - bitPos - bitsPerValue);
      }

      // take bits from the first byte
      int remainingBits = bitsPerValue - 8 + bitPos;
      long result = (b0 & ((1L << (8 - bitPos)) - 1)) << remainingBits;

      // add bits from inner bytes
      while (remainingBits >= 8) {
        remainingBits -= 8;
        result |= (in.readByte() & 0xFFL) << remainingBits;
      }

      // take bits from the last byte
      if (remainingBits > 0) {
        result |= (in.readByte() & 0xFFL) >>> (8 - remainingBits);
      }

      return result;
    } catch (IOException ioe) {
      throw new IllegalStateException("failed", ioe);
    }
  }

  @Override
  public long ramBytesUsed() {
    return 0;
  }
}
