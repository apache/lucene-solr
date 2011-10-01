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

import org.apache.lucene.store.DataOutput;

import java.io.IOException;

// Packs high order byte first, to match
// IndexOutput.writeInt/Long/Short byte order

/**
 * Generic writer for space-optimal packed values. The resulting bits can be
 * used directly by Packed32, Packed64 and PackedDirect* and will always be
 * long-aligned.
 */

class PackedWriter extends PackedInts.Writer {
  private long pending;
  private int pendingBitPos;

  // masks[n-1] masks for bottom n bits
  private final long[] masks;
  private int written = 0;

  public PackedWriter(DataOutput out, int valueCount, int bitsPerValue)
                                                            throws IOException {
    super(out, valueCount, bitsPerValue);

    pendingBitPos = 64;
    masks = new long[bitsPerValue - 1];

    long v = 1;
    for (int i = 0; i < bitsPerValue - 1; i++) {
      v *= 2;
      masks[i] = v - 1;
    }
  }

  /**
   * Do not call this after finish
   */
  @Override
  public void add(long v) throws IOException {
    assert v <= PackedInts.maxValue(bitsPerValue) : "v=" + v
            + " maxValue=" + PackedInts.maxValue(bitsPerValue);
    assert v >= 0;
    //System.out.println("    packedw add v=" + v + " pendingBitPos=" + pendingBitPos);

    // TODO
    if (pendingBitPos >= bitsPerValue) {
      // not split

      // write-once, so we can |= w/o first masking to 0s
      pending |= v << (pendingBitPos - bitsPerValue);
      if (pendingBitPos == bitsPerValue) {
        // flush
        out.writeLong(pending);
        pending = 0;
        pendingBitPos = 64;
      } else {
        pendingBitPos -= bitsPerValue;
      }

    } else {
      // split

      // write top pendingBitPos bits of value into bottom bits of pending
      pending |= (v >> (bitsPerValue - pendingBitPos)) & masks[pendingBitPos - 1];
      //System.out.println("      part1 (v >> " + (bitsPerValue - pendingBitPos) + ") & " + masks[pendingBitPos-1]);

      // flush
      out.writeLong(pending);

      // write bottom (bitsPerValue - pendingBitPos) bits of value into top bits of pending
      pendingBitPos = 64 - bitsPerValue + pendingBitPos;
      //System.out.println("      part2 v << " + pendingBitPos);
      pending = (v << pendingBitPos);
    }
    written++;
  }

  @Override
  public void finish() throws IOException {
    while (written < valueCount) {
      add(0L); // Auto flush
    }

    if (pendingBitPos != 64) {
      out.writeLong(pending);
    }
  }

  @Override
  public String toString() {
    return "PackedWriter(written " + written + "/" + valueCount + " with "
            + bitsPerValue + " bits/value)";
  }
}
