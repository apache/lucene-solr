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
package org.apache.lucene.codecs.lucene84;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

public class EliasFanoUtil {

  private final ForUtil forUtil;

  EliasFanoUtil(ForUtil forUtil) {
    this.forUtil = forUtil;
  }

  /**
   * Encode 128 8-bits integers from {@code data} into {@code out}.
   */
  void encode(long[] longs, DataOutput out) throws IOException {
    final int highBitsPerValue = 7;
    final int lowBitsPerValue = Math.max(PackedInts.bitsRequired(longs[ForUtil.BLOCK_SIZE - 1]) - highBitsPerValue, 0);
    out.writeByte((byte) lowBitsPerValue);
    encodeHighBits(longs, lowBitsPerValue, out);
    if (lowBitsPerValue > 0) {
      for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
        longs[i] &= (1L << lowBitsPerValue) - 1;
      }
      forUtil.encode(longs, lowBitsPerValue, out);
    }
  }

  void decode(DataInput in, EliasFanoSequence efSequence) throws IOException {
    efSequence.upperIndex = -1;
    efSequence.i = -1;
    final int lowBitsPerValue = in.readByte();
    efSequence.lowBitsPerValue = lowBitsPerValue;
    for (int i = 0; i < ForUtil.BLOCK_SIZE >>> 5; ++i) {
      efSequence.upperBits[i] = in.readLong();
    }
    if (lowBitsPerValue == 0) {
      Arrays.fill(efSequence.lowerBits, 0L);
    } else {
      forUtil.decode(lowBitsPerValue, in, efSequence.lowerBits);
    }
  }

  void skip(DataInput in) throws IOException {
    final int lowBitsPerValue = in.readByte();
    in.skipBytes((ForUtil.BLOCK_SIZE >>> 2) + forUtil.numBytes(lowBitsPerValue));
  }

  private void encodeHighBits(long[] longs, int lowBitsPerValue, DataOutput out) throws IOException {
    final long[] encoded = new long[ForUtil.BLOCK_SIZE >>> 5];
    long previousHighBits = 0;
    int index = -1;
    for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
      long highBits = longs[i] >>> lowBitsPerValue;
      index += 1 + (highBits - previousHighBits);
      encoded[index >>> 6] |= 1L << index;
      previousHighBits = highBits;
    }
    for (long l : encoded) {
      out.writeLong(l);
    }
  }

}
