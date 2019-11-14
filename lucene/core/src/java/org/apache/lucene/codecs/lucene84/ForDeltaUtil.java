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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Utility class to encode/decode increasing sequences of 128 integers.
 */
public class ForDeltaUtil {

  private final ForUtil forUtil;

  ForDeltaUtil(ForUtil forUtil) {
    this.forUtil = forUtil;
  }

  /**
   * Encode deltas of a strictly monotonically increasing sequence of integers.
   * The provided {@code longs} are expected to be deltas between consecutive values.
   */
  void encodeDeltas(long[] longs, DataOutput out) throws IOException {
    if (longs[0] == 1 && PForUtil.allEqual(longs)) { // happens with very dense postings
      out.writeByte((byte) 0);
    } else {
      long or = 0;
      for (long l : longs) {
        or |= l;
      }
      assert or != 0;
      final int bitsPerValue = PackedInts.bitsRequired(or);
      out.writeByte((byte) bitsPerValue);
      forUtil.encode(longs, bitsPerValue, out);
    }
  }

  /**
   * Decode deltas, compute the prefix sum and add {@code base} to all decoded longs.
   */
  void decodeAndPrefixSum(DataInput in, long base, long[] longs) throws IOException {
    final int bitsPerValue = Byte.toUnsignedInt(in.readByte());
    if (bitsPerValue == 0) {
      // Note: can this special-case be optimized?
      for (int i = 0; i < ForUtil.BLOCK_SIZE; ++i) {
        longs[i] = base + 1L + i;
      }
    } else {
      forUtil.decodeAndPrefixSum(bitsPerValue, in, base, longs);
    }
  }

  /**
   * Skip a sequence of 128 longs.
   */
  void skip(DataInput in) throws IOException {
    final int bitsPerValue = Byte.toUnsignedInt(in.readByte());
    if (bitsPerValue != 0) {
      in.skipBytes(forUtil.numBytes(bitsPerValue));
    }
  }

}
