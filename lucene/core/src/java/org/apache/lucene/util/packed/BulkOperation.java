// This file has been automatically generated, DO NOT EDIT

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

import java.nio.ByteBuffer;

/**
 * Efficient sequential read/write of packed integers.
 */
abstract class BulkOperation implements PackedInts.Decoder, PackedInts.Encoder {
  private static final BulkOperation[] packedBulkOps = new BulkOperation[] {
    new BulkOperationPacked1(),
    new BulkOperationPacked2(),
    new BulkOperationPacked3(),
    new BulkOperationPacked4(),
    new BulkOperationPacked5(),
    new BulkOperationPacked6(),
    new BulkOperationPacked7(),
    new BulkOperationPacked8(),
    new BulkOperationPacked9(),
    new BulkOperationPacked10(),
    new BulkOperationPacked11(),
    new BulkOperationPacked12(),
    new BulkOperationPacked13(),
    new BulkOperationPacked14(),
    new BulkOperationPacked15(),
    new BulkOperationPacked16(),
    new BulkOperationPacked17(),
    new BulkOperationPacked18(),
    new BulkOperationPacked19(),
    new BulkOperationPacked20(),
    new BulkOperationPacked21(),
    new BulkOperationPacked22(),
    new BulkOperationPacked23(),
    new BulkOperationPacked24(),
    new BulkOperationPacked25(),
    new BulkOperationPacked26(),
    new BulkOperationPacked27(),
    new BulkOperationPacked28(),
    new BulkOperationPacked29(),
    new BulkOperationPacked30(),
    new BulkOperationPacked31(),
    new BulkOperationPacked32(),
    new BulkOperationPacked33(),
    new BulkOperationPacked34(),
    new BulkOperationPacked35(),
    new BulkOperationPacked36(),
    new BulkOperationPacked37(),
    new BulkOperationPacked38(),
    new BulkOperationPacked39(),
    new BulkOperationPacked40(),
    new BulkOperationPacked41(),
    new BulkOperationPacked42(),
    new BulkOperationPacked43(),
    new BulkOperationPacked44(),
    new BulkOperationPacked45(),
    new BulkOperationPacked46(),
    new BulkOperationPacked47(),
    new BulkOperationPacked48(),
    new BulkOperationPacked49(),
    new BulkOperationPacked50(),
    new BulkOperationPacked51(),
    new BulkOperationPacked52(),
    new BulkOperationPacked53(),
    new BulkOperationPacked54(),
    new BulkOperationPacked55(),
    new BulkOperationPacked56(),
    new BulkOperationPacked57(),
    new BulkOperationPacked58(),
    new BulkOperationPacked59(),
    new BulkOperationPacked60(),
    new BulkOperationPacked61(),
    new BulkOperationPacked62(),
    new BulkOperationPacked63(),
    new BulkOperationPacked64(),
  };

  // NOTE: this is sparse (some entries are null):
  private static final BulkOperation[] packedSingleBlockBulkOps = new BulkOperation[] {
    new BulkOperationPackedSingleBlock1(),
    new BulkOperationPackedSingleBlock2(),
    new BulkOperationPackedSingleBlock3(),
    new BulkOperationPackedSingleBlock4(),
    new BulkOperationPackedSingleBlock5(),
    new BulkOperationPackedSingleBlock6(),
    new BulkOperationPackedSingleBlock7(),
    new BulkOperationPackedSingleBlock8(),
    new BulkOperationPackedSingleBlock9(),
    new BulkOperationPackedSingleBlock10(),
    null,
    new BulkOperationPackedSingleBlock12(),
    null,
    null,
    null,
    new BulkOperationPackedSingleBlock16(),
    null,
    null,
    null,
    null,
    new BulkOperationPackedSingleBlock21(),
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    new BulkOperationPackedSingleBlock32(),
  };


  public static BulkOperation of(PackedInts.Format format, int bitsPerValue) {
    switch (format) {
    case PACKED:
      assert packedBulkOps[bitsPerValue - 1] != null;
      return packedBulkOps[bitsPerValue - 1];
    case PACKED_SINGLE_BLOCK:
      assert packedSingleBlockBulkOps[bitsPerValue - 1] != null;
      return packedSingleBlockBulkOps[bitsPerValue - 1];
    default:
      throw new AssertionError();
    }
  }


  private static long[] toLongArray(int[] ints, int offset, int length) {
    long[] arr = new long[length];
    for (int i = 0; i < length; ++i) {
      arr[i] = ints[offset + i];
    }
    return arr;
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
    encode(toLongArray(values, valuesOffset, iterations * valueCount()), 0, blocks, blocksOffset, iterations);
  }

  @Override
  public void encode(long[] values, int valuesOffset, byte[] blocks, int blocksOffset, int iterations) {
    final long[] longBLocks = new long[blockCount() * iterations];
    encode(values, valuesOffset, longBLocks, 0, iterations);
    ByteBuffer.wrap(blocks, blocksOffset, 8 * iterations * blockCount()).asLongBuffer().put(longBLocks);
  }

  @Override
  public void encode(int[] values, int valuesOffset, byte[] blocks, int blocksOffset, int iterations) {
    final long[] longBLocks = new long[blockCount() * iterations];
    encode(values, valuesOffset, longBLocks, 0, iterations);
    ByteBuffer.wrap(blocks, blocksOffset, 8 * iterations * blockCount()).asLongBuffer().put(longBLocks);
  }

  /**
   * For every number of bits per value, there is a minimum number of
   * blocks (b) / values (v) you need to write in order to reach the next block
   * boundary:
   *  - 16 bits per value -> b=1, v=4
   *  - 24 bits per value -> b=3, v=8
   *  - 50 bits per value -> b=25, v=32
   *  - 63 bits per value -> b=63, v=64
   *  - ...
   *
   * A bulk read consists in copying <code>iterations*v</code> values that are
   * contained in <code>iterations*b</code> blocks into a <code>long[]</code>
   * (higher values of <code>iterations</code> are likely to yield a better
   * throughput) => this requires n * (b + v) longs in memory.
   *
   * This method computes <code>iterations</code> as
   * <code>ramBudget / (8 * (b + v))</code> (since a long is 8 bytes).
   */
  public final int computeIterations(int valueCount, int ramBudget) {
    final int iterations = (ramBudget >>> 3) / (blockCount() + valueCount());
    if (iterations == 0) {
      // at least 1
      return 1;
    } else if ((iterations - 1) * blockCount() >= valueCount) {
      // don't allocate for more than the size of the reader
      return (int) Math.ceil((double) valueCount / valueCount());
    } else {
      return iterations;
    }
  }
}
