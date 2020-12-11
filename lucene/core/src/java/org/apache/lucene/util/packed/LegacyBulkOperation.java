// This file has been automatically generated, DO NOT EDIT

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
package org.apache.lucene.util.packed;


/**
 * Efficient sequential read/write of packed integers.
 */
abstract class LegacyBulkOperation implements PackedInts.Decoder, PackedInts.Encoder {
  private static final LegacyBulkOperation[] packedBulkOps = new LegacyBulkOperation[] {
    new LegacyBulkOperationPacked1(),
    new LegacyBulkOperationPacked2(),
    new LegacyBulkOperationPacked3(),
    new LegacyBulkOperationPacked4(),
    new LegacyBulkOperationPacked5(),
    new LegacyBulkOperationPacked6(),
    new LegacyBulkOperationPacked7(),
    new LegacyBulkOperationPacked8(),
    new LegacyBulkOperationPacked9(),
    new LegacyBulkOperationPacked10(),
    new LegacyBulkOperationPacked11(),
    new LegacyBulkOperationPacked12(),
    new LegacyBulkOperationPacked13(),
    new LegacyBulkOperationPacked14(),
    new LegacyBulkOperationPacked15(),
    new LegacyBulkOperationPacked16(),
    new LegacyBulkOperationPacked17(),
    new LegacyBulkOperationPacked18(),
    new LegacyBulkOperationPacked19(),
    new LegacyBulkOperationPacked20(),
    new LegacyBulkOperationPacked21(),
    new LegacyBulkOperationPacked22(),
    new LegacyBulkOperationPacked23(),
    new LegacyBulkOperationPacked24(),
    new LegacyBulkOperationPacked(25),
    new LegacyBulkOperationPacked(26),
    new LegacyBulkOperationPacked(27),
    new LegacyBulkOperationPacked(28),
    new LegacyBulkOperationPacked(29),
    new LegacyBulkOperationPacked(30),
    new LegacyBulkOperationPacked(31),
    new LegacyBulkOperationPacked(32),
    new LegacyBulkOperationPacked(33),
    new LegacyBulkOperationPacked(34),
    new LegacyBulkOperationPacked(35),
    new LegacyBulkOperationPacked(36),
    new LegacyBulkOperationPacked(37),
    new LegacyBulkOperationPacked(38),
    new LegacyBulkOperationPacked(39),
    new LegacyBulkOperationPacked(40),
    new LegacyBulkOperationPacked(41),
    new LegacyBulkOperationPacked(42),
    new LegacyBulkOperationPacked(43),
    new LegacyBulkOperationPacked(44),
    new LegacyBulkOperationPacked(45),
    new LegacyBulkOperationPacked(46),
    new LegacyBulkOperationPacked(47),
    new LegacyBulkOperationPacked(48),
    new LegacyBulkOperationPacked(49),
    new LegacyBulkOperationPacked(50),
    new LegacyBulkOperationPacked(51),
    new LegacyBulkOperationPacked(52),
    new LegacyBulkOperationPacked(53),
    new LegacyBulkOperationPacked(54),
    new LegacyBulkOperationPacked(55),
    new LegacyBulkOperationPacked(56),
    new LegacyBulkOperationPacked(57),
    new LegacyBulkOperationPacked(58),
    new LegacyBulkOperationPacked(59),
    new LegacyBulkOperationPacked(60),
    new LegacyBulkOperationPacked(61),
    new LegacyBulkOperationPacked(62),
    new LegacyBulkOperationPacked(63),
    new LegacyBulkOperationPacked(64),
  };

  // NOTE: this is sparse (some entries are null):
  private static final LegacyBulkOperation[] packedSingleBlockBulkOps = new LegacyBulkOperation[] {
    new BulkOperationPackedSingleBlock(1),
    new BulkOperationPackedSingleBlock(2),
    new BulkOperationPackedSingleBlock(3),
    new BulkOperationPackedSingleBlock(4),
    new BulkOperationPackedSingleBlock(5),
    new BulkOperationPackedSingleBlock(6),
    new BulkOperationPackedSingleBlock(7),
    new BulkOperationPackedSingleBlock(8),
    new BulkOperationPackedSingleBlock(9),
    new BulkOperationPackedSingleBlock(10),
    null,
    new BulkOperationPackedSingleBlock(12),
    null,
    null,
    null,
    new BulkOperationPackedSingleBlock(16),
    null,
    null,
    null,
    null,
    new BulkOperationPackedSingleBlock(21),
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
    new BulkOperationPackedSingleBlock(32),
  };


  public static LegacyBulkOperation of(PackedInts.Format format, int bitsPerValue) {
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

  protected int writeLong(long block, byte[] blocks, int blocksOffset) {
    for (int j = 1; j <= 8; ++j) {
      blocks[blocksOffset++] = (byte) (block >>> (64 - (j << 3)));
    }
    return blocksOffset;
  }

  /**
   * For every number of bits per value, there is a minimum number of
   * blocks (b) / values (v) you need to write in order to reach the next block
   * boundary:
   *  - 16 bits per value -&gt; b=2, v=1
   *  - 24 bits per value -&gt; b=3, v=1
   *  - 50 bits per value -&gt; b=25, v=4
   *  - 63 bits per value -&gt; b=63, v=8
   *  - ...
   *
   * A bulk read consists in copying <code>iterations*v</code> values that are
   * contained in <code>iterations*b</code> blocks into a <code>long[]</code>
   * (higher values of <code>iterations</code> are likely to yield a better
   * throughput): this requires n * (b + 8v) bytes of memory.
   *
   * This method computes <code>iterations</code> as
   * <code>ramBudget / (b + 8v)</code> (since a long is 8 bytes).
   */
  public final int computeIterations(int valueCount, int ramBudget) {
    final int iterations = ramBudget / (byteBlockCount() + 8 * byteValueCount());
    if (iterations == 0) {
      // at least 1
      return 1;
    } else if ((iterations - 1) * byteValueCount() >= valueCount) {
      // don't allocate for more than the size of the reader
      return (int) Math.ceil((double) valueCount / byteValueCount());
    } else {
      return iterations;
    }
  }
}
