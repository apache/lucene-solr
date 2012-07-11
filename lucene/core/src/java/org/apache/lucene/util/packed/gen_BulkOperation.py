#! /usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from fractions import gcd

"""Code generation for bulk operations"""

PACKED_64_SINGLE_BLOCK_BPV = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32]
OUTPUT_FILE = "BulkOperation.java"
HEADER = """// This file has been automatically generated, DO NOT EDIT

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

import java.util.EnumMap;

/**
 * Efficient sequential read/write of packed integers.
 */
abstract class BulkOperation {

  static final EnumMap<PackedInts.Format, BulkOperation[]> BULK_OPERATIONS = new EnumMap<PackedInts.Format, BulkOperation[]>(PackedInts.Format.class);

  public static BulkOperation of(PackedInts.Format format, int bitsPerValue) {
    assert bitsPerValue > 0 && bitsPerValue <= 64;
    BulkOperation[] ops = BULK_OPERATIONS.get(format);
    if (ops == null || ops[bitsPerValue] == null) {
      throw new IllegalArgumentException("format: " + format + ", bitsPerValue: " + bitsPerValue);
    }
    return ops[bitsPerValue];
  }

  /**
   * For every number of bits per value, there is a minimum number of
   * blocks (b) / values (v) you need to write in order to reach the next block
   * boundary:
   *  - 16 bits per value -> b=1, v=4
   *  - 24 bits per value -> b=3, v=8
   *  - 50 bits per value -> b=25, v=32
   *  - 63 bits per value -> b=63, v = 64
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
    final int iterations = (ramBudget >>> 3) / (blocks() + values());
    if (iterations == 0) {
      // at least 1
      return 1;
    } else if ((iterations - 1) * blocks() >= valueCount) {
      // don't allocate for more than the size of the reader
      return (int) Math.ceil((double) valueCount / values());
    } else {
      return iterations;
    }
  }

  /**
   * The minimum number of blocks required to perform a bulk get/set.
   */
  public abstract int blocks();

  /**
   * The number of values that can be stored in <code>blocks()</code> blocks.
   */
  public abstract int values();

  /**
   * Get <code>n * values()</code> values from <code>n * blocks()</code> blocks.
   */
  public abstract void get(long[] blocks, int blockIndex, long[] values, int valuesIndex, int iterations);

  /**
   * Set <code>n * values()</code> values into <code>n * blocks()</code> blocks.
   */
  public abstract void set(long[] blocks, int blockIndex, long[] values, int valuesIndex, int iterations);

"""

FOOTER = "}"

def packed64singleblock(bpv, f):
  values = 64 / bpv
  f.write("\n  static final class Packed64SingleBlockBulkOperation%d extends BulkOperation {\n\n" %bpv)
  f.write("    public int blocks() {\n")
  f.write("      return 1;\n")
  f.write("     }\n\n")
  f.write("    public int values() {\n")
  f.write("      return %d;\n" %values)
  f.write("    }\n\n")

  f.write("    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {\n")
  f.write("      assert bi + iterations * blocks() <= blocks.length;\n")
  f.write("      assert vi + iterations * values() <= values.length;\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  f.write("        final long block = blocks[bi++];\n")
  mask = (1 << bpv) - 1
  for i in xrange(values):
    block_offset = i / values
    offset_in_block = i % values
    if i == 0:
      f.write("        values[vi++] = block & %dL;\n" %mask)
    elif i == values - 1:
      f.write("        values[vi++] = block >>> %d;\n" %(i * bpv))
    else:
      f.write("        values[vi++] = (block >>> %d) & %dL;\n" %(i * bpv, mask))
  f.write("      }\n")
  f.write("    }\n\n")

  f.write("    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {\n")
  f.write("      assert bi + iterations * blocks() <= blocks.length;\n")
  f.write("      assert vi + iterations * values() <= values.length;\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  for i in xrange(values):
    block_offset = i / values
    offset_in_block = i % values
    if i == 0:
      f.write("        blocks[bi++] = values[vi++]")
    else:
      f.write(" | (values[vi++] << %d)" %(i * bpv))
      if i == values - 1:
        f.write(";\n")
  f.write("      }\n")
  f.write("    }\n")

  f.write("  }\n")

def packed64(bpv, f):
  blocks = bpv
  values = blocks * 64 / bpv
  while blocks % 2 == 0 and values % 2 == 0:
    blocks /= 2
    values /= 2
  assert values * bpv == 64 * blocks, "%d values, %d blocks, %d bits per value" %(values, blocks, bpv)
  mask = (1 << bpv) - 1
  f.write("  static final class Packed64BulkOperation%d extends BulkOperation {\n\n" %bpv)
  f.write("    public int blocks() {\n")
  f.write("      return %d;\n" %blocks)
  f.write("    }\n\n")
  f.write("    public int values() {\n")
  f.write("      return %d;\n" %values)
  f.write("    }\n\n")

  if bpv == 64:
    f.write("""    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      System.arraycopy(blocks, bi, values, vi, iterations);
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      System.arraycopy(values, bi, blocks, vi, iterations);
    }
  }
""")
    return

  f.write("    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {\n")
  f.write("      assert bi + iterations * blocks() <= blocks.length;\n")
  f.write("      assert vi + iterations * values() <= values.length;\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  for i in xrange(0, values):
    block_offset = i * bpv / 64
    bit_offset = (i * bpv) % 64
    if bit_offset == 0:
      # start of block
      f.write("        final long block%d = blocks[bi++];\n" %block_offset);
      f.write("        values[vi++] = block%d >>> %d;\n" %(block_offset, 64 - bpv))
    elif bit_offset + bpv == 64:
      # end of block
      f.write("        values[vi++] = block%d & %dL;\n" %(block_offset, mask))
    elif bit_offset + bpv < 64:
      # middle of block
      f.write("        values[vi++] = (block%d >>> %d) & %dL;\n" %(block_offset, 64 - bit_offset - bpv, mask))
    else:
      # value spans across 2 blocks
      mask1 = (1 << (64 - bit_offset)) -1
      shift1 = bit_offset + bpv - 64
      shift2 = 64 - shift1
      f.write("        final long block%d = blocks[bi++];\n" %(block_offset + 1));
      f.write("        values[vi++] = ((block%d & %dL) << %d) | (block%d >>> %d);\n" %(block_offset, mask1, shift1, block_offset + 1, shift2))
  f.write("      }\n")
  f.write("    }\n\n")

  f.write("    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {\n")
  f.write("      assert bi + iterations * blocks() <= blocks.length;\n")
  f.write("      assert vi + iterations * values() <= values.length;\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  for i in xrange(0, values):
    block_offset = i * bpv / 64
    bit_offset = (i * bpv) % 64
    if bit_offset == 0:
      # start of block
      f.write("        blocks[bi++] = (values[vi++] << %d)" %(64 - bpv))
    elif bit_offset + bpv == 64:
      # end of block
      f.write(" | values[vi++];\n")
    elif bit_offset + bpv < 64:
      # inside a block
      f.write(" | (values[vi++] << %d)" %(64 - bit_offset - bpv))
    else:
      # value spans across 2 blocks
      right_bits = bit_offset + bpv - 64
      f.write(" | (values[vi] >>> %d);\n" %right_bits)
      f.write("        blocks[bi++] = (values[vi++] << %d)" %(64 - right_bits))
  f.write("      }\n")
  f.write("    }\n")

  f.write("  }\n\n")



if __name__ == '__main__':
  p64_bpv = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32]
  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  f.write("  static {\n")
  f.write("    BULK_OPERATIONS.put(PackedInts.Format.PACKED, new BulkOperation[65]);")
  for bpv in xrange(1, 65):
    f.write("    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[%d] = new Packed64BulkOperation%d();\n" %(bpv, bpv))
  f.write("    BULK_OPERATIONS.put(PackedInts.Format.PACKED_SINGLE_BLOCK, new BulkOperation[65]);\n")
  for bpv in PACKED_64_SINGLE_BLOCK_BPV:
    f.write("    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[%d] = new Packed64SingleBlockBulkOperation%d();\n" %(bpv, bpv))
  f.write("  }\n")
  for bpv in xrange(1, 65):
    packed64(bpv, f)
  for bpv in PACKED_64_SINGLE_BLOCK_BPV:
    packed64singleblock(bpv,f)
  f.write(FOOTER)
  f.close()
