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

import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.EnumMap;

/**
 * Efficient sequential read/write of packed integers.
 */
abstract class BulkOperation implements PackedInts.Decoder, PackedInts.Encoder {

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

"""

FOOTER = "}"

def casts(typ):
  cast_start = "(%s) (" %typ
  cast_end = ")"
  if typ == "long":
    cast_start = ""
    cast_end = ""
  return cast_start, cast_end

def masks(bits):
  if bits == 64:
    return "", ""
  return "(", " & %sL)" %(hex((1 << bits) - 1))

def get_type(bits):
  if bits == 8:
    return "byte"
  elif bits == 16:
    return "short"
  elif bits == 32:
    return "int"
  elif bits == 64:
    return "long"
  else:
    assert False

def packed64singleblock(bpv, f):
  values = 64 / bpv
  f.write("\n  static final class Packed64SingleBlockBulkOperation%d extends BulkOperation {\n\n" %bpv)
  f.write("    public int blocks() {\n")
  f.write("      return 1;\n")
  f.write("     }\n\n")
  f.write("    public int values() {\n")
  f.write("      return %d;\n" %values)
  f.write("    }\n\n")
  p64sb_decode(bpv, 32)
  p64sb_decode(bpv, 64)
  p64sb_encode(bpv, 32)
  p64sb_encode(bpv, 64)
  f.write("  }\n")

def p64sb_decode(bpv, bits):
  values = 64 / bpv
  typ = get_type(bits)
  buf = typ.title() + "Buffer"
  cast_start, cast_end = casts(typ)
  f.write("    public void decode(LongBuffer blocks, %s values, int iterations) {\n" %buf)
  if bits < bpv:
    f.write("      throw new UnsupportedOperationException();\n")
    f.write("    }\n\n")
    return 
  f.write("      assert blocks.position() + iterations * blocks() <= blocks.limit();\n")
  f.write("      assert values.position() + iterations * values() <= values.limit();\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  f.write("        final long block = blocks.get();\n")
  mask = (1 << bpv) - 1
  for i in xrange(values):
    block_offset = i / values
    offset_in_block = i % values
    if i == 0:
      f.write("        values.put(%sblock & %dL%s);\n" %(cast_start, mask, cast_end))
    elif i == values - 1:
      f.write("        values.put(%sblock >>> %d%s);\n" %(cast_start, i * bpv, cast_end))
    else:
      f.write("        values.put(%s(block >>> %d) & %dL%s);\n" %(cast_start, i * bpv, mask, cast_end))
  f.write("      }\n")
  f.write("    }\n\n")

def p64sb_encode(bpv, bits):
  values = 64 / bpv
  typ = get_type(bits)
  buf = typ.title() + "Buffer"
  mask_start, mask_end = masks(bits)
  f.write("    public void encode(%s values, LongBuffer blocks, int iterations) {\n" %buf)
  f.write("      assert blocks.position() + iterations * blocks() <= blocks.limit();\n")
  f.write("      assert values.position() + iterations * values() <= values.limit();\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  for i in xrange(values):
    block_offset = i / values
    offset_in_block = i % values
    if i == 0:
      f.write("        blocks.put(%svalues.get()%s" %(mask_start, mask_end))
    else:
      f.write(" | (%svalues.get()%s << %d)" %(mask_start, mask_end, i * bpv))
      if i == values - 1:
        f.write(");\n")
  f.write("      }\n")
  f.write("    }\n\n")

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
    f.write("""    public void decode(LongBuffer blocks, LongBuffer values, int iterations) {
      final int originalLimit = blocks.limit();
      blocks.limit(blocks.position() + iterations * blocks());
      values.put(blocks);
      blocks.limit(originalLimit);
    }

    public void decode(LongBuffer blocks, IntBuffer values, int iterations) {
      throw new UnsupportedOperationException();
    }

    public void encode(LongBuffer values, LongBuffer blocks, int iterations) {
      final int originalLimit = values.limit();
      values.limit(values.position() + iterations * values());
      blocks.put(values);
      values.limit(originalLimit);
    }

    public void encode(IntBuffer values, LongBuffer blocks, int iterations) {
      for (int i = values.position(), end = values.position() + iterations, j = blocks.position(); i < end; ++i, ++j) {
        blocks.put(j, values.get(i));
      }
    }
  }
""")
  else:
    p64_decode(bpv, 32, values)
    p64_decode(bpv, 64, values)
    p64_encode(bpv, 32, values)
    p64_encode(bpv, 64, values)
    f.write("  }\n")

def p64_decode(bpv, bits, values):
  typ = get_type(bits)
  buf = typ.title() + "Buffer"
  cast_start, cast_end = casts(typ)
  f.write("    public void decode(LongBuffer blocks, %s values, int iterations) {\n" %buf)
  if bits < bpv:
    f.write("      throw new UnsupportedOperationException();\n")
    f.write("    }\n\n")
    return
  f.write("      assert blocks.position() + iterations * blocks() <= blocks.limit();\n")
  f.write("      assert values.position() + iterations * values() <= values.limit();\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  mask = (1 << bpv) - 1
  for i in xrange(0, values):
    block_offset = i * bpv / 64
    bit_offset = (i * bpv) % 64
    if bit_offset == 0:
      # start of block
      f.write("        final long block%d = blocks.get();\n" %block_offset);
      f.write("        values.put(%sblock%d >>> %d%s);\n" %(cast_start, block_offset, 64 - bpv, cast_end))
    elif bit_offset + bpv == 64:
      # end of block
      f.write("        values.put(%sblock%d & %dL%s);\n" %(cast_start, block_offset, mask, cast_end))
    elif bit_offset + bpv < 64:
      # middle of block
      f.write("        values.put(%s(block%d >>> %d) & %dL%s);\n" %(cast_start, block_offset, 64 - bit_offset - bpv, mask, cast_end))
    else:
      # value spans across 2 blocks
      mask1 = (1 << (64 - bit_offset)) -1
      shift1 = bit_offset + bpv - 64
      shift2 = 64 - shift1
      f.write("        final long block%d = blocks.get();\n" %(block_offset + 1));
      f.write("        values.put(%s((block%d & %dL) << %d) | (block%d >>> %d)%s);\n" %(cast_start, block_offset, mask1, shift1, block_offset + 1, shift2, cast_end))
  f.write("      }\n")
  f.write("    }\n\n")

def p64_encode(bpv, bits, values):
  typ = get_type(bits)
  buf = typ.title() + "Buffer"
  mask_start, mask_end = masks(bits)
  f.write("    public void encode(%s values, LongBuffer blocks, int iterations) {\n" %buf)
  f.write("      assert blocks.position() + iterations * blocks() <= blocks.limit();\n")
  f.write("      assert values.position() + iterations * values() <= values.limit();\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  for i in xrange(0, values):
    block_offset = i * bpv / 64
    bit_offset = (i * bpv) % 64
    if bit_offset == 0:
      # start of block
      f.write("        blocks.put((%svalues.get()%s << %d)" %(mask_start, mask_end, 64 - bpv))
    elif bit_offset + bpv == 64:
      # end of block
      f.write(" | %svalues.get()%s);\n" %(mask_start, mask_end))
    elif bit_offset + bpv < 64:
      # inside a block
      f.write(" | (%svalues.get()%s << %d)" %(mask_start, mask_end, 64 - bit_offset - bpv))
    else:
      # value spans across 2 blocks
      right_bits = bit_offset + bpv - 64
      f.write(" | (%svalues.get(values.position())%s >>> %d));\n" %(mask_start, mask_end, right_bits))
      f.write("        blocks.put((%svalues.get()%s << %d)" %(mask_start, mask_end, 64 - right_bits))
  f.write("      }\n")
  f.write("    }\n\n")


if __name__ == '__main__':
  p64_bpv = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32]
  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  f.write("  static {\n")
  f.write("    BULK_OPERATIONS.put(PackedInts.Format.PACKED, new BulkOperation[65]);\n")
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
