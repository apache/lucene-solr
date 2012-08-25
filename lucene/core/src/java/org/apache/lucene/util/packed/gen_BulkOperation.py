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

"""

FOOTER="""

  private static long[] toLongArray(int[] ints, int offset, int length) {
    long[] arr = new long[length];
    for (int i = 0; i < length; ++i) {
      arr[i] = ints[offset + i];
    }
    return arr;
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
"""

def casts(typ):
  cast_start = "(%s) (" %typ
  cast_end = ")"
  if typ == "long":
    cast_start = ""
    cast_end = ""
  return cast_start, cast_end

def hexNoLSuffix(n):
  # On 32 bit Python values > (1 << 31)-1 will have L appended by hex function:
  s = hex(n)
  if s.endswith('L'):
    s = s[:-1]
  return s

def masks(bits):
  if bits == 64:
    return "", ""
  return "(", " & %sL)" %(hexNoLSuffix((1 << bits) - 1))

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
  f.write("    @Override\n")
  f.write("    public int blockCount() {\n")
  f.write("      return 1;\n")
  f.write("     }\n\n")
  f.write("    @Override\n")
  f.write("    public int valueCount() {\n")
  f.write("      return %d;\n" %values)
  f.write("    }\n\n")
  p64sb_decode(bpv, f, 32)
  p64sb_decode(bpv, f, 64)
  p64sb_encode(bpv, f, 32)
  p64sb_encode(bpv, f, 64)

def p64sb_decode(bpv, f, bits):
  values = 64 / bpv
  typ = get_type(bits)
  cast_start, cast_end = casts(typ)
  f.write("    @Override\n")
  f.write("    public void decode(long[] blocks, int blocksOffset, %s[] values, int valuesOffset, int iterations) {\n" %typ)
  if bits < bpv:
    f.write("      throw new UnsupportedOperationException();\n")
    f.write("    }\n\n")
    return 
  f.write("      assert blocksOffset + iterations * blockCount() <= blocks.length;\n")
  f.write("      assert valuesOffset + iterations * valueCount() <= values.length;\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  f.write("        final long block = blocks[blocksOffset++];\n")
  mask = (1 << bpv) - 1
  for i in xrange(values):
    block_offset = i / values
    offset_in_block = i % values
    if i == 0:
      f.write("        values[valuesOffset++] = %sblock & %dL%s;\n" %(cast_start, mask, cast_end))
    elif i == values - 1:
      f.write("        values[valuesOffset++] = %sblock >>> %d%s;\n" %(cast_start, i * bpv, cast_end))
    else:
      f.write("        values[valuesOffset++] = %s(block >>> %d) & %dL%s;\n" %(cast_start, i * bpv, mask, cast_end))
  f.write("      }\n")
  f.write("    }\n\n")

  f.write("    @Override\n")
  f.write("    public void decode(byte[] blocks, int blocksOffset, %s[] values, int valuesOffset, int iterations) {\n" %typ)
  if bits < bpv:
    f.write("      throw new UnsupportedOperationException();\n")
    f.write("    }\n\n")
  f.write("      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;\n")
  f.write("      assert valuesOffset + iterations * valueCount() <= values.length;\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  if bpv >= 32 and bits > 32:
    for i in xrange(7, -1, -1):
      f.write("        final long byte%d = blocks[blocksOffset++] & 0xFF;\n" %i)
  else:
    for i in xrange(7, -1, -1):
      f.write("        final int byte%d = blocks[blocksOffset++] & 0xFF;\n" %i)
  for i in xrange(values):
    byte_start = (i * bpv) / 8
    bit_start = (i * bpv) % 8
    byte_end = ((i + 1) * bpv - 1) / 8
    bit_end = ((i + 1) * bpv - 1) % 8
    f.write("        values[valuesOffset++] =")
    if byte_start == byte_end:
      # only one byte
      if bit_start == 0:
        if bit_end == 7:
          f.write(" byte%d" %byte_start)
        else:
          f.write(" byte%d & %d" %(byte_start, mask))
      else:
        if bit_end == 7:
          f.write(" byte%d >>> %d" %(byte_start, bit_start))
        else:
          f.write(" (byte%d >>> %d) & %d" %(byte_start, bit_start, mask))
    else:
      if bit_start == 0:
        f.write(" byte%d" %byte_start)
      else:
        f.write(" (byte%d >>> %d)" %(byte_start, bit_start))
      for b in xrange(byte_start + 1, byte_end):
        f.write(" | (byte%d << %d)" %(b, 8 * (b - byte_start) - bit_start))
      if bit_end == 7:
        f.write(" | (byte%d << %d)" %(byte_end, 8 * (byte_end - byte_start) - bit_start))
      else:
        f.write(" | ((byte%d & %d) << %d)" %(byte_end, 2 ** (bit_end + 1) - 1, 8 * (byte_end - byte_start) - bit_start))
    f.write(";\n")
  f.write("      }\n")
  f.write("    }\n\n")

def p64sb_encode(bpv, f, bits):
  values = 64 / bpv
  typ = get_type(bits)
  mask_start, mask_end = masks(bits)
  f.write("    @Override\n")
  f.write("    public void encode(%s[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {\n" %typ)
  if bits < bpv:
    f.write("      throw new UnsupportedOperationException();\n")
    f.write("    }\n\n")
    return
  f.write("      assert blocksOffset + iterations * blockCount() <= blocks.length;\n")
  f.write("      assert valuesOffset + iterations * valueCount() <= values.length;\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  for i in xrange(values):
    block_offset = i / values
    offset_in_block = i % values
    if i == 0:
      f.write("        blocks[blocksOffset++] = %svalues[valuesOffset++]%s" %(mask_start, mask_end))
    else:
      f.write(" | (%svalues[valuesOffset++]%s << %d)" %(mask_start, mask_end, i * bpv))
      if i == values - 1:
        f.write(";\n")
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
  f.write("    @Override\n")
  f.write("    public int blockCount() {\n")
  f.write("      return %d;\n" %blocks)
  f.write("    }\n\n")
  f.write("    @Override\n")
  f.write("    public int valueCount() {\n")
  f.write("      return %d;\n" %values)
  f.write("    }\n\n")

  if bpv == 64:
    f.write("""    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      System.arraycopy(blocks, blocksOffset, values, valuesOffset, valueCount() * iterations);
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
    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      LongBuffer.wrap(values, valuesOffset, iterations * valueCount()).put(ByteBuffer.wrap(blocks, blocksOffset, 8 * iterations * blockCount()).asLongBuffer());
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      System.arraycopy(values, valuesOffset, blocks, blocksOffset, valueCount() * iterations);
    }
""")
  else:
    p64_decode(bpv, f, 32, values)
    p64_decode(bpv, f, 64, values)
    p64_encode(bpv, f, 32, values)
    p64_encode(bpv, f, 64, values)

def p64_decode(bpv, f, bits, values):
  typ = get_type(bits)
  cast_start, cast_end = casts(typ)

  f.write("    @Override\n")
  f.write("    public void decode(long[] blocks, int blocksOffset, %s[] values, int valuesOffset, int iterations) {\n" %typ)
  if bits < bpv:
    f.write("      throw new UnsupportedOperationException();\n")
  else:
    f.write("      assert blocksOffset + iterations * blockCount() <= blocks.length;\n")
    f.write("      assert valuesOffset + iterations * valueCount() <= values.length;\n")
    f.write("      for (int i = 0; i < iterations; ++i) {\n")
    mask = (1 << bpv) - 1
    for i in xrange(0, values):
      block_offset = i * bpv / 64
      bit_offset = (i * bpv) % 64
      if bit_offset == 0:
        # start of block
        f.write("        final long block%d = blocks[blocksOffset++];\n" %block_offset);
        f.write("        values[valuesOffset++] = %sblock%d >>> %d%s;\n" %(cast_start, block_offset, 64 - bpv, cast_end))
      elif bit_offset + bpv == 64:
        # end of block
        f.write("        values[valuesOffset++] = %sblock%d & %dL%s;\n" %(cast_start, block_offset, mask, cast_end))
      elif bit_offset + bpv < 64:
        # middle of block
        f.write("        values[valuesOffset++] = %s(block%d >>> %d) & %dL%s;\n" %(cast_start, block_offset, 64 - bit_offset - bpv, mask, cast_end))
      else:
        # value spans across 2 blocks
        mask1 = (1 << (64 - bit_offset)) -1
        shift1 = bit_offset + bpv - 64
        shift2 = 64 - shift1
        f.write("        final long block%d = blocks[blocksOffset++];\n" %(block_offset + 1));
        f.write("        values[valuesOffset++] = %s((block%d & %dL) << %d) | (block%d >>> %d)%s;\n" %(cast_start, block_offset, mask1, shift1, block_offset + 1, shift2, cast_end))
    f.write("      }\n")
  f.write("    }\n\n")

  f.write("    @Override\n")
  f.write("    public void decode(byte[] blocks, int blocksOffset, %s[] values, int valuesOffset, int iterations) {\n" %typ)
  if bits < bpv:
    f.write("      throw new UnsupportedOperationException();\n")
  else:
    f.write("      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;\n")
    f.write("      assert valuesOffset + iterations * valueCount() <= values.length;\n")
    f.write("      for (int i = 0; i < iterations; ++i) {\n")
    blocks = values * bpv / 8
    for i in xrange(0, values):
      byte_start = i * bpv / 8
      bit_start = (i * bpv) % 8
      byte_end = ((i + 1) * bpv - 1) / 8
      bit_end = ((i + 1) * bpv - 1) % 8
      shift = lambda b: 8 * (byte_end - b - 1) + 1 + bit_end
      if bit_start == 0:
        f.write("        final %s byte%d = blocks[blocksOffset++] & 0xFF;\n" %(typ, byte_start))
      for b in xrange(byte_start + 1, byte_end + 1):
        f.write("        final %s byte%d = blocks[blocksOffset++] & 0xFF;\n" %(typ, b))
      f.write("        values[valuesOffset++] =")
      if byte_start == byte_end:
        if bit_start == 0:
          if bit_end == 7:
            f.write(" byte%d" %byte_start)
          else:
            f.write(" byte%d >>> %d" %(byte_start, 7 - bit_end))
        else:
          if bit_end == 7:
            f.write(" byte%d & %d" %(byte_start, 2 ** (8 - bit_start) - 1))
          else:
            f.write(" (byte%d >>> %d) & %d" %(byte_start, 7 - bit_end, 2 ** (bit_end - bit_start + 1) - 1))
      else:
        if bit_start == 0:
          f.write(" (byte%d << %d)" %(byte_start, shift(byte_start)))
        else:
          f.write(" ((byte%d & %d) << %d)" %(byte_start, 2 ** (8 - bit_start) - 1, shift(byte_start)))
        for b in xrange(byte_start + 1, byte_end):
          f.write(" | (byte%d << %d)" %(b, shift(b)))
        if bit_end == 7:
          f.write(" | byte%d" %byte_end)
        else:
          f.write(" | (byte%d >>> %d)" %(byte_end, 7 - bit_end))
      f.write(";\n")
    f.write("      }\n")
  f.write("    }\n\n")

def p64_encode(bpv, f, bits, values):
  typ = get_type(bits)
  mask_start, mask_end = masks(bits)
  f.write("    @Override\n")
  f.write("    public void encode(%s[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {\n" %typ)
  f.write("      assert blocksOffset + iterations * blockCount() <= blocks.length;\n")
  f.write("      assert valuesOffset + iterations * valueCount() <= values.length;\n")
  f.write("      for (int i = 0; i < iterations; ++i) {\n")
  for i in xrange(0, values):
    block_offset = i * bpv / 64
    bit_offset = (i * bpv) % 64
    if bit_offset == 0:
      # start of block
      f.write("        blocks[blocksOffset++] = (%svalues[valuesOffset++]%s << %d)" %(mask_start, mask_end, 64 - bpv))
    elif bit_offset + bpv == 64:
      # end of block
      f.write(" | %svalues[valuesOffset++]%s;\n" %(mask_start, mask_end))
    elif bit_offset + bpv < 64:
      # inside a block
      f.write(" | (%svalues[valuesOffset++]%s << %d)" %(mask_start, mask_end, 64 - bit_offset - bpv))
    else:
      # value spans across 2 blocks
      right_bits = bit_offset + bpv - 64
      f.write(" | (%svalues[valuesOffset]%s >>> %d);\n" %(mask_start, mask_end, right_bits))
      f.write("        blocks[blocksOffset++] = (%svalues[valuesOffset++]%s << %d)" %(mask_start, mask_end, 64 - right_bits))
  f.write("      }\n")
  f.write("    }\n\n")


if __name__ == '__main__':
  p64_bpv = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32]

  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  f.write('import java.nio.ByteBuffer;\n')
  f.write('\n')
  f.write('''/**
 * Efficient sequential read/write of packed integers.
 */\n''')

  f.write('abstract class BulkOperation implements PackedInts.Decoder, PackedInts.Encoder {\n')
  f.write('  private static final BulkOperation[] packedBulkOps = new BulkOperation[] {\n')
    
  for bpv in xrange(1, 65):
    f2 = open('BulkOperationPacked%d.java' % bpv, 'w')
    f2.write(HEADER)
    if bpv == 64:
      f2.write('import java.nio.LongBuffer;\n')
      f2.write('import java.nio.ByteBuffer;\n')
      f2.write('\n')
    f2.write('''/**
 * Efficient sequential read/write of packed integers.
 */\n''')
    f2.write('final class BulkOperationPacked%d extends BulkOperation {\n' % bpv)
    packed64(bpv, f2)
    f2.write('}\n')
    f2.close()
    f.write('    new BulkOperationPacked%d(),\n' % bpv)
    
  f.write('  };\n')
  f.write('\n')
    
  f.write('  // NOTE: this is sparse (some entries are null):\n')
  f.write('  private static final BulkOperation[] packedSingleBlockBulkOps = new BulkOperation[] {\n')
  for bpv in xrange(1, max(PACKED_64_SINGLE_BLOCK_BPV)+1):
    if bpv in PACKED_64_SINGLE_BLOCK_BPV:
      f2 = open('BulkOperationPackedSingleBlock%d.java' % bpv, 'w')
      f2.write(HEADER)
      f2.write('''/**
 * Efficient sequential read/write of packed integers.
 */\n''')
      f2.write('final class BulkOperationPackedSingleBlock%d extends BulkOperation {\n' % bpv)
      packed64singleblock(bpv,f2)
      f2.write('}\n')
      f2.close()
      f.write('    new BulkOperationPackedSingleBlock%d(),\n' % bpv)
    else:
      f.write('    null,\n')
  f.write('  };\n')
  f.write('\n')
      
  f.write("\n")
  f.write("  public static BulkOperation of(PackedInts.Format format, int bitsPerValue) {\n")
  f.write("    switch (format) {\n")

  f.write("    case PACKED:\n")
  f.write("      assert packedBulkOps[bitsPerValue - 1] != null;\n")
  f.write("      return packedBulkOps[bitsPerValue - 1];\n")
  f.write("    case PACKED_SINGLE_BLOCK:\n")
  f.write("      assert packedSingleBlockBulkOps[bitsPerValue - 1] != null;\n")
  f.write("      return packedSingleBlockBulkOps[bitsPerValue - 1];\n")
  f.write("    default:\n")
  f.write("      throw new AssertionError();\n")
  f.write("    }\n")
  f.write("  }\n")
  f.write(FOOTER)
  f.close()
