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

MAX_SPECIALIZED_BITS_PER_VALUE = 24
PACKED_64_SINGLE_BLOCK_BPV = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32]
OUTPUT_FILE = "BulkOperation.java"
HEADER = """// This file has been automatically generated, DO NOT EDIT

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

"""

FOOTER = """
}
"""

def is_power_of_two(n):
  return n & (n - 1) == 0

def casts(typ):
  cast_start = "(%s) (" % typ
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
  return "(", " & %sL)" % (hexNoLSuffix((1 << bits) - 1))

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

def block_value_count(bpv, bits=64):
  blocks = bpv
  values = blocks * bits // bpv
  while blocks % 2 == 0 and values % 2 == 0:
    blocks //= 2
    values //= 2
  assert values * bpv == bits * blocks, "%d values, %d blocks, %d bits per value" % (values, blocks, bpv)
  return (blocks, values)

def packed64(bpv, f):
  mask = (1 << bpv) - 1

  f.write("\n")
  f.write("  public BulkOperationPacked%d() {\n" % bpv)
  f.write("    super(%d);\n" % bpv)
  f.write("  }\n\n")

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
""")
  else:
    p64_decode(bpv, f, 32)
    p64_decode(bpv, f, 64)

def p64_decode(bpv, f, bits):
  blocks, values = block_value_count(bpv)
  typ = get_type(bits)
  cast_start, cast_end = casts(typ)

  f.write("  @Override\n")
  f.write("  public void decode(long[] blocks, int blocksOffset, %s[] values, int valuesOffset, int iterations) {\n" % typ)
  if bits < bpv:
    f.write("    throw new UnsupportedOperationException();\n")
  else:
    f.write("    for (int i = 0; i < iterations; ++i) {\n")
    mask = (1 << bpv) - 1

    if is_power_of_two(bpv):
      f.write("      final long block = blocks[blocksOffset++];\n")
      f.write("      for (int shift = 0; shift <= %d; shift += %d) {\n" % (64 - bpv, bpv))
      f.write("        values[valuesOffset++] = %s(block >>> shift) & %d%s;\n" % (cast_start, mask, cast_end))
      f.write("      }\n")
    else:
      for i in range(0, values):
        block_offset = i * bpv // 64
        bit_offset = (i * bpv) % 64
        if bit_offset == 0:
          # start of block
          f.write("      final long block%d = blocks[blocksOffset++];\n" % block_offset)
          f.write("      values[valuesOffset++] = %sblock%d & %dL%s;\n" % (cast_start, block_offset, mask, cast_end))
        elif bit_offset + bpv == 64:
          # end of block
          f.write("      values[valuesOffset++] = %sblock%d >>> %d%s;\n" % (cast_start, block_offset, 64 - bpv, cast_end))
        elif bit_offset + bpv < 64:
          # middle of block
          f.write("      values[valuesOffset++] = %s(block%d >>> %d) & %dL%s;\n" % (cast_start, block_offset, bit_offset, mask, cast_end))
        else:
          # value spans across 2 blocks
          mask1 = (1 << (bit_offset + bpv - 64)) - 1
          shift1 = (64 - bit_offset)
          shift2 = 64 - shift1
          f.write("      final long block%d = blocks[blocksOffset++];\n" % (block_offset + 1))
          f.write("      values[valuesOffset++] = %s((block%d & %dL) << %d) | (block%d >>> %d)%s;\n" % (cast_start, block_offset + 1, mask1, shift1, block_offset, shift2, cast_end))
    f.write("    }\n")
  f.write("  }\n\n")

  byte_blocks, byte_values = block_value_count(bpv, 8)

  f.write("  @Override\n")
  f.write("  public void decode(byte[] blocks, int blocksOffset, %s[] values, int valuesOffset, int iterations) {\n" % typ)
  if bits < bpv:
    f.write("    throw new UnsupportedOperationException();\n")
  else:
    if is_power_of_two(bpv) and bpv < 8:
      f.write("    for (int j = 0; j < iterations; ++j) {\n")
      f.write("      final byte block = blocks[blocksOffset++];\n")
      f.write("      values[valuesOffset++] = block & %d;\n" % mask)
      for shift in range(8 - bpv, 0, -bpv):
        f.write("      values[valuesOffset++] = (block >>> %d) & %d;\n" % (8 - shift, mask))
      f.write("    }\n")
    elif bpv == 8:
      f.write("    for (int j = 0; j < iterations; ++j) {\n")
      f.write("      values[valuesOffset++] = blocks[blocksOffset++] & 0xFF;\n")
      f.write("    }\n")
    elif is_power_of_two(bpv) and bpv > 8:
      f.write("    for (int j = 0; j < iterations; ++j) {\n")
      m = bits <= 32 and "0xFF" or "0xFFL"
      f.write("      values[valuesOffset++] =")
      for i in range(bpv // 8 - 1):
        f.write(" (blocks[blocksOffset++] & %s)  |" % m)
      f.write(" ((blocks[blocksOffset++] & %s) << %d);\n" % (m, bpv - 8))
      f.write("    }\n")
    else:
      f.write("    for (int i = 0; i < iterations; ++i) {\n")
      for i in range(0, byte_values):
        byte_start = i * bpv // 8
        bit_start = (i * bpv) % 8
        byte_end = ((i + 1) * bpv - 1) // 8
        bit_end = ((i + 1) * bpv - 1) % 8
        shift = lambda b: bpv - (8 * (byte_end - b) + 1 + bit_end)
        if bit_start == 0:
          f.write("      final %s byte%d = blocks[blocksOffset++] & 0xFF;\n" % (typ, byte_start))
        for b in range(byte_start + 1, byte_end + 1):
          f.write("      final %s byte%d = blocks[blocksOffset++] & 0xFF;\n" % (typ, b))
        f.write("      values[valuesOffset++] =")
        if byte_start == byte_end:
          if bit_start == 0:
            if bit_end == 7:
              f.write(" byte%d" % byte_start)
            else:
              f.write(" byte%d & %d" % (byte_start, 2 ** (bit_end - bit_start + 1) - 1))
          else:
            if bit_end == 7:
              f.write(" (byte%d >>> %d)" % (byte_start, bit_start))
            else:
              f.write(" (byte%d >>> %d) & %d" % (byte_start, bit_start, 2 ** (bit_end - bit_start + 1) - 1))
        else:
          if bit_end == 7:
            f.write(" (byte%d << %d)" % (byte_end, shift(byte_end)))
          else:
            f.write(" ((byte%d & %d) << %d)" % (byte_end, 2 ** (bit_end + 1) - 1, shift(byte_end)))
           
          for b in range(byte_end - 1, byte_start, -1):  
            f.write(" | (byte%d << %d)" % (b, shift(b)))
          if bit_start == 0:
            f.write(" | byte%d" % byte_start)
          else:
            f.write(" | (byte%d >>> %d)" % (byte_start, bit_start))
        f.write(";\n")
      f.write("    }\n")
  f.write("  }\n\n")

if __name__ == '__main__':
  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  f.write('\n')
  f.write('''/**
 * Efficient sequential read/write of packed integers.
 */\n''')

  f.write('abstract class BulkOperation implements PackedInts.Decoder, PackedInts.Encoder {\n')
  f.write('  private static final BulkOperation[] packedBulkOps = new BulkOperation[] {\n')

  for bpv in range(1, 65):
    if bpv > MAX_SPECIALIZED_BITS_PER_VALUE:
      f.write('    new BulkOperationPacked(%d),\n' % bpv)
      continue
    f2 = open('BulkOperationPacked%d.java' % bpv, 'w')
    f2.write(HEADER)
    if bpv == 64:
      f2.write('import java.nio.LongBuffer;\n')
      f2.write('import java.nio.ByteBuffer;\n')
      f2.write('\n')
    f2.write('''/**
 * Efficient sequential read/write of packed integers.
 */\n''')
    f2.write('final class BulkOperationPacked%d extends BulkOperationPacked {\n' % bpv)
    packed64(bpv, f2)
    f2.write('}\n')
    f2.close()
    f.write('    new BulkOperationPacked%d(),\n' % bpv)

  f.write('  };\n')
  f.write('\n')
  f.write("  public static BulkOperation of(PackedInts.Format format, int bitsPerValue) {\n")
  f.write("    assert packedBulkOps[bitsPerValue - 1] != null;\n")
  f.write("    return packedBulkOps[bitsPerValue - 1];\n")
  f.write("  }\n")
  f.write(FOOTER)
  f.close()
