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

"""Code generation for ForUtil.java"""

MAX_SPECIALIZED_BITS_PER_VALUE = 24
OUTPUT_FILE = "ForUtil.java"
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
package org.apache.lucene.codecs.lucene84;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.function.IntToLongFunction;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

// Inspired from https://fulmicoton.com/posts/bitpacking/
// Encodes multiple integers in a long to get SIMD-like speedups.
// If bitsPerValue <= 8 then we pack 8 ints per long
// else if bitsPerValue <= 16 we pack 4 ints per long
// else we pack 2 ints per long
final class ForUtil {

  static final int BLOCK_SIZE = 128;
  private static final int BLOCK_SIZE_LOG2 = 7;

  private static long expandMask32(long mask32) {
    return mask32 | (mask32 << 32);
  }

  private static long expandMask16(long mask16) {
    return expandMask32(mask16 | (mask16 << 16));
  }

  private static long expandMask8(long mask8) {
    return expandMask16(mask8 | (mask8 << 8));
  }

  private static long mask32(int bitsPerValue) {
    return expandMask32((1L << bitsPerValue) - 1);
  }

  private static long mask16(int bitsPerValue) {
    return expandMask16((1L << bitsPerValue) - 1);
  }

  private static long mask8(int bitsPerValue) {
    return expandMask8((1L << bitsPerValue) - 1);
  }

  private static void expand8(long[] arr) {
    for (int i = 0; i < 16; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 56) & 0xFFL;
      arr[16+i] = (l >>> 48) & 0xFFL;
      arr[32+i] = (l >>> 40) & 0xFFL;
      arr[48+i] = (l >>> 32) & 0xFFL;
      arr[64+i] = (l >>> 24) & 0xFFL;
      arr[80+i] = (l >>> 16) & 0xFFL;
      arr[96+i] = (l >>> 8) & 0xFFL;
      arr[112+i] = l & 0xFFL;
    }
  }

  private static void collapse8(long[] arr) {
    for (int i = 0; i < 16; ++i) {
      arr[i] = (arr[i] << 56) | (arr[16+i] << 48) | (arr[32+i] << 40) | (arr[48+i] << 32) | (arr[64+i] << 24) | (arr[80+i] << 16) | (arr[96+i] << 8) | arr[112+i];
    }
  }

  private static void expand16(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      long l = arr[i];
      arr[i] = (l >>> 48) & 0xFFFFL;
      arr[32+i] = (l >>> 32) & 0xFFFFL;
      arr[64+i] = (l >>> 16) & 0xFFFFL;
      arr[96+i] = l & 0xFFFFL;
    }
  }

  private static void collapse16(long[] arr) {
    for (int i = 0; i < 32; ++i) {
      arr[i] = (arr[i] << 48) | (arr[32+i] << 32) | (arr[64+i] << 16) | arr[96+i];
    }
  }

  private static void expand32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      long l = arr[i];
      arr[i] = l >>> 32;
      arr[64 + i] = l & 0xFFFFFFFFL;
    }
  }

  private static void collapse32(long[] arr) {
    for (int i = 0; i < 64; ++i) {
      arr[i] = (arr[i] << 32) | arr[64+i];
    }
  }

  private final ByteOrder byteOrder;
  private final long[] tmp = new long[BLOCK_SIZE/2];

  ForUtil(ByteOrder byteOrder) {
    this.byteOrder = byteOrder;
  }

  /**
   * Encode 128 8-bits integers from {@code data} into {@code out}.
   */
  void encode(long[] longs, int bitsPerValue, DataOutput out) throws IOException {
    final int nextPrimitive;
    final int numLongs;
    final IntToLongFunction maskFunction;
    if (bitsPerValue <= 8) {
      nextPrimitive = 8;
      numLongs = BLOCK_SIZE / 8;
      maskFunction = ForUtil::mask8;
      collapse8(longs);
    } else if (bitsPerValue <= 16) {
      nextPrimitive = 16;
      numLongs = BLOCK_SIZE / 4;
      maskFunction = ForUtil::mask16;
      collapse16(longs);
    } else {
      nextPrimitive = 32;
      numLongs = BLOCK_SIZE / 2;
      maskFunction = ForUtil::mask32;
      collapse32(longs);
    }

    final int numLongsPerShift = bitsPerValue * 2;
    Arrays.fill(tmp, 0L);
    int idx = 0;
    for (int shift = nextPrimitive - bitsPerValue; shift >= 0; shift -= bitsPerValue) {
      for (int i = 0; i < numLongsPerShift; ++i) {
        tmp[i] |= longs[idx++] << shift;
      }
    }

    final int remainingBitsPerLong = nextPrimitive % bitsPerValue;
    int tmpIdx = 0;
    int remainingBitsPerValue = bitsPerValue;
    while (idx < numLongs) {
      if (remainingBitsPerValue > remainingBitsPerLong) {
        remainingBitsPerValue -= remainingBitsPerLong;
        tmp[tmpIdx++] |= (longs[idx] >>> remainingBitsPerValue) & maskFunction.applyAsLong(remainingBitsPerLong);
        if (remainingBitsPerValue == 0) {
          idx++;
          remainingBitsPerValue = bitsPerValue;
        }
      } else {
        tmp[tmpIdx] |= (longs[idx++] & maskFunction.applyAsLong(remainingBitsPerValue)) << (remainingBitsPerLong - remainingBitsPerValue);
        remainingBitsPerValue = bitsPerValue - remainingBitsPerLong + remainingBitsPerValue;
        tmp[tmpIdx++] |= (longs[idx] >>> remainingBitsPerValue) & maskFunction.applyAsLong(bitsPerValue - remainingBitsPerValue);
      }
    }

    for (int i = 0; i < numLongsPerShift; ++i) {
      long l = tmp[i];
      if (byteOrder != ByteOrder.BIG_ENDIAN) {
        l = Long.reverseBytes(l);
      }
      out.writeLong(l);
    }
  }

  /**
   * Number of bytes required to encode 128 integers of {@code bitsPerValue}.
   */
  int numBytes(int bitsPerValue) throws IOException {
    return bitsPerValue << (BLOCK_SIZE_LOG2 - 3);
  }

  private static void decodeSlow(int bitsPerValue, DataInput in, ByteOrder byteOrder, long[] tmp, long[] longs) throws IOException {
    final int numLongs = bitsPerValue << 1;
    in.readLongs(byteOrder, tmp, 0, numLongs);
    final long mask = mask32(bitsPerValue);
    int longsIdx = 0;
    int shift = 32 - bitsPerValue;
    for (; shift >= 0; shift -= bitsPerValue) {
      shiftLongs(tmp, numLongs, longs, longsIdx, shift, mask);
      longsIdx += numLongs;
    }
    final int remainingBitsPerLong = shift + bitsPerValue;
    final long mask32RemainingBitsPerLong = mask32(remainingBitsPerLong);
    int tmpIdx = 0;
    int remainingBits = remainingBitsPerLong;
    for (; longsIdx < BLOCK_SIZE / 2; ++longsIdx) {
      int b = bitsPerValue - remainingBits;
      long l = (tmp[tmpIdx++] & mask32(remainingBits)) << b;
      while (b >= remainingBitsPerLong) {
        b -= remainingBitsPerLong;
        l |= (tmp[tmpIdx++] & mask32RemainingBitsPerLong) << b;
      }
      if (b > 0) {
        l |= (tmp[tmpIdx] >>> (remainingBitsPerLong-b)) & mask32(b);
        remainingBits = remainingBitsPerLong - b;
      } else {
        remainingBits = remainingBitsPerLong;
      }
      longs[longsIdx] = l;
    }

    expand32(longs);
  }

  /**
   * The pattern that this shiftLongs method applies is recognized by the C2
   * compiler, which generates SIMD instructions for it in order to shift
   * multiple longs at once.
   */
  private static void shiftLongs(long[] a, int count, long[] b, int bi, int shift, long mask) {
    for (int i = 0; i < count; ++i) {
      b[bi+i] = (a[i] >>> shift) & mask;
    }
  }

"""

def writeRemainder(bpv, next_primitive, remaining_bits_per_long, o, num_values, f):
  iteration = 1
  num_longs = bpv * num_values / remaining_bits_per_long
  while num_longs % 2 == 0 and num_values % 2 == 0:
    num_longs /= 2
    num_values /= 2
    iteration *= 2
  f.write('    for(int iter = 0, tmpIdx = 0, longsIdx = %d; iter < %d; ++iter, tmpIdx += %d, longsIdx += %d) {\n' %(o, iteration, num_longs, num_values))
  i = 0
  remaining_bits = 0
  tmp_idx = 0
  for i in range(num_values):
    b = bpv
    if remaining_bits == 0:
      b -= remaining_bits_per_long
      f.write('      long l%d = (tmp[tmpIdx+%d] & MASK%d_%d) << %d;\n' %(i, tmp_idx, next_primitive, remaining_bits_per_long, b))
    else:
      b -= remaining_bits
      f.write('      long l%d = (tmp[tmpIdx+%d] & MASK%d_%d) << %d;\n' %(i, tmp_idx, next_primitive, remaining_bits, b))
    tmp_idx += 1
    while b >= remaining_bits_per_long:
      b -= remaining_bits_per_long
      f.write('      l%d |= (tmp[tmpIdx+%d] & MASK%d_%d) << %d;\n' %(i, tmp_idx, next_primitive, remaining_bits_per_long, b))
      tmp_idx += 1
    if b > 0:
      f.write('      l%d |= (tmp[tmpIdx+%d] >>> %d) & MASK%d_%d;\n' %(i, tmp_idx, remaining_bits_per_long-b, next_primitive, b))
      remaining_bits = remaining_bits_per_long-b
    f.write('      longs[longsIdx+%d] = l%d;\n' %(i, i))
  f.write('    }\n')
  
  

def writeDecode(bpv, f):
  next_primitive = 32
  if bpv <= 8:
    next_primitive = 8
  elif bpv <= 16:
    next_primitive = 16
  f.write('  private static void decode%d(DataInput in, ByteOrder byteOrder, long[] tmp, long[] longs) throws IOException {\n' %bpv)
  num_values_per_long = 64 / next_primitive
  if bpv == next_primitive:
    f.write('    in.readLongs(byteOrder, longs, 0, %d);\n' %(bpv*2))
  else:
    f.write('    in.readLongs(byteOrder, tmp, 0, %d);\n' %(bpv*2))
    shift = next_primitive - bpv
    o = 0
    while shift >= 0:
      f.write('    shiftLongs(tmp, %d, longs, %d, %d, MASK%d_%d);\n' %(bpv*2, o, shift, next_primitive, bpv))
      o += bpv*2
      shift -= bpv
    if shift + bpv > 0:
      writeRemainder(bpv, next_primitive, shift + bpv, o, 128/num_values_per_long - o, f)
  f.write('    expand%d(longs);\n' %next_primitive)
  f.write('  }\n')
  f.write('\n')

if __name__ == '__main__':
  f = open(OUTPUT_FILE, 'w')
  f.write(HEADER)
  for primitive_size in [8, 16, 32]:
    for bpv in range(1, min(MAX_SPECIALIZED_BITS_PER_VALUE + 1, primitive_size)):
      if bpv * 2 != primitive_size or primitive_size == 8:
        f.write('  private static final long MASK%d_%d = mask%d(%d);\n' %(primitive_size, bpv, primitive_size, bpv))
  f.write('\n')
  f.write("""
  /**
   * Decode 128 integers into {@code ints}.
   */
  void decode(int bitsPerValue, DataInput in, long[] longs) throws IOException {
    switch (bitsPerValue) {
""")
  for i in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    f.write('    case %d:\n' %i)
    f.write('      decode%d(in, byteOrder, tmp, longs);\n' %i)
    f.write('      break;\n')
  f.write('    default:\n')
  f.write('      decodeSlow(bitsPerValue, in, byteOrder, tmp, longs);\n')
  f.write('      break;\n')
  f.write('    }\n')
  f.write('  }\n')

  f.write('\n')
  for i in range(1, MAX_SPECIALIZED_BITS_PER_VALUE+1):
    writeDecode(i, f)

  f.write('}\n')
