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
  private static final BulkOperationPacked1 packed1 = new BulkOperationPacked1();
  private static final BulkOperationPacked2 packed2 = new BulkOperationPacked2();
  private static final BulkOperationPacked3 packed3 = new BulkOperationPacked3();
  private static final BulkOperationPacked4 packed4 = new BulkOperationPacked4();
  private static final BulkOperationPacked5 packed5 = new BulkOperationPacked5();
  private static final BulkOperationPacked6 packed6 = new BulkOperationPacked6();
  private static final BulkOperationPacked7 packed7 = new BulkOperationPacked7();
  private static final BulkOperationPacked8 packed8 = new BulkOperationPacked8();
  private static final BulkOperationPacked9 packed9 = new BulkOperationPacked9();
  private static final BulkOperationPacked10 packed10 = new BulkOperationPacked10();
  private static final BulkOperationPacked11 packed11 = new BulkOperationPacked11();
  private static final BulkOperationPacked12 packed12 = new BulkOperationPacked12();
  private static final BulkOperationPacked13 packed13 = new BulkOperationPacked13();
  private static final BulkOperationPacked14 packed14 = new BulkOperationPacked14();
  private static final BulkOperationPacked15 packed15 = new BulkOperationPacked15();
  private static final BulkOperationPacked16 packed16 = new BulkOperationPacked16();
  private static final BulkOperationPacked17 packed17 = new BulkOperationPacked17();
  private static final BulkOperationPacked18 packed18 = new BulkOperationPacked18();
  private static final BulkOperationPacked19 packed19 = new BulkOperationPacked19();
  private static final BulkOperationPacked20 packed20 = new BulkOperationPacked20();
  private static final BulkOperationPacked21 packed21 = new BulkOperationPacked21();
  private static final BulkOperationPacked22 packed22 = new BulkOperationPacked22();
  private static final BulkOperationPacked23 packed23 = new BulkOperationPacked23();
  private static final BulkOperationPacked24 packed24 = new BulkOperationPacked24();
  private static final BulkOperationPacked25 packed25 = new BulkOperationPacked25();
  private static final BulkOperationPacked26 packed26 = new BulkOperationPacked26();
  private static final BulkOperationPacked27 packed27 = new BulkOperationPacked27();
  private static final BulkOperationPacked28 packed28 = new BulkOperationPacked28();
  private static final BulkOperationPacked29 packed29 = new BulkOperationPacked29();
  private static final BulkOperationPacked30 packed30 = new BulkOperationPacked30();
  private static final BulkOperationPacked31 packed31 = new BulkOperationPacked31();
  private static final BulkOperationPacked32 packed32 = new BulkOperationPacked32();
  private static final BulkOperationPacked33 packed33 = new BulkOperationPacked33();
  private static final BulkOperationPacked34 packed34 = new BulkOperationPacked34();
  private static final BulkOperationPacked35 packed35 = new BulkOperationPacked35();
  private static final BulkOperationPacked36 packed36 = new BulkOperationPacked36();
  private static final BulkOperationPacked37 packed37 = new BulkOperationPacked37();
  private static final BulkOperationPacked38 packed38 = new BulkOperationPacked38();
  private static final BulkOperationPacked39 packed39 = new BulkOperationPacked39();
  private static final BulkOperationPacked40 packed40 = new BulkOperationPacked40();
  private static final BulkOperationPacked41 packed41 = new BulkOperationPacked41();
  private static final BulkOperationPacked42 packed42 = new BulkOperationPacked42();
  private static final BulkOperationPacked43 packed43 = new BulkOperationPacked43();
  private static final BulkOperationPacked44 packed44 = new BulkOperationPacked44();
  private static final BulkOperationPacked45 packed45 = new BulkOperationPacked45();
  private static final BulkOperationPacked46 packed46 = new BulkOperationPacked46();
  private static final BulkOperationPacked47 packed47 = new BulkOperationPacked47();
  private static final BulkOperationPacked48 packed48 = new BulkOperationPacked48();
  private static final BulkOperationPacked49 packed49 = new BulkOperationPacked49();
  private static final BulkOperationPacked50 packed50 = new BulkOperationPacked50();
  private static final BulkOperationPacked51 packed51 = new BulkOperationPacked51();
  private static final BulkOperationPacked52 packed52 = new BulkOperationPacked52();
  private static final BulkOperationPacked53 packed53 = new BulkOperationPacked53();
  private static final BulkOperationPacked54 packed54 = new BulkOperationPacked54();
  private static final BulkOperationPacked55 packed55 = new BulkOperationPacked55();
  private static final BulkOperationPacked56 packed56 = new BulkOperationPacked56();
  private static final BulkOperationPacked57 packed57 = new BulkOperationPacked57();
  private static final BulkOperationPacked58 packed58 = new BulkOperationPacked58();
  private static final BulkOperationPacked59 packed59 = new BulkOperationPacked59();
  private static final BulkOperationPacked60 packed60 = new BulkOperationPacked60();
  private static final BulkOperationPacked61 packed61 = new BulkOperationPacked61();
  private static final BulkOperationPacked62 packed62 = new BulkOperationPacked62();
  private static final BulkOperationPacked63 packed63 = new BulkOperationPacked63();
  private static final BulkOperationPacked64 packed64 = new BulkOperationPacked64();
  private static final BulkOperationPackedSingleBlock1 packedSingleBlock1 = new BulkOperationPackedSingleBlock1();
  private static final BulkOperationPackedSingleBlock2 packedSingleBlock2 = new BulkOperationPackedSingleBlock2();
  private static final BulkOperationPackedSingleBlock3 packedSingleBlock3 = new BulkOperationPackedSingleBlock3();
  private static final BulkOperationPackedSingleBlock4 packedSingleBlock4 = new BulkOperationPackedSingleBlock4();
  private static final BulkOperationPackedSingleBlock5 packedSingleBlock5 = new BulkOperationPackedSingleBlock5();
  private static final BulkOperationPackedSingleBlock6 packedSingleBlock6 = new BulkOperationPackedSingleBlock6();
  private static final BulkOperationPackedSingleBlock7 packedSingleBlock7 = new BulkOperationPackedSingleBlock7();
  private static final BulkOperationPackedSingleBlock8 packedSingleBlock8 = new BulkOperationPackedSingleBlock8();
  private static final BulkOperationPackedSingleBlock9 packedSingleBlock9 = new BulkOperationPackedSingleBlock9();
  private static final BulkOperationPackedSingleBlock10 packedSingleBlock10 = new BulkOperationPackedSingleBlock10();
  private static final BulkOperationPackedSingleBlock12 packedSingleBlock12 = new BulkOperationPackedSingleBlock12();
  private static final BulkOperationPackedSingleBlock16 packedSingleBlock16 = new BulkOperationPackedSingleBlock16();
  private static final BulkOperationPackedSingleBlock21 packedSingleBlock21 = new BulkOperationPackedSingleBlock21();
  private static final BulkOperationPackedSingleBlock32 packedSingleBlock32 = new BulkOperationPackedSingleBlock32();
  public static BulkOperation of(PackedInts.Format format, int bitsPerValue) {
    switch (format) {
    case PACKED:
      switch (bitsPerValue) {
      case 1:
        return packed1;
      case 2:
        return packed2;
      case 3:
        return packed3;
      case 4:
        return packed4;
      case 5:
        return packed5;
      case 6:
        return packed6;
      case 7:
        return packed7;
      case 8:
        return packed8;
      case 9:
        return packed9;
      case 10:
        return packed10;
      case 11:
        return packed11;
      case 12:
        return packed12;
      case 13:
        return packed13;
      case 14:
        return packed14;
      case 15:
        return packed15;
      case 16:
        return packed16;
      case 17:
        return packed17;
      case 18:
        return packed18;
      case 19:
        return packed19;
      case 20:
        return packed20;
      case 21:
        return packed21;
      case 22:
        return packed22;
      case 23:
        return packed23;
      case 24:
        return packed24;
      case 25:
        return packed25;
      case 26:
        return packed26;
      case 27:
        return packed27;
      case 28:
        return packed28;
      case 29:
        return packed29;
      case 30:
        return packed30;
      case 31:
        return packed31;
      case 32:
        return packed32;
      case 33:
        return packed33;
      case 34:
        return packed34;
      case 35:
        return packed35;
      case 36:
        return packed36;
      case 37:
        return packed37;
      case 38:
        return packed38;
      case 39:
        return packed39;
      case 40:
        return packed40;
      case 41:
        return packed41;
      case 42:
        return packed42;
      case 43:
        return packed43;
      case 44:
        return packed44;
      case 45:
        return packed45;
      case 46:
        return packed46;
      case 47:
        return packed47;
      case 48:
        return packed48;
      case 49:
        return packed49;
      case 50:
        return packed50;
      case 51:
        return packed51;
      case 52:
        return packed52;
      case 53:
        return packed53;
      case 54:
        return packed54;
      case 55:
        return packed55;
      case 56:
        return packed56;
      case 57:
        return packed57;
      case 58:
        return packed58;
      case 59:
        return packed59;
      case 60:
        return packed60;
      case 61:
        return packed61;
      case 62:
        return packed62;
      case 63:
        return packed63;
      case 64:
        return packed64;
      default:
        throw new AssertionError();
      }
    case PACKED_SINGLE_BLOCK:
      switch (bitsPerValue) {
      case 1:
        return packedSingleBlock1;
      case 2:
        return packedSingleBlock2;
      case 3:
        return packedSingleBlock3;
      case 4:
        return packedSingleBlock4;
      case 5:
        return packedSingleBlock5;
      case 6:
        return packedSingleBlock6;
      case 7:
        return packedSingleBlock7;
      case 8:
        return packedSingleBlock8;
      case 9:
        return packedSingleBlock9;
      case 10:
        return packedSingleBlock10;
      case 12:
        return packedSingleBlock12;
      case 16:
        return packedSingleBlock16;
      case 21:
        return packedSingleBlock21;
      case 32:
        return packedSingleBlock32;
      default:
        throw new AssertionError();
      }
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
