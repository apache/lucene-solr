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

  static {
    BULK_OPERATIONS.put(PackedInts.Format.PACKED, new BulkOperation[65]);    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[1] = new Packed64BulkOperation1();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[2] = new Packed64BulkOperation2();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[3] = new Packed64BulkOperation3();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[4] = new Packed64BulkOperation4();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[5] = new Packed64BulkOperation5();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[6] = new Packed64BulkOperation6();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[7] = new Packed64BulkOperation7();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[8] = new Packed64BulkOperation8();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[9] = new Packed64BulkOperation9();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[10] = new Packed64BulkOperation10();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[11] = new Packed64BulkOperation11();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[12] = new Packed64BulkOperation12();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[13] = new Packed64BulkOperation13();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[14] = new Packed64BulkOperation14();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[15] = new Packed64BulkOperation15();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[16] = new Packed64BulkOperation16();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[17] = new Packed64BulkOperation17();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[18] = new Packed64BulkOperation18();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[19] = new Packed64BulkOperation19();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[20] = new Packed64BulkOperation20();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[21] = new Packed64BulkOperation21();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[22] = new Packed64BulkOperation22();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[23] = new Packed64BulkOperation23();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[24] = new Packed64BulkOperation24();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[25] = new Packed64BulkOperation25();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[26] = new Packed64BulkOperation26();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[27] = new Packed64BulkOperation27();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[28] = new Packed64BulkOperation28();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[29] = new Packed64BulkOperation29();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[30] = new Packed64BulkOperation30();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[31] = new Packed64BulkOperation31();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[32] = new Packed64BulkOperation32();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[33] = new Packed64BulkOperation33();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[34] = new Packed64BulkOperation34();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[35] = new Packed64BulkOperation35();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[36] = new Packed64BulkOperation36();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[37] = new Packed64BulkOperation37();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[38] = new Packed64BulkOperation38();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[39] = new Packed64BulkOperation39();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[40] = new Packed64BulkOperation40();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[41] = new Packed64BulkOperation41();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[42] = new Packed64BulkOperation42();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[43] = new Packed64BulkOperation43();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[44] = new Packed64BulkOperation44();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[45] = new Packed64BulkOperation45();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[46] = new Packed64BulkOperation46();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[47] = new Packed64BulkOperation47();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[48] = new Packed64BulkOperation48();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[49] = new Packed64BulkOperation49();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[50] = new Packed64BulkOperation50();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[51] = new Packed64BulkOperation51();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[52] = new Packed64BulkOperation52();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[53] = new Packed64BulkOperation53();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[54] = new Packed64BulkOperation54();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[55] = new Packed64BulkOperation55();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[56] = new Packed64BulkOperation56();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[57] = new Packed64BulkOperation57();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[58] = new Packed64BulkOperation58();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[59] = new Packed64BulkOperation59();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[60] = new Packed64BulkOperation60();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[61] = new Packed64BulkOperation61();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[62] = new Packed64BulkOperation62();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[63] = new Packed64BulkOperation63();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED)[64] = new Packed64BulkOperation64();
    BULK_OPERATIONS.put(PackedInts.Format.PACKED_SINGLE_BLOCK, new BulkOperation[65]);
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[1] = new Packed64SingleBlockBulkOperation1();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[2] = new Packed64SingleBlockBulkOperation2();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[3] = new Packed64SingleBlockBulkOperation3();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[4] = new Packed64SingleBlockBulkOperation4();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[5] = new Packed64SingleBlockBulkOperation5();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[6] = new Packed64SingleBlockBulkOperation6();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[7] = new Packed64SingleBlockBulkOperation7();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[8] = new Packed64SingleBlockBulkOperation8();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[9] = new Packed64SingleBlockBulkOperation9();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[10] = new Packed64SingleBlockBulkOperation10();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[12] = new Packed64SingleBlockBulkOperation12();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[16] = new Packed64SingleBlockBulkOperation16();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[21] = new Packed64SingleBlockBulkOperation21();
    BULK_OPERATIONS.get(PackedInts.Format.PACKED_SINGLE_BLOCK)[32] = new Packed64SingleBlockBulkOperation32();
  }
  static final class Packed64BulkOperation1 extends BulkOperation {

    public int blocks() {
      return 1;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 63;
        values[vi++] = (block0 >>> 62) & 1L;
        values[vi++] = (block0 >>> 61) & 1L;
        values[vi++] = (block0 >>> 60) & 1L;
        values[vi++] = (block0 >>> 59) & 1L;
        values[vi++] = (block0 >>> 58) & 1L;
        values[vi++] = (block0 >>> 57) & 1L;
        values[vi++] = (block0 >>> 56) & 1L;
        values[vi++] = (block0 >>> 55) & 1L;
        values[vi++] = (block0 >>> 54) & 1L;
        values[vi++] = (block0 >>> 53) & 1L;
        values[vi++] = (block0 >>> 52) & 1L;
        values[vi++] = (block0 >>> 51) & 1L;
        values[vi++] = (block0 >>> 50) & 1L;
        values[vi++] = (block0 >>> 49) & 1L;
        values[vi++] = (block0 >>> 48) & 1L;
        values[vi++] = (block0 >>> 47) & 1L;
        values[vi++] = (block0 >>> 46) & 1L;
        values[vi++] = (block0 >>> 45) & 1L;
        values[vi++] = (block0 >>> 44) & 1L;
        values[vi++] = (block0 >>> 43) & 1L;
        values[vi++] = (block0 >>> 42) & 1L;
        values[vi++] = (block0 >>> 41) & 1L;
        values[vi++] = (block0 >>> 40) & 1L;
        values[vi++] = (block0 >>> 39) & 1L;
        values[vi++] = (block0 >>> 38) & 1L;
        values[vi++] = (block0 >>> 37) & 1L;
        values[vi++] = (block0 >>> 36) & 1L;
        values[vi++] = (block0 >>> 35) & 1L;
        values[vi++] = (block0 >>> 34) & 1L;
        values[vi++] = (block0 >>> 33) & 1L;
        values[vi++] = (block0 >>> 32) & 1L;
        values[vi++] = (block0 >>> 31) & 1L;
        values[vi++] = (block0 >>> 30) & 1L;
        values[vi++] = (block0 >>> 29) & 1L;
        values[vi++] = (block0 >>> 28) & 1L;
        values[vi++] = (block0 >>> 27) & 1L;
        values[vi++] = (block0 >>> 26) & 1L;
        values[vi++] = (block0 >>> 25) & 1L;
        values[vi++] = (block0 >>> 24) & 1L;
        values[vi++] = (block0 >>> 23) & 1L;
        values[vi++] = (block0 >>> 22) & 1L;
        values[vi++] = (block0 >>> 21) & 1L;
        values[vi++] = (block0 >>> 20) & 1L;
        values[vi++] = (block0 >>> 19) & 1L;
        values[vi++] = (block0 >>> 18) & 1L;
        values[vi++] = (block0 >>> 17) & 1L;
        values[vi++] = (block0 >>> 16) & 1L;
        values[vi++] = (block0 >>> 15) & 1L;
        values[vi++] = (block0 >>> 14) & 1L;
        values[vi++] = (block0 >>> 13) & 1L;
        values[vi++] = (block0 >>> 12) & 1L;
        values[vi++] = (block0 >>> 11) & 1L;
        values[vi++] = (block0 >>> 10) & 1L;
        values[vi++] = (block0 >>> 9) & 1L;
        values[vi++] = (block0 >>> 8) & 1L;
        values[vi++] = (block0 >>> 7) & 1L;
        values[vi++] = (block0 >>> 6) & 1L;
        values[vi++] = (block0 >>> 5) & 1L;
        values[vi++] = (block0 >>> 4) & 1L;
        values[vi++] = (block0 >>> 3) & 1L;
        values[vi++] = (block0 >>> 2) & 1L;
        values[vi++] = (block0 >>> 1) & 1L;
        values[vi++] = block0 & 1L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 62) | (values[vi++] << 61) | (values[vi++] << 60) | (values[vi++] << 59) | (values[vi++] << 58) | (values[vi++] << 57) | (values[vi++] << 56) | (values[vi++] << 55) | (values[vi++] << 54) | (values[vi++] << 53) | (values[vi++] << 52) | (values[vi++] << 51) | (values[vi++] << 50) | (values[vi++] << 49) | (values[vi++] << 48) | (values[vi++] << 47) | (values[vi++] << 46) | (values[vi++] << 45) | (values[vi++] << 44) | (values[vi++] << 43) | (values[vi++] << 42) | (values[vi++] << 41) | (values[vi++] << 40) | (values[vi++] << 39) | (values[vi++] << 38) | (values[vi++] << 37) | (values[vi++] << 36) | (values[vi++] << 35) | (values[vi++] << 34) | (values[vi++] << 33) | (values[vi++] << 32) | (values[vi++] << 31) | (values[vi++] << 30) | (values[vi++] << 29) | (values[vi++] << 28) | (values[vi++] << 27) | (values[vi++] << 26) | (values[vi++] << 25) | (values[vi++] << 24) | (values[vi++] << 23) | (values[vi++] << 22) | (values[vi++] << 21) | (values[vi++] << 20) | (values[vi++] << 19) | (values[vi++] << 18) | (values[vi++] << 17) | (values[vi++] << 16) | (values[vi++] << 15) | (values[vi++] << 14) | (values[vi++] << 13) | (values[vi++] << 12) | (values[vi++] << 11) | (values[vi++] << 10) | (values[vi++] << 9) | (values[vi++] << 8) | (values[vi++] << 7) | (values[vi++] << 6) | (values[vi++] << 5) | (values[vi++] << 4) | (values[vi++] << 3) | (values[vi++] << 2) | (values[vi++] << 1) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation2 extends BulkOperation {

    public int blocks() {
      return 1;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 62;
        values[vi++] = (block0 >>> 60) & 3L;
        values[vi++] = (block0 >>> 58) & 3L;
        values[vi++] = (block0 >>> 56) & 3L;
        values[vi++] = (block0 >>> 54) & 3L;
        values[vi++] = (block0 >>> 52) & 3L;
        values[vi++] = (block0 >>> 50) & 3L;
        values[vi++] = (block0 >>> 48) & 3L;
        values[vi++] = (block0 >>> 46) & 3L;
        values[vi++] = (block0 >>> 44) & 3L;
        values[vi++] = (block0 >>> 42) & 3L;
        values[vi++] = (block0 >>> 40) & 3L;
        values[vi++] = (block0 >>> 38) & 3L;
        values[vi++] = (block0 >>> 36) & 3L;
        values[vi++] = (block0 >>> 34) & 3L;
        values[vi++] = (block0 >>> 32) & 3L;
        values[vi++] = (block0 >>> 30) & 3L;
        values[vi++] = (block0 >>> 28) & 3L;
        values[vi++] = (block0 >>> 26) & 3L;
        values[vi++] = (block0 >>> 24) & 3L;
        values[vi++] = (block0 >>> 22) & 3L;
        values[vi++] = (block0 >>> 20) & 3L;
        values[vi++] = (block0 >>> 18) & 3L;
        values[vi++] = (block0 >>> 16) & 3L;
        values[vi++] = (block0 >>> 14) & 3L;
        values[vi++] = (block0 >>> 12) & 3L;
        values[vi++] = (block0 >>> 10) & 3L;
        values[vi++] = (block0 >>> 8) & 3L;
        values[vi++] = (block0 >>> 6) & 3L;
        values[vi++] = (block0 >>> 4) & 3L;
        values[vi++] = (block0 >>> 2) & 3L;
        values[vi++] = block0 & 3L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 60) | (values[vi++] << 58) | (values[vi++] << 56) | (values[vi++] << 54) | (values[vi++] << 52) | (values[vi++] << 50) | (values[vi++] << 48) | (values[vi++] << 46) | (values[vi++] << 44) | (values[vi++] << 42) | (values[vi++] << 40) | (values[vi++] << 38) | (values[vi++] << 36) | (values[vi++] << 34) | (values[vi++] << 32) | (values[vi++] << 30) | (values[vi++] << 28) | (values[vi++] << 26) | (values[vi++] << 24) | (values[vi++] << 22) | (values[vi++] << 20) | (values[vi++] << 18) | (values[vi++] << 16) | (values[vi++] << 14) | (values[vi++] << 12) | (values[vi++] << 10) | (values[vi++] << 8) | (values[vi++] << 6) | (values[vi++] << 4) | (values[vi++] << 2) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation3 extends BulkOperation {

    public int blocks() {
      return 3;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 61;
        values[vi++] = (block0 >>> 58) & 7L;
        values[vi++] = (block0 >>> 55) & 7L;
        values[vi++] = (block0 >>> 52) & 7L;
        values[vi++] = (block0 >>> 49) & 7L;
        values[vi++] = (block0 >>> 46) & 7L;
        values[vi++] = (block0 >>> 43) & 7L;
        values[vi++] = (block0 >>> 40) & 7L;
        values[vi++] = (block0 >>> 37) & 7L;
        values[vi++] = (block0 >>> 34) & 7L;
        values[vi++] = (block0 >>> 31) & 7L;
        values[vi++] = (block0 >>> 28) & 7L;
        values[vi++] = (block0 >>> 25) & 7L;
        values[vi++] = (block0 >>> 22) & 7L;
        values[vi++] = (block0 >>> 19) & 7L;
        values[vi++] = (block0 >>> 16) & 7L;
        values[vi++] = (block0 >>> 13) & 7L;
        values[vi++] = (block0 >>> 10) & 7L;
        values[vi++] = (block0 >>> 7) & 7L;
        values[vi++] = (block0 >>> 4) & 7L;
        values[vi++] = (block0 >>> 1) & 7L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1L) << 2) | (block1 >>> 62);
        values[vi++] = (block1 >>> 59) & 7L;
        values[vi++] = (block1 >>> 56) & 7L;
        values[vi++] = (block1 >>> 53) & 7L;
        values[vi++] = (block1 >>> 50) & 7L;
        values[vi++] = (block1 >>> 47) & 7L;
        values[vi++] = (block1 >>> 44) & 7L;
        values[vi++] = (block1 >>> 41) & 7L;
        values[vi++] = (block1 >>> 38) & 7L;
        values[vi++] = (block1 >>> 35) & 7L;
        values[vi++] = (block1 >>> 32) & 7L;
        values[vi++] = (block1 >>> 29) & 7L;
        values[vi++] = (block1 >>> 26) & 7L;
        values[vi++] = (block1 >>> 23) & 7L;
        values[vi++] = (block1 >>> 20) & 7L;
        values[vi++] = (block1 >>> 17) & 7L;
        values[vi++] = (block1 >>> 14) & 7L;
        values[vi++] = (block1 >>> 11) & 7L;
        values[vi++] = (block1 >>> 8) & 7L;
        values[vi++] = (block1 >>> 5) & 7L;
        values[vi++] = (block1 >>> 2) & 7L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 3L) << 1) | (block2 >>> 63);
        values[vi++] = (block2 >>> 60) & 7L;
        values[vi++] = (block2 >>> 57) & 7L;
        values[vi++] = (block2 >>> 54) & 7L;
        values[vi++] = (block2 >>> 51) & 7L;
        values[vi++] = (block2 >>> 48) & 7L;
        values[vi++] = (block2 >>> 45) & 7L;
        values[vi++] = (block2 >>> 42) & 7L;
        values[vi++] = (block2 >>> 39) & 7L;
        values[vi++] = (block2 >>> 36) & 7L;
        values[vi++] = (block2 >>> 33) & 7L;
        values[vi++] = (block2 >>> 30) & 7L;
        values[vi++] = (block2 >>> 27) & 7L;
        values[vi++] = (block2 >>> 24) & 7L;
        values[vi++] = (block2 >>> 21) & 7L;
        values[vi++] = (block2 >>> 18) & 7L;
        values[vi++] = (block2 >>> 15) & 7L;
        values[vi++] = (block2 >>> 12) & 7L;
        values[vi++] = (block2 >>> 9) & 7L;
        values[vi++] = (block2 >>> 6) & 7L;
        values[vi++] = (block2 >>> 3) & 7L;
        values[vi++] = block2 & 7L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 58) | (values[vi++] << 55) | (values[vi++] << 52) | (values[vi++] << 49) | (values[vi++] << 46) | (values[vi++] << 43) | (values[vi++] << 40) | (values[vi++] << 37) | (values[vi++] << 34) | (values[vi++] << 31) | (values[vi++] << 28) | (values[vi++] << 25) | (values[vi++] << 22) | (values[vi++] << 19) | (values[vi++] << 16) | (values[vi++] << 13) | (values[vi++] << 10) | (values[vi++] << 7) | (values[vi++] << 4) | (values[vi++] << 1) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 59) | (values[vi++] << 56) | (values[vi++] << 53) | (values[vi++] << 50) | (values[vi++] << 47) | (values[vi++] << 44) | (values[vi++] << 41) | (values[vi++] << 38) | (values[vi++] << 35) | (values[vi++] << 32) | (values[vi++] << 29) | (values[vi++] << 26) | (values[vi++] << 23) | (values[vi++] << 20) | (values[vi++] << 17) | (values[vi++] << 14) | (values[vi++] << 11) | (values[vi++] << 8) | (values[vi++] << 5) | (values[vi++] << 2) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 60) | (values[vi++] << 57) | (values[vi++] << 54) | (values[vi++] << 51) | (values[vi++] << 48) | (values[vi++] << 45) | (values[vi++] << 42) | (values[vi++] << 39) | (values[vi++] << 36) | (values[vi++] << 33) | (values[vi++] << 30) | (values[vi++] << 27) | (values[vi++] << 24) | (values[vi++] << 21) | (values[vi++] << 18) | (values[vi++] << 15) | (values[vi++] << 12) | (values[vi++] << 9) | (values[vi++] << 6) | (values[vi++] << 3) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation4 extends BulkOperation {

    public int blocks() {
      return 1;
    }

    public int values() {
      return 16;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 60;
        values[vi++] = (block0 >>> 56) & 15L;
        values[vi++] = (block0 >>> 52) & 15L;
        values[vi++] = (block0 >>> 48) & 15L;
        values[vi++] = (block0 >>> 44) & 15L;
        values[vi++] = (block0 >>> 40) & 15L;
        values[vi++] = (block0 >>> 36) & 15L;
        values[vi++] = (block0 >>> 32) & 15L;
        values[vi++] = (block0 >>> 28) & 15L;
        values[vi++] = (block0 >>> 24) & 15L;
        values[vi++] = (block0 >>> 20) & 15L;
        values[vi++] = (block0 >>> 16) & 15L;
        values[vi++] = (block0 >>> 12) & 15L;
        values[vi++] = (block0 >>> 8) & 15L;
        values[vi++] = (block0 >>> 4) & 15L;
        values[vi++] = block0 & 15L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 56) | (values[vi++] << 52) | (values[vi++] << 48) | (values[vi++] << 44) | (values[vi++] << 40) | (values[vi++] << 36) | (values[vi++] << 32) | (values[vi++] << 28) | (values[vi++] << 24) | (values[vi++] << 20) | (values[vi++] << 16) | (values[vi++] << 12) | (values[vi++] << 8) | (values[vi++] << 4) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation5 extends BulkOperation {

    public int blocks() {
      return 5;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 59;
        values[vi++] = (block0 >>> 54) & 31L;
        values[vi++] = (block0 >>> 49) & 31L;
        values[vi++] = (block0 >>> 44) & 31L;
        values[vi++] = (block0 >>> 39) & 31L;
        values[vi++] = (block0 >>> 34) & 31L;
        values[vi++] = (block0 >>> 29) & 31L;
        values[vi++] = (block0 >>> 24) & 31L;
        values[vi++] = (block0 >>> 19) & 31L;
        values[vi++] = (block0 >>> 14) & 31L;
        values[vi++] = (block0 >>> 9) & 31L;
        values[vi++] = (block0 >>> 4) & 31L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 15L) << 1) | (block1 >>> 63);
        values[vi++] = (block1 >>> 58) & 31L;
        values[vi++] = (block1 >>> 53) & 31L;
        values[vi++] = (block1 >>> 48) & 31L;
        values[vi++] = (block1 >>> 43) & 31L;
        values[vi++] = (block1 >>> 38) & 31L;
        values[vi++] = (block1 >>> 33) & 31L;
        values[vi++] = (block1 >>> 28) & 31L;
        values[vi++] = (block1 >>> 23) & 31L;
        values[vi++] = (block1 >>> 18) & 31L;
        values[vi++] = (block1 >>> 13) & 31L;
        values[vi++] = (block1 >>> 8) & 31L;
        values[vi++] = (block1 >>> 3) & 31L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 7L) << 2) | (block2 >>> 62);
        values[vi++] = (block2 >>> 57) & 31L;
        values[vi++] = (block2 >>> 52) & 31L;
        values[vi++] = (block2 >>> 47) & 31L;
        values[vi++] = (block2 >>> 42) & 31L;
        values[vi++] = (block2 >>> 37) & 31L;
        values[vi++] = (block2 >>> 32) & 31L;
        values[vi++] = (block2 >>> 27) & 31L;
        values[vi++] = (block2 >>> 22) & 31L;
        values[vi++] = (block2 >>> 17) & 31L;
        values[vi++] = (block2 >>> 12) & 31L;
        values[vi++] = (block2 >>> 7) & 31L;
        values[vi++] = (block2 >>> 2) & 31L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 3L) << 3) | (block3 >>> 61);
        values[vi++] = (block3 >>> 56) & 31L;
        values[vi++] = (block3 >>> 51) & 31L;
        values[vi++] = (block3 >>> 46) & 31L;
        values[vi++] = (block3 >>> 41) & 31L;
        values[vi++] = (block3 >>> 36) & 31L;
        values[vi++] = (block3 >>> 31) & 31L;
        values[vi++] = (block3 >>> 26) & 31L;
        values[vi++] = (block3 >>> 21) & 31L;
        values[vi++] = (block3 >>> 16) & 31L;
        values[vi++] = (block3 >>> 11) & 31L;
        values[vi++] = (block3 >>> 6) & 31L;
        values[vi++] = (block3 >>> 1) & 31L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 1L) << 4) | (block4 >>> 60);
        values[vi++] = (block4 >>> 55) & 31L;
        values[vi++] = (block4 >>> 50) & 31L;
        values[vi++] = (block4 >>> 45) & 31L;
        values[vi++] = (block4 >>> 40) & 31L;
        values[vi++] = (block4 >>> 35) & 31L;
        values[vi++] = (block4 >>> 30) & 31L;
        values[vi++] = (block4 >>> 25) & 31L;
        values[vi++] = (block4 >>> 20) & 31L;
        values[vi++] = (block4 >>> 15) & 31L;
        values[vi++] = (block4 >>> 10) & 31L;
        values[vi++] = (block4 >>> 5) & 31L;
        values[vi++] = block4 & 31L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 54) | (values[vi++] << 49) | (values[vi++] << 44) | (values[vi++] << 39) | (values[vi++] << 34) | (values[vi++] << 29) | (values[vi++] << 24) | (values[vi++] << 19) | (values[vi++] << 14) | (values[vi++] << 9) | (values[vi++] << 4) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 58) | (values[vi++] << 53) | (values[vi++] << 48) | (values[vi++] << 43) | (values[vi++] << 38) | (values[vi++] << 33) | (values[vi++] << 28) | (values[vi++] << 23) | (values[vi++] << 18) | (values[vi++] << 13) | (values[vi++] << 8) | (values[vi++] << 3) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 57) | (values[vi++] << 52) | (values[vi++] << 47) | (values[vi++] << 42) | (values[vi++] << 37) | (values[vi++] << 32) | (values[vi++] << 27) | (values[vi++] << 22) | (values[vi++] << 17) | (values[vi++] << 12) | (values[vi++] << 7) | (values[vi++] << 2) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 56) | (values[vi++] << 51) | (values[vi++] << 46) | (values[vi++] << 41) | (values[vi++] << 36) | (values[vi++] << 31) | (values[vi++] << 26) | (values[vi++] << 21) | (values[vi++] << 16) | (values[vi++] << 11) | (values[vi++] << 6) | (values[vi++] << 1) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 55) | (values[vi++] << 50) | (values[vi++] << 45) | (values[vi++] << 40) | (values[vi++] << 35) | (values[vi++] << 30) | (values[vi++] << 25) | (values[vi++] << 20) | (values[vi++] << 15) | (values[vi++] << 10) | (values[vi++] << 5) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation6 extends BulkOperation {

    public int blocks() {
      return 3;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 58;
        values[vi++] = (block0 >>> 52) & 63L;
        values[vi++] = (block0 >>> 46) & 63L;
        values[vi++] = (block0 >>> 40) & 63L;
        values[vi++] = (block0 >>> 34) & 63L;
        values[vi++] = (block0 >>> 28) & 63L;
        values[vi++] = (block0 >>> 22) & 63L;
        values[vi++] = (block0 >>> 16) & 63L;
        values[vi++] = (block0 >>> 10) & 63L;
        values[vi++] = (block0 >>> 4) & 63L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 15L) << 2) | (block1 >>> 62);
        values[vi++] = (block1 >>> 56) & 63L;
        values[vi++] = (block1 >>> 50) & 63L;
        values[vi++] = (block1 >>> 44) & 63L;
        values[vi++] = (block1 >>> 38) & 63L;
        values[vi++] = (block1 >>> 32) & 63L;
        values[vi++] = (block1 >>> 26) & 63L;
        values[vi++] = (block1 >>> 20) & 63L;
        values[vi++] = (block1 >>> 14) & 63L;
        values[vi++] = (block1 >>> 8) & 63L;
        values[vi++] = (block1 >>> 2) & 63L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 3L) << 4) | (block2 >>> 60);
        values[vi++] = (block2 >>> 54) & 63L;
        values[vi++] = (block2 >>> 48) & 63L;
        values[vi++] = (block2 >>> 42) & 63L;
        values[vi++] = (block2 >>> 36) & 63L;
        values[vi++] = (block2 >>> 30) & 63L;
        values[vi++] = (block2 >>> 24) & 63L;
        values[vi++] = (block2 >>> 18) & 63L;
        values[vi++] = (block2 >>> 12) & 63L;
        values[vi++] = (block2 >>> 6) & 63L;
        values[vi++] = block2 & 63L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 52) | (values[vi++] << 46) | (values[vi++] << 40) | (values[vi++] << 34) | (values[vi++] << 28) | (values[vi++] << 22) | (values[vi++] << 16) | (values[vi++] << 10) | (values[vi++] << 4) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 56) | (values[vi++] << 50) | (values[vi++] << 44) | (values[vi++] << 38) | (values[vi++] << 32) | (values[vi++] << 26) | (values[vi++] << 20) | (values[vi++] << 14) | (values[vi++] << 8) | (values[vi++] << 2) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 54) | (values[vi++] << 48) | (values[vi++] << 42) | (values[vi++] << 36) | (values[vi++] << 30) | (values[vi++] << 24) | (values[vi++] << 18) | (values[vi++] << 12) | (values[vi++] << 6) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation7 extends BulkOperation {

    public int blocks() {
      return 7;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 57;
        values[vi++] = (block0 >>> 50) & 127L;
        values[vi++] = (block0 >>> 43) & 127L;
        values[vi++] = (block0 >>> 36) & 127L;
        values[vi++] = (block0 >>> 29) & 127L;
        values[vi++] = (block0 >>> 22) & 127L;
        values[vi++] = (block0 >>> 15) & 127L;
        values[vi++] = (block0 >>> 8) & 127L;
        values[vi++] = (block0 >>> 1) & 127L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1L) << 6) | (block1 >>> 58);
        values[vi++] = (block1 >>> 51) & 127L;
        values[vi++] = (block1 >>> 44) & 127L;
        values[vi++] = (block1 >>> 37) & 127L;
        values[vi++] = (block1 >>> 30) & 127L;
        values[vi++] = (block1 >>> 23) & 127L;
        values[vi++] = (block1 >>> 16) & 127L;
        values[vi++] = (block1 >>> 9) & 127L;
        values[vi++] = (block1 >>> 2) & 127L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 3L) << 5) | (block2 >>> 59);
        values[vi++] = (block2 >>> 52) & 127L;
        values[vi++] = (block2 >>> 45) & 127L;
        values[vi++] = (block2 >>> 38) & 127L;
        values[vi++] = (block2 >>> 31) & 127L;
        values[vi++] = (block2 >>> 24) & 127L;
        values[vi++] = (block2 >>> 17) & 127L;
        values[vi++] = (block2 >>> 10) & 127L;
        values[vi++] = (block2 >>> 3) & 127L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 7L) << 4) | (block3 >>> 60);
        values[vi++] = (block3 >>> 53) & 127L;
        values[vi++] = (block3 >>> 46) & 127L;
        values[vi++] = (block3 >>> 39) & 127L;
        values[vi++] = (block3 >>> 32) & 127L;
        values[vi++] = (block3 >>> 25) & 127L;
        values[vi++] = (block3 >>> 18) & 127L;
        values[vi++] = (block3 >>> 11) & 127L;
        values[vi++] = (block3 >>> 4) & 127L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 15L) << 3) | (block4 >>> 61);
        values[vi++] = (block4 >>> 54) & 127L;
        values[vi++] = (block4 >>> 47) & 127L;
        values[vi++] = (block4 >>> 40) & 127L;
        values[vi++] = (block4 >>> 33) & 127L;
        values[vi++] = (block4 >>> 26) & 127L;
        values[vi++] = (block4 >>> 19) & 127L;
        values[vi++] = (block4 >>> 12) & 127L;
        values[vi++] = (block4 >>> 5) & 127L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 31L) << 2) | (block5 >>> 62);
        values[vi++] = (block5 >>> 55) & 127L;
        values[vi++] = (block5 >>> 48) & 127L;
        values[vi++] = (block5 >>> 41) & 127L;
        values[vi++] = (block5 >>> 34) & 127L;
        values[vi++] = (block5 >>> 27) & 127L;
        values[vi++] = (block5 >>> 20) & 127L;
        values[vi++] = (block5 >>> 13) & 127L;
        values[vi++] = (block5 >>> 6) & 127L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 63L) << 1) | (block6 >>> 63);
        values[vi++] = (block6 >>> 56) & 127L;
        values[vi++] = (block6 >>> 49) & 127L;
        values[vi++] = (block6 >>> 42) & 127L;
        values[vi++] = (block6 >>> 35) & 127L;
        values[vi++] = (block6 >>> 28) & 127L;
        values[vi++] = (block6 >>> 21) & 127L;
        values[vi++] = (block6 >>> 14) & 127L;
        values[vi++] = (block6 >>> 7) & 127L;
        values[vi++] = block6 & 127L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 50) | (values[vi++] << 43) | (values[vi++] << 36) | (values[vi++] << 29) | (values[vi++] << 22) | (values[vi++] << 15) | (values[vi++] << 8) | (values[vi++] << 1) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 51) | (values[vi++] << 44) | (values[vi++] << 37) | (values[vi++] << 30) | (values[vi++] << 23) | (values[vi++] << 16) | (values[vi++] << 9) | (values[vi++] << 2) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 52) | (values[vi++] << 45) | (values[vi++] << 38) | (values[vi++] << 31) | (values[vi++] << 24) | (values[vi++] << 17) | (values[vi++] << 10) | (values[vi++] << 3) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 53) | (values[vi++] << 46) | (values[vi++] << 39) | (values[vi++] << 32) | (values[vi++] << 25) | (values[vi++] << 18) | (values[vi++] << 11) | (values[vi++] << 4) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 54) | (values[vi++] << 47) | (values[vi++] << 40) | (values[vi++] << 33) | (values[vi++] << 26) | (values[vi++] << 19) | (values[vi++] << 12) | (values[vi++] << 5) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 55) | (values[vi++] << 48) | (values[vi++] << 41) | (values[vi++] << 34) | (values[vi++] << 27) | (values[vi++] << 20) | (values[vi++] << 13) | (values[vi++] << 6) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 56) | (values[vi++] << 49) | (values[vi++] << 42) | (values[vi++] << 35) | (values[vi++] << 28) | (values[vi++] << 21) | (values[vi++] << 14) | (values[vi++] << 7) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation8 extends BulkOperation {

    public int blocks() {
      return 1;
    }

    public int values() {
      return 8;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 56;
        values[vi++] = (block0 >>> 48) & 255L;
        values[vi++] = (block0 >>> 40) & 255L;
        values[vi++] = (block0 >>> 32) & 255L;
        values[vi++] = (block0 >>> 24) & 255L;
        values[vi++] = (block0 >>> 16) & 255L;
        values[vi++] = (block0 >>> 8) & 255L;
        values[vi++] = block0 & 255L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 48) | (values[vi++] << 40) | (values[vi++] << 32) | (values[vi++] << 24) | (values[vi++] << 16) | (values[vi++] << 8) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation9 extends BulkOperation {

    public int blocks() {
      return 9;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 55;
        values[vi++] = (block0 >>> 46) & 511L;
        values[vi++] = (block0 >>> 37) & 511L;
        values[vi++] = (block0 >>> 28) & 511L;
        values[vi++] = (block0 >>> 19) & 511L;
        values[vi++] = (block0 >>> 10) & 511L;
        values[vi++] = (block0 >>> 1) & 511L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1L) << 8) | (block1 >>> 56);
        values[vi++] = (block1 >>> 47) & 511L;
        values[vi++] = (block1 >>> 38) & 511L;
        values[vi++] = (block1 >>> 29) & 511L;
        values[vi++] = (block1 >>> 20) & 511L;
        values[vi++] = (block1 >>> 11) & 511L;
        values[vi++] = (block1 >>> 2) & 511L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 3L) << 7) | (block2 >>> 57);
        values[vi++] = (block2 >>> 48) & 511L;
        values[vi++] = (block2 >>> 39) & 511L;
        values[vi++] = (block2 >>> 30) & 511L;
        values[vi++] = (block2 >>> 21) & 511L;
        values[vi++] = (block2 >>> 12) & 511L;
        values[vi++] = (block2 >>> 3) & 511L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 7L) << 6) | (block3 >>> 58);
        values[vi++] = (block3 >>> 49) & 511L;
        values[vi++] = (block3 >>> 40) & 511L;
        values[vi++] = (block3 >>> 31) & 511L;
        values[vi++] = (block3 >>> 22) & 511L;
        values[vi++] = (block3 >>> 13) & 511L;
        values[vi++] = (block3 >>> 4) & 511L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 15L) << 5) | (block4 >>> 59);
        values[vi++] = (block4 >>> 50) & 511L;
        values[vi++] = (block4 >>> 41) & 511L;
        values[vi++] = (block4 >>> 32) & 511L;
        values[vi++] = (block4 >>> 23) & 511L;
        values[vi++] = (block4 >>> 14) & 511L;
        values[vi++] = (block4 >>> 5) & 511L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 31L) << 4) | (block5 >>> 60);
        values[vi++] = (block5 >>> 51) & 511L;
        values[vi++] = (block5 >>> 42) & 511L;
        values[vi++] = (block5 >>> 33) & 511L;
        values[vi++] = (block5 >>> 24) & 511L;
        values[vi++] = (block5 >>> 15) & 511L;
        values[vi++] = (block5 >>> 6) & 511L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 63L) << 3) | (block6 >>> 61);
        values[vi++] = (block6 >>> 52) & 511L;
        values[vi++] = (block6 >>> 43) & 511L;
        values[vi++] = (block6 >>> 34) & 511L;
        values[vi++] = (block6 >>> 25) & 511L;
        values[vi++] = (block6 >>> 16) & 511L;
        values[vi++] = (block6 >>> 7) & 511L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 127L) << 2) | (block7 >>> 62);
        values[vi++] = (block7 >>> 53) & 511L;
        values[vi++] = (block7 >>> 44) & 511L;
        values[vi++] = (block7 >>> 35) & 511L;
        values[vi++] = (block7 >>> 26) & 511L;
        values[vi++] = (block7 >>> 17) & 511L;
        values[vi++] = (block7 >>> 8) & 511L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 255L) << 1) | (block8 >>> 63);
        values[vi++] = (block8 >>> 54) & 511L;
        values[vi++] = (block8 >>> 45) & 511L;
        values[vi++] = (block8 >>> 36) & 511L;
        values[vi++] = (block8 >>> 27) & 511L;
        values[vi++] = (block8 >>> 18) & 511L;
        values[vi++] = (block8 >>> 9) & 511L;
        values[vi++] = block8 & 511L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 46) | (values[vi++] << 37) | (values[vi++] << 28) | (values[vi++] << 19) | (values[vi++] << 10) | (values[vi++] << 1) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 47) | (values[vi++] << 38) | (values[vi++] << 29) | (values[vi++] << 20) | (values[vi++] << 11) | (values[vi++] << 2) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 48) | (values[vi++] << 39) | (values[vi++] << 30) | (values[vi++] << 21) | (values[vi++] << 12) | (values[vi++] << 3) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 49) | (values[vi++] << 40) | (values[vi++] << 31) | (values[vi++] << 22) | (values[vi++] << 13) | (values[vi++] << 4) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 50) | (values[vi++] << 41) | (values[vi++] << 32) | (values[vi++] << 23) | (values[vi++] << 14) | (values[vi++] << 5) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 51) | (values[vi++] << 42) | (values[vi++] << 33) | (values[vi++] << 24) | (values[vi++] << 15) | (values[vi++] << 6) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 52) | (values[vi++] << 43) | (values[vi++] << 34) | (values[vi++] << 25) | (values[vi++] << 16) | (values[vi++] << 7) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 53) | (values[vi++] << 44) | (values[vi++] << 35) | (values[vi++] << 26) | (values[vi++] << 17) | (values[vi++] << 8) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 54) | (values[vi++] << 45) | (values[vi++] << 36) | (values[vi++] << 27) | (values[vi++] << 18) | (values[vi++] << 9) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation10 extends BulkOperation {

    public int blocks() {
      return 5;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 54;
        values[vi++] = (block0 >>> 44) & 1023L;
        values[vi++] = (block0 >>> 34) & 1023L;
        values[vi++] = (block0 >>> 24) & 1023L;
        values[vi++] = (block0 >>> 14) & 1023L;
        values[vi++] = (block0 >>> 4) & 1023L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 15L) << 6) | (block1 >>> 58);
        values[vi++] = (block1 >>> 48) & 1023L;
        values[vi++] = (block1 >>> 38) & 1023L;
        values[vi++] = (block1 >>> 28) & 1023L;
        values[vi++] = (block1 >>> 18) & 1023L;
        values[vi++] = (block1 >>> 8) & 1023L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 255L) << 2) | (block2 >>> 62);
        values[vi++] = (block2 >>> 52) & 1023L;
        values[vi++] = (block2 >>> 42) & 1023L;
        values[vi++] = (block2 >>> 32) & 1023L;
        values[vi++] = (block2 >>> 22) & 1023L;
        values[vi++] = (block2 >>> 12) & 1023L;
        values[vi++] = (block2 >>> 2) & 1023L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 3L) << 8) | (block3 >>> 56);
        values[vi++] = (block3 >>> 46) & 1023L;
        values[vi++] = (block3 >>> 36) & 1023L;
        values[vi++] = (block3 >>> 26) & 1023L;
        values[vi++] = (block3 >>> 16) & 1023L;
        values[vi++] = (block3 >>> 6) & 1023L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 63L) << 4) | (block4 >>> 60);
        values[vi++] = (block4 >>> 50) & 1023L;
        values[vi++] = (block4 >>> 40) & 1023L;
        values[vi++] = (block4 >>> 30) & 1023L;
        values[vi++] = (block4 >>> 20) & 1023L;
        values[vi++] = (block4 >>> 10) & 1023L;
        values[vi++] = block4 & 1023L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 44) | (values[vi++] << 34) | (values[vi++] << 24) | (values[vi++] << 14) | (values[vi++] << 4) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 48) | (values[vi++] << 38) | (values[vi++] << 28) | (values[vi++] << 18) | (values[vi++] << 8) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 52) | (values[vi++] << 42) | (values[vi++] << 32) | (values[vi++] << 22) | (values[vi++] << 12) | (values[vi++] << 2) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 46) | (values[vi++] << 36) | (values[vi++] << 26) | (values[vi++] << 16) | (values[vi++] << 6) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 50) | (values[vi++] << 40) | (values[vi++] << 30) | (values[vi++] << 20) | (values[vi++] << 10) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation11 extends BulkOperation {

    public int blocks() {
      return 11;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 53;
        values[vi++] = (block0 >>> 42) & 2047L;
        values[vi++] = (block0 >>> 31) & 2047L;
        values[vi++] = (block0 >>> 20) & 2047L;
        values[vi++] = (block0 >>> 9) & 2047L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 511L) << 2) | (block1 >>> 62);
        values[vi++] = (block1 >>> 51) & 2047L;
        values[vi++] = (block1 >>> 40) & 2047L;
        values[vi++] = (block1 >>> 29) & 2047L;
        values[vi++] = (block1 >>> 18) & 2047L;
        values[vi++] = (block1 >>> 7) & 2047L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 127L) << 4) | (block2 >>> 60);
        values[vi++] = (block2 >>> 49) & 2047L;
        values[vi++] = (block2 >>> 38) & 2047L;
        values[vi++] = (block2 >>> 27) & 2047L;
        values[vi++] = (block2 >>> 16) & 2047L;
        values[vi++] = (block2 >>> 5) & 2047L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 31L) << 6) | (block3 >>> 58);
        values[vi++] = (block3 >>> 47) & 2047L;
        values[vi++] = (block3 >>> 36) & 2047L;
        values[vi++] = (block3 >>> 25) & 2047L;
        values[vi++] = (block3 >>> 14) & 2047L;
        values[vi++] = (block3 >>> 3) & 2047L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 7L) << 8) | (block4 >>> 56);
        values[vi++] = (block4 >>> 45) & 2047L;
        values[vi++] = (block4 >>> 34) & 2047L;
        values[vi++] = (block4 >>> 23) & 2047L;
        values[vi++] = (block4 >>> 12) & 2047L;
        values[vi++] = (block4 >>> 1) & 2047L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1L) << 10) | (block5 >>> 54);
        values[vi++] = (block5 >>> 43) & 2047L;
        values[vi++] = (block5 >>> 32) & 2047L;
        values[vi++] = (block5 >>> 21) & 2047L;
        values[vi++] = (block5 >>> 10) & 2047L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 1023L) << 1) | (block6 >>> 63);
        values[vi++] = (block6 >>> 52) & 2047L;
        values[vi++] = (block6 >>> 41) & 2047L;
        values[vi++] = (block6 >>> 30) & 2047L;
        values[vi++] = (block6 >>> 19) & 2047L;
        values[vi++] = (block6 >>> 8) & 2047L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 255L) << 3) | (block7 >>> 61);
        values[vi++] = (block7 >>> 50) & 2047L;
        values[vi++] = (block7 >>> 39) & 2047L;
        values[vi++] = (block7 >>> 28) & 2047L;
        values[vi++] = (block7 >>> 17) & 2047L;
        values[vi++] = (block7 >>> 6) & 2047L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 63L) << 5) | (block8 >>> 59);
        values[vi++] = (block8 >>> 48) & 2047L;
        values[vi++] = (block8 >>> 37) & 2047L;
        values[vi++] = (block8 >>> 26) & 2047L;
        values[vi++] = (block8 >>> 15) & 2047L;
        values[vi++] = (block8 >>> 4) & 2047L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 15L) << 7) | (block9 >>> 57);
        values[vi++] = (block9 >>> 46) & 2047L;
        values[vi++] = (block9 >>> 35) & 2047L;
        values[vi++] = (block9 >>> 24) & 2047L;
        values[vi++] = (block9 >>> 13) & 2047L;
        values[vi++] = (block9 >>> 2) & 2047L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 3L) << 9) | (block10 >>> 55);
        values[vi++] = (block10 >>> 44) & 2047L;
        values[vi++] = (block10 >>> 33) & 2047L;
        values[vi++] = (block10 >>> 22) & 2047L;
        values[vi++] = (block10 >>> 11) & 2047L;
        values[vi++] = block10 & 2047L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 42) | (values[vi++] << 31) | (values[vi++] << 20) | (values[vi++] << 9) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 51) | (values[vi++] << 40) | (values[vi++] << 29) | (values[vi++] << 18) | (values[vi++] << 7) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 49) | (values[vi++] << 38) | (values[vi++] << 27) | (values[vi++] << 16) | (values[vi++] << 5) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 47) | (values[vi++] << 36) | (values[vi++] << 25) | (values[vi++] << 14) | (values[vi++] << 3) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 45) | (values[vi++] << 34) | (values[vi++] << 23) | (values[vi++] << 12) | (values[vi++] << 1) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 43) | (values[vi++] << 32) | (values[vi++] << 21) | (values[vi++] << 10) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 52) | (values[vi++] << 41) | (values[vi++] << 30) | (values[vi++] << 19) | (values[vi++] << 8) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 50) | (values[vi++] << 39) | (values[vi++] << 28) | (values[vi++] << 17) | (values[vi++] << 6) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 48) | (values[vi++] << 37) | (values[vi++] << 26) | (values[vi++] << 15) | (values[vi++] << 4) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 46) | (values[vi++] << 35) | (values[vi++] << 24) | (values[vi++] << 13) | (values[vi++] << 2) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 44) | (values[vi++] << 33) | (values[vi++] << 22) | (values[vi++] << 11) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation12 extends BulkOperation {

    public int blocks() {
      return 3;
    }

    public int values() {
      return 16;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 52;
        values[vi++] = (block0 >>> 40) & 4095L;
        values[vi++] = (block0 >>> 28) & 4095L;
        values[vi++] = (block0 >>> 16) & 4095L;
        values[vi++] = (block0 >>> 4) & 4095L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 15L) << 8) | (block1 >>> 56);
        values[vi++] = (block1 >>> 44) & 4095L;
        values[vi++] = (block1 >>> 32) & 4095L;
        values[vi++] = (block1 >>> 20) & 4095L;
        values[vi++] = (block1 >>> 8) & 4095L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 255L) << 4) | (block2 >>> 60);
        values[vi++] = (block2 >>> 48) & 4095L;
        values[vi++] = (block2 >>> 36) & 4095L;
        values[vi++] = (block2 >>> 24) & 4095L;
        values[vi++] = (block2 >>> 12) & 4095L;
        values[vi++] = block2 & 4095L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 40) | (values[vi++] << 28) | (values[vi++] << 16) | (values[vi++] << 4) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 44) | (values[vi++] << 32) | (values[vi++] << 20) | (values[vi++] << 8) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 48) | (values[vi++] << 36) | (values[vi++] << 24) | (values[vi++] << 12) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation13 extends BulkOperation {

    public int blocks() {
      return 13;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 51;
        values[vi++] = (block0 >>> 38) & 8191L;
        values[vi++] = (block0 >>> 25) & 8191L;
        values[vi++] = (block0 >>> 12) & 8191L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 4095L) << 1) | (block1 >>> 63);
        values[vi++] = (block1 >>> 50) & 8191L;
        values[vi++] = (block1 >>> 37) & 8191L;
        values[vi++] = (block1 >>> 24) & 8191L;
        values[vi++] = (block1 >>> 11) & 8191L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 2047L) << 2) | (block2 >>> 62);
        values[vi++] = (block2 >>> 49) & 8191L;
        values[vi++] = (block2 >>> 36) & 8191L;
        values[vi++] = (block2 >>> 23) & 8191L;
        values[vi++] = (block2 >>> 10) & 8191L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 1023L) << 3) | (block3 >>> 61);
        values[vi++] = (block3 >>> 48) & 8191L;
        values[vi++] = (block3 >>> 35) & 8191L;
        values[vi++] = (block3 >>> 22) & 8191L;
        values[vi++] = (block3 >>> 9) & 8191L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 511L) << 4) | (block4 >>> 60);
        values[vi++] = (block4 >>> 47) & 8191L;
        values[vi++] = (block4 >>> 34) & 8191L;
        values[vi++] = (block4 >>> 21) & 8191L;
        values[vi++] = (block4 >>> 8) & 8191L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 255L) << 5) | (block5 >>> 59);
        values[vi++] = (block5 >>> 46) & 8191L;
        values[vi++] = (block5 >>> 33) & 8191L;
        values[vi++] = (block5 >>> 20) & 8191L;
        values[vi++] = (block5 >>> 7) & 8191L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 127L) << 6) | (block6 >>> 58);
        values[vi++] = (block6 >>> 45) & 8191L;
        values[vi++] = (block6 >>> 32) & 8191L;
        values[vi++] = (block6 >>> 19) & 8191L;
        values[vi++] = (block6 >>> 6) & 8191L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 63L) << 7) | (block7 >>> 57);
        values[vi++] = (block7 >>> 44) & 8191L;
        values[vi++] = (block7 >>> 31) & 8191L;
        values[vi++] = (block7 >>> 18) & 8191L;
        values[vi++] = (block7 >>> 5) & 8191L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 31L) << 8) | (block8 >>> 56);
        values[vi++] = (block8 >>> 43) & 8191L;
        values[vi++] = (block8 >>> 30) & 8191L;
        values[vi++] = (block8 >>> 17) & 8191L;
        values[vi++] = (block8 >>> 4) & 8191L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 15L) << 9) | (block9 >>> 55);
        values[vi++] = (block9 >>> 42) & 8191L;
        values[vi++] = (block9 >>> 29) & 8191L;
        values[vi++] = (block9 >>> 16) & 8191L;
        values[vi++] = (block9 >>> 3) & 8191L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 7L) << 10) | (block10 >>> 54);
        values[vi++] = (block10 >>> 41) & 8191L;
        values[vi++] = (block10 >>> 28) & 8191L;
        values[vi++] = (block10 >>> 15) & 8191L;
        values[vi++] = (block10 >>> 2) & 8191L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 3L) << 11) | (block11 >>> 53);
        values[vi++] = (block11 >>> 40) & 8191L;
        values[vi++] = (block11 >>> 27) & 8191L;
        values[vi++] = (block11 >>> 14) & 8191L;
        values[vi++] = (block11 >>> 1) & 8191L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 1L) << 12) | (block12 >>> 52);
        values[vi++] = (block12 >>> 39) & 8191L;
        values[vi++] = (block12 >>> 26) & 8191L;
        values[vi++] = (block12 >>> 13) & 8191L;
        values[vi++] = block12 & 8191L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 38) | (values[vi++] << 25) | (values[vi++] << 12) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 50) | (values[vi++] << 37) | (values[vi++] << 24) | (values[vi++] << 11) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 49) | (values[vi++] << 36) | (values[vi++] << 23) | (values[vi++] << 10) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 48) | (values[vi++] << 35) | (values[vi++] << 22) | (values[vi++] << 9) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 47) | (values[vi++] << 34) | (values[vi++] << 21) | (values[vi++] << 8) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 46) | (values[vi++] << 33) | (values[vi++] << 20) | (values[vi++] << 7) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 45) | (values[vi++] << 32) | (values[vi++] << 19) | (values[vi++] << 6) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 44) | (values[vi++] << 31) | (values[vi++] << 18) | (values[vi++] << 5) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 43) | (values[vi++] << 30) | (values[vi++] << 17) | (values[vi++] << 4) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 42) | (values[vi++] << 29) | (values[vi++] << 16) | (values[vi++] << 3) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 41) | (values[vi++] << 28) | (values[vi++] << 15) | (values[vi++] << 2) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 40) | (values[vi++] << 27) | (values[vi++] << 14) | (values[vi++] << 1) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 39) | (values[vi++] << 26) | (values[vi++] << 13) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation14 extends BulkOperation {

    public int blocks() {
      return 7;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 50;
        values[vi++] = (block0 >>> 36) & 16383L;
        values[vi++] = (block0 >>> 22) & 16383L;
        values[vi++] = (block0 >>> 8) & 16383L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 255L) << 6) | (block1 >>> 58);
        values[vi++] = (block1 >>> 44) & 16383L;
        values[vi++] = (block1 >>> 30) & 16383L;
        values[vi++] = (block1 >>> 16) & 16383L;
        values[vi++] = (block1 >>> 2) & 16383L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 3L) << 12) | (block2 >>> 52);
        values[vi++] = (block2 >>> 38) & 16383L;
        values[vi++] = (block2 >>> 24) & 16383L;
        values[vi++] = (block2 >>> 10) & 16383L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 1023L) << 4) | (block3 >>> 60);
        values[vi++] = (block3 >>> 46) & 16383L;
        values[vi++] = (block3 >>> 32) & 16383L;
        values[vi++] = (block3 >>> 18) & 16383L;
        values[vi++] = (block3 >>> 4) & 16383L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 15L) << 10) | (block4 >>> 54);
        values[vi++] = (block4 >>> 40) & 16383L;
        values[vi++] = (block4 >>> 26) & 16383L;
        values[vi++] = (block4 >>> 12) & 16383L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 4095L) << 2) | (block5 >>> 62);
        values[vi++] = (block5 >>> 48) & 16383L;
        values[vi++] = (block5 >>> 34) & 16383L;
        values[vi++] = (block5 >>> 20) & 16383L;
        values[vi++] = (block5 >>> 6) & 16383L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 63L) << 8) | (block6 >>> 56);
        values[vi++] = (block6 >>> 42) & 16383L;
        values[vi++] = (block6 >>> 28) & 16383L;
        values[vi++] = (block6 >>> 14) & 16383L;
        values[vi++] = block6 & 16383L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 36) | (values[vi++] << 22) | (values[vi++] << 8) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 44) | (values[vi++] << 30) | (values[vi++] << 16) | (values[vi++] << 2) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 38) | (values[vi++] << 24) | (values[vi++] << 10) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 46) | (values[vi++] << 32) | (values[vi++] << 18) | (values[vi++] << 4) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 40) | (values[vi++] << 26) | (values[vi++] << 12) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 48) | (values[vi++] << 34) | (values[vi++] << 20) | (values[vi++] << 6) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 42) | (values[vi++] << 28) | (values[vi++] << 14) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation15 extends BulkOperation {

    public int blocks() {
      return 15;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 49;
        values[vi++] = (block0 >>> 34) & 32767L;
        values[vi++] = (block0 >>> 19) & 32767L;
        values[vi++] = (block0 >>> 4) & 32767L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 15L) << 11) | (block1 >>> 53);
        values[vi++] = (block1 >>> 38) & 32767L;
        values[vi++] = (block1 >>> 23) & 32767L;
        values[vi++] = (block1 >>> 8) & 32767L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 255L) << 7) | (block2 >>> 57);
        values[vi++] = (block2 >>> 42) & 32767L;
        values[vi++] = (block2 >>> 27) & 32767L;
        values[vi++] = (block2 >>> 12) & 32767L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 4095L) << 3) | (block3 >>> 61);
        values[vi++] = (block3 >>> 46) & 32767L;
        values[vi++] = (block3 >>> 31) & 32767L;
        values[vi++] = (block3 >>> 16) & 32767L;
        values[vi++] = (block3 >>> 1) & 32767L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 1L) << 14) | (block4 >>> 50);
        values[vi++] = (block4 >>> 35) & 32767L;
        values[vi++] = (block4 >>> 20) & 32767L;
        values[vi++] = (block4 >>> 5) & 32767L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 31L) << 10) | (block5 >>> 54);
        values[vi++] = (block5 >>> 39) & 32767L;
        values[vi++] = (block5 >>> 24) & 32767L;
        values[vi++] = (block5 >>> 9) & 32767L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 511L) << 6) | (block6 >>> 58);
        values[vi++] = (block6 >>> 43) & 32767L;
        values[vi++] = (block6 >>> 28) & 32767L;
        values[vi++] = (block6 >>> 13) & 32767L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 8191L) << 2) | (block7 >>> 62);
        values[vi++] = (block7 >>> 47) & 32767L;
        values[vi++] = (block7 >>> 32) & 32767L;
        values[vi++] = (block7 >>> 17) & 32767L;
        values[vi++] = (block7 >>> 2) & 32767L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 3L) << 13) | (block8 >>> 51);
        values[vi++] = (block8 >>> 36) & 32767L;
        values[vi++] = (block8 >>> 21) & 32767L;
        values[vi++] = (block8 >>> 6) & 32767L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 63L) << 9) | (block9 >>> 55);
        values[vi++] = (block9 >>> 40) & 32767L;
        values[vi++] = (block9 >>> 25) & 32767L;
        values[vi++] = (block9 >>> 10) & 32767L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1023L) << 5) | (block10 >>> 59);
        values[vi++] = (block10 >>> 44) & 32767L;
        values[vi++] = (block10 >>> 29) & 32767L;
        values[vi++] = (block10 >>> 14) & 32767L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 16383L) << 1) | (block11 >>> 63);
        values[vi++] = (block11 >>> 48) & 32767L;
        values[vi++] = (block11 >>> 33) & 32767L;
        values[vi++] = (block11 >>> 18) & 32767L;
        values[vi++] = (block11 >>> 3) & 32767L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 7L) << 12) | (block12 >>> 52);
        values[vi++] = (block12 >>> 37) & 32767L;
        values[vi++] = (block12 >>> 22) & 32767L;
        values[vi++] = (block12 >>> 7) & 32767L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 127L) << 8) | (block13 >>> 56);
        values[vi++] = (block13 >>> 41) & 32767L;
        values[vi++] = (block13 >>> 26) & 32767L;
        values[vi++] = (block13 >>> 11) & 32767L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 2047L) << 4) | (block14 >>> 60);
        values[vi++] = (block14 >>> 45) & 32767L;
        values[vi++] = (block14 >>> 30) & 32767L;
        values[vi++] = (block14 >>> 15) & 32767L;
        values[vi++] = block14 & 32767L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 34) | (values[vi++] << 19) | (values[vi++] << 4) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 38) | (values[vi++] << 23) | (values[vi++] << 8) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 42) | (values[vi++] << 27) | (values[vi++] << 12) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 46) | (values[vi++] << 31) | (values[vi++] << 16) | (values[vi++] << 1) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 35) | (values[vi++] << 20) | (values[vi++] << 5) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 39) | (values[vi++] << 24) | (values[vi++] << 9) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 43) | (values[vi++] << 28) | (values[vi++] << 13) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 47) | (values[vi++] << 32) | (values[vi++] << 17) | (values[vi++] << 2) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 36) | (values[vi++] << 21) | (values[vi++] << 6) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 40) | (values[vi++] << 25) | (values[vi++] << 10) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 44) | (values[vi++] << 29) | (values[vi++] << 14) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 48) | (values[vi++] << 33) | (values[vi++] << 18) | (values[vi++] << 3) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 37) | (values[vi++] << 22) | (values[vi++] << 7) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 41) | (values[vi++] << 26) | (values[vi++] << 11) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 45) | (values[vi++] << 30) | (values[vi++] << 15) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation16 extends BulkOperation {

    public int blocks() {
      return 1;
    }

    public int values() {
      return 4;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 48;
        values[vi++] = (block0 >>> 32) & 65535L;
        values[vi++] = (block0 >>> 16) & 65535L;
        values[vi++] = block0 & 65535L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 32) | (values[vi++] << 16) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation17 extends BulkOperation {

    public int blocks() {
      return 17;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 47;
        values[vi++] = (block0 >>> 30) & 131071L;
        values[vi++] = (block0 >>> 13) & 131071L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 8191L) << 4) | (block1 >>> 60);
        values[vi++] = (block1 >>> 43) & 131071L;
        values[vi++] = (block1 >>> 26) & 131071L;
        values[vi++] = (block1 >>> 9) & 131071L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 511L) << 8) | (block2 >>> 56);
        values[vi++] = (block2 >>> 39) & 131071L;
        values[vi++] = (block2 >>> 22) & 131071L;
        values[vi++] = (block2 >>> 5) & 131071L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 31L) << 12) | (block3 >>> 52);
        values[vi++] = (block3 >>> 35) & 131071L;
        values[vi++] = (block3 >>> 18) & 131071L;
        values[vi++] = (block3 >>> 1) & 131071L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 1L) << 16) | (block4 >>> 48);
        values[vi++] = (block4 >>> 31) & 131071L;
        values[vi++] = (block4 >>> 14) & 131071L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 16383L) << 3) | (block5 >>> 61);
        values[vi++] = (block5 >>> 44) & 131071L;
        values[vi++] = (block5 >>> 27) & 131071L;
        values[vi++] = (block5 >>> 10) & 131071L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 1023L) << 7) | (block6 >>> 57);
        values[vi++] = (block6 >>> 40) & 131071L;
        values[vi++] = (block6 >>> 23) & 131071L;
        values[vi++] = (block6 >>> 6) & 131071L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 63L) << 11) | (block7 >>> 53);
        values[vi++] = (block7 >>> 36) & 131071L;
        values[vi++] = (block7 >>> 19) & 131071L;
        values[vi++] = (block7 >>> 2) & 131071L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 3L) << 15) | (block8 >>> 49);
        values[vi++] = (block8 >>> 32) & 131071L;
        values[vi++] = (block8 >>> 15) & 131071L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 32767L) << 2) | (block9 >>> 62);
        values[vi++] = (block9 >>> 45) & 131071L;
        values[vi++] = (block9 >>> 28) & 131071L;
        values[vi++] = (block9 >>> 11) & 131071L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 2047L) << 6) | (block10 >>> 58);
        values[vi++] = (block10 >>> 41) & 131071L;
        values[vi++] = (block10 >>> 24) & 131071L;
        values[vi++] = (block10 >>> 7) & 131071L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 127L) << 10) | (block11 >>> 54);
        values[vi++] = (block11 >>> 37) & 131071L;
        values[vi++] = (block11 >>> 20) & 131071L;
        values[vi++] = (block11 >>> 3) & 131071L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 7L) << 14) | (block12 >>> 50);
        values[vi++] = (block12 >>> 33) & 131071L;
        values[vi++] = (block12 >>> 16) & 131071L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 65535L) << 1) | (block13 >>> 63);
        values[vi++] = (block13 >>> 46) & 131071L;
        values[vi++] = (block13 >>> 29) & 131071L;
        values[vi++] = (block13 >>> 12) & 131071L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 4095L) << 5) | (block14 >>> 59);
        values[vi++] = (block14 >>> 42) & 131071L;
        values[vi++] = (block14 >>> 25) & 131071L;
        values[vi++] = (block14 >>> 8) & 131071L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 255L) << 9) | (block15 >>> 55);
        values[vi++] = (block15 >>> 38) & 131071L;
        values[vi++] = (block15 >>> 21) & 131071L;
        values[vi++] = (block15 >>> 4) & 131071L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 15L) << 13) | (block16 >>> 51);
        values[vi++] = (block16 >>> 34) & 131071L;
        values[vi++] = (block16 >>> 17) & 131071L;
        values[vi++] = block16 & 131071L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 30) | (values[vi++] << 13) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 43) | (values[vi++] << 26) | (values[vi++] << 9) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 39) | (values[vi++] << 22) | (values[vi++] << 5) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 35) | (values[vi++] << 18) | (values[vi++] << 1) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 31) | (values[vi++] << 14) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 44) | (values[vi++] << 27) | (values[vi++] << 10) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 40) | (values[vi++] << 23) | (values[vi++] << 6) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 36) | (values[vi++] << 19) | (values[vi++] << 2) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 32) | (values[vi++] << 15) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 45) | (values[vi++] << 28) | (values[vi++] << 11) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 41) | (values[vi++] << 24) | (values[vi++] << 7) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 37) | (values[vi++] << 20) | (values[vi++] << 3) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 33) | (values[vi++] << 16) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 46) | (values[vi++] << 29) | (values[vi++] << 12) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 42) | (values[vi++] << 25) | (values[vi++] << 8) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 38) | (values[vi++] << 21) | (values[vi++] << 4) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 34) | (values[vi++] << 17) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation18 extends BulkOperation {

    public int blocks() {
      return 9;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 46;
        values[vi++] = (block0 >>> 28) & 262143L;
        values[vi++] = (block0 >>> 10) & 262143L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1023L) << 8) | (block1 >>> 56);
        values[vi++] = (block1 >>> 38) & 262143L;
        values[vi++] = (block1 >>> 20) & 262143L;
        values[vi++] = (block1 >>> 2) & 262143L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 3L) << 16) | (block2 >>> 48);
        values[vi++] = (block2 >>> 30) & 262143L;
        values[vi++] = (block2 >>> 12) & 262143L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 4095L) << 6) | (block3 >>> 58);
        values[vi++] = (block3 >>> 40) & 262143L;
        values[vi++] = (block3 >>> 22) & 262143L;
        values[vi++] = (block3 >>> 4) & 262143L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 15L) << 14) | (block4 >>> 50);
        values[vi++] = (block4 >>> 32) & 262143L;
        values[vi++] = (block4 >>> 14) & 262143L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 16383L) << 4) | (block5 >>> 60);
        values[vi++] = (block5 >>> 42) & 262143L;
        values[vi++] = (block5 >>> 24) & 262143L;
        values[vi++] = (block5 >>> 6) & 262143L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 63L) << 12) | (block6 >>> 52);
        values[vi++] = (block6 >>> 34) & 262143L;
        values[vi++] = (block6 >>> 16) & 262143L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 65535L) << 2) | (block7 >>> 62);
        values[vi++] = (block7 >>> 44) & 262143L;
        values[vi++] = (block7 >>> 26) & 262143L;
        values[vi++] = (block7 >>> 8) & 262143L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 255L) << 10) | (block8 >>> 54);
        values[vi++] = (block8 >>> 36) & 262143L;
        values[vi++] = (block8 >>> 18) & 262143L;
        values[vi++] = block8 & 262143L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 28) | (values[vi++] << 10) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 38) | (values[vi++] << 20) | (values[vi++] << 2) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 30) | (values[vi++] << 12) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 40) | (values[vi++] << 22) | (values[vi++] << 4) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 32) | (values[vi++] << 14) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 42) | (values[vi++] << 24) | (values[vi++] << 6) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 34) | (values[vi++] << 16) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 44) | (values[vi++] << 26) | (values[vi++] << 8) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 36) | (values[vi++] << 18) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation19 extends BulkOperation {

    public int blocks() {
      return 19;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 45;
        values[vi++] = (block0 >>> 26) & 524287L;
        values[vi++] = (block0 >>> 7) & 524287L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 127L) << 12) | (block1 >>> 52);
        values[vi++] = (block1 >>> 33) & 524287L;
        values[vi++] = (block1 >>> 14) & 524287L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 16383L) << 5) | (block2 >>> 59);
        values[vi++] = (block2 >>> 40) & 524287L;
        values[vi++] = (block2 >>> 21) & 524287L;
        values[vi++] = (block2 >>> 2) & 524287L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 3L) << 17) | (block3 >>> 47);
        values[vi++] = (block3 >>> 28) & 524287L;
        values[vi++] = (block3 >>> 9) & 524287L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 511L) << 10) | (block4 >>> 54);
        values[vi++] = (block4 >>> 35) & 524287L;
        values[vi++] = (block4 >>> 16) & 524287L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 65535L) << 3) | (block5 >>> 61);
        values[vi++] = (block5 >>> 42) & 524287L;
        values[vi++] = (block5 >>> 23) & 524287L;
        values[vi++] = (block5 >>> 4) & 524287L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 15L) << 15) | (block6 >>> 49);
        values[vi++] = (block6 >>> 30) & 524287L;
        values[vi++] = (block6 >>> 11) & 524287L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 2047L) << 8) | (block7 >>> 56);
        values[vi++] = (block7 >>> 37) & 524287L;
        values[vi++] = (block7 >>> 18) & 524287L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 262143L) << 1) | (block8 >>> 63);
        values[vi++] = (block8 >>> 44) & 524287L;
        values[vi++] = (block8 >>> 25) & 524287L;
        values[vi++] = (block8 >>> 6) & 524287L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 63L) << 13) | (block9 >>> 51);
        values[vi++] = (block9 >>> 32) & 524287L;
        values[vi++] = (block9 >>> 13) & 524287L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 8191L) << 6) | (block10 >>> 58);
        values[vi++] = (block10 >>> 39) & 524287L;
        values[vi++] = (block10 >>> 20) & 524287L;
        values[vi++] = (block10 >>> 1) & 524287L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 1L) << 18) | (block11 >>> 46);
        values[vi++] = (block11 >>> 27) & 524287L;
        values[vi++] = (block11 >>> 8) & 524287L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 255L) << 11) | (block12 >>> 53);
        values[vi++] = (block12 >>> 34) & 524287L;
        values[vi++] = (block12 >>> 15) & 524287L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 32767L) << 4) | (block13 >>> 60);
        values[vi++] = (block13 >>> 41) & 524287L;
        values[vi++] = (block13 >>> 22) & 524287L;
        values[vi++] = (block13 >>> 3) & 524287L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 7L) << 16) | (block14 >>> 48);
        values[vi++] = (block14 >>> 29) & 524287L;
        values[vi++] = (block14 >>> 10) & 524287L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 1023L) << 9) | (block15 >>> 55);
        values[vi++] = (block15 >>> 36) & 524287L;
        values[vi++] = (block15 >>> 17) & 524287L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 131071L) << 2) | (block16 >>> 62);
        values[vi++] = (block16 >>> 43) & 524287L;
        values[vi++] = (block16 >>> 24) & 524287L;
        values[vi++] = (block16 >>> 5) & 524287L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 31L) << 14) | (block17 >>> 50);
        values[vi++] = (block17 >>> 31) & 524287L;
        values[vi++] = (block17 >>> 12) & 524287L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 4095L) << 7) | (block18 >>> 57);
        values[vi++] = (block18 >>> 38) & 524287L;
        values[vi++] = (block18 >>> 19) & 524287L;
        values[vi++] = block18 & 524287L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 26) | (values[vi++] << 7) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 33) | (values[vi++] << 14) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 40) | (values[vi++] << 21) | (values[vi++] << 2) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 28) | (values[vi++] << 9) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 35) | (values[vi++] << 16) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 42) | (values[vi++] << 23) | (values[vi++] << 4) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 30) | (values[vi++] << 11) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 37) | (values[vi++] << 18) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 44) | (values[vi++] << 25) | (values[vi++] << 6) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 32) | (values[vi++] << 13) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 39) | (values[vi++] << 20) | (values[vi++] << 1) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 27) | (values[vi++] << 8) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 34) | (values[vi++] << 15) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 41) | (values[vi++] << 22) | (values[vi++] << 3) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 29) | (values[vi++] << 10) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 36) | (values[vi++] << 17) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 43) | (values[vi++] << 24) | (values[vi++] << 5) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 31) | (values[vi++] << 12) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 38) | (values[vi++] << 19) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation20 extends BulkOperation {

    public int blocks() {
      return 5;
    }

    public int values() {
      return 16;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 44;
        values[vi++] = (block0 >>> 24) & 1048575L;
        values[vi++] = (block0 >>> 4) & 1048575L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 15L) << 16) | (block1 >>> 48);
        values[vi++] = (block1 >>> 28) & 1048575L;
        values[vi++] = (block1 >>> 8) & 1048575L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 255L) << 12) | (block2 >>> 52);
        values[vi++] = (block2 >>> 32) & 1048575L;
        values[vi++] = (block2 >>> 12) & 1048575L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 4095L) << 8) | (block3 >>> 56);
        values[vi++] = (block3 >>> 36) & 1048575L;
        values[vi++] = (block3 >>> 16) & 1048575L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 65535L) << 4) | (block4 >>> 60);
        values[vi++] = (block4 >>> 40) & 1048575L;
        values[vi++] = (block4 >>> 20) & 1048575L;
        values[vi++] = block4 & 1048575L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 24) | (values[vi++] << 4) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 28) | (values[vi++] << 8) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 32) | (values[vi++] << 12) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 36) | (values[vi++] << 16) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 40) | (values[vi++] << 20) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation21 extends BulkOperation {

    public int blocks() {
      return 21;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 43;
        values[vi++] = (block0 >>> 22) & 2097151L;
        values[vi++] = (block0 >>> 1) & 2097151L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1L) << 20) | (block1 >>> 44);
        values[vi++] = (block1 >>> 23) & 2097151L;
        values[vi++] = (block1 >>> 2) & 2097151L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 3L) << 19) | (block2 >>> 45);
        values[vi++] = (block2 >>> 24) & 2097151L;
        values[vi++] = (block2 >>> 3) & 2097151L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 7L) << 18) | (block3 >>> 46);
        values[vi++] = (block3 >>> 25) & 2097151L;
        values[vi++] = (block3 >>> 4) & 2097151L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 15L) << 17) | (block4 >>> 47);
        values[vi++] = (block4 >>> 26) & 2097151L;
        values[vi++] = (block4 >>> 5) & 2097151L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 31L) << 16) | (block5 >>> 48);
        values[vi++] = (block5 >>> 27) & 2097151L;
        values[vi++] = (block5 >>> 6) & 2097151L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 63L) << 15) | (block6 >>> 49);
        values[vi++] = (block6 >>> 28) & 2097151L;
        values[vi++] = (block6 >>> 7) & 2097151L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 127L) << 14) | (block7 >>> 50);
        values[vi++] = (block7 >>> 29) & 2097151L;
        values[vi++] = (block7 >>> 8) & 2097151L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 255L) << 13) | (block8 >>> 51);
        values[vi++] = (block8 >>> 30) & 2097151L;
        values[vi++] = (block8 >>> 9) & 2097151L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 511L) << 12) | (block9 >>> 52);
        values[vi++] = (block9 >>> 31) & 2097151L;
        values[vi++] = (block9 >>> 10) & 2097151L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1023L) << 11) | (block10 >>> 53);
        values[vi++] = (block10 >>> 32) & 2097151L;
        values[vi++] = (block10 >>> 11) & 2097151L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 2047L) << 10) | (block11 >>> 54);
        values[vi++] = (block11 >>> 33) & 2097151L;
        values[vi++] = (block11 >>> 12) & 2097151L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 4095L) << 9) | (block12 >>> 55);
        values[vi++] = (block12 >>> 34) & 2097151L;
        values[vi++] = (block12 >>> 13) & 2097151L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 8191L) << 8) | (block13 >>> 56);
        values[vi++] = (block13 >>> 35) & 2097151L;
        values[vi++] = (block13 >>> 14) & 2097151L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 16383L) << 7) | (block14 >>> 57);
        values[vi++] = (block14 >>> 36) & 2097151L;
        values[vi++] = (block14 >>> 15) & 2097151L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 32767L) << 6) | (block15 >>> 58);
        values[vi++] = (block15 >>> 37) & 2097151L;
        values[vi++] = (block15 >>> 16) & 2097151L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 65535L) << 5) | (block16 >>> 59);
        values[vi++] = (block16 >>> 38) & 2097151L;
        values[vi++] = (block16 >>> 17) & 2097151L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 131071L) << 4) | (block17 >>> 60);
        values[vi++] = (block17 >>> 39) & 2097151L;
        values[vi++] = (block17 >>> 18) & 2097151L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 262143L) << 3) | (block18 >>> 61);
        values[vi++] = (block18 >>> 40) & 2097151L;
        values[vi++] = (block18 >>> 19) & 2097151L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 524287L) << 2) | (block19 >>> 62);
        values[vi++] = (block19 >>> 41) & 2097151L;
        values[vi++] = (block19 >>> 20) & 2097151L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 1048575L) << 1) | (block20 >>> 63);
        values[vi++] = (block20 >>> 42) & 2097151L;
        values[vi++] = (block20 >>> 21) & 2097151L;
        values[vi++] = block20 & 2097151L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 22) | (values[vi++] << 1) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 23) | (values[vi++] << 2) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 24) | (values[vi++] << 3) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 25) | (values[vi++] << 4) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 26) | (values[vi++] << 5) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 27) | (values[vi++] << 6) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 28) | (values[vi++] << 7) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 29) | (values[vi++] << 8) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 30) | (values[vi++] << 9) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 31) | (values[vi++] << 10) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 32) | (values[vi++] << 11) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 33) | (values[vi++] << 12) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 34) | (values[vi++] << 13) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 35) | (values[vi++] << 14) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 36) | (values[vi++] << 15) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 37) | (values[vi++] << 16) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 38) | (values[vi++] << 17) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 39) | (values[vi++] << 18) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 40) | (values[vi++] << 19) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 41) | (values[vi++] << 20) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 42) | (values[vi++] << 21) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation22 extends BulkOperation {

    public int blocks() {
      return 11;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 42;
        values[vi++] = (block0 >>> 20) & 4194303L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1048575L) << 2) | (block1 >>> 62);
        values[vi++] = (block1 >>> 40) & 4194303L;
        values[vi++] = (block1 >>> 18) & 4194303L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 262143L) << 4) | (block2 >>> 60);
        values[vi++] = (block2 >>> 38) & 4194303L;
        values[vi++] = (block2 >>> 16) & 4194303L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 65535L) << 6) | (block3 >>> 58);
        values[vi++] = (block3 >>> 36) & 4194303L;
        values[vi++] = (block3 >>> 14) & 4194303L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 16383L) << 8) | (block4 >>> 56);
        values[vi++] = (block4 >>> 34) & 4194303L;
        values[vi++] = (block4 >>> 12) & 4194303L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 4095L) << 10) | (block5 >>> 54);
        values[vi++] = (block5 >>> 32) & 4194303L;
        values[vi++] = (block5 >>> 10) & 4194303L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 1023L) << 12) | (block6 >>> 52);
        values[vi++] = (block6 >>> 30) & 4194303L;
        values[vi++] = (block6 >>> 8) & 4194303L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 255L) << 14) | (block7 >>> 50);
        values[vi++] = (block7 >>> 28) & 4194303L;
        values[vi++] = (block7 >>> 6) & 4194303L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 63L) << 16) | (block8 >>> 48);
        values[vi++] = (block8 >>> 26) & 4194303L;
        values[vi++] = (block8 >>> 4) & 4194303L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 15L) << 18) | (block9 >>> 46);
        values[vi++] = (block9 >>> 24) & 4194303L;
        values[vi++] = (block9 >>> 2) & 4194303L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 3L) << 20) | (block10 >>> 44);
        values[vi++] = (block10 >>> 22) & 4194303L;
        values[vi++] = block10 & 4194303L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 20) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 40) | (values[vi++] << 18) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 38) | (values[vi++] << 16) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 36) | (values[vi++] << 14) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 34) | (values[vi++] << 12) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 32) | (values[vi++] << 10) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 30) | (values[vi++] << 8) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 28) | (values[vi++] << 6) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 26) | (values[vi++] << 4) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 24) | (values[vi++] << 2) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 22) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation23 extends BulkOperation {

    public int blocks() {
      return 23;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 41;
        values[vi++] = (block0 >>> 18) & 8388607L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 262143L) << 5) | (block1 >>> 59);
        values[vi++] = (block1 >>> 36) & 8388607L;
        values[vi++] = (block1 >>> 13) & 8388607L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 8191L) << 10) | (block2 >>> 54);
        values[vi++] = (block2 >>> 31) & 8388607L;
        values[vi++] = (block2 >>> 8) & 8388607L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 255L) << 15) | (block3 >>> 49);
        values[vi++] = (block3 >>> 26) & 8388607L;
        values[vi++] = (block3 >>> 3) & 8388607L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 7L) << 20) | (block4 >>> 44);
        values[vi++] = (block4 >>> 21) & 8388607L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 2097151L) << 2) | (block5 >>> 62);
        values[vi++] = (block5 >>> 39) & 8388607L;
        values[vi++] = (block5 >>> 16) & 8388607L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 65535L) << 7) | (block6 >>> 57);
        values[vi++] = (block6 >>> 34) & 8388607L;
        values[vi++] = (block6 >>> 11) & 8388607L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 2047L) << 12) | (block7 >>> 52);
        values[vi++] = (block7 >>> 29) & 8388607L;
        values[vi++] = (block7 >>> 6) & 8388607L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 63L) << 17) | (block8 >>> 47);
        values[vi++] = (block8 >>> 24) & 8388607L;
        values[vi++] = (block8 >>> 1) & 8388607L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 1L) << 22) | (block9 >>> 42);
        values[vi++] = (block9 >>> 19) & 8388607L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 524287L) << 4) | (block10 >>> 60);
        values[vi++] = (block10 >>> 37) & 8388607L;
        values[vi++] = (block10 >>> 14) & 8388607L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 16383L) << 9) | (block11 >>> 55);
        values[vi++] = (block11 >>> 32) & 8388607L;
        values[vi++] = (block11 >>> 9) & 8388607L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 511L) << 14) | (block12 >>> 50);
        values[vi++] = (block12 >>> 27) & 8388607L;
        values[vi++] = (block12 >>> 4) & 8388607L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 15L) << 19) | (block13 >>> 45);
        values[vi++] = (block13 >>> 22) & 8388607L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 4194303L) << 1) | (block14 >>> 63);
        values[vi++] = (block14 >>> 40) & 8388607L;
        values[vi++] = (block14 >>> 17) & 8388607L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 131071L) << 6) | (block15 >>> 58);
        values[vi++] = (block15 >>> 35) & 8388607L;
        values[vi++] = (block15 >>> 12) & 8388607L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 4095L) << 11) | (block16 >>> 53);
        values[vi++] = (block16 >>> 30) & 8388607L;
        values[vi++] = (block16 >>> 7) & 8388607L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 127L) << 16) | (block17 >>> 48);
        values[vi++] = (block17 >>> 25) & 8388607L;
        values[vi++] = (block17 >>> 2) & 8388607L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 3L) << 21) | (block18 >>> 43);
        values[vi++] = (block18 >>> 20) & 8388607L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 1048575L) << 3) | (block19 >>> 61);
        values[vi++] = (block19 >>> 38) & 8388607L;
        values[vi++] = (block19 >>> 15) & 8388607L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 32767L) << 8) | (block20 >>> 56);
        values[vi++] = (block20 >>> 33) & 8388607L;
        values[vi++] = (block20 >>> 10) & 8388607L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 1023L) << 13) | (block21 >>> 51);
        values[vi++] = (block21 >>> 28) & 8388607L;
        values[vi++] = (block21 >>> 5) & 8388607L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 31L) << 18) | (block22 >>> 46);
        values[vi++] = (block22 >>> 23) & 8388607L;
        values[vi++] = block22 & 8388607L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 41) | (values[vi++] << 18) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 36) | (values[vi++] << 13) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 31) | (values[vi++] << 8) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 26) | (values[vi++] << 3) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 21) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 39) | (values[vi++] << 16) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 34) | (values[vi++] << 11) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 29) | (values[vi++] << 6) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 24) | (values[vi++] << 1) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 19) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 37) | (values[vi++] << 14) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 32) | (values[vi++] << 9) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 27) | (values[vi++] << 4) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 22) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 40) | (values[vi++] << 17) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 35) | (values[vi++] << 12) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 30) | (values[vi++] << 7) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 25) | (values[vi++] << 2) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 20) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 38) | (values[vi++] << 15) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 33) | (values[vi++] << 10) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 28) | (values[vi++] << 5) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 23) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation24 extends BulkOperation {

    public int blocks() {
      return 3;
    }

    public int values() {
      return 8;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 40;
        values[vi++] = (block0 >>> 16) & 16777215L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 65535L) << 8) | (block1 >>> 56);
        values[vi++] = (block1 >>> 32) & 16777215L;
        values[vi++] = (block1 >>> 8) & 16777215L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 255L) << 16) | (block2 >>> 48);
        values[vi++] = (block2 >>> 24) & 16777215L;
        values[vi++] = block2 & 16777215L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 16) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 32) | (values[vi++] << 8) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 24) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation25 extends BulkOperation {

    public int blocks() {
      return 25;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 39;
        values[vi++] = (block0 >>> 14) & 33554431L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 16383L) << 11) | (block1 >>> 53);
        values[vi++] = (block1 >>> 28) & 33554431L;
        values[vi++] = (block1 >>> 3) & 33554431L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 7L) << 22) | (block2 >>> 42);
        values[vi++] = (block2 >>> 17) & 33554431L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 131071L) << 8) | (block3 >>> 56);
        values[vi++] = (block3 >>> 31) & 33554431L;
        values[vi++] = (block3 >>> 6) & 33554431L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 63L) << 19) | (block4 >>> 45);
        values[vi++] = (block4 >>> 20) & 33554431L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1048575L) << 5) | (block5 >>> 59);
        values[vi++] = (block5 >>> 34) & 33554431L;
        values[vi++] = (block5 >>> 9) & 33554431L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 511L) << 16) | (block6 >>> 48);
        values[vi++] = (block6 >>> 23) & 33554431L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 8388607L) << 2) | (block7 >>> 62);
        values[vi++] = (block7 >>> 37) & 33554431L;
        values[vi++] = (block7 >>> 12) & 33554431L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 4095L) << 13) | (block8 >>> 51);
        values[vi++] = (block8 >>> 26) & 33554431L;
        values[vi++] = (block8 >>> 1) & 33554431L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 1L) << 24) | (block9 >>> 40);
        values[vi++] = (block9 >>> 15) & 33554431L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 32767L) << 10) | (block10 >>> 54);
        values[vi++] = (block10 >>> 29) & 33554431L;
        values[vi++] = (block10 >>> 4) & 33554431L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 15L) << 21) | (block11 >>> 43);
        values[vi++] = (block11 >>> 18) & 33554431L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 262143L) << 7) | (block12 >>> 57);
        values[vi++] = (block12 >>> 32) & 33554431L;
        values[vi++] = (block12 >>> 7) & 33554431L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 127L) << 18) | (block13 >>> 46);
        values[vi++] = (block13 >>> 21) & 33554431L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 2097151L) << 4) | (block14 >>> 60);
        values[vi++] = (block14 >>> 35) & 33554431L;
        values[vi++] = (block14 >>> 10) & 33554431L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 1023L) << 15) | (block15 >>> 49);
        values[vi++] = (block15 >>> 24) & 33554431L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 16777215L) << 1) | (block16 >>> 63);
        values[vi++] = (block16 >>> 38) & 33554431L;
        values[vi++] = (block16 >>> 13) & 33554431L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 8191L) << 12) | (block17 >>> 52);
        values[vi++] = (block17 >>> 27) & 33554431L;
        values[vi++] = (block17 >>> 2) & 33554431L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 3L) << 23) | (block18 >>> 41);
        values[vi++] = (block18 >>> 16) & 33554431L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 65535L) << 9) | (block19 >>> 55);
        values[vi++] = (block19 >>> 30) & 33554431L;
        values[vi++] = (block19 >>> 5) & 33554431L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 31L) << 20) | (block20 >>> 44);
        values[vi++] = (block20 >>> 19) & 33554431L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 524287L) << 6) | (block21 >>> 58);
        values[vi++] = (block21 >>> 33) & 33554431L;
        values[vi++] = (block21 >>> 8) & 33554431L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 255L) << 17) | (block22 >>> 47);
        values[vi++] = (block22 >>> 22) & 33554431L;
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 4194303L) << 3) | (block23 >>> 61);
        values[vi++] = (block23 >>> 36) & 33554431L;
        values[vi++] = (block23 >>> 11) & 33554431L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 2047L) << 14) | (block24 >>> 50);
        values[vi++] = (block24 >>> 25) & 33554431L;
        values[vi++] = block24 & 33554431L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 39) | (values[vi++] << 14) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 28) | (values[vi++] << 3) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 17) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 31) | (values[vi++] << 6) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 20) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 34) | (values[vi++] << 9) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 23) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 37) | (values[vi++] << 12) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 26) | (values[vi++] << 1) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 15) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 29) | (values[vi++] << 4) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 18) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 32) | (values[vi++] << 7) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 21) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 35) | (values[vi++] << 10) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 24) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 38) | (values[vi++] << 13) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 27) | (values[vi++] << 2) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi++] << 16) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 30) | (values[vi++] << 5) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 19) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 33) | (values[vi++] << 8) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 22) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 36) | (values[vi++] << 11) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 25) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation26 extends BulkOperation {

    public int blocks() {
      return 13;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 38;
        values[vi++] = (block0 >>> 12) & 67108863L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 4095L) << 14) | (block1 >>> 50);
        values[vi++] = (block1 >>> 24) & 67108863L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 16777215L) << 2) | (block2 >>> 62);
        values[vi++] = (block2 >>> 36) & 67108863L;
        values[vi++] = (block2 >>> 10) & 67108863L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 1023L) << 16) | (block3 >>> 48);
        values[vi++] = (block3 >>> 22) & 67108863L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 4194303L) << 4) | (block4 >>> 60);
        values[vi++] = (block4 >>> 34) & 67108863L;
        values[vi++] = (block4 >>> 8) & 67108863L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 255L) << 18) | (block5 >>> 46);
        values[vi++] = (block5 >>> 20) & 67108863L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 1048575L) << 6) | (block6 >>> 58);
        values[vi++] = (block6 >>> 32) & 67108863L;
        values[vi++] = (block6 >>> 6) & 67108863L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 63L) << 20) | (block7 >>> 44);
        values[vi++] = (block7 >>> 18) & 67108863L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 262143L) << 8) | (block8 >>> 56);
        values[vi++] = (block8 >>> 30) & 67108863L;
        values[vi++] = (block8 >>> 4) & 67108863L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 15L) << 22) | (block9 >>> 42);
        values[vi++] = (block9 >>> 16) & 67108863L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 65535L) << 10) | (block10 >>> 54);
        values[vi++] = (block10 >>> 28) & 67108863L;
        values[vi++] = (block10 >>> 2) & 67108863L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 3L) << 24) | (block11 >>> 40);
        values[vi++] = (block11 >>> 14) & 67108863L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 16383L) << 12) | (block12 >>> 52);
        values[vi++] = (block12 >>> 26) & 67108863L;
        values[vi++] = block12 & 67108863L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 38) | (values[vi++] << 12) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 24) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 36) | (values[vi++] << 10) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 22) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 34) | (values[vi++] << 8) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 20) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 32) | (values[vi++] << 6) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 18) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 30) | (values[vi++] << 4) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 16) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 28) | (values[vi++] << 2) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 14) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 26) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation27 extends BulkOperation {

    public int blocks() {
      return 27;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 37;
        values[vi++] = (block0 >>> 10) & 134217727L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1023L) << 17) | (block1 >>> 47);
        values[vi++] = (block1 >>> 20) & 134217727L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 1048575L) << 7) | (block2 >>> 57);
        values[vi++] = (block2 >>> 30) & 134217727L;
        values[vi++] = (block2 >>> 3) & 134217727L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 7L) << 24) | (block3 >>> 40);
        values[vi++] = (block3 >>> 13) & 134217727L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 8191L) << 14) | (block4 >>> 50);
        values[vi++] = (block4 >>> 23) & 134217727L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 8388607L) << 4) | (block5 >>> 60);
        values[vi++] = (block5 >>> 33) & 134217727L;
        values[vi++] = (block5 >>> 6) & 134217727L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 63L) << 21) | (block6 >>> 43);
        values[vi++] = (block6 >>> 16) & 134217727L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 65535L) << 11) | (block7 >>> 53);
        values[vi++] = (block7 >>> 26) & 134217727L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 67108863L) << 1) | (block8 >>> 63);
        values[vi++] = (block8 >>> 36) & 134217727L;
        values[vi++] = (block8 >>> 9) & 134217727L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 511L) << 18) | (block9 >>> 46);
        values[vi++] = (block9 >>> 19) & 134217727L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 524287L) << 8) | (block10 >>> 56);
        values[vi++] = (block10 >>> 29) & 134217727L;
        values[vi++] = (block10 >>> 2) & 134217727L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 3L) << 25) | (block11 >>> 39);
        values[vi++] = (block11 >>> 12) & 134217727L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 4095L) << 15) | (block12 >>> 49);
        values[vi++] = (block12 >>> 22) & 134217727L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 4194303L) << 5) | (block13 >>> 59);
        values[vi++] = (block13 >>> 32) & 134217727L;
        values[vi++] = (block13 >>> 5) & 134217727L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 31L) << 22) | (block14 >>> 42);
        values[vi++] = (block14 >>> 15) & 134217727L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 32767L) << 12) | (block15 >>> 52);
        values[vi++] = (block15 >>> 25) & 134217727L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 33554431L) << 2) | (block16 >>> 62);
        values[vi++] = (block16 >>> 35) & 134217727L;
        values[vi++] = (block16 >>> 8) & 134217727L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 255L) << 19) | (block17 >>> 45);
        values[vi++] = (block17 >>> 18) & 134217727L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 262143L) << 9) | (block18 >>> 55);
        values[vi++] = (block18 >>> 28) & 134217727L;
        values[vi++] = (block18 >>> 1) & 134217727L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 1L) << 26) | (block19 >>> 38);
        values[vi++] = (block19 >>> 11) & 134217727L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 2047L) << 16) | (block20 >>> 48);
        values[vi++] = (block20 >>> 21) & 134217727L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 2097151L) << 6) | (block21 >>> 58);
        values[vi++] = (block21 >>> 31) & 134217727L;
        values[vi++] = (block21 >>> 4) & 134217727L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 15L) << 23) | (block22 >>> 41);
        values[vi++] = (block22 >>> 14) & 134217727L;
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 16383L) << 13) | (block23 >>> 51);
        values[vi++] = (block23 >>> 24) & 134217727L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 16777215L) << 3) | (block24 >>> 61);
        values[vi++] = (block24 >>> 34) & 134217727L;
        values[vi++] = (block24 >>> 7) & 134217727L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 127L) << 20) | (block25 >>> 44);
        values[vi++] = (block25 >>> 17) & 134217727L;
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 131071L) << 10) | (block26 >>> 54);
        values[vi++] = (block26 >>> 27) & 134217727L;
        values[vi++] = block26 & 134217727L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 37) | (values[vi++] << 10) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 20) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 30) | (values[vi++] << 3) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 13) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 23) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 33) | (values[vi++] << 6) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 16) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 26) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 36) | (values[vi++] << 9) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 19) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 29) | (values[vi++] << 2) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi++] << 12) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 22) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 32) | (values[vi++] << 5) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 15) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 25) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 35) | (values[vi++] << 8) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 18) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 28) | (values[vi++] << 1) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi++] << 11) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 21) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 31) | (values[vi++] << 4) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi++] << 14) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 24) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 34) | (values[vi++] << 7) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 17) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 27) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation28 extends BulkOperation {

    public int blocks() {
      return 7;
    }

    public int values() {
      return 16;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 36;
        values[vi++] = (block0 >>> 8) & 268435455L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 255L) << 20) | (block1 >>> 44);
        values[vi++] = (block1 >>> 16) & 268435455L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 65535L) << 12) | (block2 >>> 52);
        values[vi++] = (block2 >>> 24) & 268435455L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 16777215L) << 4) | (block3 >>> 60);
        values[vi++] = (block3 >>> 32) & 268435455L;
        values[vi++] = (block3 >>> 4) & 268435455L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 15L) << 24) | (block4 >>> 40);
        values[vi++] = (block4 >>> 12) & 268435455L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 4095L) << 16) | (block5 >>> 48);
        values[vi++] = (block5 >>> 20) & 268435455L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 1048575L) << 8) | (block6 >>> 56);
        values[vi++] = (block6 >>> 28) & 268435455L;
        values[vi++] = block6 & 268435455L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 36) | (values[vi++] << 8) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 16) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 24) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 32) | (values[vi++] << 4) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 12) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 20) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 28) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation29 extends BulkOperation {

    public int blocks() {
      return 29;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 35;
        values[vi++] = (block0 >>> 6) & 536870911L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 63L) << 23) | (block1 >>> 41);
        values[vi++] = (block1 >>> 12) & 536870911L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 4095L) << 17) | (block2 >>> 47);
        values[vi++] = (block2 >>> 18) & 536870911L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 262143L) << 11) | (block3 >>> 53);
        values[vi++] = (block3 >>> 24) & 536870911L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 16777215L) << 5) | (block4 >>> 59);
        values[vi++] = (block4 >>> 30) & 536870911L;
        values[vi++] = (block4 >>> 1) & 536870911L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1L) << 28) | (block5 >>> 36);
        values[vi++] = (block5 >>> 7) & 536870911L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 127L) << 22) | (block6 >>> 42);
        values[vi++] = (block6 >>> 13) & 536870911L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 8191L) << 16) | (block7 >>> 48);
        values[vi++] = (block7 >>> 19) & 536870911L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 524287L) << 10) | (block8 >>> 54);
        values[vi++] = (block8 >>> 25) & 536870911L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 33554431L) << 4) | (block9 >>> 60);
        values[vi++] = (block9 >>> 31) & 536870911L;
        values[vi++] = (block9 >>> 2) & 536870911L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 3L) << 27) | (block10 >>> 37);
        values[vi++] = (block10 >>> 8) & 536870911L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 255L) << 21) | (block11 >>> 43);
        values[vi++] = (block11 >>> 14) & 536870911L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 16383L) << 15) | (block12 >>> 49);
        values[vi++] = (block12 >>> 20) & 536870911L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 1048575L) << 9) | (block13 >>> 55);
        values[vi++] = (block13 >>> 26) & 536870911L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 67108863L) << 3) | (block14 >>> 61);
        values[vi++] = (block14 >>> 32) & 536870911L;
        values[vi++] = (block14 >>> 3) & 536870911L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 7L) << 26) | (block15 >>> 38);
        values[vi++] = (block15 >>> 9) & 536870911L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 511L) << 20) | (block16 >>> 44);
        values[vi++] = (block16 >>> 15) & 536870911L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 32767L) << 14) | (block17 >>> 50);
        values[vi++] = (block17 >>> 21) & 536870911L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 2097151L) << 8) | (block18 >>> 56);
        values[vi++] = (block18 >>> 27) & 536870911L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 134217727L) << 2) | (block19 >>> 62);
        values[vi++] = (block19 >>> 33) & 536870911L;
        values[vi++] = (block19 >>> 4) & 536870911L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 15L) << 25) | (block20 >>> 39);
        values[vi++] = (block20 >>> 10) & 536870911L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 1023L) << 19) | (block21 >>> 45);
        values[vi++] = (block21 >>> 16) & 536870911L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 65535L) << 13) | (block22 >>> 51);
        values[vi++] = (block22 >>> 22) & 536870911L;
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 4194303L) << 7) | (block23 >>> 57);
        values[vi++] = (block23 >>> 28) & 536870911L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 268435455L) << 1) | (block24 >>> 63);
        values[vi++] = (block24 >>> 34) & 536870911L;
        values[vi++] = (block24 >>> 5) & 536870911L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 31L) << 24) | (block25 >>> 40);
        values[vi++] = (block25 >>> 11) & 536870911L;
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 2047L) << 18) | (block26 >>> 46);
        values[vi++] = (block26 >>> 17) & 536870911L;
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 131071L) << 12) | (block27 >>> 52);
        values[vi++] = (block27 >>> 23) & 536870911L;
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 8388607L) << 6) | (block28 >>> 58);
        values[vi++] = (block28 >>> 29) & 536870911L;
        values[vi++] = block28 & 536870911L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 35) | (values[vi++] << 6) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi++] << 12) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 18) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 24) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 30) | (values[vi++] << 1) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi++] << 7) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 13) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 19) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 25) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 31) | (values[vi++] << 2) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi++] << 8) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 14) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 20) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 26) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 32) | (values[vi++] << 3) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi++] << 9) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 15) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 21) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 27) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 33) | (values[vi++] << 4) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi++] << 10) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 16) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 22) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 28) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 34) | (values[vi++] << 5) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 11) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 17) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 23) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 29) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation30 extends BulkOperation {

    public int blocks() {
      return 15;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 34;
        values[vi++] = (block0 >>> 4) & 1073741823L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 15L) << 26) | (block1 >>> 38);
        values[vi++] = (block1 >>> 8) & 1073741823L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 255L) << 22) | (block2 >>> 42);
        values[vi++] = (block2 >>> 12) & 1073741823L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 4095L) << 18) | (block3 >>> 46);
        values[vi++] = (block3 >>> 16) & 1073741823L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 65535L) << 14) | (block4 >>> 50);
        values[vi++] = (block4 >>> 20) & 1073741823L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1048575L) << 10) | (block5 >>> 54);
        values[vi++] = (block5 >>> 24) & 1073741823L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 16777215L) << 6) | (block6 >>> 58);
        values[vi++] = (block6 >>> 28) & 1073741823L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 268435455L) << 2) | (block7 >>> 62);
        values[vi++] = (block7 >>> 32) & 1073741823L;
        values[vi++] = (block7 >>> 2) & 1073741823L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 3L) << 28) | (block8 >>> 36);
        values[vi++] = (block8 >>> 6) & 1073741823L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 63L) << 24) | (block9 >>> 40);
        values[vi++] = (block9 >>> 10) & 1073741823L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1023L) << 20) | (block10 >>> 44);
        values[vi++] = (block10 >>> 14) & 1073741823L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 16383L) << 16) | (block11 >>> 48);
        values[vi++] = (block11 >>> 18) & 1073741823L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 262143L) << 12) | (block12 >>> 52);
        values[vi++] = (block12 >>> 22) & 1073741823L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 4194303L) << 8) | (block13 >>> 56);
        values[vi++] = (block13 >>> 26) & 1073741823L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 67108863L) << 4) | (block14 >>> 60);
        values[vi++] = (block14 >>> 30) & 1073741823L;
        values[vi++] = block14 & 1073741823L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 34) | (values[vi++] << 4) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi++] << 8) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 12) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 16) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 20) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 24) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 28) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 32) | (values[vi++] << 2) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi++] << 6) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 10) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 14) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 18) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 22) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 26) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 30) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation31 extends BulkOperation {

    public int blocks() {
      return 31;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 33;
        values[vi++] = (block0 >>> 2) & 2147483647L;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 3L) << 29) | (block1 >>> 35);
        values[vi++] = (block1 >>> 4) & 2147483647L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 15L) << 27) | (block2 >>> 37);
        values[vi++] = (block2 >>> 6) & 2147483647L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 63L) << 25) | (block3 >>> 39);
        values[vi++] = (block3 >>> 8) & 2147483647L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 255L) << 23) | (block4 >>> 41);
        values[vi++] = (block4 >>> 10) & 2147483647L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1023L) << 21) | (block5 >>> 43);
        values[vi++] = (block5 >>> 12) & 2147483647L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 4095L) << 19) | (block6 >>> 45);
        values[vi++] = (block6 >>> 14) & 2147483647L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 16383L) << 17) | (block7 >>> 47);
        values[vi++] = (block7 >>> 16) & 2147483647L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 65535L) << 15) | (block8 >>> 49);
        values[vi++] = (block8 >>> 18) & 2147483647L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 262143L) << 13) | (block9 >>> 51);
        values[vi++] = (block9 >>> 20) & 2147483647L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1048575L) << 11) | (block10 >>> 53);
        values[vi++] = (block10 >>> 22) & 2147483647L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 4194303L) << 9) | (block11 >>> 55);
        values[vi++] = (block11 >>> 24) & 2147483647L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 16777215L) << 7) | (block12 >>> 57);
        values[vi++] = (block12 >>> 26) & 2147483647L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 67108863L) << 5) | (block13 >>> 59);
        values[vi++] = (block13 >>> 28) & 2147483647L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 268435455L) << 3) | (block14 >>> 61);
        values[vi++] = (block14 >>> 30) & 2147483647L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 1073741823L) << 1) | (block15 >>> 63);
        values[vi++] = (block15 >>> 32) & 2147483647L;
        values[vi++] = (block15 >>> 1) & 2147483647L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 1L) << 30) | (block16 >>> 34);
        values[vi++] = (block16 >>> 3) & 2147483647L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 7L) << 28) | (block17 >>> 36);
        values[vi++] = (block17 >>> 5) & 2147483647L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 31L) << 26) | (block18 >>> 38);
        values[vi++] = (block18 >>> 7) & 2147483647L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 127L) << 24) | (block19 >>> 40);
        values[vi++] = (block19 >>> 9) & 2147483647L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 511L) << 22) | (block20 >>> 42);
        values[vi++] = (block20 >>> 11) & 2147483647L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 2047L) << 20) | (block21 >>> 44);
        values[vi++] = (block21 >>> 13) & 2147483647L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 8191L) << 18) | (block22 >>> 46);
        values[vi++] = (block22 >>> 15) & 2147483647L;
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 32767L) << 16) | (block23 >>> 48);
        values[vi++] = (block23 >>> 17) & 2147483647L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 131071L) << 14) | (block24 >>> 50);
        values[vi++] = (block24 >>> 19) & 2147483647L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 524287L) << 12) | (block25 >>> 52);
        values[vi++] = (block25 >>> 21) & 2147483647L;
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 2097151L) << 10) | (block26 >>> 54);
        values[vi++] = (block26 >>> 23) & 2147483647L;
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 8388607L) << 8) | (block27 >>> 56);
        values[vi++] = (block27 >>> 25) & 2147483647L;
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 33554431L) << 6) | (block28 >>> 58);
        values[vi++] = (block28 >>> 27) & 2147483647L;
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 134217727L) << 4) | (block29 >>> 60);
        values[vi++] = (block29 >>> 29) & 2147483647L;
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 536870911L) << 2) | (block30 >>> 62);
        values[vi++] = (block30 >>> 31) & 2147483647L;
        values[vi++] = block30 & 2147483647L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 33) | (values[vi++] << 2) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi++] << 4) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi++] << 6) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi++] << 8) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi++] << 10) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 12) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 14) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 16) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 18) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 20) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 22) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 24) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 26) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 28) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 30) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 32) | (values[vi++] << 1) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi++] << 3) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi++] << 5) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi++] << 7) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 9) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 11) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 13) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 15) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 17) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 19) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 21) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 23) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 25) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 27) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 29) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 31) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation32 extends BulkOperation {

    public int blocks() {
      return 1;
    }

    public int values() {
      return 2;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 32;
        values[vi++] = block0 & 4294967295L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 32) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation33 extends BulkOperation {

    public int blocks() {
      return 33;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 31;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 2147483647L) << 2) | (block1 >>> 62);
        values[vi++] = (block1 >>> 29) & 8589934591L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 536870911L) << 4) | (block2 >>> 60);
        values[vi++] = (block2 >>> 27) & 8589934591L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 134217727L) << 6) | (block3 >>> 58);
        values[vi++] = (block3 >>> 25) & 8589934591L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 33554431L) << 8) | (block4 >>> 56);
        values[vi++] = (block4 >>> 23) & 8589934591L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 8388607L) << 10) | (block5 >>> 54);
        values[vi++] = (block5 >>> 21) & 8589934591L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 2097151L) << 12) | (block6 >>> 52);
        values[vi++] = (block6 >>> 19) & 8589934591L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 524287L) << 14) | (block7 >>> 50);
        values[vi++] = (block7 >>> 17) & 8589934591L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 131071L) << 16) | (block8 >>> 48);
        values[vi++] = (block8 >>> 15) & 8589934591L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 32767L) << 18) | (block9 >>> 46);
        values[vi++] = (block9 >>> 13) & 8589934591L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 8191L) << 20) | (block10 >>> 44);
        values[vi++] = (block10 >>> 11) & 8589934591L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 2047L) << 22) | (block11 >>> 42);
        values[vi++] = (block11 >>> 9) & 8589934591L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 511L) << 24) | (block12 >>> 40);
        values[vi++] = (block12 >>> 7) & 8589934591L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 127L) << 26) | (block13 >>> 38);
        values[vi++] = (block13 >>> 5) & 8589934591L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 31L) << 28) | (block14 >>> 36);
        values[vi++] = (block14 >>> 3) & 8589934591L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 7L) << 30) | (block15 >>> 34);
        values[vi++] = (block15 >>> 1) & 8589934591L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 1L) << 32) | (block16 >>> 32);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 4294967295L) << 1) | (block17 >>> 63);
        values[vi++] = (block17 >>> 30) & 8589934591L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 1073741823L) << 3) | (block18 >>> 61);
        values[vi++] = (block18 >>> 28) & 8589934591L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 268435455L) << 5) | (block19 >>> 59);
        values[vi++] = (block19 >>> 26) & 8589934591L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 67108863L) << 7) | (block20 >>> 57);
        values[vi++] = (block20 >>> 24) & 8589934591L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 16777215L) << 9) | (block21 >>> 55);
        values[vi++] = (block21 >>> 22) & 8589934591L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 4194303L) << 11) | (block22 >>> 53);
        values[vi++] = (block22 >>> 20) & 8589934591L;
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 1048575L) << 13) | (block23 >>> 51);
        values[vi++] = (block23 >>> 18) & 8589934591L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 262143L) << 15) | (block24 >>> 49);
        values[vi++] = (block24 >>> 16) & 8589934591L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 65535L) << 17) | (block25 >>> 47);
        values[vi++] = (block25 >>> 14) & 8589934591L;
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 16383L) << 19) | (block26 >>> 45);
        values[vi++] = (block26 >>> 12) & 8589934591L;
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 4095L) << 21) | (block27 >>> 43);
        values[vi++] = (block27 >>> 10) & 8589934591L;
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 1023L) << 23) | (block28 >>> 41);
        values[vi++] = (block28 >>> 8) & 8589934591L;
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 255L) << 25) | (block29 >>> 39);
        values[vi++] = (block29 >>> 6) & 8589934591L;
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 63L) << 27) | (block30 >>> 37);
        values[vi++] = (block30 >>> 4) & 8589934591L;
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 15L) << 29) | (block31 >>> 35);
        values[vi++] = (block31 >>> 2) & 8589934591L;
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 3L) << 31) | (block32 >>> 33);
        values[vi++] = block32 & 8589934591L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 29) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 27) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 25) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 23) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 21) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 19) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 17) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 15) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 13) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 11) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 9) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 7) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi++] << 5) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi++] << 3) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi++] << 1) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 30) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 28) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 26) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 24) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 22) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 20) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 18) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 16) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 14) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 12) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 10) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi++] << 8) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi++] << 6) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi++] << 4) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi++] << 2) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation34 extends BulkOperation {

    public int blocks() {
      return 17;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 30;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1073741823L) << 4) | (block1 >>> 60);
        values[vi++] = (block1 >>> 26) & 17179869183L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 67108863L) << 8) | (block2 >>> 56);
        values[vi++] = (block2 >>> 22) & 17179869183L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 4194303L) << 12) | (block3 >>> 52);
        values[vi++] = (block3 >>> 18) & 17179869183L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 262143L) << 16) | (block4 >>> 48);
        values[vi++] = (block4 >>> 14) & 17179869183L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 16383L) << 20) | (block5 >>> 44);
        values[vi++] = (block5 >>> 10) & 17179869183L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 1023L) << 24) | (block6 >>> 40);
        values[vi++] = (block6 >>> 6) & 17179869183L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 63L) << 28) | (block7 >>> 36);
        values[vi++] = (block7 >>> 2) & 17179869183L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 3L) << 32) | (block8 >>> 32);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 4294967295L) << 2) | (block9 >>> 62);
        values[vi++] = (block9 >>> 28) & 17179869183L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 268435455L) << 6) | (block10 >>> 58);
        values[vi++] = (block10 >>> 24) & 17179869183L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 16777215L) << 10) | (block11 >>> 54);
        values[vi++] = (block11 >>> 20) & 17179869183L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 1048575L) << 14) | (block12 >>> 50);
        values[vi++] = (block12 >>> 16) & 17179869183L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 65535L) << 18) | (block13 >>> 46);
        values[vi++] = (block13 >>> 12) & 17179869183L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 4095L) << 22) | (block14 >>> 42);
        values[vi++] = (block14 >>> 8) & 17179869183L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 255L) << 26) | (block15 >>> 38);
        values[vi++] = (block15 >>> 4) & 17179869183L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 15L) << 30) | (block16 >>> 34);
        values[vi++] = block16 & 17179869183L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 26) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 22) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 18) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 14) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 10) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 6) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi++] << 2) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 28) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 24) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 20) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 16) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 12) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 8) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi++] << 4) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation35 extends BulkOperation {

    public int blocks() {
      return 35;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 29;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 536870911L) << 6) | (block1 >>> 58);
        values[vi++] = (block1 >>> 23) & 34359738367L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 8388607L) << 12) | (block2 >>> 52);
        values[vi++] = (block2 >>> 17) & 34359738367L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 131071L) << 18) | (block3 >>> 46);
        values[vi++] = (block3 >>> 11) & 34359738367L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 2047L) << 24) | (block4 >>> 40);
        values[vi++] = (block4 >>> 5) & 34359738367L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 31L) << 30) | (block5 >>> 34);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 17179869183L) << 1) | (block6 >>> 63);
        values[vi++] = (block6 >>> 28) & 34359738367L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 268435455L) << 7) | (block7 >>> 57);
        values[vi++] = (block7 >>> 22) & 34359738367L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 4194303L) << 13) | (block8 >>> 51);
        values[vi++] = (block8 >>> 16) & 34359738367L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 65535L) << 19) | (block9 >>> 45);
        values[vi++] = (block9 >>> 10) & 34359738367L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1023L) << 25) | (block10 >>> 39);
        values[vi++] = (block10 >>> 4) & 34359738367L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 15L) << 31) | (block11 >>> 33);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 8589934591L) << 2) | (block12 >>> 62);
        values[vi++] = (block12 >>> 27) & 34359738367L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 134217727L) << 8) | (block13 >>> 56);
        values[vi++] = (block13 >>> 21) & 34359738367L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 2097151L) << 14) | (block14 >>> 50);
        values[vi++] = (block14 >>> 15) & 34359738367L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 32767L) << 20) | (block15 >>> 44);
        values[vi++] = (block15 >>> 9) & 34359738367L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 511L) << 26) | (block16 >>> 38);
        values[vi++] = (block16 >>> 3) & 34359738367L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 7L) << 32) | (block17 >>> 32);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 4294967295L) << 3) | (block18 >>> 61);
        values[vi++] = (block18 >>> 26) & 34359738367L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 67108863L) << 9) | (block19 >>> 55);
        values[vi++] = (block19 >>> 20) & 34359738367L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 1048575L) << 15) | (block20 >>> 49);
        values[vi++] = (block20 >>> 14) & 34359738367L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 16383L) << 21) | (block21 >>> 43);
        values[vi++] = (block21 >>> 8) & 34359738367L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 255L) << 27) | (block22 >>> 37);
        values[vi++] = (block22 >>> 2) & 34359738367L;
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 3L) << 33) | (block23 >>> 31);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 2147483647L) << 4) | (block24 >>> 60);
        values[vi++] = (block24 >>> 25) & 34359738367L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 33554431L) << 10) | (block25 >>> 54);
        values[vi++] = (block25 >>> 19) & 34359738367L;
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 524287L) << 16) | (block26 >>> 48);
        values[vi++] = (block26 >>> 13) & 34359738367L;
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 8191L) << 22) | (block27 >>> 42);
        values[vi++] = (block27 >>> 7) & 34359738367L;
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 127L) << 28) | (block28 >>> 36);
        values[vi++] = (block28 >>> 1) & 34359738367L;
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 1L) << 34) | (block29 >>> 30);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 1073741823L) << 5) | (block30 >>> 59);
        values[vi++] = (block30 >>> 24) & 34359738367L;
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 16777215L) << 11) | (block31 >>> 53);
        values[vi++] = (block31 >>> 18) & 34359738367L;
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 262143L) << 17) | (block32 >>> 47);
        values[vi++] = (block32 >>> 12) & 34359738367L;
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 4095L) << 23) | (block33 >>> 41);
        values[vi++] = (block33 >>> 6) & 34359738367L;
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 63L) << 29) | (block34 >>> 35);
        values[vi++] = block34 & 34359738367L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 23) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 17) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 11) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 5) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 28) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 22) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 16) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 10) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi++] << 4) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 27) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 21) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 15) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 9) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi++] << 3) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 26) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 20) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 14) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 8) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi++] << 2) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 25) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 19) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 13) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 7) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi++] << 1) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 24) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 18) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 12) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi++] << 6) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation36 extends BulkOperation {

    public int blocks() {
      return 9;
    }

    public int values() {
      return 16;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 28;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 268435455L) << 8) | (block1 >>> 56);
        values[vi++] = (block1 >>> 20) & 68719476735L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 1048575L) << 16) | (block2 >>> 48);
        values[vi++] = (block2 >>> 12) & 68719476735L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 4095L) << 24) | (block3 >>> 40);
        values[vi++] = (block3 >>> 4) & 68719476735L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 15L) << 32) | (block4 >>> 32);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 4294967295L) << 4) | (block5 >>> 60);
        values[vi++] = (block5 >>> 24) & 68719476735L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 16777215L) << 12) | (block6 >>> 52);
        values[vi++] = (block6 >>> 16) & 68719476735L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 65535L) << 20) | (block7 >>> 44);
        values[vi++] = (block7 >>> 8) & 68719476735L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 255L) << 28) | (block8 >>> 36);
        values[vi++] = block8 & 68719476735L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 20) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 12) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 4) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 24) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 16) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 8) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation37 extends BulkOperation {

    public int blocks() {
      return 37;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 27;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 134217727L) << 10) | (block1 >>> 54);
        values[vi++] = (block1 >>> 17) & 137438953471L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 131071L) << 20) | (block2 >>> 44);
        values[vi++] = (block2 >>> 7) & 137438953471L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 127L) << 30) | (block3 >>> 34);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 17179869183L) << 3) | (block4 >>> 61);
        values[vi++] = (block4 >>> 24) & 137438953471L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 16777215L) << 13) | (block5 >>> 51);
        values[vi++] = (block5 >>> 14) & 137438953471L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 16383L) << 23) | (block6 >>> 41);
        values[vi++] = (block6 >>> 4) & 137438953471L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 15L) << 33) | (block7 >>> 31);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 2147483647L) << 6) | (block8 >>> 58);
        values[vi++] = (block8 >>> 21) & 137438953471L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 2097151L) << 16) | (block9 >>> 48);
        values[vi++] = (block9 >>> 11) & 137438953471L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 2047L) << 26) | (block10 >>> 38);
        values[vi++] = (block10 >>> 1) & 137438953471L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 1L) << 36) | (block11 >>> 28);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 268435455L) << 9) | (block12 >>> 55);
        values[vi++] = (block12 >>> 18) & 137438953471L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 262143L) << 19) | (block13 >>> 45);
        values[vi++] = (block13 >>> 8) & 137438953471L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 255L) << 29) | (block14 >>> 35);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 34359738367L) << 2) | (block15 >>> 62);
        values[vi++] = (block15 >>> 25) & 137438953471L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 33554431L) << 12) | (block16 >>> 52);
        values[vi++] = (block16 >>> 15) & 137438953471L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 32767L) << 22) | (block17 >>> 42);
        values[vi++] = (block17 >>> 5) & 137438953471L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 31L) << 32) | (block18 >>> 32);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 4294967295L) << 5) | (block19 >>> 59);
        values[vi++] = (block19 >>> 22) & 137438953471L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 4194303L) << 15) | (block20 >>> 49);
        values[vi++] = (block20 >>> 12) & 137438953471L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 4095L) << 25) | (block21 >>> 39);
        values[vi++] = (block21 >>> 2) & 137438953471L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 3L) << 35) | (block22 >>> 29);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 536870911L) << 8) | (block23 >>> 56);
        values[vi++] = (block23 >>> 19) & 137438953471L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 524287L) << 18) | (block24 >>> 46);
        values[vi++] = (block24 >>> 9) & 137438953471L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 511L) << 28) | (block25 >>> 36);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 68719476735L) << 1) | (block26 >>> 63);
        values[vi++] = (block26 >>> 26) & 137438953471L;
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 67108863L) << 11) | (block27 >>> 53);
        values[vi++] = (block27 >>> 16) & 137438953471L;
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 65535L) << 21) | (block28 >>> 43);
        values[vi++] = (block28 >>> 6) & 137438953471L;
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 63L) << 31) | (block29 >>> 33);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 8589934591L) << 4) | (block30 >>> 60);
        values[vi++] = (block30 >>> 23) & 137438953471L;
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 8388607L) << 14) | (block31 >>> 50);
        values[vi++] = (block31 >>> 13) & 137438953471L;
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 8191L) << 24) | (block32 >>> 40);
        values[vi++] = (block32 >>> 3) & 137438953471L;
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 7L) << 34) | (block33 >>> 30);
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 1073741823L) << 7) | (block34 >>> 57);
        values[vi++] = (block34 >>> 20) & 137438953471L;
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 1048575L) << 17) | (block35 >>> 47);
        values[vi++] = (block35 >>> 10) & 137438953471L;
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 1023L) << 27) | (block36 >>> 37);
        values[vi++] = block36 & 137438953471L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 17) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 7) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 24) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 14) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi++] << 4) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 21) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 11) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi++] << 1) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 18) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 8) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 25) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 15) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 5) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 22) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 12) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi++] << 2) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 19) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 9) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 26) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 16) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 6) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 23) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 13) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 3) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 20) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 10) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation38 extends BulkOperation {

    public int blocks() {
      return 19;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 26;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 67108863L) << 12) | (block1 >>> 52);
        values[vi++] = (block1 >>> 14) & 274877906943L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 16383L) << 24) | (block2 >>> 40);
        values[vi++] = (block2 >>> 2) & 274877906943L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 3L) << 36) | (block3 >>> 28);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 268435455L) << 10) | (block4 >>> 54);
        values[vi++] = (block4 >>> 16) & 274877906943L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 65535L) << 22) | (block5 >>> 42);
        values[vi++] = (block5 >>> 4) & 274877906943L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 15L) << 34) | (block6 >>> 30);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 1073741823L) << 8) | (block7 >>> 56);
        values[vi++] = (block7 >>> 18) & 274877906943L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 262143L) << 20) | (block8 >>> 44);
        values[vi++] = (block8 >>> 6) & 274877906943L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 63L) << 32) | (block9 >>> 32);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 4294967295L) << 6) | (block10 >>> 58);
        values[vi++] = (block10 >>> 20) & 274877906943L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 1048575L) << 18) | (block11 >>> 46);
        values[vi++] = (block11 >>> 8) & 274877906943L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 255L) << 30) | (block12 >>> 34);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 17179869183L) << 4) | (block13 >>> 60);
        values[vi++] = (block13 >>> 22) & 274877906943L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 4194303L) << 16) | (block14 >>> 48);
        values[vi++] = (block14 >>> 10) & 274877906943L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 1023L) << 28) | (block15 >>> 36);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 68719476735L) << 2) | (block16 >>> 62);
        values[vi++] = (block16 >>> 24) & 274877906943L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 16777215L) << 14) | (block17 >>> 50);
        values[vi++] = (block17 >>> 12) & 274877906943L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 4095L) << 26) | (block18 >>> 38);
        values[vi++] = block18 & 274877906943L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 14) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 2) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 16) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 4) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 18) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 6) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 20) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 8) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 22) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 10) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 24) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 12) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation39 extends BulkOperation {

    public int blocks() {
      return 39;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 25;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 33554431L) << 14) | (block1 >>> 50);
        values[vi++] = (block1 >>> 11) & 549755813887L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 2047L) << 28) | (block2 >>> 36);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 68719476735L) << 3) | (block3 >>> 61);
        values[vi++] = (block3 >>> 22) & 549755813887L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 4194303L) << 17) | (block4 >>> 47);
        values[vi++] = (block4 >>> 8) & 549755813887L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 255L) << 31) | (block5 >>> 33);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 8589934591L) << 6) | (block6 >>> 58);
        values[vi++] = (block6 >>> 19) & 549755813887L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 524287L) << 20) | (block7 >>> 44);
        values[vi++] = (block7 >>> 5) & 549755813887L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 31L) << 34) | (block8 >>> 30);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 1073741823L) << 9) | (block9 >>> 55);
        values[vi++] = (block9 >>> 16) & 549755813887L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 65535L) << 23) | (block10 >>> 41);
        values[vi++] = (block10 >>> 2) & 549755813887L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 3L) << 37) | (block11 >>> 27);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 134217727L) << 12) | (block12 >>> 52);
        values[vi++] = (block12 >>> 13) & 549755813887L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 8191L) << 26) | (block13 >>> 38);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 274877906943L) << 1) | (block14 >>> 63);
        values[vi++] = (block14 >>> 24) & 549755813887L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 16777215L) << 15) | (block15 >>> 49);
        values[vi++] = (block15 >>> 10) & 549755813887L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 1023L) << 29) | (block16 >>> 35);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 34359738367L) << 4) | (block17 >>> 60);
        values[vi++] = (block17 >>> 21) & 549755813887L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 2097151L) << 18) | (block18 >>> 46);
        values[vi++] = (block18 >>> 7) & 549755813887L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 127L) << 32) | (block19 >>> 32);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 4294967295L) << 7) | (block20 >>> 57);
        values[vi++] = (block20 >>> 18) & 549755813887L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 262143L) << 21) | (block21 >>> 43);
        values[vi++] = (block21 >>> 4) & 549755813887L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 15L) << 35) | (block22 >>> 29);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 536870911L) << 10) | (block23 >>> 54);
        values[vi++] = (block23 >>> 15) & 549755813887L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 32767L) << 24) | (block24 >>> 40);
        values[vi++] = (block24 >>> 1) & 549755813887L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 1L) << 38) | (block25 >>> 26);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 67108863L) << 13) | (block26 >>> 51);
        values[vi++] = (block26 >>> 12) & 549755813887L;
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 4095L) << 27) | (block27 >>> 37);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 137438953471L) << 2) | (block28 >>> 62);
        values[vi++] = (block28 >>> 23) & 549755813887L;
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 8388607L) << 16) | (block29 >>> 48);
        values[vi++] = (block29 >>> 9) & 549755813887L;
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 511L) << 30) | (block30 >>> 34);
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 17179869183L) << 5) | (block31 >>> 59);
        values[vi++] = (block31 >>> 20) & 549755813887L;
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 1048575L) << 19) | (block32 >>> 45);
        values[vi++] = (block32 >>> 6) & 549755813887L;
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 63L) << 33) | (block33 >>> 31);
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 2147483647L) << 8) | (block34 >>> 56);
        values[vi++] = (block34 >>> 17) & 549755813887L;
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 131071L) << 22) | (block35 >>> 42);
        values[vi++] = (block35 >>> 3) & 549755813887L;
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 7L) << 36) | (block36 >>> 28);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 268435455L) << 11) | (block37 >>> 53);
        values[vi++] = (block37 >>> 14) & 549755813887L;
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 16383L) << 25) | (block38 >>> 39);
        values[vi++] = block38 & 549755813887L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 11) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 22) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 8) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 19) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 5) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 16) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi++] << 2) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 13) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 24) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 10) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 21) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 7) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 18) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 4) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 15) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi++] << 1) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 12) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 23) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 9) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 20) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 6) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 17) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 3) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 14) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation40 extends BulkOperation {

    public int blocks() {
      return 5;
    }

    public int values() {
      return 8;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 24;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 16777215L) << 16) | (block1 >>> 48);
        values[vi++] = (block1 >>> 8) & 1099511627775L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 255L) << 32) | (block2 >>> 32);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 4294967295L) << 8) | (block3 >>> 56);
        values[vi++] = (block3 >>> 16) & 1099511627775L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 65535L) << 24) | (block4 >>> 40);
        values[vi++] = block4 & 1099511627775L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 8) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 16) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation41 extends BulkOperation {

    public int blocks() {
      return 41;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 23;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 8388607L) << 18) | (block1 >>> 46);
        values[vi++] = (block1 >>> 5) & 2199023255551L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 31L) << 36) | (block2 >>> 28);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 268435455L) << 13) | (block3 >>> 51);
        values[vi++] = (block3 >>> 10) & 2199023255551L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 1023L) << 31) | (block4 >>> 33);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 8589934591L) << 8) | (block5 >>> 56);
        values[vi++] = (block5 >>> 15) & 2199023255551L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 32767L) << 26) | (block6 >>> 38);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 274877906943L) << 3) | (block7 >>> 61);
        values[vi++] = (block7 >>> 20) & 2199023255551L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 1048575L) << 21) | (block8 >>> 43);
        values[vi++] = (block8 >>> 2) & 2199023255551L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 3L) << 39) | (block9 >>> 25);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 33554431L) << 16) | (block10 >>> 48);
        values[vi++] = (block10 >>> 7) & 2199023255551L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 127L) << 34) | (block11 >>> 30);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 1073741823L) << 11) | (block12 >>> 53);
        values[vi++] = (block12 >>> 12) & 2199023255551L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 4095L) << 29) | (block13 >>> 35);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 34359738367L) << 6) | (block14 >>> 58);
        values[vi++] = (block14 >>> 17) & 2199023255551L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 131071L) << 24) | (block15 >>> 40);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 1099511627775L) << 1) | (block16 >>> 63);
        values[vi++] = (block16 >>> 22) & 2199023255551L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 4194303L) << 19) | (block17 >>> 45);
        values[vi++] = (block17 >>> 4) & 2199023255551L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 15L) << 37) | (block18 >>> 27);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 134217727L) << 14) | (block19 >>> 50);
        values[vi++] = (block19 >>> 9) & 2199023255551L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 511L) << 32) | (block20 >>> 32);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 4294967295L) << 9) | (block21 >>> 55);
        values[vi++] = (block21 >>> 14) & 2199023255551L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 16383L) << 27) | (block22 >>> 37);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 137438953471L) << 4) | (block23 >>> 60);
        values[vi++] = (block23 >>> 19) & 2199023255551L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 524287L) << 22) | (block24 >>> 42);
        values[vi++] = (block24 >>> 1) & 2199023255551L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 1L) << 40) | (block25 >>> 24);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 16777215L) << 17) | (block26 >>> 47);
        values[vi++] = (block26 >>> 6) & 2199023255551L;
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 63L) << 35) | (block27 >>> 29);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 536870911L) << 12) | (block28 >>> 52);
        values[vi++] = (block28 >>> 11) & 2199023255551L;
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 2047L) << 30) | (block29 >>> 34);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 17179869183L) << 7) | (block30 >>> 57);
        values[vi++] = (block30 >>> 16) & 2199023255551L;
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 65535L) << 25) | (block31 >>> 39);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 549755813887L) << 2) | (block32 >>> 62);
        values[vi++] = (block32 >>> 21) & 2199023255551L;
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 2097151L) << 20) | (block33 >>> 44);
        values[vi++] = (block33 >>> 3) & 2199023255551L;
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 7L) << 38) | (block34 >>> 26);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 67108863L) << 15) | (block35 >>> 49);
        values[vi++] = (block35 >>> 8) & 2199023255551L;
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 255L) << 33) | (block36 >>> 31);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 2147483647L) << 10) | (block37 >>> 54);
        values[vi++] = (block37 >>> 13) & 2199023255551L;
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 8191L) << 28) | (block38 >>> 36);
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 68719476735L) << 5) | (block39 >>> 59);
        values[vi++] = (block39 >>> 18) & 2199023255551L;
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 262143L) << 23) | (block40 >>> 41);
        values[vi++] = block40 & 2199023255551L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 5) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 10) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 15) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 20) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi++] << 2) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 7) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 12) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 17) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 22) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 4) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 9) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 14) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 19) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi++] << 1) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 6) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 11) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 16) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 21) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 3) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 8) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 13) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 18) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation42 extends BulkOperation {

    public int blocks() {
      return 21;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 22;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 4194303L) << 20) | (block1 >>> 44);
        values[vi++] = (block1 >>> 2) & 4398046511103L;
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 3L) << 40) | (block2 >>> 24);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 16777215L) << 18) | (block3 >>> 46);
        values[vi++] = (block3 >>> 4) & 4398046511103L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 15L) << 38) | (block4 >>> 26);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 67108863L) << 16) | (block5 >>> 48);
        values[vi++] = (block5 >>> 6) & 4398046511103L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 63L) << 36) | (block6 >>> 28);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 268435455L) << 14) | (block7 >>> 50);
        values[vi++] = (block7 >>> 8) & 4398046511103L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 255L) << 34) | (block8 >>> 30);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 1073741823L) << 12) | (block9 >>> 52);
        values[vi++] = (block9 >>> 10) & 4398046511103L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1023L) << 32) | (block10 >>> 32);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 4294967295L) << 10) | (block11 >>> 54);
        values[vi++] = (block11 >>> 12) & 4398046511103L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 4095L) << 30) | (block12 >>> 34);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 17179869183L) << 8) | (block13 >>> 56);
        values[vi++] = (block13 >>> 14) & 4398046511103L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 16383L) << 28) | (block14 >>> 36);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 68719476735L) << 6) | (block15 >>> 58);
        values[vi++] = (block15 >>> 16) & 4398046511103L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 65535L) << 26) | (block16 >>> 38);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 274877906943L) << 4) | (block17 >>> 60);
        values[vi++] = (block17 >>> 18) & 4398046511103L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 262143L) << 24) | (block18 >>> 40);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 1099511627775L) << 2) | (block19 >>> 62);
        values[vi++] = (block19 >>> 20) & 4398046511103L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 1048575L) << 22) | (block20 >>> 42);
        values[vi++] = block20 & 4398046511103L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 2) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 4) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 6) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 8) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 10) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 12) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 14) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 16) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 18) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 20) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation43 extends BulkOperation {

    public int blocks() {
      return 43;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 21;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 2097151L) << 22) | (block1 >>> 42);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 4398046511103L) << 1) | (block2 >>> 63);
        values[vi++] = (block2 >>> 20) & 8796093022207L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 1048575L) << 23) | (block3 >>> 41);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 2199023255551L) << 2) | (block4 >>> 62);
        values[vi++] = (block4 >>> 19) & 8796093022207L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 524287L) << 24) | (block5 >>> 40);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 1099511627775L) << 3) | (block6 >>> 61);
        values[vi++] = (block6 >>> 18) & 8796093022207L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 262143L) << 25) | (block7 >>> 39);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 549755813887L) << 4) | (block8 >>> 60);
        values[vi++] = (block8 >>> 17) & 8796093022207L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 131071L) << 26) | (block9 >>> 38);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 274877906943L) << 5) | (block10 >>> 59);
        values[vi++] = (block10 >>> 16) & 8796093022207L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 65535L) << 27) | (block11 >>> 37);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 137438953471L) << 6) | (block12 >>> 58);
        values[vi++] = (block12 >>> 15) & 8796093022207L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 32767L) << 28) | (block13 >>> 36);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 68719476735L) << 7) | (block14 >>> 57);
        values[vi++] = (block14 >>> 14) & 8796093022207L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 16383L) << 29) | (block15 >>> 35);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 34359738367L) << 8) | (block16 >>> 56);
        values[vi++] = (block16 >>> 13) & 8796093022207L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 8191L) << 30) | (block17 >>> 34);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 17179869183L) << 9) | (block18 >>> 55);
        values[vi++] = (block18 >>> 12) & 8796093022207L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 4095L) << 31) | (block19 >>> 33);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 8589934591L) << 10) | (block20 >>> 54);
        values[vi++] = (block20 >>> 11) & 8796093022207L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 2047L) << 32) | (block21 >>> 32);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 4294967295L) << 11) | (block22 >>> 53);
        values[vi++] = (block22 >>> 10) & 8796093022207L;
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 1023L) << 33) | (block23 >>> 31);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 2147483647L) << 12) | (block24 >>> 52);
        values[vi++] = (block24 >>> 9) & 8796093022207L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 511L) << 34) | (block25 >>> 30);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 1073741823L) << 13) | (block26 >>> 51);
        values[vi++] = (block26 >>> 8) & 8796093022207L;
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 255L) << 35) | (block27 >>> 29);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 536870911L) << 14) | (block28 >>> 50);
        values[vi++] = (block28 >>> 7) & 8796093022207L;
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 127L) << 36) | (block29 >>> 28);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 268435455L) << 15) | (block30 >>> 49);
        values[vi++] = (block30 >>> 6) & 8796093022207L;
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 63L) << 37) | (block31 >>> 27);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 134217727L) << 16) | (block32 >>> 48);
        values[vi++] = (block32 >>> 5) & 8796093022207L;
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 31L) << 38) | (block33 >>> 26);
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 67108863L) << 17) | (block34 >>> 47);
        values[vi++] = (block34 >>> 4) & 8796093022207L;
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 15L) << 39) | (block35 >>> 25);
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 33554431L) << 18) | (block36 >>> 46);
        values[vi++] = (block36 >>> 3) & 8796093022207L;
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 7L) << 40) | (block37 >>> 24);
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 16777215L) << 19) | (block38 >>> 45);
        values[vi++] = (block38 >>> 2) & 8796093022207L;
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 3L) << 41) | (block39 >>> 23);
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 8388607L) << 20) | (block40 >>> 44);
        values[vi++] = (block40 >>> 1) & 8796093022207L;
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 1L) << 42) | (block41 >>> 22);
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 4194303L) << 21) | (block42 >>> 43);
        values[vi++] = block42 & 8796093022207L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 20) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 19) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 18) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 17) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 16) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 15) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 14) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 13) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 12) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 11) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 10) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 9) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 8) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 7) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 6) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 5) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 4) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 3) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi++] << 2) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi++] << 1) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation44 extends BulkOperation {

    public int blocks() {
      return 11;
    }

    public int values() {
      return 16;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 20;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1048575L) << 24) | (block1 >>> 40);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 1099511627775L) << 4) | (block2 >>> 60);
        values[vi++] = (block2 >>> 16) & 17592186044415L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 65535L) << 28) | (block3 >>> 36);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 68719476735L) << 8) | (block4 >>> 56);
        values[vi++] = (block4 >>> 12) & 17592186044415L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 4095L) << 32) | (block5 >>> 32);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 4294967295L) << 12) | (block6 >>> 52);
        values[vi++] = (block6 >>> 8) & 17592186044415L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 255L) << 36) | (block7 >>> 28);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 268435455L) << 16) | (block8 >>> 48);
        values[vi++] = (block8 >>> 4) & 17592186044415L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 15L) << 40) | (block9 >>> 24);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 16777215L) << 20) | (block10 >>> 44);
        values[vi++] = block10 & 17592186044415L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 16) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 12) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 8) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 4) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation45 extends BulkOperation {

    public int blocks() {
      return 45;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 19;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 524287L) << 26) | (block1 >>> 38);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 274877906943L) << 7) | (block2 >>> 57);
        values[vi++] = (block2 >>> 12) & 35184372088831L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 4095L) << 33) | (block3 >>> 31);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 2147483647L) << 14) | (block4 >>> 50);
        values[vi++] = (block4 >>> 5) & 35184372088831L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 31L) << 40) | (block5 >>> 24);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 16777215L) << 21) | (block6 >>> 43);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 8796093022207L) << 2) | (block7 >>> 62);
        values[vi++] = (block7 >>> 17) & 35184372088831L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 131071L) << 28) | (block8 >>> 36);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 68719476735L) << 9) | (block9 >>> 55);
        values[vi++] = (block9 >>> 10) & 35184372088831L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1023L) << 35) | (block10 >>> 29);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 536870911L) << 16) | (block11 >>> 48);
        values[vi++] = (block11 >>> 3) & 35184372088831L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 7L) << 42) | (block12 >>> 22);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 4194303L) << 23) | (block13 >>> 41);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 2199023255551L) << 4) | (block14 >>> 60);
        values[vi++] = (block14 >>> 15) & 35184372088831L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 32767L) << 30) | (block15 >>> 34);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 17179869183L) << 11) | (block16 >>> 53);
        values[vi++] = (block16 >>> 8) & 35184372088831L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 255L) << 37) | (block17 >>> 27);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 134217727L) << 18) | (block18 >>> 46);
        values[vi++] = (block18 >>> 1) & 35184372088831L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 1L) << 44) | (block19 >>> 20);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 1048575L) << 25) | (block20 >>> 39);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 549755813887L) << 6) | (block21 >>> 58);
        values[vi++] = (block21 >>> 13) & 35184372088831L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 8191L) << 32) | (block22 >>> 32);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 4294967295L) << 13) | (block23 >>> 51);
        values[vi++] = (block23 >>> 6) & 35184372088831L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 63L) << 39) | (block24 >>> 25);
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 33554431L) << 20) | (block25 >>> 44);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 17592186044415L) << 1) | (block26 >>> 63);
        values[vi++] = (block26 >>> 18) & 35184372088831L;
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 262143L) << 27) | (block27 >>> 37);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 137438953471L) << 8) | (block28 >>> 56);
        values[vi++] = (block28 >>> 11) & 35184372088831L;
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 2047L) << 34) | (block29 >>> 30);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 1073741823L) << 15) | (block30 >>> 49);
        values[vi++] = (block30 >>> 4) & 35184372088831L;
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 15L) << 41) | (block31 >>> 23);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 8388607L) << 22) | (block32 >>> 42);
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 4398046511103L) << 3) | (block33 >>> 61);
        values[vi++] = (block33 >>> 16) & 35184372088831L;
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 65535L) << 29) | (block34 >>> 35);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 34359738367L) << 10) | (block35 >>> 54);
        values[vi++] = (block35 >>> 9) & 35184372088831L;
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 511L) << 36) | (block36 >>> 28);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 268435455L) << 17) | (block37 >>> 47);
        values[vi++] = (block37 >>> 2) & 35184372088831L;
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 3L) << 43) | (block38 >>> 21);
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 2097151L) << 24) | (block39 >>> 40);
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 1099511627775L) << 5) | (block40 >>> 59);
        values[vi++] = (block40 >>> 14) & 35184372088831L;
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 16383L) << 31) | (block41 >>> 33);
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 8589934591L) << 12) | (block42 >>> 52);
        values[vi++] = (block42 >>> 7) & 35184372088831L;
        final long block43 = blocks[bi++];
        values[vi++] = ((block42 & 127L) << 38) | (block43 >>> 26);
        final long block44 = blocks[bi++];
        values[vi++] = ((block43 & 67108863L) << 19) | (block44 >>> 45);
        values[vi++] = block44 & 35184372088831L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 19) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 12) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 5) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 17) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 10) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 3) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 15) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 8) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi++] << 1) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 13) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 6) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 18) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 11) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 4) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 16) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 9) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi++] << 2) | (values[vi] >>> 43);
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 14) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 7) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation46 extends BulkOperation {

    public int blocks() {
      return 23;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 18;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 262143L) << 28) | (block1 >>> 36);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 68719476735L) << 10) | (block2 >>> 54);
        values[vi++] = (block2 >>> 8) & 70368744177663L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 255L) << 38) | (block3 >>> 26);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 67108863L) << 20) | (block4 >>> 44);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 17592186044415L) << 2) | (block5 >>> 62);
        values[vi++] = (block5 >>> 16) & 70368744177663L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 65535L) << 30) | (block6 >>> 34);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 17179869183L) << 12) | (block7 >>> 52);
        values[vi++] = (block7 >>> 6) & 70368744177663L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 63L) << 40) | (block8 >>> 24);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 16777215L) << 22) | (block9 >>> 42);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 4398046511103L) << 4) | (block10 >>> 60);
        values[vi++] = (block10 >>> 14) & 70368744177663L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 16383L) << 32) | (block11 >>> 32);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 4294967295L) << 14) | (block12 >>> 50);
        values[vi++] = (block12 >>> 4) & 70368744177663L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 15L) << 42) | (block13 >>> 22);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 4194303L) << 24) | (block14 >>> 40);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 1099511627775L) << 6) | (block15 >>> 58);
        values[vi++] = (block15 >>> 12) & 70368744177663L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 4095L) << 34) | (block16 >>> 30);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 1073741823L) << 16) | (block17 >>> 48);
        values[vi++] = (block17 >>> 2) & 70368744177663L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 3L) << 44) | (block18 >>> 20);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 1048575L) << 26) | (block19 >>> 38);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 274877906943L) << 8) | (block20 >>> 56);
        values[vi++] = (block20 >>> 10) & 70368744177663L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 1023L) << 36) | (block21 >>> 28);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 268435455L) << 18) | (block22 >>> 46);
        values[vi++] = block22 & 70368744177663L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 8) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 16) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 6) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 14) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 4) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 12) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 2) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 10) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation47 extends BulkOperation {

    public int blocks() {
      return 47;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 17;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 131071L) << 30) | (block1 >>> 34);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 17179869183L) << 13) | (block2 >>> 51);
        values[vi++] = (block2 >>> 4) & 140737488355327L;
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 15L) << 43) | (block3 >>> 21);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 2097151L) << 26) | (block4 >>> 38);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 274877906943L) << 9) | (block5 >>> 55);
        values[vi++] = (block5 >>> 8) & 140737488355327L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 255L) << 39) | (block6 >>> 25);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 33554431L) << 22) | (block7 >>> 42);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 4398046511103L) << 5) | (block8 >>> 59);
        values[vi++] = (block8 >>> 12) & 140737488355327L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 4095L) << 35) | (block9 >>> 29);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 536870911L) << 18) | (block10 >>> 46);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 70368744177663L) << 1) | (block11 >>> 63);
        values[vi++] = (block11 >>> 16) & 140737488355327L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 65535L) << 31) | (block12 >>> 33);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 8589934591L) << 14) | (block13 >>> 50);
        values[vi++] = (block13 >>> 3) & 140737488355327L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 7L) << 44) | (block14 >>> 20);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 1048575L) << 27) | (block15 >>> 37);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 137438953471L) << 10) | (block16 >>> 54);
        values[vi++] = (block16 >>> 7) & 140737488355327L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 127L) << 40) | (block17 >>> 24);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 16777215L) << 23) | (block18 >>> 41);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 2199023255551L) << 6) | (block19 >>> 58);
        values[vi++] = (block19 >>> 11) & 140737488355327L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 2047L) << 36) | (block20 >>> 28);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 268435455L) << 19) | (block21 >>> 45);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 35184372088831L) << 2) | (block22 >>> 62);
        values[vi++] = (block22 >>> 15) & 140737488355327L;
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 32767L) << 32) | (block23 >>> 32);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 4294967295L) << 15) | (block24 >>> 49);
        values[vi++] = (block24 >>> 2) & 140737488355327L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 3L) << 45) | (block25 >>> 19);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 524287L) << 28) | (block26 >>> 36);
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 68719476735L) << 11) | (block27 >>> 53);
        values[vi++] = (block27 >>> 6) & 140737488355327L;
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 63L) << 41) | (block28 >>> 23);
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 8388607L) << 24) | (block29 >>> 40);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 1099511627775L) << 7) | (block30 >>> 57);
        values[vi++] = (block30 >>> 10) & 140737488355327L;
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 1023L) << 37) | (block31 >>> 27);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 134217727L) << 20) | (block32 >>> 44);
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 17592186044415L) << 3) | (block33 >>> 61);
        values[vi++] = (block33 >>> 14) & 140737488355327L;
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 16383L) << 33) | (block34 >>> 31);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 2147483647L) << 16) | (block35 >>> 48);
        values[vi++] = (block35 >>> 1) & 140737488355327L;
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 1L) << 46) | (block36 >>> 18);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 262143L) << 29) | (block37 >>> 35);
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 34359738367L) << 12) | (block38 >>> 52);
        values[vi++] = (block38 >>> 5) & 140737488355327L;
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 31L) << 42) | (block39 >>> 22);
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 4194303L) << 25) | (block40 >>> 39);
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 549755813887L) << 8) | (block41 >>> 56);
        values[vi++] = (block41 >>> 9) & 140737488355327L;
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 511L) << 38) | (block42 >>> 26);
        final long block43 = blocks[bi++];
        values[vi++] = ((block42 & 67108863L) << 21) | (block43 >>> 43);
        final long block44 = blocks[bi++];
        values[vi++] = ((block43 & 8796093022207L) << 4) | (block44 >>> 60);
        values[vi++] = (block44 >>> 13) & 140737488355327L;
        final long block45 = blocks[bi++];
        values[vi++] = ((block44 & 8191L) << 34) | (block45 >>> 30);
        final long block46 = blocks[bi++];
        values[vi++] = ((block45 & 1073741823L) << 17) | (block46 >>> 47);
        values[vi++] = block46 & 140737488355327L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 17) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 4) | (values[vi] >>> 43);
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 8) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 12) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 16) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 3) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 7) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 11) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 15) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi++] << 2) | (values[vi] >>> 45);
        blocks[bi++] = (values[vi++] << 19) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 6) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 10) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 14) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi++] << 1) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 5) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 9) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 13) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation48 extends BulkOperation {

    public int blocks() {
      return 3;
    }

    public int values() {
      return 4;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 16;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 65535L) << 32) | (block1 >>> 32);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 4294967295L) << 16) | (block2 >>> 48);
        values[vi++] = block2 & 281474976710655L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation49 extends BulkOperation {

    public int blocks() {
      return 49;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 15;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 32767L) << 34) | (block1 >>> 30);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 1073741823L) << 19) | (block2 >>> 45);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 35184372088831L) << 4) | (block3 >>> 60);
        values[vi++] = (block3 >>> 11) & 562949953421311L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 2047L) << 38) | (block4 >>> 26);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 67108863L) << 23) | (block5 >>> 41);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 2199023255551L) << 8) | (block6 >>> 56);
        values[vi++] = (block6 >>> 7) & 562949953421311L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 127L) << 42) | (block7 >>> 22);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 4194303L) << 27) | (block8 >>> 37);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 137438953471L) << 12) | (block9 >>> 52);
        values[vi++] = (block9 >>> 3) & 562949953421311L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 7L) << 46) | (block10 >>> 18);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 262143L) << 31) | (block11 >>> 33);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 8589934591L) << 16) | (block12 >>> 48);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 281474976710655L) << 1) | (block13 >>> 63);
        values[vi++] = (block13 >>> 14) & 562949953421311L;
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 16383L) << 35) | (block14 >>> 29);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 536870911L) << 20) | (block15 >>> 44);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 17592186044415L) << 5) | (block16 >>> 59);
        values[vi++] = (block16 >>> 10) & 562949953421311L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 1023L) << 39) | (block17 >>> 25);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 33554431L) << 24) | (block18 >>> 40);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 1099511627775L) << 9) | (block19 >>> 55);
        values[vi++] = (block19 >>> 6) & 562949953421311L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 63L) << 43) | (block20 >>> 21);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 2097151L) << 28) | (block21 >>> 36);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 68719476735L) << 13) | (block22 >>> 51);
        values[vi++] = (block22 >>> 2) & 562949953421311L;
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 3L) << 47) | (block23 >>> 17);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 131071L) << 32) | (block24 >>> 32);
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 4294967295L) << 17) | (block25 >>> 47);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 140737488355327L) << 2) | (block26 >>> 62);
        values[vi++] = (block26 >>> 13) & 562949953421311L;
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 8191L) << 36) | (block27 >>> 28);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 268435455L) << 21) | (block28 >>> 43);
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 8796093022207L) << 6) | (block29 >>> 58);
        values[vi++] = (block29 >>> 9) & 562949953421311L;
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 511L) << 40) | (block30 >>> 24);
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 16777215L) << 25) | (block31 >>> 39);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 549755813887L) << 10) | (block32 >>> 54);
        values[vi++] = (block32 >>> 5) & 562949953421311L;
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 31L) << 44) | (block33 >>> 20);
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 1048575L) << 29) | (block34 >>> 35);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 34359738367L) << 14) | (block35 >>> 50);
        values[vi++] = (block35 >>> 1) & 562949953421311L;
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 1L) << 48) | (block36 >>> 16);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 65535L) << 33) | (block37 >>> 31);
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 2147483647L) << 18) | (block38 >>> 46);
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 70368744177663L) << 3) | (block39 >>> 61);
        values[vi++] = (block39 >>> 12) & 562949953421311L;
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 4095L) << 37) | (block40 >>> 27);
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 134217727L) << 22) | (block41 >>> 42);
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 4398046511103L) << 7) | (block42 >>> 57);
        values[vi++] = (block42 >>> 8) & 562949953421311L;
        final long block43 = blocks[bi++];
        values[vi++] = ((block42 & 255L) << 41) | (block43 >>> 23);
        final long block44 = blocks[bi++];
        values[vi++] = ((block43 & 8388607L) << 26) | (block44 >>> 38);
        final long block45 = blocks[bi++];
        values[vi++] = ((block44 & 274877906943L) << 11) | (block45 >>> 53);
        values[vi++] = (block45 >>> 4) & 562949953421311L;
        final long block46 = blocks[bi++];
        values[vi++] = ((block45 & 15L) << 45) | (block46 >>> 19);
        final long block47 = blocks[bi++];
        values[vi++] = ((block46 & 524287L) << 30) | (block47 >>> 34);
        final long block48 = blocks[bi++];
        values[vi++] = ((block47 & 17179869183L) << 15) | (block48 >>> 49);
        values[vi++] = block48 & 562949953421311L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 15) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 11) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 7) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 3) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 14) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 10) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 6) | (values[vi] >>> 43);
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi++] << 2) | (values[vi] >>> 47);
        blocks[bi++] = (values[vi++] << 17) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 13) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 9) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 5) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi++] << 1) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 12) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 8) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 4) | (values[vi] >>> 45);
        blocks[bi++] = (values[vi++] << 19) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation50 extends BulkOperation {

    public int blocks() {
      return 25;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 14;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 16383L) << 36) | (block1 >>> 28);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 268435455L) << 22) | (block2 >>> 42);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 4398046511103L) << 8) | (block3 >>> 56);
        values[vi++] = (block3 >>> 6) & 1125899906842623L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 63L) << 44) | (block4 >>> 20);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1048575L) << 30) | (block5 >>> 34);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 17179869183L) << 16) | (block6 >>> 48);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 281474976710655L) << 2) | (block7 >>> 62);
        values[vi++] = (block7 >>> 12) & 1125899906842623L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 4095L) << 38) | (block8 >>> 26);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 67108863L) << 24) | (block9 >>> 40);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1099511627775L) << 10) | (block10 >>> 54);
        values[vi++] = (block10 >>> 4) & 1125899906842623L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 15L) << 46) | (block11 >>> 18);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 262143L) << 32) | (block12 >>> 32);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 4294967295L) << 18) | (block13 >>> 46);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 70368744177663L) << 4) | (block14 >>> 60);
        values[vi++] = (block14 >>> 10) & 1125899906842623L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 1023L) << 40) | (block15 >>> 24);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 16777215L) << 26) | (block16 >>> 38);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 274877906943L) << 12) | (block17 >>> 52);
        values[vi++] = (block17 >>> 2) & 1125899906842623L;
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 3L) << 48) | (block18 >>> 16);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 65535L) << 34) | (block19 >>> 30);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 1073741823L) << 20) | (block20 >>> 44);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 17592186044415L) << 6) | (block21 >>> 58);
        values[vi++] = (block21 >>> 8) & 1125899906842623L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 255L) << 42) | (block22 >>> 22);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 4194303L) << 28) | (block23 >>> 36);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 68719476735L) << 14) | (block24 >>> 50);
        values[vi++] = block24 & 1125899906842623L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 6) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 12) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 4) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 10) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 2) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 8) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation51 extends BulkOperation {

    public int blocks() {
      return 51;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 13;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 8191L) << 38) | (block1 >>> 26);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 67108863L) << 25) | (block2 >>> 39);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 549755813887L) << 12) | (block3 >>> 52);
        values[vi++] = (block3 >>> 1) & 2251799813685247L;
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 1L) << 50) | (block4 >>> 14);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 16383L) << 37) | (block5 >>> 27);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 134217727L) << 24) | (block6 >>> 40);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 1099511627775L) << 11) | (block7 >>> 53);
        values[vi++] = (block7 >>> 2) & 2251799813685247L;
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 3L) << 49) | (block8 >>> 15);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 32767L) << 36) | (block9 >>> 28);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 268435455L) << 23) | (block10 >>> 41);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 2199023255551L) << 10) | (block11 >>> 54);
        values[vi++] = (block11 >>> 3) & 2251799813685247L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 7L) << 48) | (block12 >>> 16);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 65535L) << 35) | (block13 >>> 29);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 536870911L) << 22) | (block14 >>> 42);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 4398046511103L) << 9) | (block15 >>> 55);
        values[vi++] = (block15 >>> 4) & 2251799813685247L;
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 15L) << 47) | (block16 >>> 17);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 131071L) << 34) | (block17 >>> 30);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 1073741823L) << 21) | (block18 >>> 43);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 8796093022207L) << 8) | (block19 >>> 56);
        values[vi++] = (block19 >>> 5) & 2251799813685247L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 31L) << 46) | (block20 >>> 18);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 262143L) << 33) | (block21 >>> 31);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 2147483647L) << 20) | (block22 >>> 44);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 17592186044415L) << 7) | (block23 >>> 57);
        values[vi++] = (block23 >>> 6) & 2251799813685247L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 63L) << 45) | (block24 >>> 19);
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 524287L) << 32) | (block25 >>> 32);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 4294967295L) << 19) | (block26 >>> 45);
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 35184372088831L) << 6) | (block27 >>> 58);
        values[vi++] = (block27 >>> 7) & 2251799813685247L;
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 127L) << 44) | (block28 >>> 20);
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 1048575L) << 31) | (block29 >>> 33);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 8589934591L) << 18) | (block30 >>> 46);
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 70368744177663L) << 5) | (block31 >>> 59);
        values[vi++] = (block31 >>> 8) & 2251799813685247L;
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 255L) << 43) | (block32 >>> 21);
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 2097151L) << 30) | (block33 >>> 34);
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 17179869183L) << 17) | (block34 >>> 47);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 140737488355327L) << 4) | (block35 >>> 60);
        values[vi++] = (block35 >>> 9) & 2251799813685247L;
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 511L) << 42) | (block36 >>> 22);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 4194303L) << 29) | (block37 >>> 35);
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 34359738367L) << 16) | (block38 >>> 48);
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 281474976710655L) << 3) | (block39 >>> 61);
        values[vi++] = (block39 >>> 10) & 2251799813685247L;
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 1023L) << 41) | (block40 >>> 23);
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 8388607L) << 28) | (block41 >>> 36);
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 68719476735L) << 15) | (block42 >>> 49);
        final long block43 = blocks[bi++];
        values[vi++] = ((block42 & 562949953421311L) << 2) | (block43 >>> 62);
        values[vi++] = (block43 >>> 11) & 2251799813685247L;
        final long block44 = blocks[bi++];
        values[vi++] = ((block43 & 2047L) << 40) | (block44 >>> 24);
        final long block45 = blocks[bi++];
        values[vi++] = ((block44 & 16777215L) << 27) | (block45 >>> 37);
        final long block46 = blocks[bi++];
        values[vi++] = ((block45 & 137438953471L) << 14) | (block46 >>> 50);
        final long block47 = blocks[bi++];
        values[vi++] = ((block46 & 1125899906842623L) << 1) | (block47 >>> 63);
        values[vi++] = (block47 >>> 12) & 2251799813685247L;
        final long block48 = blocks[bi++];
        values[vi++] = ((block47 & 4095L) << 39) | (block48 >>> 25);
        final long block49 = blocks[bi++];
        values[vi++] = ((block48 & 33554431L) << 26) | (block49 >>> 38);
        final long block50 = blocks[bi++];
        values[vi++] = ((block49 & 274877906943L) << 13) | (block50 >>> 51);
        values[vi++] = block50 & 2251799813685247L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 13) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi++] << 1) | (values[vi] >>> 50);
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi++] << 2) | (values[vi] >>> 49);
        blocks[bi++] = (values[vi++] << 15) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 3) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 4) | (values[vi] >>> 47);
        blocks[bi++] = (values[vi++] << 17) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 5) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 6) | (values[vi] >>> 45);
        blocks[bi++] = (values[vi++] << 19) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 7) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 8) | (values[vi] >>> 43);
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 9) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 10) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 11) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 12) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation52 extends BulkOperation {

    public int blocks() {
      return 13;
    }

    public int values() {
      return 16;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 12;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 4095L) << 40) | (block1 >>> 24);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 16777215L) << 28) | (block2 >>> 36);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 68719476735L) << 16) | (block3 >>> 48);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 281474976710655L) << 4) | (block4 >>> 60);
        values[vi++] = (block4 >>> 8) & 4503599627370495L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 255L) << 44) | (block5 >>> 20);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 1048575L) << 32) | (block6 >>> 32);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 4294967295L) << 20) | (block7 >>> 44);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 17592186044415L) << 8) | (block8 >>> 56);
        values[vi++] = (block8 >>> 4) & 4503599627370495L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 15L) << 48) | (block9 >>> 16);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 65535L) << 36) | (block10 >>> 28);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 268435455L) << 24) | (block11 >>> 40);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 1099511627775L) << 12) | (block12 >>> 52);
        values[vi++] = block12 & 4503599627370495L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 8) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 4) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation53 extends BulkOperation {

    public int blocks() {
      return 53;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 11;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 2047L) << 42) | (block1 >>> 22);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 4194303L) << 31) | (block2 >>> 33);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 8589934591L) << 20) | (block3 >>> 44);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 17592186044415L) << 9) | (block4 >>> 55);
        values[vi++] = (block4 >>> 2) & 9007199254740991L;
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 3L) << 51) | (block5 >>> 13);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 8191L) << 40) | (block6 >>> 24);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 16777215L) << 29) | (block7 >>> 35);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 34359738367L) << 18) | (block8 >>> 46);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 70368744177663L) << 7) | (block9 >>> 57);
        values[vi++] = (block9 >>> 4) & 9007199254740991L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 15L) << 49) | (block10 >>> 15);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 32767L) << 38) | (block11 >>> 26);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 67108863L) << 27) | (block12 >>> 37);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 137438953471L) << 16) | (block13 >>> 48);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 281474976710655L) << 5) | (block14 >>> 59);
        values[vi++] = (block14 >>> 6) & 9007199254740991L;
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 63L) << 47) | (block15 >>> 17);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 131071L) << 36) | (block16 >>> 28);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 268435455L) << 25) | (block17 >>> 39);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 549755813887L) << 14) | (block18 >>> 50);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 1125899906842623L) << 3) | (block19 >>> 61);
        values[vi++] = (block19 >>> 8) & 9007199254740991L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 255L) << 45) | (block20 >>> 19);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 524287L) << 34) | (block21 >>> 30);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 1073741823L) << 23) | (block22 >>> 41);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 2199023255551L) << 12) | (block23 >>> 52);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 4503599627370495L) << 1) | (block24 >>> 63);
        values[vi++] = (block24 >>> 10) & 9007199254740991L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 1023L) << 43) | (block25 >>> 21);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 2097151L) << 32) | (block26 >>> 32);
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 4294967295L) << 21) | (block27 >>> 43);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 8796093022207L) << 10) | (block28 >>> 54);
        values[vi++] = (block28 >>> 1) & 9007199254740991L;
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 1L) << 52) | (block29 >>> 12);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 4095L) << 41) | (block30 >>> 23);
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 8388607L) << 30) | (block31 >>> 34);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 17179869183L) << 19) | (block32 >>> 45);
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 35184372088831L) << 8) | (block33 >>> 56);
        values[vi++] = (block33 >>> 3) & 9007199254740991L;
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 7L) << 50) | (block34 >>> 14);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 16383L) << 39) | (block35 >>> 25);
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 33554431L) << 28) | (block36 >>> 36);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 68719476735L) << 17) | (block37 >>> 47);
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 140737488355327L) << 6) | (block38 >>> 58);
        values[vi++] = (block38 >>> 5) & 9007199254740991L;
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 31L) << 48) | (block39 >>> 16);
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 65535L) << 37) | (block40 >>> 27);
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 134217727L) << 26) | (block41 >>> 38);
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 274877906943L) << 15) | (block42 >>> 49);
        final long block43 = blocks[bi++];
        values[vi++] = ((block42 & 562949953421311L) << 4) | (block43 >>> 60);
        values[vi++] = (block43 >>> 7) & 9007199254740991L;
        final long block44 = blocks[bi++];
        values[vi++] = ((block43 & 127L) << 46) | (block44 >>> 18);
        final long block45 = blocks[bi++];
        values[vi++] = ((block44 & 262143L) << 35) | (block45 >>> 29);
        final long block46 = blocks[bi++];
        values[vi++] = ((block45 & 536870911L) << 24) | (block46 >>> 40);
        final long block47 = blocks[bi++];
        values[vi++] = ((block46 & 1099511627775L) << 13) | (block47 >>> 51);
        final long block48 = blocks[bi++];
        values[vi++] = ((block47 & 2251799813685247L) << 2) | (block48 >>> 62);
        values[vi++] = (block48 >>> 9) & 9007199254740991L;
        final long block49 = blocks[bi++];
        values[vi++] = ((block48 & 511L) << 44) | (block49 >>> 20);
        final long block50 = blocks[bi++];
        values[vi++] = ((block49 & 1048575L) << 33) | (block50 >>> 31);
        final long block51 = blocks[bi++];
        values[vi++] = ((block50 & 2147483647L) << 22) | (block51 >>> 42);
        final long block52 = blocks[bi++];
        values[vi++] = ((block51 & 4398046511103L) << 11) | (block52 >>> 53);
        values[vi++] = block52 & 9007199254740991L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 11) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi++] << 2) | (values[vi] >>> 51);
        blocks[bi++] = (values[vi++] << 13) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 4) | (values[vi] >>> 49);
        blocks[bi++] = (values[vi++] << 15) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 6) | (values[vi] >>> 47);
        blocks[bi++] = (values[vi++] << 17) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 8) | (values[vi] >>> 45);
        blocks[bi++] = (values[vi++] << 19) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 10) | (values[vi] >>> 43);
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi++] << 1) | (values[vi] >>> 52);
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 3) | (values[vi] >>> 50);
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 5) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 7) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 9) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation54 extends BulkOperation {

    public int blocks() {
      return 27;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 10;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1023L) << 44) | (block1 >>> 20);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 1048575L) << 34) | (block2 >>> 30);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 1073741823L) << 24) | (block3 >>> 40);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 1099511627775L) << 14) | (block4 >>> 50);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1125899906842623L) << 4) | (block5 >>> 60);
        values[vi++] = (block5 >>> 6) & 18014398509481983L;
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 63L) << 48) | (block6 >>> 16);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 65535L) << 38) | (block7 >>> 26);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 67108863L) << 28) | (block8 >>> 36);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 68719476735L) << 18) | (block9 >>> 46);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 70368744177663L) << 8) | (block10 >>> 56);
        values[vi++] = (block10 >>> 2) & 18014398509481983L;
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 3L) << 52) | (block11 >>> 12);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 4095L) << 42) | (block12 >>> 22);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 4194303L) << 32) | (block13 >>> 32);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 4294967295L) << 22) | (block14 >>> 42);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 4398046511103L) << 12) | (block15 >>> 52);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 4503599627370495L) << 2) | (block16 >>> 62);
        values[vi++] = (block16 >>> 8) & 18014398509481983L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 255L) << 46) | (block17 >>> 18);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 262143L) << 36) | (block18 >>> 28);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 268435455L) << 26) | (block19 >>> 38);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 274877906943L) << 16) | (block20 >>> 48);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 281474976710655L) << 6) | (block21 >>> 58);
        values[vi++] = (block21 >>> 4) & 18014398509481983L;
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 15L) << 50) | (block22 >>> 14);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 16383L) << 40) | (block23 >>> 24);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 16777215L) << 30) | (block24 >>> 34);
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 17179869183L) << 20) | (block25 >>> 44);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 17592186044415L) << 10) | (block26 >>> 54);
        values[vi++] = block26 & 18014398509481983L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 10) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 6) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 2) | (values[vi] >>> 52);
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 8) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 4) | (values[vi] >>> 50);
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation55 extends BulkOperation {

    public int blocks() {
      return 55;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 9;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 511L) << 46) | (block1 >>> 18);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 262143L) << 37) | (block2 >>> 27);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 134217727L) << 28) | (block3 >>> 36);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 68719476735L) << 19) | (block4 >>> 45);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 35184372088831L) << 10) | (block5 >>> 54);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 18014398509481983L) << 1) | (block6 >>> 63);
        values[vi++] = (block6 >>> 8) & 36028797018963967L;
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 255L) << 47) | (block7 >>> 17);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 131071L) << 38) | (block8 >>> 26);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 67108863L) << 29) | (block9 >>> 35);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 34359738367L) << 20) | (block10 >>> 44);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 17592186044415L) << 11) | (block11 >>> 53);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 9007199254740991L) << 2) | (block12 >>> 62);
        values[vi++] = (block12 >>> 7) & 36028797018963967L;
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 127L) << 48) | (block13 >>> 16);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 65535L) << 39) | (block14 >>> 25);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 33554431L) << 30) | (block15 >>> 34);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 17179869183L) << 21) | (block16 >>> 43);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 8796093022207L) << 12) | (block17 >>> 52);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 4503599627370495L) << 3) | (block18 >>> 61);
        values[vi++] = (block18 >>> 6) & 36028797018963967L;
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 63L) << 49) | (block19 >>> 15);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 32767L) << 40) | (block20 >>> 24);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 16777215L) << 31) | (block21 >>> 33);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 8589934591L) << 22) | (block22 >>> 42);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 4398046511103L) << 13) | (block23 >>> 51);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 2251799813685247L) << 4) | (block24 >>> 60);
        values[vi++] = (block24 >>> 5) & 36028797018963967L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 31L) << 50) | (block25 >>> 14);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 16383L) << 41) | (block26 >>> 23);
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 8388607L) << 32) | (block27 >>> 32);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 4294967295L) << 23) | (block28 >>> 41);
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 2199023255551L) << 14) | (block29 >>> 50);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 1125899906842623L) << 5) | (block30 >>> 59);
        values[vi++] = (block30 >>> 4) & 36028797018963967L;
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 15L) << 51) | (block31 >>> 13);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 8191L) << 42) | (block32 >>> 22);
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 4194303L) << 33) | (block33 >>> 31);
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 2147483647L) << 24) | (block34 >>> 40);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 1099511627775L) << 15) | (block35 >>> 49);
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 562949953421311L) << 6) | (block36 >>> 58);
        values[vi++] = (block36 >>> 3) & 36028797018963967L;
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 7L) << 52) | (block37 >>> 12);
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 4095L) << 43) | (block38 >>> 21);
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 2097151L) << 34) | (block39 >>> 30);
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 1073741823L) << 25) | (block40 >>> 39);
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 549755813887L) << 16) | (block41 >>> 48);
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 281474976710655L) << 7) | (block42 >>> 57);
        values[vi++] = (block42 >>> 2) & 36028797018963967L;
        final long block43 = blocks[bi++];
        values[vi++] = ((block42 & 3L) << 53) | (block43 >>> 11);
        final long block44 = blocks[bi++];
        values[vi++] = ((block43 & 2047L) << 44) | (block44 >>> 20);
        final long block45 = blocks[bi++];
        values[vi++] = ((block44 & 1048575L) << 35) | (block45 >>> 29);
        final long block46 = blocks[bi++];
        values[vi++] = ((block45 & 536870911L) << 26) | (block46 >>> 38);
        final long block47 = blocks[bi++];
        values[vi++] = ((block46 & 274877906943L) << 17) | (block47 >>> 47);
        final long block48 = blocks[bi++];
        values[vi++] = ((block47 & 140737488355327L) << 8) | (block48 >>> 56);
        values[vi++] = (block48 >>> 1) & 36028797018963967L;
        final long block49 = blocks[bi++];
        values[vi++] = ((block48 & 1L) << 54) | (block49 >>> 10);
        final long block50 = blocks[bi++];
        values[vi++] = ((block49 & 1023L) << 45) | (block50 >>> 19);
        final long block51 = blocks[bi++];
        values[vi++] = ((block50 & 524287L) << 36) | (block51 >>> 28);
        final long block52 = blocks[bi++];
        values[vi++] = ((block51 & 268435455L) << 27) | (block52 >>> 37);
        final long block53 = blocks[bi++];
        values[vi++] = ((block52 & 137438953471L) << 18) | (block53 >>> 46);
        final long block54 = blocks[bi++];
        values[vi++] = ((block53 & 70368744177663L) << 9) | (block54 >>> 55);
        values[vi++] = block54 & 36028797018963967L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 9) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 8) | (values[vi] >>> 47);
        blocks[bi++] = (values[vi++] << 17) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 7) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 6) | (values[vi] >>> 49);
        blocks[bi++] = (values[vi++] << 15) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 5) | (values[vi] >>> 50);
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 4) | (values[vi] >>> 51);
        blocks[bi++] = (values[vi++] << 13) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 3) | (values[vi] >>> 52);
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 43);
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi++] << 2) | (values[vi] >>> 53);
        blocks[bi++] = (values[vi++] << 11) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi++] << 1) | (values[vi] >>> 54);
        blocks[bi++] = (values[vi++] << 10) | (values[vi] >>> 45);
        blocks[bi++] = (values[vi++] << 19) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation56 extends BulkOperation {

    public int blocks() {
      return 7;
    }

    public int values() {
      return 8;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 8;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 255L) << 48) | (block1 >>> 16);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 65535L) << 40) | (block2 >>> 24);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 16777215L) << 32) | (block3 >>> 32);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 4294967295L) << 24) | (block4 >>> 40);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1099511627775L) << 16) | (block5 >>> 48);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 281474976710655L) << 8) | (block6 >>> 56);
        values[vi++] = block6 & 72057594037927935L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 8) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation57 extends BulkOperation {

    public int blocks() {
      return 57;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 7;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 127L) << 50) | (block1 >>> 14);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 16383L) << 43) | (block2 >>> 21);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 2097151L) << 36) | (block3 >>> 28);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 268435455L) << 29) | (block4 >>> 35);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 34359738367L) << 22) | (block5 >>> 42);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 4398046511103L) << 15) | (block6 >>> 49);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 562949953421311L) << 8) | (block7 >>> 56);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 72057594037927935L) << 1) | (block8 >>> 63);
        values[vi++] = (block8 >>> 6) & 144115188075855871L;
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 63L) << 51) | (block9 >>> 13);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 8191L) << 44) | (block10 >>> 20);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 1048575L) << 37) | (block11 >>> 27);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 134217727L) << 30) | (block12 >>> 34);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 17179869183L) << 23) | (block13 >>> 41);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 2199023255551L) << 16) | (block14 >>> 48);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 281474976710655L) << 9) | (block15 >>> 55);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 36028797018963967L) << 2) | (block16 >>> 62);
        values[vi++] = (block16 >>> 5) & 144115188075855871L;
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 31L) << 52) | (block17 >>> 12);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 4095L) << 45) | (block18 >>> 19);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 524287L) << 38) | (block19 >>> 26);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 67108863L) << 31) | (block20 >>> 33);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 8589934591L) << 24) | (block21 >>> 40);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 1099511627775L) << 17) | (block22 >>> 47);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 140737488355327L) << 10) | (block23 >>> 54);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 18014398509481983L) << 3) | (block24 >>> 61);
        values[vi++] = (block24 >>> 4) & 144115188075855871L;
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 15L) << 53) | (block25 >>> 11);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 2047L) << 46) | (block26 >>> 18);
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 262143L) << 39) | (block27 >>> 25);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 33554431L) << 32) | (block28 >>> 32);
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 4294967295L) << 25) | (block29 >>> 39);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 549755813887L) << 18) | (block30 >>> 46);
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 70368744177663L) << 11) | (block31 >>> 53);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 9007199254740991L) << 4) | (block32 >>> 60);
        values[vi++] = (block32 >>> 3) & 144115188075855871L;
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 7L) << 54) | (block33 >>> 10);
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 1023L) << 47) | (block34 >>> 17);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 131071L) << 40) | (block35 >>> 24);
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 16777215L) << 33) | (block36 >>> 31);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 2147483647L) << 26) | (block37 >>> 38);
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 274877906943L) << 19) | (block38 >>> 45);
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 35184372088831L) << 12) | (block39 >>> 52);
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 4503599627370495L) << 5) | (block40 >>> 59);
        values[vi++] = (block40 >>> 2) & 144115188075855871L;
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 3L) << 55) | (block41 >>> 9);
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 511L) << 48) | (block42 >>> 16);
        final long block43 = blocks[bi++];
        values[vi++] = ((block42 & 65535L) << 41) | (block43 >>> 23);
        final long block44 = blocks[bi++];
        values[vi++] = ((block43 & 8388607L) << 34) | (block44 >>> 30);
        final long block45 = blocks[bi++];
        values[vi++] = ((block44 & 1073741823L) << 27) | (block45 >>> 37);
        final long block46 = blocks[bi++];
        values[vi++] = ((block45 & 137438953471L) << 20) | (block46 >>> 44);
        final long block47 = blocks[bi++];
        values[vi++] = ((block46 & 17592186044415L) << 13) | (block47 >>> 51);
        final long block48 = blocks[bi++];
        values[vi++] = ((block47 & 2251799813685247L) << 6) | (block48 >>> 58);
        values[vi++] = (block48 >>> 1) & 144115188075855871L;
        final long block49 = blocks[bi++];
        values[vi++] = ((block48 & 1L) << 56) | (block49 >>> 8);
        final long block50 = blocks[bi++];
        values[vi++] = ((block49 & 255L) << 49) | (block50 >>> 15);
        final long block51 = blocks[bi++];
        values[vi++] = ((block50 & 32767L) << 42) | (block51 >>> 22);
        final long block52 = blocks[bi++];
        values[vi++] = ((block51 & 4194303L) << 35) | (block52 >>> 29);
        final long block53 = blocks[bi++];
        values[vi++] = ((block52 & 536870911L) << 28) | (block53 >>> 36);
        final long block54 = blocks[bi++];
        values[vi++] = ((block53 & 68719476735L) << 21) | (block54 >>> 43);
        final long block55 = blocks[bi++];
        values[vi++] = ((block54 & 8796093022207L) << 14) | (block55 >>> 50);
        final long block56 = blocks[bi++];
        values[vi++] = ((block55 & 1125899906842623L) << 7) | (block56 >>> 57);
        values[vi++] = block56 & 144115188075855871L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 7) | (values[vi] >>> 50);
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 43);
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 6) | (values[vi] >>> 51);
        blocks[bi++] = (values[vi++] << 13) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 5) | (values[vi] >>> 52);
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 45);
        blocks[bi++] = (values[vi++] << 19) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 4) | (values[vi] >>> 53);
        blocks[bi++] = (values[vi++] << 11) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 3) | (values[vi] >>> 54);
        blocks[bi++] = (values[vi++] << 10) | (values[vi] >>> 47);
        blocks[bi++] = (values[vi++] << 17) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi++] << 2) | (values[vi] >>> 55);
        blocks[bi++] = (values[vi++] << 9) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi++] << 1) | (values[vi] >>> 56);
        blocks[bi++] = (values[vi++] << 8) | (values[vi] >>> 49);
        blocks[bi++] = (values[vi++] << 15) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation58 extends BulkOperation {

    public int blocks() {
      return 29;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 6;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 63L) << 52) | (block1 >>> 12);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 4095L) << 46) | (block2 >>> 18);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 262143L) << 40) | (block3 >>> 24);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 16777215L) << 34) | (block4 >>> 30);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1073741823L) << 28) | (block5 >>> 36);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 68719476735L) << 22) | (block6 >>> 42);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 4398046511103L) << 16) | (block7 >>> 48);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 281474976710655L) << 10) | (block8 >>> 54);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 18014398509481983L) << 4) | (block9 >>> 60);
        values[vi++] = (block9 >>> 2) & 288230376151711743L;
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 3L) << 56) | (block10 >>> 8);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 255L) << 50) | (block11 >>> 14);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 16383L) << 44) | (block12 >>> 20);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 1048575L) << 38) | (block13 >>> 26);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 67108863L) << 32) | (block14 >>> 32);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 4294967295L) << 26) | (block15 >>> 38);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 274877906943L) << 20) | (block16 >>> 44);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 17592186044415L) << 14) | (block17 >>> 50);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 1125899906842623L) << 8) | (block18 >>> 56);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 72057594037927935L) << 2) | (block19 >>> 62);
        values[vi++] = (block19 >>> 4) & 288230376151711743L;
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 15L) << 54) | (block20 >>> 10);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 1023L) << 48) | (block21 >>> 16);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 65535L) << 42) | (block22 >>> 22);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 4194303L) << 36) | (block23 >>> 28);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 268435455L) << 30) | (block24 >>> 34);
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 17179869183L) << 24) | (block25 >>> 40);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 1099511627775L) << 18) | (block26 >>> 46);
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 70368744177663L) << 12) | (block27 >>> 52);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 4503599627370495L) << 6) | (block28 >>> 58);
        values[vi++] = block28 & 288230376151711743L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 6) | (values[vi] >>> 52);
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 2) | (values[vi] >>> 56);
        blocks[bi++] = (values[vi++] << 8) | (values[vi] >>> 50);
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 4) | (values[vi] >>> 54);
        blocks[bi++] = (values[vi++] << 10) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation59 extends BulkOperation {

    public int blocks() {
      return 59;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 5;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 31L) << 54) | (block1 >>> 10);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 1023L) << 49) | (block2 >>> 15);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 32767L) << 44) | (block3 >>> 20);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 1048575L) << 39) | (block4 >>> 25);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 33554431L) << 34) | (block5 >>> 30);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 1073741823L) << 29) | (block6 >>> 35);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 34359738367L) << 24) | (block7 >>> 40);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 1099511627775L) << 19) | (block8 >>> 45);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 35184372088831L) << 14) | (block9 >>> 50);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1125899906842623L) << 9) | (block10 >>> 55);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 36028797018963967L) << 4) | (block11 >>> 60);
        values[vi++] = (block11 >>> 1) & 576460752303423487L;
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 1L) << 58) | (block12 >>> 6);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 63L) << 53) | (block13 >>> 11);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 2047L) << 48) | (block14 >>> 16);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 65535L) << 43) | (block15 >>> 21);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 2097151L) << 38) | (block16 >>> 26);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 67108863L) << 33) | (block17 >>> 31);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 2147483647L) << 28) | (block18 >>> 36);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 68719476735L) << 23) | (block19 >>> 41);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 2199023255551L) << 18) | (block20 >>> 46);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 70368744177663L) << 13) | (block21 >>> 51);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 2251799813685247L) << 8) | (block22 >>> 56);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 72057594037927935L) << 3) | (block23 >>> 61);
        values[vi++] = (block23 >>> 2) & 576460752303423487L;
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 3L) << 57) | (block24 >>> 7);
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 127L) << 52) | (block25 >>> 12);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 4095L) << 47) | (block26 >>> 17);
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 131071L) << 42) | (block27 >>> 22);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 4194303L) << 37) | (block28 >>> 27);
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 134217727L) << 32) | (block29 >>> 32);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 4294967295L) << 27) | (block30 >>> 37);
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 137438953471L) << 22) | (block31 >>> 42);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 4398046511103L) << 17) | (block32 >>> 47);
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 140737488355327L) << 12) | (block33 >>> 52);
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 4503599627370495L) << 7) | (block34 >>> 57);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 144115188075855871L) << 2) | (block35 >>> 62);
        values[vi++] = (block35 >>> 3) & 576460752303423487L;
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 7L) << 56) | (block36 >>> 8);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 255L) << 51) | (block37 >>> 13);
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 8191L) << 46) | (block38 >>> 18);
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 262143L) << 41) | (block39 >>> 23);
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 8388607L) << 36) | (block40 >>> 28);
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 268435455L) << 31) | (block41 >>> 33);
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 8589934591L) << 26) | (block42 >>> 38);
        final long block43 = blocks[bi++];
        values[vi++] = ((block42 & 274877906943L) << 21) | (block43 >>> 43);
        final long block44 = blocks[bi++];
        values[vi++] = ((block43 & 8796093022207L) << 16) | (block44 >>> 48);
        final long block45 = blocks[bi++];
        values[vi++] = ((block44 & 281474976710655L) << 11) | (block45 >>> 53);
        final long block46 = blocks[bi++];
        values[vi++] = ((block45 & 9007199254740991L) << 6) | (block46 >>> 58);
        final long block47 = blocks[bi++];
        values[vi++] = ((block46 & 288230376151711743L) << 1) | (block47 >>> 63);
        values[vi++] = (block47 >>> 4) & 576460752303423487L;
        final long block48 = blocks[bi++];
        values[vi++] = ((block47 & 15L) << 55) | (block48 >>> 9);
        final long block49 = blocks[bi++];
        values[vi++] = ((block48 & 511L) << 50) | (block49 >>> 14);
        final long block50 = blocks[bi++];
        values[vi++] = ((block49 & 16383L) << 45) | (block50 >>> 19);
        final long block51 = blocks[bi++];
        values[vi++] = ((block50 & 524287L) << 40) | (block51 >>> 24);
        final long block52 = blocks[bi++];
        values[vi++] = ((block51 & 16777215L) << 35) | (block52 >>> 29);
        final long block53 = blocks[bi++];
        values[vi++] = ((block52 & 536870911L) << 30) | (block53 >>> 34);
        final long block54 = blocks[bi++];
        values[vi++] = ((block53 & 17179869183L) << 25) | (block54 >>> 39);
        final long block55 = blocks[bi++];
        values[vi++] = ((block54 & 549755813887L) << 20) | (block55 >>> 44);
        final long block56 = blocks[bi++];
        values[vi++] = ((block55 & 17592186044415L) << 15) | (block56 >>> 49);
        final long block57 = blocks[bi++];
        values[vi++] = ((block56 & 562949953421311L) << 10) | (block57 >>> 54);
        final long block58 = blocks[bi++];
        values[vi++] = ((block57 & 18014398509481983L) << 5) | (block58 >>> 59);
        values[vi++] = block58 & 576460752303423487L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 5) | (values[vi] >>> 54);
        blocks[bi++] = (values[vi++] << 10) | (values[vi] >>> 49);
        blocks[bi++] = (values[vi++] << 15) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi++] << 1) | (values[vi] >>> 58);
        blocks[bi++] = (values[vi++] << 6) | (values[vi] >>> 53);
        blocks[bi++] = (values[vi++] << 11) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 43);
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi++] << 2) | (values[vi] >>> 57);
        blocks[bi++] = (values[vi++] << 7) | (values[vi] >>> 52);
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 47);
        blocks[bi++] = (values[vi++] << 17) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 3) | (values[vi] >>> 56);
        blocks[bi++] = (values[vi++] << 8) | (values[vi] >>> 51);
        blocks[bi++] = (values[vi++] << 13) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 4) | (values[vi] >>> 55);
        blocks[bi++] = (values[vi++] << 9) | (values[vi] >>> 50);
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 45);
        blocks[bi++] = (values[vi++] << 19) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation60 extends BulkOperation {

    public int blocks() {
      return 15;
    }

    public int values() {
      return 16;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 4;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 15L) << 56) | (block1 >>> 8);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 255L) << 52) | (block2 >>> 12);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 4095L) << 48) | (block3 >>> 16);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 65535L) << 44) | (block4 >>> 20);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1048575L) << 40) | (block5 >>> 24);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 16777215L) << 36) | (block6 >>> 28);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 268435455L) << 32) | (block7 >>> 32);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 4294967295L) << 28) | (block8 >>> 36);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 68719476735L) << 24) | (block9 >>> 40);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1099511627775L) << 20) | (block10 >>> 44);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 17592186044415L) << 16) | (block11 >>> 48);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 281474976710655L) << 12) | (block12 >>> 52);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 4503599627370495L) << 8) | (block13 >>> 56);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 72057594037927935L) << 4) | (block14 >>> 60);
        values[vi++] = block14 & 1152921504606846975L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 4) | (values[vi] >>> 56);
        blocks[bi++] = (values[vi++] << 8) | (values[vi] >>> 52);
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation61 extends BulkOperation {

    public int blocks() {
      return 61;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 3;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 7L) << 58) | (block1 >>> 6);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 63L) << 55) | (block2 >>> 9);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 511L) << 52) | (block3 >>> 12);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 4095L) << 49) | (block4 >>> 15);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 32767L) << 46) | (block5 >>> 18);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 262143L) << 43) | (block6 >>> 21);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 2097151L) << 40) | (block7 >>> 24);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 16777215L) << 37) | (block8 >>> 27);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 134217727L) << 34) | (block9 >>> 30);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1073741823L) << 31) | (block10 >>> 33);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 8589934591L) << 28) | (block11 >>> 36);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 68719476735L) << 25) | (block12 >>> 39);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 549755813887L) << 22) | (block13 >>> 42);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 4398046511103L) << 19) | (block14 >>> 45);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 35184372088831L) << 16) | (block15 >>> 48);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 281474976710655L) << 13) | (block16 >>> 51);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 2251799813685247L) << 10) | (block17 >>> 54);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 18014398509481983L) << 7) | (block18 >>> 57);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 144115188075855871L) << 4) | (block19 >>> 60);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 1152921504606846975L) << 1) | (block20 >>> 63);
        values[vi++] = (block20 >>> 2) & 2305843009213693951L;
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 3L) << 59) | (block21 >>> 5);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 31L) << 56) | (block22 >>> 8);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 255L) << 53) | (block23 >>> 11);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 2047L) << 50) | (block24 >>> 14);
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 16383L) << 47) | (block25 >>> 17);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 131071L) << 44) | (block26 >>> 20);
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 1048575L) << 41) | (block27 >>> 23);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 8388607L) << 38) | (block28 >>> 26);
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 67108863L) << 35) | (block29 >>> 29);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 536870911L) << 32) | (block30 >>> 32);
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 4294967295L) << 29) | (block31 >>> 35);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 34359738367L) << 26) | (block32 >>> 38);
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 274877906943L) << 23) | (block33 >>> 41);
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 2199023255551L) << 20) | (block34 >>> 44);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 17592186044415L) << 17) | (block35 >>> 47);
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 140737488355327L) << 14) | (block36 >>> 50);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 1125899906842623L) << 11) | (block37 >>> 53);
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 9007199254740991L) << 8) | (block38 >>> 56);
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 72057594037927935L) << 5) | (block39 >>> 59);
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 576460752303423487L) << 2) | (block40 >>> 62);
        values[vi++] = (block40 >>> 1) & 2305843009213693951L;
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 1L) << 60) | (block41 >>> 4);
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 15L) << 57) | (block42 >>> 7);
        final long block43 = blocks[bi++];
        values[vi++] = ((block42 & 127L) << 54) | (block43 >>> 10);
        final long block44 = blocks[bi++];
        values[vi++] = ((block43 & 1023L) << 51) | (block44 >>> 13);
        final long block45 = blocks[bi++];
        values[vi++] = ((block44 & 8191L) << 48) | (block45 >>> 16);
        final long block46 = blocks[bi++];
        values[vi++] = ((block45 & 65535L) << 45) | (block46 >>> 19);
        final long block47 = blocks[bi++];
        values[vi++] = ((block46 & 524287L) << 42) | (block47 >>> 22);
        final long block48 = blocks[bi++];
        values[vi++] = ((block47 & 4194303L) << 39) | (block48 >>> 25);
        final long block49 = blocks[bi++];
        values[vi++] = ((block48 & 33554431L) << 36) | (block49 >>> 28);
        final long block50 = blocks[bi++];
        values[vi++] = ((block49 & 268435455L) << 33) | (block50 >>> 31);
        final long block51 = blocks[bi++];
        values[vi++] = ((block50 & 2147483647L) << 30) | (block51 >>> 34);
        final long block52 = blocks[bi++];
        values[vi++] = ((block51 & 17179869183L) << 27) | (block52 >>> 37);
        final long block53 = blocks[bi++];
        values[vi++] = ((block52 & 137438953471L) << 24) | (block53 >>> 40);
        final long block54 = blocks[bi++];
        values[vi++] = ((block53 & 1099511627775L) << 21) | (block54 >>> 43);
        final long block55 = blocks[bi++];
        values[vi++] = ((block54 & 8796093022207L) << 18) | (block55 >>> 46);
        final long block56 = blocks[bi++];
        values[vi++] = ((block55 & 70368744177663L) << 15) | (block56 >>> 49);
        final long block57 = blocks[bi++];
        values[vi++] = ((block56 & 562949953421311L) << 12) | (block57 >>> 52);
        final long block58 = blocks[bi++];
        values[vi++] = ((block57 & 4503599627370495L) << 9) | (block58 >>> 55);
        final long block59 = blocks[bi++];
        values[vi++] = ((block58 & 36028797018963967L) << 6) | (block59 >>> 58);
        final long block60 = blocks[bi++];
        values[vi++] = ((block59 & 288230376151711743L) << 3) | (block60 >>> 61);
        values[vi++] = block60 & 2305843009213693951L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 3) | (values[vi] >>> 58);
        blocks[bi++] = (values[vi++] << 6) | (values[vi] >>> 55);
        blocks[bi++] = (values[vi++] << 9) | (values[vi] >>> 52);
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 49);
        blocks[bi++] = (values[vi++] << 15) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 43);
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | (values[vi++] << 2) | (values[vi] >>> 59);
        blocks[bi++] = (values[vi++] << 5) | (values[vi] >>> 56);
        blocks[bi++] = (values[vi++] << 8) | (values[vi] >>> 53);
        blocks[bi++] = (values[vi++] << 11) | (values[vi] >>> 50);
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 47);
        blocks[bi++] = (values[vi++] << 17) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi++] << 1) | (values[vi] >>> 60);
        blocks[bi++] = (values[vi++] << 4) | (values[vi] >>> 57);
        blocks[bi++] = (values[vi++] << 7) | (values[vi] >>> 54);
        blocks[bi++] = (values[vi++] << 10) | (values[vi] >>> 51);
        blocks[bi++] = (values[vi++] << 13) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 45);
        blocks[bi++] = (values[vi++] << 19) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation62 extends BulkOperation {

    public int blocks() {
      return 31;
    }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 2;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 3L) << 60) | (block1 >>> 4);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 15L) << 58) | (block2 >>> 6);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 63L) << 56) | (block3 >>> 8);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 255L) << 54) | (block4 >>> 10);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 1023L) << 52) | (block5 >>> 12);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 4095L) << 50) | (block6 >>> 14);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 16383L) << 48) | (block7 >>> 16);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 65535L) << 46) | (block8 >>> 18);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 262143L) << 44) | (block9 >>> 20);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1048575L) << 42) | (block10 >>> 22);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 4194303L) << 40) | (block11 >>> 24);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 16777215L) << 38) | (block12 >>> 26);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 67108863L) << 36) | (block13 >>> 28);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 268435455L) << 34) | (block14 >>> 30);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 1073741823L) << 32) | (block15 >>> 32);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 4294967295L) << 30) | (block16 >>> 34);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 17179869183L) << 28) | (block17 >>> 36);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 68719476735L) << 26) | (block18 >>> 38);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 274877906943L) << 24) | (block19 >>> 40);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 1099511627775L) << 22) | (block20 >>> 42);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 4398046511103L) << 20) | (block21 >>> 44);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 17592186044415L) << 18) | (block22 >>> 46);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 70368744177663L) << 16) | (block23 >>> 48);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 281474976710655L) << 14) | (block24 >>> 50);
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 1125899906842623L) << 12) | (block25 >>> 52);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 4503599627370495L) << 10) | (block26 >>> 54);
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 18014398509481983L) << 8) | (block27 >>> 56);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 72057594037927935L) << 6) | (block28 >>> 58);
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 288230376151711743L) << 4) | (block29 >>> 60);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 1152921504606846975L) << 2) | (block30 >>> 62);
        values[vi++] = block30 & 4611686018427387903L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 2) | (values[vi] >>> 60);
        blocks[bi++] = (values[vi++] << 4) | (values[vi] >>> 58);
        blocks[bi++] = (values[vi++] << 6) | (values[vi] >>> 56);
        blocks[bi++] = (values[vi++] << 8) | (values[vi] >>> 54);
        blocks[bi++] = (values[vi++] << 10) | (values[vi] >>> 52);
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 50);
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation63 extends BulkOperation {

    public int blocks() {
      return 63;
    }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[bi++];
        values[vi++] = block0 >>> 1;
        final long block1 = blocks[bi++];
        values[vi++] = ((block0 & 1L) << 62) | (block1 >>> 2);
        final long block2 = blocks[bi++];
        values[vi++] = ((block1 & 3L) << 61) | (block2 >>> 3);
        final long block3 = blocks[bi++];
        values[vi++] = ((block2 & 7L) << 60) | (block3 >>> 4);
        final long block4 = blocks[bi++];
        values[vi++] = ((block3 & 15L) << 59) | (block4 >>> 5);
        final long block5 = blocks[bi++];
        values[vi++] = ((block4 & 31L) << 58) | (block5 >>> 6);
        final long block6 = blocks[bi++];
        values[vi++] = ((block5 & 63L) << 57) | (block6 >>> 7);
        final long block7 = blocks[bi++];
        values[vi++] = ((block6 & 127L) << 56) | (block7 >>> 8);
        final long block8 = blocks[bi++];
        values[vi++] = ((block7 & 255L) << 55) | (block8 >>> 9);
        final long block9 = blocks[bi++];
        values[vi++] = ((block8 & 511L) << 54) | (block9 >>> 10);
        final long block10 = blocks[bi++];
        values[vi++] = ((block9 & 1023L) << 53) | (block10 >>> 11);
        final long block11 = blocks[bi++];
        values[vi++] = ((block10 & 2047L) << 52) | (block11 >>> 12);
        final long block12 = blocks[bi++];
        values[vi++] = ((block11 & 4095L) << 51) | (block12 >>> 13);
        final long block13 = blocks[bi++];
        values[vi++] = ((block12 & 8191L) << 50) | (block13 >>> 14);
        final long block14 = blocks[bi++];
        values[vi++] = ((block13 & 16383L) << 49) | (block14 >>> 15);
        final long block15 = blocks[bi++];
        values[vi++] = ((block14 & 32767L) << 48) | (block15 >>> 16);
        final long block16 = blocks[bi++];
        values[vi++] = ((block15 & 65535L) << 47) | (block16 >>> 17);
        final long block17 = blocks[bi++];
        values[vi++] = ((block16 & 131071L) << 46) | (block17 >>> 18);
        final long block18 = blocks[bi++];
        values[vi++] = ((block17 & 262143L) << 45) | (block18 >>> 19);
        final long block19 = blocks[bi++];
        values[vi++] = ((block18 & 524287L) << 44) | (block19 >>> 20);
        final long block20 = blocks[bi++];
        values[vi++] = ((block19 & 1048575L) << 43) | (block20 >>> 21);
        final long block21 = blocks[bi++];
        values[vi++] = ((block20 & 2097151L) << 42) | (block21 >>> 22);
        final long block22 = blocks[bi++];
        values[vi++] = ((block21 & 4194303L) << 41) | (block22 >>> 23);
        final long block23 = blocks[bi++];
        values[vi++] = ((block22 & 8388607L) << 40) | (block23 >>> 24);
        final long block24 = blocks[bi++];
        values[vi++] = ((block23 & 16777215L) << 39) | (block24 >>> 25);
        final long block25 = blocks[bi++];
        values[vi++] = ((block24 & 33554431L) << 38) | (block25 >>> 26);
        final long block26 = blocks[bi++];
        values[vi++] = ((block25 & 67108863L) << 37) | (block26 >>> 27);
        final long block27 = blocks[bi++];
        values[vi++] = ((block26 & 134217727L) << 36) | (block27 >>> 28);
        final long block28 = blocks[bi++];
        values[vi++] = ((block27 & 268435455L) << 35) | (block28 >>> 29);
        final long block29 = blocks[bi++];
        values[vi++] = ((block28 & 536870911L) << 34) | (block29 >>> 30);
        final long block30 = blocks[bi++];
        values[vi++] = ((block29 & 1073741823L) << 33) | (block30 >>> 31);
        final long block31 = blocks[bi++];
        values[vi++] = ((block30 & 2147483647L) << 32) | (block31 >>> 32);
        final long block32 = blocks[bi++];
        values[vi++] = ((block31 & 4294967295L) << 31) | (block32 >>> 33);
        final long block33 = blocks[bi++];
        values[vi++] = ((block32 & 8589934591L) << 30) | (block33 >>> 34);
        final long block34 = blocks[bi++];
        values[vi++] = ((block33 & 17179869183L) << 29) | (block34 >>> 35);
        final long block35 = blocks[bi++];
        values[vi++] = ((block34 & 34359738367L) << 28) | (block35 >>> 36);
        final long block36 = blocks[bi++];
        values[vi++] = ((block35 & 68719476735L) << 27) | (block36 >>> 37);
        final long block37 = blocks[bi++];
        values[vi++] = ((block36 & 137438953471L) << 26) | (block37 >>> 38);
        final long block38 = blocks[bi++];
        values[vi++] = ((block37 & 274877906943L) << 25) | (block38 >>> 39);
        final long block39 = blocks[bi++];
        values[vi++] = ((block38 & 549755813887L) << 24) | (block39 >>> 40);
        final long block40 = blocks[bi++];
        values[vi++] = ((block39 & 1099511627775L) << 23) | (block40 >>> 41);
        final long block41 = blocks[bi++];
        values[vi++] = ((block40 & 2199023255551L) << 22) | (block41 >>> 42);
        final long block42 = blocks[bi++];
        values[vi++] = ((block41 & 4398046511103L) << 21) | (block42 >>> 43);
        final long block43 = blocks[bi++];
        values[vi++] = ((block42 & 8796093022207L) << 20) | (block43 >>> 44);
        final long block44 = blocks[bi++];
        values[vi++] = ((block43 & 17592186044415L) << 19) | (block44 >>> 45);
        final long block45 = blocks[bi++];
        values[vi++] = ((block44 & 35184372088831L) << 18) | (block45 >>> 46);
        final long block46 = blocks[bi++];
        values[vi++] = ((block45 & 70368744177663L) << 17) | (block46 >>> 47);
        final long block47 = blocks[bi++];
        values[vi++] = ((block46 & 140737488355327L) << 16) | (block47 >>> 48);
        final long block48 = blocks[bi++];
        values[vi++] = ((block47 & 281474976710655L) << 15) | (block48 >>> 49);
        final long block49 = blocks[bi++];
        values[vi++] = ((block48 & 562949953421311L) << 14) | (block49 >>> 50);
        final long block50 = blocks[bi++];
        values[vi++] = ((block49 & 1125899906842623L) << 13) | (block50 >>> 51);
        final long block51 = blocks[bi++];
        values[vi++] = ((block50 & 2251799813685247L) << 12) | (block51 >>> 52);
        final long block52 = blocks[bi++];
        values[vi++] = ((block51 & 4503599627370495L) << 11) | (block52 >>> 53);
        final long block53 = blocks[bi++];
        values[vi++] = ((block52 & 9007199254740991L) << 10) | (block53 >>> 54);
        final long block54 = blocks[bi++];
        values[vi++] = ((block53 & 18014398509481983L) << 9) | (block54 >>> 55);
        final long block55 = blocks[bi++];
        values[vi++] = ((block54 & 36028797018963967L) << 8) | (block55 >>> 56);
        final long block56 = blocks[bi++];
        values[vi++] = ((block55 & 72057594037927935L) << 7) | (block56 >>> 57);
        final long block57 = blocks[bi++];
        values[vi++] = ((block56 & 144115188075855871L) << 6) | (block57 >>> 58);
        final long block58 = blocks[bi++];
        values[vi++] = ((block57 & 288230376151711743L) << 5) | (block58 >>> 59);
        final long block59 = blocks[bi++];
        values[vi++] = ((block58 & 576460752303423487L) << 4) | (block59 >>> 60);
        final long block60 = blocks[bi++];
        values[vi++] = ((block59 & 1152921504606846975L) << 3) | (block60 >>> 61);
        final long block61 = blocks[bi++];
        values[vi++] = ((block60 & 2305843009213693951L) << 2) | (block61 >>> 62);
        final long block62 = blocks[bi++];
        values[vi++] = ((block61 & 4611686018427387903L) << 1) | (block62 >>> 63);
        values[vi++] = block62 & 9223372036854775807L;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = (values[vi++] << 1) | (values[vi] >>> 62);
        blocks[bi++] = (values[vi++] << 2) | (values[vi] >>> 61);
        blocks[bi++] = (values[vi++] << 3) | (values[vi] >>> 60);
        blocks[bi++] = (values[vi++] << 4) | (values[vi] >>> 59);
        blocks[bi++] = (values[vi++] << 5) | (values[vi] >>> 58);
        blocks[bi++] = (values[vi++] << 6) | (values[vi] >>> 57);
        blocks[bi++] = (values[vi++] << 7) | (values[vi] >>> 56);
        blocks[bi++] = (values[vi++] << 8) | (values[vi] >>> 55);
        blocks[bi++] = (values[vi++] << 9) | (values[vi] >>> 54);
        blocks[bi++] = (values[vi++] << 10) | (values[vi] >>> 53);
        blocks[bi++] = (values[vi++] << 11) | (values[vi] >>> 52);
        blocks[bi++] = (values[vi++] << 12) | (values[vi] >>> 51);
        blocks[bi++] = (values[vi++] << 13) | (values[vi] >>> 50);
        blocks[bi++] = (values[vi++] << 14) | (values[vi] >>> 49);
        blocks[bi++] = (values[vi++] << 15) | (values[vi] >>> 48);
        blocks[bi++] = (values[vi++] << 16) | (values[vi] >>> 47);
        blocks[bi++] = (values[vi++] << 17) | (values[vi] >>> 46);
        blocks[bi++] = (values[vi++] << 18) | (values[vi] >>> 45);
        blocks[bi++] = (values[vi++] << 19) | (values[vi] >>> 44);
        blocks[bi++] = (values[vi++] << 20) | (values[vi] >>> 43);
        blocks[bi++] = (values[vi++] << 21) | (values[vi] >>> 42);
        blocks[bi++] = (values[vi++] << 22) | (values[vi] >>> 41);
        blocks[bi++] = (values[vi++] << 23) | (values[vi] >>> 40);
        blocks[bi++] = (values[vi++] << 24) | (values[vi] >>> 39);
        blocks[bi++] = (values[vi++] << 25) | (values[vi] >>> 38);
        blocks[bi++] = (values[vi++] << 26) | (values[vi] >>> 37);
        blocks[bi++] = (values[vi++] << 27) | (values[vi] >>> 36);
        blocks[bi++] = (values[vi++] << 28) | (values[vi] >>> 35);
        blocks[bi++] = (values[vi++] << 29) | (values[vi] >>> 34);
        blocks[bi++] = (values[vi++] << 30) | (values[vi] >>> 33);
        blocks[bi++] = (values[vi++] << 31) | (values[vi] >>> 32);
        blocks[bi++] = (values[vi++] << 32) | (values[vi] >>> 31);
        blocks[bi++] = (values[vi++] << 33) | (values[vi] >>> 30);
        blocks[bi++] = (values[vi++] << 34) | (values[vi] >>> 29);
        blocks[bi++] = (values[vi++] << 35) | (values[vi] >>> 28);
        blocks[bi++] = (values[vi++] << 36) | (values[vi] >>> 27);
        blocks[bi++] = (values[vi++] << 37) | (values[vi] >>> 26);
        blocks[bi++] = (values[vi++] << 38) | (values[vi] >>> 25);
        blocks[bi++] = (values[vi++] << 39) | (values[vi] >>> 24);
        blocks[bi++] = (values[vi++] << 40) | (values[vi] >>> 23);
        blocks[bi++] = (values[vi++] << 41) | (values[vi] >>> 22);
        blocks[bi++] = (values[vi++] << 42) | (values[vi] >>> 21);
        blocks[bi++] = (values[vi++] << 43) | (values[vi] >>> 20);
        blocks[bi++] = (values[vi++] << 44) | (values[vi] >>> 19);
        blocks[bi++] = (values[vi++] << 45) | (values[vi] >>> 18);
        blocks[bi++] = (values[vi++] << 46) | (values[vi] >>> 17);
        blocks[bi++] = (values[vi++] << 47) | (values[vi] >>> 16);
        blocks[bi++] = (values[vi++] << 48) | (values[vi] >>> 15);
        blocks[bi++] = (values[vi++] << 49) | (values[vi] >>> 14);
        blocks[bi++] = (values[vi++] << 50) | (values[vi] >>> 13);
        blocks[bi++] = (values[vi++] << 51) | (values[vi] >>> 12);
        blocks[bi++] = (values[vi++] << 52) | (values[vi] >>> 11);
        blocks[bi++] = (values[vi++] << 53) | (values[vi] >>> 10);
        blocks[bi++] = (values[vi++] << 54) | (values[vi] >>> 9);
        blocks[bi++] = (values[vi++] << 55) | (values[vi] >>> 8);
        blocks[bi++] = (values[vi++] << 56) | (values[vi] >>> 7);
        blocks[bi++] = (values[vi++] << 57) | (values[vi] >>> 6);
        blocks[bi++] = (values[vi++] << 58) | (values[vi] >>> 5);
        blocks[bi++] = (values[vi++] << 59) | (values[vi] >>> 4);
        blocks[bi++] = (values[vi++] << 60) | (values[vi] >>> 3);
        blocks[bi++] = (values[vi++] << 61) | (values[vi] >>> 2);
        blocks[bi++] = (values[vi++] << 62) | (values[vi] >>> 1);
        blocks[bi++] = (values[vi++] << 63) | values[vi++];
      }
    }
  }

  static final class Packed64BulkOperation64 extends BulkOperation {

    public int blocks() {
      return 1;
    }

    public int values() {
      return 1;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      System.arraycopy(blocks, bi, values, vi, iterations);
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      System.arraycopy(values, bi, blocks, vi, iterations);
    }
  }

  static final class Packed64SingleBlockBulkOperation1 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 64;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 1L;
        values[vi++] = (block >>> 1) & 1L;
        values[vi++] = (block >>> 2) & 1L;
        values[vi++] = (block >>> 3) & 1L;
        values[vi++] = (block >>> 4) & 1L;
        values[vi++] = (block >>> 5) & 1L;
        values[vi++] = (block >>> 6) & 1L;
        values[vi++] = (block >>> 7) & 1L;
        values[vi++] = (block >>> 8) & 1L;
        values[vi++] = (block >>> 9) & 1L;
        values[vi++] = (block >>> 10) & 1L;
        values[vi++] = (block >>> 11) & 1L;
        values[vi++] = (block >>> 12) & 1L;
        values[vi++] = (block >>> 13) & 1L;
        values[vi++] = (block >>> 14) & 1L;
        values[vi++] = (block >>> 15) & 1L;
        values[vi++] = (block >>> 16) & 1L;
        values[vi++] = (block >>> 17) & 1L;
        values[vi++] = (block >>> 18) & 1L;
        values[vi++] = (block >>> 19) & 1L;
        values[vi++] = (block >>> 20) & 1L;
        values[vi++] = (block >>> 21) & 1L;
        values[vi++] = (block >>> 22) & 1L;
        values[vi++] = (block >>> 23) & 1L;
        values[vi++] = (block >>> 24) & 1L;
        values[vi++] = (block >>> 25) & 1L;
        values[vi++] = (block >>> 26) & 1L;
        values[vi++] = (block >>> 27) & 1L;
        values[vi++] = (block >>> 28) & 1L;
        values[vi++] = (block >>> 29) & 1L;
        values[vi++] = (block >>> 30) & 1L;
        values[vi++] = (block >>> 31) & 1L;
        values[vi++] = (block >>> 32) & 1L;
        values[vi++] = (block >>> 33) & 1L;
        values[vi++] = (block >>> 34) & 1L;
        values[vi++] = (block >>> 35) & 1L;
        values[vi++] = (block >>> 36) & 1L;
        values[vi++] = (block >>> 37) & 1L;
        values[vi++] = (block >>> 38) & 1L;
        values[vi++] = (block >>> 39) & 1L;
        values[vi++] = (block >>> 40) & 1L;
        values[vi++] = (block >>> 41) & 1L;
        values[vi++] = (block >>> 42) & 1L;
        values[vi++] = (block >>> 43) & 1L;
        values[vi++] = (block >>> 44) & 1L;
        values[vi++] = (block >>> 45) & 1L;
        values[vi++] = (block >>> 46) & 1L;
        values[vi++] = (block >>> 47) & 1L;
        values[vi++] = (block >>> 48) & 1L;
        values[vi++] = (block >>> 49) & 1L;
        values[vi++] = (block >>> 50) & 1L;
        values[vi++] = (block >>> 51) & 1L;
        values[vi++] = (block >>> 52) & 1L;
        values[vi++] = (block >>> 53) & 1L;
        values[vi++] = (block >>> 54) & 1L;
        values[vi++] = (block >>> 55) & 1L;
        values[vi++] = (block >>> 56) & 1L;
        values[vi++] = (block >>> 57) & 1L;
        values[vi++] = (block >>> 58) & 1L;
        values[vi++] = (block >>> 59) & 1L;
        values[vi++] = (block >>> 60) & 1L;
        values[vi++] = (block >>> 61) & 1L;
        values[vi++] = (block >>> 62) & 1L;
        values[vi++] = block >>> 63;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 1) | (values[vi++] << 2) | (values[vi++] << 3) | (values[vi++] << 4) | (values[vi++] << 5) | (values[vi++] << 6) | (values[vi++] << 7) | (values[vi++] << 8) | (values[vi++] << 9) | (values[vi++] << 10) | (values[vi++] << 11) | (values[vi++] << 12) | (values[vi++] << 13) | (values[vi++] << 14) | (values[vi++] << 15) | (values[vi++] << 16) | (values[vi++] << 17) | (values[vi++] << 18) | (values[vi++] << 19) | (values[vi++] << 20) | (values[vi++] << 21) | (values[vi++] << 22) | (values[vi++] << 23) | (values[vi++] << 24) | (values[vi++] << 25) | (values[vi++] << 26) | (values[vi++] << 27) | (values[vi++] << 28) | (values[vi++] << 29) | (values[vi++] << 30) | (values[vi++] << 31) | (values[vi++] << 32) | (values[vi++] << 33) | (values[vi++] << 34) | (values[vi++] << 35) | (values[vi++] << 36) | (values[vi++] << 37) | (values[vi++] << 38) | (values[vi++] << 39) | (values[vi++] << 40) | (values[vi++] << 41) | (values[vi++] << 42) | (values[vi++] << 43) | (values[vi++] << 44) | (values[vi++] << 45) | (values[vi++] << 46) | (values[vi++] << 47) | (values[vi++] << 48) | (values[vi++] << 49) | (values[vi++] << 50) | (values[vi++] << 51) | (values[vi++] << 52) | (values[vi++] << 53) | (values[vi++] << 54) | (values[vi++] << 55) | (values[vi++] << 56) | (values[vi++] << 57) | (values[vi++] << 58) | (values[vi++] << 59) | (values[vi++] << 60) | (values[vi++] << 61) | (values[vi++] << 62) | (values[vi++] << 63);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation2 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 32;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 3L;
        values[vi++] = (block >>> 2) & 3L;
        values[vi++] = (block >>> 4) & 3L;
        values[vi++] = (block >>> 6) & 3L;
        values[vi++] = (block >>> 8) & 3L;
        values[vi++] = (block >>> 10) & 3L;
        values[vi++] = (block >>> 12) & 3L;
        values[vi++] = (block >>> 14) & 3L;
        values[vi++] = (block >>> 16) & 3L;
        values[vi++] = (block >>> 18) & 3L;
        values[vi++] = (block >>> 20) & 3L;
        values[vi++] = (block >>> 22) & 3L;
        values[vi++] = (block >>> 24) & 3L;
        values[vi++] = (block >>> 26) & 3L;
        values[vi++] = (block >>> 28) & 3L;
        values[vi++] = (block >>> 30) & 3L;
        values[vi++] = (block >>> 32) & 3L;
        values[vi++] = (block >>> 34) & 3L;
        values[vi++] = (block >>> 36) & 3L;
        values[vi++] = (block >>> 38) & 3L;
        values[vi++] = (block >>> 40) & 3L;
        values[vi++] = (block >>> 42) & 3L;
        values[vi++] = (block >>> 44) & 3L;
        values[vi++] = (block >>> 46) & 3L;
        values[vi++] = (block >>> 48) & 3L;
        values[vi++] = (block >>> 50) & 3L;
        values[vi++] = (block >>> 52) & 3L;
        values[vi++] = (block >>> 54) & 3L;
        values[vi++] = (block >>> 56) & 3L;
        values[vi++] = (block >>> 58) & 3L;
        values[vi++] = (block >>> 60) & 3L;
        values[vi++] = block >>> 62;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 2) | (values[vi++] << 4) | (values[vi++] << 6) | (values[vi++] << 8) | (values[vi++] << 10) | (values[vi++] << 12) | (values[vi++] << 14) | (values[vi++] << 16) | (values[vi++] << 18) | (values[vi++] << 20) | (values[vi++] << 22) | (values[vi++] << 24) | (values[vi++] << 26) | (values[vi++] << 28) | (values[vi++] << 30) | (values[vi++] << 32) | (values[vi++] << 34) | (values[vi++] << 36) | (values[vi++] << 38) | (values[vi++] << 40) | (values[vi++] << 42) | (values[vi++] << 44) | (values[vi++] << 46) | (values[vi++] << 48) | (values[vi++] << 50) | (values[vi++] << 52) | (values[vi++] << 54) | (values[vi++] << 56) | (values[vi++] << 58) | (values[vi++] << 60) | (values[vi++] << 62);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation3 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 21;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 7L;
        values[vi++] = (block >>> 3) & 7L;
        values[vi++] = (block >>> 6) & 7L;
        values[vi++] = (block >>> 9) & 7L;
        values[vi++] = (block >>> 12) & 7L;
        values[vi++] = (block >>> 15) & 7L;
        values[vi++] = (block >>> 18) & 7L;
        values[vi++] = (block >>> 21) & 7L;
        values[vi++] = (block >>> 24) & 7L;
        values[vi++] = (block >>> 27) & 7L;
        values[vi++] = (block >>> 30) & 7L;
        values[vi++] = (block >>> 33) & 7L;
        values[vi++] = (block >>> 36) & 7L;
        values[vi++] = (block >>> 39) & 7L;
        values[vi++] = (block >>> 42) & 7L;
        values[vi++] = (block >>> 45) & 7L;
        values[vi++] = (block >>> 48) & 7L;
        values[vi++] = (block >>> 51) & 7L;
        values[vi++] = (block >>> 54) & 7L;
        values[vi++] = (block >>> 57) & 7L;
        values[vi++] = block >>> 60;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 3) | (values[vi++] << 6) | (values[vi++] << 9) | (values[vi++] << 12) | (values[vi++] << 15) | (values[vi++] << 18) | (values[vi++] << 21) | (values[vi++] << 24) | (values[vi++] << 27) | (values[vi++] << 30) | (values[vi++] << 33) | (values[vi++] << 36) | (values[vi++] << 39) | (values[vi++] << 42) | (values[vi++] << 45) | (values[vi++] << 48) | (values[vi++] << 51) | (values[vi++] << 54) | (values[vi++] << 57) | (values[vi++] << 60);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation4 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 16;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 15L;
        values[vi++] = (block >>> 4) & 15L;
        values[vi++] = (block >>> 8) & 15L;
        values[vi++] = (block >>> 12) & 15L;
        values[vi++] = (block >>> 16) & 15L;
        values[vi++] = (block >>> 20) & 15L;
        values[vi++] = (block >>> 24) & 15L;
        values[vi++] = (block >>> 28) & 15L;
        values[vi++] = (block >>> 32) & 15L;
        values[vi++] = (block >>> 36) & 15L;
        values[vi++] = (block >>> 40) & 15L;
        values[vi++] = (block >>> 44) & 15L;
        values[vi++] = (block >>> 48) & 15L;
        values[vi++] = (block >>> 52) & 15L;
        values[vi++] = (block >>> 56) & 15L;
        values[vi++] = block >>> 60;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 4) | (values[vi++] << 8) | (values[vi++] << 12) | (values[vi++] << 16) | (values[vi++] << 20) | (values[vi++] << 24) | (values[vi++] << 28) | (values[vi++] << 32) | (values[vi++] << 36) | (values[vi++] << 40) | (values[vi++] << 44) | (values[vi++] << 48) | (values[vi++] << 52) | (values[vi++] << 56) | (values[vi++] << 60);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation5 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 12;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 31L;
        values[vi++] = (block >>> 5) & 31L;
        values[vi++] = (block >>> 10) & 31L;
        values[vi++] = (block >>> 15) & 31L;
        values[vi++] = (block >>> 20) & 31L;
        values[vi++] = (block >>> 25) & 31L;
        values[vi++] = (block >>> 30) & 31L;
        values[vi++] = (block >>> 35) & 31L;
        values[vi++] = (block >>> 40) & 31L;
        values[vi++] = (block >>> 45) & 31L;
        values[vi++] = (block >>> 50) & 31L;
        values[vi++] = block >>> 55;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 5) | (values[vi++] << 10) | (values[vi++] << 15) | (values[vi++] << 20) | (values[vi++] << 25) | (values[vi++] << 30) | (values[vi++] << 35) | (values[vi++] << 40) | (values[vi++] << 45) | (values[vi++] << 50) | (values[vi++] << 55);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation6 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 10;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 63L;
        values[vi++] = (block >>> 6) & 63L;
        values[vi++] = (block >>> 12) & 63L;
        values[vi++] = (block >>> 18) & 63L;
        values[vi++] = (block >>> 24) & 63L;
        values[vi++] = (block >>> 30) & 63L;
        values[vi++] = (block >>> 36) & 63L;
        values[vi++] = (block >>> 42) & 63L;
        values[vi++] = (block >>> 48) & 63L;
        values[vi++] = block >>> 54;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 6) | (values[vi++] << 12) | (values[vi++] << 18) | (values[vi++] << 24) | (values[vi++] << 30) | (values[vi++] << 36) | (values[vi++] << 42) | (values[vi++] << 48) | (values[vi++] << 54);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation7 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 9;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 127L;
        values[vi++] = (block >>> 7) & 127L;
        values[vi++] = (block >>> 14) & 127L;
        values[vi++] = (block >>> 21) & 127L;
        values[vi++] = (block >>> 28) & 127L;
        values[vi++] = (block >>> 35) & 127L;
        values[vi++] = (block >>> 42) & 127L;
        values[vi++] = (block >>> 49) & 127L;
        values[vi++] = block >>> 56;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 7) | (values[vi++] << 14) | (values[vi++] << 21) | (values[vi++] << 28) | (values[vi++] << 35) | (values[vi++] << 42) | (values[vi++] << 49) | (values[vi++] << 56);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation8 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 8;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 255L;
        values[vi++] = (block >>> 8) & 255L;
        values[vi++] = (block >>> 16) & 255L;
        values[vi++] = (block >>> 24) & 255L;
        values[vi++] = (block >>> 32) & 255L;
        values[vi++] = (block >>> 40) & 255L;
        values[vi++] = (block >>> 48) & 255L;
        values[vi++] = block >>> 56;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 8) | (values[vi++] << 16) | (values[vi++] << 24) | (values[vi++] << 32) | (values[vi++] << 40) | (values[vi++] << 48) | (values[vi++] << 56);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation9 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 7;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 511L;
        values[vi++] = (block >>> 9) & 511L;
        values[vi++] = (block >>> 18) & 511L;
        values[vi++] = (block >>> 27) & 511L;
        values[vi++] = (block >>> 36) & 511L;
        values[vi++] = (block >>> 45) & 511L;
        values[vi++] = block >>> 54;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 9) | (values[vi++] << 18) | (values[vi++] << 27) | (values[vi++] << 36) | (values[vi++] << 45) | (values[vi++] << 54);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation10 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 6;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 1023L;
        values[vi++] = (block >>> 10) & 1023L;
        values[vi++] = (block >>> 20) & 1023L;
        values[vi++] = (block >>> 30) & 1023L;
        values[vi++] = (block >>> 40) & 1023L;
        values[vi++] = block >>> 50;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 10) | (values[vi++] << 20) | (values[vi++] << 30) | (values[vi++] << 40) | (values[vi++] << 50);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation12 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 5;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 4095L;
        values[vi++] = (block >>> 12) & 4095L;
        values[vi++] = (block >>> 24) & 4095L;
        values[vi++] = (block >>> 36) & 4095L;
        values[vi++] = block >>> 48;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 12) | (values[vi++] << 24) | (values[vi++] << 36) | (values[vi++] << 48);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation16 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 4;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 65535L;
        values[vi++] = (block >>> 16) & 65535L;
        values[vi++] = (block >>> 32) & 65535L;
        values[vi++] = block >>> 48;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 16) | (values[vi++] << 32) | (values[vi++] << 48);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation21 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 3;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 2097151L;
        values[vi++] = (block >>> 21) & 2097151L;
        values[vi++] = block >>> 42;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 21) | (values[vi++] << 42);
      }
    }
  }

  static final class Packed64SingleBlockBulkOperation32 extends BulkOperation {

    public int blocks() {
      return 1;
     }

    public int values() {
      return 2;
    }

    public void get(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[bi++];
        values[vi++] = block & 4294967295L;
        values[vi++] = block >>> 32;
      }
    }

    public void set(long[] blocks, int bi, long[] values, int vi, int iterations) {
      assert bi + iterations * blocks() <= blocks.length;
      assert vi + iterations * values() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[bi++] = values[vi++] | (values[vi++] << 32);
      }
    }
  }
}