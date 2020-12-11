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
final class BulkOperationPacked19 extends BulkOperationPacked {

  public BulkOperationPacked19() {
    super(19);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 524287L);
      values[valuesOffset++] = (int) ((block0 >>> 19) & 524287L);
      values[valuesOffset++] = (int) ((block0 >>> 38) & 524287L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 4095L) << 7) | (block0 >>> 57));
      values[valuesOffset++] = (int) ((block1 >>> 12) & 524287L);
      values[valuesOffset++] = (int) ((block1 >>> 31) & 524287L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 31L) << 14) | (block1 >>> 50));
      values[valuesOffset++] = (int) ((block2 >>> 5) & 524287L);
      values[valuesOffset++] = (int) ((block2 >>> 24) & 524287L);
      values[valuesOffset++] = (int) ((block2 >>> 43) & 524287L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 131071L) << 2) | (block2 >>> 62));
      values[valuesOffset++] = (int) ((block3 >>> 17) & 524287L);
      values[valuesOffset++] = (int) ((block3 >>> 36) & 524287L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 1023L) << 9) | (block3 >>> 55));
      values[valuesOffset++] = (int) ((block4 >>> 10) & 524287L);
      values[valuesOffset++] = (int) ((block4 >>> 29) & 524287L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 7L) << 16) | (block4 >>> 48));
      values[valuesOffset++] = (int) ((block5 >>> 3) & 524287L);
      values[valuesOffset++] = (int) ((block5 >>> 22) & 524287L);
      values[valuesOffset++] = (int) ((block5 >>> 41) & 524287L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 32767L) << 4) | (block5 >>> 60));
      values[valuesOffset++] = (int) ((block6 >>> 15) & 524287L);
      values[valuesOffset++] = (int) ((block6 >>> 34) & 524287L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 255L) << 11) | (block6 >>> 53));
      values[valuesOffset++] = (int) ((block7 >>> 8) & 524287L);
      values[valuesOffset++] = (int) ((block7 >>> 27) & 524287L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 1L) << 18) | (block7 >>> 46));
      values[valuesOffset++] = (int) ((block8 >>> 1) & 524287L);
      values[valuesOffset++] = (int) ((block8 >>> 20) & 524287L);
      values[valuesOffset++] = (int) ((block8 >>> 39) & 524287L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 8191L) << 6) | (block8 >>> 58));
      values[valuesOffset++] = (int) ((block9 >>> 13) & 524287L);
      values[valuesOffset++] = (int) ((block9 >>> 32) & 524287L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 63L) << 13) | (block9 >>> 51));
      values[valuesOffset++] = (int) ((block10 >>> 6) & 524287L);
      values[valuesOffset++] = (int) ((block10 >>> 25) & 524287L);
      values[valuesOffset++] = (int) ((block10 >>> 44) & 524287L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 262143L) << 1) | (block10 >>> 63));
      values[valuesOffset++] = (int) ((block11 >>> 18) & 524287L);
      values[valuesOffset++] = (int) ((block11 >>> 37) & 524287L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 2047L) << 8) | (block11 >>> 56));
      values[valuesOffset++] = (int) ((block12 >>> 11) & 524287L);
      values[valuesOffset++] = (int) ((block12 >>> 30) & 524287L);
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block13 & 15L) << 15) | (block12 >>> 49));
      values[valuesOffset++] = (int) ((block13 >>> 4) & 524287L);
      values[valuesOffset++] = (int) ((block13 >>> 23) & 524287L);
      values[valuesOffset++] = (int) ((block13 >>> 42) & 524287L);
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block14 & 65535L) << 3) | (block13 >>> 61));
      values[valuesOffset++] = (int) ((block14 >>> 16) & 524287L);
      values[valuesOffset++] = (int) ((block14 >>> 35) & 524287L);
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block15 & 511L) << 10) | (block14 >>> 54));
      values[valuesOffset++] = (int) ((block15 >>> 9) & 524287L);
      values[valuesOffset++] = (int) ((block15 >>> 28) & 524287L);
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block16 & 3L) << 17) | (block15 >>> 47));
      values[valuesOffset++] = (int) ((block16 >>> 2) & 524287L);
      values[valuesOffset++] = (int) ((block16 >>> 21) & 524287L);
      values[valuesOffset++] = (int) ((block16 >>> 40) & 524287L);
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block17 & 16383L) << 5) | (block16 >>> 59));
      values[valuesOffset++] = (int) ((block17 >>> 14) & 524287L);
      values[valuesOffset++] = (int) ((block17 >>> 33) & 524287L);
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block18 & 127L) << 12) | (block17 >>> 52));
      values[valuesOffset++] = (int) ((block18 >>> 7) & 524287L);
      values[valuesOffset++] = (int) ((block18 >>> 26) & 524287L);
      values[valuesOffset++] = (int) (block18 >>> 45);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 7) << 16) | (byte1 << 8) | byte0;
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 63) << 13) | (byte3 << 5) | (byte2 >>> 3);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 1) << 18) | (byte6 << 10) | (byte5 << 2) | (byte4 >>> 6);
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 15) << 15) | (byte8 << 7) | (byte7 >>> 1);
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 127) << 12) | (byte10 << 4) | (byte9 >>> 4);
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      final int byte13 = blocks[blocksOffset++] & 0xFF;
      final int byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 3) << 17) | (byte13 << 9) | (byte12 << 1) | (byte11 >>> 7);
      final int byte15 = blocks[blocksOffset++] & 0xFF;
      final int byte16 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte16 & 31) << 14) | (byte15 << 6) | (byte14 >>> 2);
      final int byte17 = blocks[blocksOffset++] & 0xFF;
      final int byte18 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte18 << 11) | (byte17 << 3) | (byte16 >>> 5);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 & 524287L;
      values[valuesOffset++] = (block0 >>> 19) & 524287L;
      values[valuesOffset++] = (block0 >>> 38) & 524287L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 4095L) << 7) | (block0 >>> 57);
      values[valuesOffset++] = (block1 >>> 12) & 524287L;
      values[valuesOffset++] = (block1 >>> 31) & 524287L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 31L) << 14) | (block1 >>> 50);
      values[valuesOffset++] = (block2 >>> 5) & 524287L;
      values[valuesOffset++] = (block2 >>> 24) & 524287L;
      values[valuesOffset++] = (block2 >>> 43) & 524287L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 131071L) << 2) | (block2 >>> 62);
      values[valuesOffset++] = (block3 >>> 17) & 524287L;
      values[valuesOffset++] = (block3 >>> 36) & 524287L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 1023L) << 9) | (block3 >>> 55);
      values[valuesOffset++] = (block4 >>> 10) & 524287L;
      values[valuesOffset++] = (block4 >>> 29) & 524287L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 7L) << 16) | (block4 >>> 48);
      values[valuesOffset++] = (block5 >>> 3) & 524287L;
      values[valuesOffset++] = (block5 >>> 22) & 524287L;
      values[valuesOffset++] = (block5 >>> 41) & 524287L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 32767L) << 4) | (block5 >>> 60);
      values[valuesOffset++] = (block6 >>> 15) & 524287L;
      values[valuesOffset++] = (block6 >>> 34) & 524287L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 255L) << 11) | (block6 >>> 53);
      values[valuesOffset++] = (block7 >>> 8) & 524287L;
      values[valuesOffset++] = (block7 >>> 27) & 524287L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 1L) << 18) | (block7 >>> 46);
      values[valuesOffset++] = (block8 >>> 1) & 524287L;
      values[valuesOffset++] = (block8 >>> 20) & 524287L;
      values[valuesOffset++] = (block8 >>> 39) & 524287L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 8191L) << 6) | (block8 >>> 58);
      values[valuesOffset++] = (block9 >>> 13) & 524287L;
      values[valuesOffset++] = (block9 >>> 32) & 524287L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 63L) << 13) | (block9 >>> 51);
      values[valuesOffset++] = (block10 >>> 6) & 524287L;
      values[valuesOffset++] = (block10 >>> 25) & 524287L;
      values[valuesOffset++] = (block10 >>> 44) & 524287L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 262143L) << 1) | (block10 >>> 63);
      values[valuesOffset++] = (block11 >>> 18) & 524287L;
      values[valuesOffset++] = (block11 >>> 37) & 524287L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 2047L) << 8) | (block11 >>> 56);
      values[valuesOffset++] = (block12 >>> 11) & 524287L;
      values[valuesOffset++] = (block12 >>> 30) & 524287L;
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block13 & 15L) << 15) | (block12 >>> 49);
      values[valuesOffset++] = (block13 >>> 4) & 524287L;
      values[valuesOffset++] = (block13 >>> 23) & 524287L;
      values[valuesOffset++] = (block13 >>> 42) & 524287L;
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block14 & 65535L) << 3) | (block13 >>> 61);
      values[valuesOffset++] = (block14 >>> 16) & 524287L;
      values[valuesOffset++] = (block14 >>> 35) & 524287L;
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block15 & 511L) << 10) | (block14 >>> 54);
      values[valuesOffset++] = (block15 >>> 9) & 524287L;
      values[valuesOffset++] = (block15 >>> 28) & 524287L;
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block16 & 3L) << 17) | (block15 >>> 47);
      values[valuesOffset++] = (block16 >>> 2) & 524287L;
      values[valuesOffset++] = (block16 >>> 21) & 524287L;
      values[valuesOffset++] = (block16 >>> 40) & 524287L;
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block17 & 16383L) << 5) | (block16 >>> 59);
      values[valuesOffset++] = (block17 >>> 14) & 524287L;
      values[valuesOffset++] = (block17 >>> 33) & 524287L;
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block18 & 127L) << 12) | (block17 >>> 52);
      values[valuesOffset++] = (block18 >>> 7) & 524287L;
      values[valuesOffset++] = (block18 >>> 26) & 524287L;
      values[valuesOffset++] = block18 >>> 45;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 7) << 16) | (byte1 << 8) | byte0;
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 63) << 13) | (byte3 << 5) | (byte2 >>> 3);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 1) << 18) | (byte6 << 10) | (byte5 << 2) | (byte4 >>> 6);
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 15) << 15) | (byte8 << 7) | (byte7 >>> 1);
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 127) << 12) | (byte10 << 4) | (byte9 >>> 4);
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      final long byte13 = blocks[blocksOffset++] & 0xFF;
      final long byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 3) << 17) | (byte13 << 9) | (byte12 << 1) | (byte11 >>> 7);
      final long byte15 = blocks[blocksOffset++] & 0xFF;
      final long byte16 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte16 & 31) << 14) | (byte15 << 6) | (byte14 >>> 2);
      final long byte17 = blocks[blocksOffset++] & 0xFF;
      final long byte18 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte18 << 11) | (byte17 << 3) | (byte16 >>> 5);
    }
  }

}
