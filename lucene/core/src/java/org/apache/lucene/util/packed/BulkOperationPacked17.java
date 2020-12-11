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
final class BulkOperationPacked17 extends BulkOperationPacked {

  public BulkOperationPacked17() {
    super(17);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 131071L);
      values[valuesOffset++] = (int) ((block0 >>> 17) & 131071L);
      values[valuesOffset++] = (int) ((block0 >>> 34) & 131071L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 15L) << 13) | (block0 >>> 51));
      values[valuesOffset++] = (int) ((block1 >>> 4) & 131071L);
      values[valuesOffset++] = (int) ((block1 >>> 21) & 131071L);
      values[valuesOffset++] = (int) ((block1 >>> 38) & 131071L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 255L) << 9) | (block1 >>> 55));
      values[valuesOffset++] = (int) ((block2 >>> 8) & 131071L);
      values[valuesOffset++] = (int) ((block2 >>> 25) & 131071L);
      values[valuesOffset++] = (int) ((block2 >>> 42) & 131071L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 4095L) << 5) | (block2 >>> 59));
      values[valuesOffset++] = (int) ((block3 >>> 12) & 131071L);
      values[valuesOffset++] = (int) ((block3 >>> 29) & 131071L);
      values[valuesOffset++] = (int) ((block3 >>> 46) & 131071L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 65535L) << 1) | (block3 >>> 63));
      values[valuesOffset++] = (int) ((block4 >>> 16) & 131071L);
      values[valuesOffset++] = (int) ((block4 >>> 33) & 131071L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 7L) << 14) | (block4 >>> 50));
      values[valuesOffset++] = (int) ((block5 >>> 3) & 131071L);
      values[valuesOffset++] = (int) ((block5 >>> 20) & 131071L);
      values[valuesOffset++] = (int) ((block5 >>> 37) & 131071L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 127L) << 10) | (block5 >>> 54));
      values[valuesOffset++] = (int) ((block6 >>> 7) & 131071L);
      values[valuesOffset++] = (int) ((block6 >>> 24) & 131071L);
      values[valuesOffset++] = (int) ((block6 >>> 41) & 131071L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 2047L) << 6) | (block6 >>> 58));
      values[valuesOffset++] = (int) ((block7 >>> 11) & 131071L);
      values[valuesOffset++] = (int) ((block7 >>> 28) & 131071L);
      values[valuesOffset++] = (int) ((block7 >>> 45) & 131071L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 32767L) << 2) | (block7 >>> 62));
      values[valuesOffset++] = (int) ((block8 >>> 15) & 131071L);
      values[valuesOffset++] = (int) ((block8 >>> 32) & 131071L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 3L) << 15) | (block8 >>> 49));
      values[valuesOffset++] = (int) ((block9 >>> 2) & 131071L);
      values[valuesOffset++] = (int) ((block9 >>> 19) & 131071L);
      values[valuesOffset++] = (int) ((block9 >>> 36) & 131071L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 63L) << 11) | (block9 >>> 53));
      values[valuesOffset++] = (int) ((block10 >>> 6) & 131071L);
      values[valuesOffset++] = (int) ((block10 >>> 23) & 131071L);
      values[valuesOffset++] = (int) ((block10 >>> 40) & 131071L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 1023L) << 7) | (block10 >>> 57));
      values[valuesOffset++] = (int) ((block11 >>> 10) & 131071L);
      values[valuesOffset++] = (int) ((block11 >>> 27) & 131071L);
      values[valuesOffset++] = (int) ((block11 >>> 44) & 131071L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 16383L) << 3) | (block11 >>> 61));
      values[valuesOffset++] = (int) ((block12 >>> 14) & 131071L);
      values[valuesOffset++] = (int) ((block12 >>> 31) & 131071L);
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block13 & 1L) << 16) | (block12 >>> 48));
      values[valuesOffset++] = (int) ((block13 >>> 1) & 131071L);
      values[valuesOffset++] = (int) ((block13 >>> 18) & 131071L);
      values[valuesOffset++] = (int) ((block13 >>> 35) & 131071L);
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block14 & 31L) << 12) | (block13 >>> 52));
      values[valuesOffset++] = (int) ((block14 >>> 5) & 131071L);
      values[valuesOffset++] = (int) ((block14 >>> 22) & 131071L);
      values[valuesOffset++] = (int) ((block14 >>> 39) & 131071L);
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block15 & 511L) << 8) | (block14 >>> 56));
      values[valuesOffset++] = (int) ((block15 >>> 9) & 131071L);
      values[valuesOffset++] = (int) ((block15 >>> 26) & 131071L);
      values[valuesOffset++] = (int) ((block15 >>> 43) & 131071L);
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block16 & 8191L) << 4) | (block15 >>> 60));
      values[valuesOffset++] = (int) ((block16 >>> 13) & 131071L);
      values[valuesOffset++] = (int) ((block16 >>> 30) & 131071L);
      values[valuesOffset++] = (int) (block16 >>> 47);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 1) << 16) | (byte1 << 8) | byte0;
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 3) << 15) | (byte3 << 7) | (byte2 >>> 1);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 7) << 14) | (byte5 << 6) | (byte4 >>> 2);
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 15) << 13) | (byte7 << 5) | (byte6 >>> 3);
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte10 & 31) << 12) | (byte9 << 4) | (byte8 >>> 4);
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte12 & 63) << 11) | (byte11 << 3) | (byte10 >>> 5);
      final int byte13 = blocks[blocksOffset++] & 0xFF;
      final int byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 127) << 10) | (byte13 << 2) | (byte12 >>> 6);
      final int byte15 = blocks[blocksOffset++] & 0xFF;
      final int byte16 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte16 << 9) | (byte15 << 1) | (byte14 >>> 7);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 & 131071L;
      values[valuesOffset++] = (block0 >>> 17) & 131071L;
      values[valuesOffset++] = (block0 >>> 34) & 131071L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 15L) << 13) | (block0 >>> 51);
      values[valuesOffset++] = (block1 >>> 4) & 131071L;
      values[valuesOffset++] = (block1 >>> 21) & 131071L;
      values[valuesOffset++] = (block1 >>> 38) & 131071L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 255L) << 9) | (block1 >>> 55);
      values[valuesOffset++] = (block2 >>> 8) & 131071L;
      values[valuesOffset++] = (block2 >>> 25) & 131071L;
      values[valuesOffset++] = (block2 >>> 42) & 131071L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 4095L) << 5) | (block2 >>> 59);
      values[valuesOffset++] = (block3 >>> 12) & 131071L;
      values[valuesOffset++] = (block3 >>> 29) & 131071L;
      values[valuesOffset++] = (block3 >>> 46) & 131071L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 65535L) << 1) | (block3 >>> 63);
      values[valuesOffset++] = (block4 >>> 16) & 131071L;
      values[valuesOffset++] = (block4 >>> 33) & 131071L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 7L) << 14) | (block4 >>> 50);
      values[valuesOffset++] = (block5 >>> 3) & 131071L;
      values[valuesOffset++] = (block5 >>> 20) & 131071L;
      values[valuesOffset++] = (block5 >>> 37) & 131071L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 127L) << 10) | (block5 >>> 54);
      values[valuesOffset++] = (block6 >>> 7) & 131071L;
      values[valuesOffset++] = (block6 >>> 24) & 131071L;
      values[valuesOffset++] = (block6 >>> 41) & 131071L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 2047L) << 6) | (block6 >>> 58);
      values[valuesOffset++] = (block7 >>> 11) & 131071L;
      values[valuesOffset++] = (block7 >>> 28) & 131071L;
      values[valuesOffset++] = (block7 >>> 45) & 131071L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 32767L) << 2) | (block7 >>> 62);
      values[valuesOffset++] = (block8 >>> 15) & 131071L;
      values[valuesOffset++] = (block8 >>> 32) & 131071L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 3L) << 15) | (block8 >>> 49);
      values[valuesOffset++] = (block9 >>> 2) & 131071L;
      values[valuesOffset++] = (block9 >>> 19) & 131071L;
      values[valuesOffset++] = (block9 >>> 36) & 131071L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 63L) << 11) | (block9 >>> 53);
      values[valuesOffset++] = (block10 >>> 6) & 131071L;
      values[valuesOffset++] = (block10 >>> 23) & 131071L;
      values[valuesOffset++] = (block10 >>> 40) & 131071L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 1023L) << 7) | (block10 >>> 57);
      values[valuesOffset++] = (block11 >>> 10) & 131071L;
      values[valuesOffset++] = (block11 >>> 27) & 131071L;
      values[valuesOffset++] = (block11 >>> 44) & 131071L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 16383L) << 3) | (block11 >>> 61);
      values[valuesOffset++] = (block12 >>> 14) & 131071L;
      values[valuesOffset++] = (block12 >>> 31) & 131071L;
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block13 & 1L) << 16) | (block12 >>> 48);
      values[valuesOffset++] = (block13 >>> 1) & 131071L;
      values[valuesOffset++] = (block13 >>> 18) & 131071L;
      values[valuesOffset++] = (block13 >>> 35) & 131071L;
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block14 & 31L) << 12) | (block13 >>> 52);
      values[valuesOffset++] = (block14 >>> 5) & 131071L;
      values[valuesOffset++] = (block14 >>> 22) & 131071L;
      values[valuesOffset++] = (block14 >>> 39) & 131071L;
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block15 & 511L) << 8) | (block14 >>> 56);
      values[valuesOffset++] = (block15 >>> 9) & 131071L;
      values[valuesOffset++] = (block15 >>> 26) & 131071L;
      values[valuesOffset++] = (block15 >>> 43) & 131071L;
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block16 & 8191L) << 4) | (block15 >>> 60);
      values[valuesOffset++] = (block16 >>> 13) & 131071L;
      values[valuesOffset++] = (block16 >>> 30) & 131071L;
      values[valuesOffset++] = block16 >>> 47;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 1) << 16) | (byte1 << 8) | byte0;
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 3) << 15) | (byte3 << 7) | (byte2 >>> 1);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 7) << 14) | (byte5 << 6) | (byte4 >>> 2);
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 15) << 13) | (byte7 << 5) | (byte6 >>> 3);
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte10 & 31) << 12) | (byte9 << 4) | (byte8 >>> 4);
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte12 & 63) << 11) | (byte11 << 3) | (byte10 >>> 5);
      final long byte13 = blocks[blocksOffset++] & 0xFF;
      final long byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 127) << 10) | (byte13 << 2) | (byte12 >>> 6);
      final long byte15 = blocks[blocksOffset++] & 0xFF;
      final long byte16 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte16 << 9) | (byte15 << 1) | (byte14 >>> 7);
    }
  }

}
