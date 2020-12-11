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
final class BulkOperationPacked15 extends BulkOperationPacked {

  public BulkOperationPacked15() {
    super(15);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 32767L);
      values[valuesOffset++] = (int) ((block0 >>> 15) & 32767L);
      values[valuesOffset++] = (int) ((block0 >>> 30) & 32767L);
      values[valuesOffset++] = (int) ((block0 >>> 45) & 32767L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 2047L) << 4) | (block0 >>> 60));
      values[valuesOffset++] = (int) ((block1 >>> 11) & 32767L);
      values[valuesOffset++] = (int) ((block1 >>> 26) & 32767L);
      values[valuesOffset++] = (int) ((block1 >>> 41) & 32767L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 127L) << 8) | (block1 >>> 56));
      values[valuesOffset++] = (int) ((block2 >>> 7) & 32767L);
      values[valuesOffset++] = (int) ((block2 >>> 22) & 32767L);
      values[valuesOffset++] = (int) ((block2 >>> 37) & 32767L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 7L) << 12) | (block2 >>> 52));
      values[valuesOffset++] = (int) ((block3 >>> 3) & 32767L);
      values[valuesOffset++] = (int) ((block3 >>> 18) & 32767L);
      values[valuesOffset++] = (int) ((block3 >>> 33) & 32767L);
      values[valuesOffset++] = (int) ((block3 >>> 48) & 32767L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 16383L) << 1) | (block3 >>> 63));
      values[valuesOffset++] = (int) ((block4 >>> 14) & 32767L);
      values[valuesOffset++] = (int) ((block4 >>> 29) & 32767L);
      values[valuesOffset++] = (int) ((block4 >>> 44) & 32767L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 1023L) << 5) | (block4 >>> 59));
      values[valuesOffset++] = (int) ((block5 >>> 10) & 32767L);
      values[valuesOffset++] = (int) ((block5 >>> 25) & 32767L);
      values[valuesOffset++] = (int) ((block5 >>> 40) & 32767L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 63L) << 9) | (block5 >>> 55));
      values[valuesOffset++] = (int) ((block6 >>> 6) & 32767L);
      values[valuesOffset++] = (int) ((block6 >>> 21) & 32767L);
      values[valuesOffset++] = (int) ((block6 >>> 36) & 32767L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 3L) << 13) | (block6 >>> 51));
      values[valuesOffset++] = (int) ((block7 >>> 2) & 32767L);
      values[valuesOffset++] = (int) ((block7 >>> 17) & 32767L);
      values[valuesOffset++] = (int) ((block7 >>> 32) & 32767L);
      values[valuesOffset++] = (int) ((block7 >>> 47) & 32767L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 8191L) << 2) | (block7 >>> 62));
      values[valuesOffset++] = (int) ((block8 >>> 13) & 32767L);
      values[valuesOffset++] = (int) ((block8 >>> 28) & 32767L);
      values[valuesOffset++] = (int) ((block8 >>> 43) & 32767L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 511L) << 6) | (block8 >>> 58));
      values[valuesOffset++] = (int) ((block9 >>> 9) & 32767L);
      values[valuesOffset++] = (int) ((block9 >>> 24) & 32767L);
      values[valuesOffset++] = (int) ((block9 >>> 39) & 32767L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 31L) << 10) | (block9 >>> 54));
      values[valuesOffset++] = (int) ((block10 >>> 5) & 32767L);
      values[valuesOffset++] = (int) ((block10 >>> 20) & 32767L);
      values[valuesOffset++] = (int) ((block10 >>> 35) & 32767L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 1L) << 14) | (block10 >>> 50));
      values[valuesOffset++] = (int) ((block11 >>> 1) & 32767L);
      values[valuesOffset++] = (int) ((block11 >>> 16) & 32767L);
      values[valuesOffset++] = (int) ((block11 >>> 31) & 32767L);
      values[valuesOffset++] = (int) ((block11 >>> 46) & 32767L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 4095L) << 3) | (block11 >>> 61));
      values[valuesOffset++] = (int) ((block12 >>> 12) & 32767L);
      values[valuesOffset++] = (int) ((block12 >>> 27) & 32767L);
      values[valuesOffset++] = (int) ((block12 >>> 42) & 32767L);
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block13 & 255L) << 7) | (block12 >>> 57));
      values[valuesOffset++] = (int) ((block13 >>> 8) & 32767L);
      values[valuesOffset++] = (int) ((block13 >>> 23) & 32767L);
      values[valuesOffset++] = (int) ((block13 >>> 38) & 32767L);
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block14 & 15L) << 11) | (block13 >>> 53));
      values[valuesOffset++] = (int) ((block14 >>> 4) & 32767L);
      values[valuesOffset++] = (int) ((block14 >>> 19) & 32767L);
      values[valuesOffset++] = (int) ((block14 >>> 34) & 32767L);
      values[valuesOffset++] = (int) (block14 >>> 49);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 127) << 8) | byte0;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 63) << 9) | (byte2 << 1) | (byte1 >>> 7);
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 31) << 10) | (byte4 << 2) | (byte3 >>> 6);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 15) << 11) | (byte6 << 3) | (byte5 >>> 5);
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 7) << 12) | (byte8 << 4) | (byte7 >>> 4);
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 3) << 13) | (byte10 << 5) | (byte9 >>> 3);
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      final int byte13 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte13 & 1) << 14) | (byte12 << 6) | (byte11 >>> 2);
      final int byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte14 << 7) | (byte13 >>> 1);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 & 32767L;
      values[valuesOffset++] = (block0 >>> 15) & 32767L;
      values[valuesOffset++] = (block0 >>> 30) & 32767L;
      values[valuesOffset++] = (block0 >>> 45) & 32767L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 2047L) << 4) | (block0 >>> 60);
      values[valuesOffset++] = (block1 >>> 11) & 32767L;
      values[valuesOffset++] = (block1 >>> 26) & 32767L;
      values[valuesOffset++] = (block1 >>> 41) & 32767L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 127L) << 8) | (block1 >>> 56);
      values[valuesOffset++] = (block2 >>> 7) & 32767L;
      values[valuesOffset++] = (block2 >>> 22) & 32767L;
      values[valuesOffset++] = (block2 >>> 37) & 32767L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 7L) << 12) | (block2 >>> 52);
      values[valuesOffset++] = (block3 >>> 3) & 32767L;
      values[valuesOffset++] = (block3 >>> 18) & 32767L;
      values[valuesOffset++] = (block3 >>> 33) & 32767L;
      values[valuesOffset++] = (block3 >>> 48) & 32767L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 16383L) << 1) | (block3 >>> 63);
      values[valuesOffset++] = (block4 >>> 14) & 32767L;
      values[valuesOffset++] = (block4 >>> 29) & 32767L;
      values[valuesOffset++] = (block4 >>> 44) & 32767L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 1023L) << 5) | (block4 >>> 59);
      values[valuesOffset++] = (block5 >>> 10) & 32767L;
      values[valuesOffset++] = (block5 >>> 25) & 32767L;
      values[valuesOffset++] = (block5 >>> 40) & 32767L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 63L) << 9) | (block5 >>> 55);
      values[valuesOffset++] = (block6 >>> 6) & 32767L;
      values[valuesOffset++] = (block6 >>> 21) & 32767L;
      values[valuesOffset++] = (block6 >>> 36) & 32767L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 3L) << 13) | (block6 >>> 51);
      values[valuesOffset++] = (block7 >>> 2) & 32767L;
      values[valuesOffset++] = (block7 >>> 17) & 32767L;
      values[valuesOffset++] = (block7 >>> 32) & 32767L;
      values[valuesOffset++] = (block7 >>> 47) & 32767L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 8191L) << 2) | (block7 >>> 62);
      values[valuesOffset++] = (block8 >>> 13) & 32767L;
      values[valuesOffset++] = (block8 >>> 28) & 32767L;
      values[valuesOffset++] = (block8 >>> 43) & 32767L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 511L) << 6) | (block8 >>> 58);
      values[valuesOffset++] = (block9 >>> 9) & 32767L;
      values[valuesOffset++] = (block9 >>> 24) & 32767L;
      values[valuesOffset++] = (block9 >>> 39) & 32767L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 31L) << 10) | (block9 >>> 54);
      values[valuesOffset++] = (block10 >>> 5) & 32767L;
      values[valuesOffset++] = (block10 >>> 20) & 32767L;
      values[valuesOffset++] = (block10 >>> 35) & 32767L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 1L) << 14) | (block10 >>> 50);
      values[valuesOffset++] = (block11 >>> 1) & 32767L;
      values[valuesOffset++] = (block11 >>> 16) & 32767L;
      values[valuesOffset++] = (block11 >>> 31) & 32767L;
      values[valuesOffset++] = (block11 >>> 46) & 32767L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 4095L) << 3) | (block11 >>> 61);
      values[valuesOffset++] = (block12 >>> 12) & 32767L;
      values[valuesOffset++] = (block12 >>> 27) & 32767L;
      values[valuesOffset++] = (block12 >>> 42) & 32767L;
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block13 & 255L) << 7) | (block12 >>> 57);
      values[valuesOffset++] = (block13 >>> 8) & 32767L;
      values[valuesOffset++] = (block13 >>> 23) & 32767L;
      values[valuesOffset++] = (block13 >>> 38) & 32767L;
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block14 & 15L) << 11) | (block13 >>> 53);
      values[valuesOffset++] = (block14 >>> 4) & 32767L;
      values[valuesOffset++] = (block14 >>> 19) & 32767L;
      values[valuesOffset++] = (block14 >>> 34) & 32767L;
      values[valuesOffset++] = block14 >>> 49;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 127) << 8) | byte0;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 63) << 9) | (byte2 << 1) | (byte1 >>> 7);
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 31) << 10) | (byte4 << 2) | (byte3 >>> 6);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 15) << 11) | (byte6 << 3) | (byte5 >>> 5);
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 7) << 12) | (byte8 << 4) | (byte7 >>> 4);
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 3) << 13) | (byte10 << 5) | (byte9 >>> 3);
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      final long byte13 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte13 & 1) << 14) | (byte12 << 6) | (byte11 >>> 2);
      final long byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte14 << 7) | (byte13 >>> 1);
    }
  }

}
