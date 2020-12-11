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
final class BulkOperationPacked23 extends BulkOperationPacked {

  public BulkOperationPacked23() {
    super(23);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 8388607L);
      values[valuesOffset++] = (int) ((block0 >>> 23) & 8388607L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 31L) << 18) | (block0 >>> 46));
      values[valuesOffset++] = (int) ((block1 >>> 5) & 8388607L);
      values[valuesOffset++] = (int) ((block1 >>> 28) & 8388607L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 1023L) << 13) | (block1 >>> 51));
      values[valuesOffset++] = (int) ((block2 >>> 10) & 8388607L);
      values[valuesOffset++] = (int) ((block2 >>> 33) & 8388607L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 32767L) << 8) | (block2 >>> 56));
      values[valuesOffset++] = (int) ((block3 >>> 15) & 8388607L);
      values[valuesOffset++] = (int) ((block3 >>> 38) & 8388607L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 1048575L) << 3) | (block3 >>> 61));
      values[valuesOffset++] = (int) ((block4 >>> 20) & 8388607L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 3L) << 21) | (block4 >>> 43));
      values[valuesOffset++] = (int) ((block5 >>> 2) & 8388607L);
      values[valuesOffset++] = (int) ((block5 >>> 25) & 8388607L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 127L) << 16) | (block5 >>> 48));
      values[valuesOffset++] = (int) ((block6 >>> 7) & 8388607L);
      values[valuesOffset++] = (int) ((block6 >>> 30) & 8388607L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 4095L) << 11) | (block6 >>> 53));
      values[valuesOffset++] = (int) ((block7 >>> 12) & 8388607L);
      values[valuesOffset++] = (int) ((block7 >>> 35) & 8388607L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 131071L) << 6) | (block7 >>> 58));
      values[valuesOffset++] = (int) ((block8 >>> 17) & 8388607L);
      values[valuesOffset++] = (int) ((block8 >>> 40) & 8388607L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 4194303L) << 1) | (block8 >>> 63));
      values[valuesOffset++] = (int) ((block9 >>> 22) & 8388607L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 15L) << 19) | (block9 >>> 45));
      values[valuesOffset++] = (int) ((block10 >>> 4) & 8388607L);
      values[valuesOffset++] = (int) ((block10 >>> 27) & 8388607L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 511L) << 14) | (block10 >>> 50));
      values[valuesOffset++] = (int) ((block11 >>> 9) & 8388607L);
      values[valuesOffset++] = (int) ((block11 >>> 32) & 8388607L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 16383L) << 9) | (block11 >>> 55));
      values[valuesOffset++] = (int) ((block12 >>> 14) & 8388607L);
      values[valuesOffset++] = (int) ((block12 >>> 37) & 8388607L);
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block13 & 524287L) << 4) | (block12 >>> 60));
      values[valuesOffset++] = (int) ((block13 >>> 19) & 8388607L);
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block14 & 1L) << 22) | (block13 >>> 42));
      values[valuesOffset++] = (int) ((block14 >>> 1) & 8388607L);
      values[valuesOffset++] = (int) ((block14 >>> 24) & 8388607L);
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block15 & 63L) << 17) | (block14 >>> 47));
      values[valuesOffset++] = (int) ((block15 >>> 6) & 8388607L);
      values[valuesOffset++] = (int) ((block15 >>> 29) & 8388607L);
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block16 & 2047L) << 12) | (block15 >>> 52));
      values[valuesOffset++] = (int) ((block16 >>> 11) & 8388607L);
      values[valuesOffset++] = (int) ((block16 >>> 34) & 8388607L);
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block17 & 65535L) << 7) | (block16 >>> 57));
      values[valuesOffset++] = (int) ((block17 >>> 16) & 8388607L);
      values[valuesOffset++] = (int) ((block17 >>> 39) & 8388607L);
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block18 & 2097151L) << 2) | (block17 >>> 62));
      values[valuesOffset++] = (int) ((block18 >>> 21) & 8388607L);
      final long block19 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block19 & 7L) << 20) | (block18 >>> 44));
      values[valuesOffset++] = (int) ((block19 >>> 3) & 8388607L);
      values[valuesOffset++] = (int) ((block19 >>> 26) & 8388607L);
      final long block20 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block20 & 255L) << 15) | (block19 >>> 49));
      values[valuesOffset++] = (int) ((block20 >>> 8) & 8388607L);
      values[valuesOffset++] = (int) ((block20 >>> 31) & 8388607L);
      final long block21 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block21 & 8191L) << 10) | (block20 >>> 54));
      values[valuesOffset++] = (int) ((block21 >>> 13) & 8388607L);
      values[valuesOffset++] = (int) ((block21 >>> 36) & 8388607L);
      final long block22 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block22 & 262143L) << 5) | (block21 >>> 59));
      values[valuesOffset++] = (int) ((block22 >>> 18) & 8388607L);
      values[valuesOffset++] = (int) (block22 >>> 41);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 127) << 16) | (byte1 << 8) | byte0;
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 63) << 17) | (byte4 << 9) | (byte3 << 1) | (byte2 >>> 7);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 31) << 18) | (byte7 << 10) | (byte6 << 2) | (byte5 >>> 6);
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 15) << 19) | (byte10 << 11) | (byte9 << 3) | (byte8 >>> 5);
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      final int byte13 = blocks[blocksOffset++] & 0xFF;
      final int byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 7) << 20) | (byte13 << 12) | (byte12 << 4) | (byte11 >>> 4);
      final int byte15 = blocks[blocksOffset++] & 0xFF;
      final int byte16 = blocks[blocksOffset++] & 0xFF;
      final int byte17 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte17 & 3) << 21) | (byte16 << 13) | (byte15 << 5) | (byte14 >>> 3);
      final int byte18 = blocks[blocksOffset++] & 0xFF;
      final int byte19 = blocks[blocksOffset++] & 0xFF;
      final int byte20 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte20 & 1) << 22) | (byte19 << 14) | (byte18 << 6) | (byte17 >>> 2);
      final int byte21 = blocks[blocksOffset++] & 0xFF;
      final int byte22 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte22 << 15) | (byte21 << 7) | (byte20 >>> 1);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 & 8388607L;
      values[valuesOffset++] = (block0 >>> 23) & 8388607L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 31L) << 18) | (block0 >>> 46);
      values[valuesOffset++] = (block1 >>> 5) & 8388607L;
      values[valuesOffset++] = (block1 >>> 28) & 8388607L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 1023L) << 13) | (block1 >>> 51);
      values[valuesOffset++] = (block2 >>> 10) & 8388607L;
      values[valuesOffset++] = (block2 >>> 33) & 8388607L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 32767L) << 8) | (block2 >>> 56);
      values[valuesOffset++] = (block3 >>> 15) & 8388607L;
      values[valuesOffset++] = (block3 >>> 38) & 8388607L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 1048575L) << 3) | (block3 >>> 61);
      values[valuesOffset++] = (block4 >>> 20) & 8388607L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 3L) << 21) | (block4 >>> 43);
      values[valuesOffset++] = (block5 >>> 2) & 8388607L;
      values[valuesOffset++] = (block5 >>> 25) & 8388607L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 127L) << 16) | (block5 >>> 48);
      values[valuesOffset++] = (block6 >>> 7) & 8388607L;
      values[valuesOffset++] = (block6 >>> 30) & 8388607L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 4095L) << 11) | (block6 >>> 53);
      values[valuesOffset++] = (block7 >>> 12) & 8388607L;
      values[valuesOffset++] = (block7 >>> 35) & 8388607L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 131071L) << 6) | (block7 >>> 58);
      values[valuesOffset++] = (block8 >>> 17) & 8388607L;
      values[valuesOffset++] = (block8 >>> 40) & 8388607L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 4194303L) << 1) | (block8 >>> 63);
      values[valuesOffset++] = (block9 >>> 22) & 8388607L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 15L) << 19) | (block9 >>> 45);
      values[valuesOffset++] = (block10 >>> 4) & 8388607L;
      values[valuesOffset++] = (block10 >>> 27) & 8388607L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 511L) << 14) | (block10 >>> 50);
      values[valuesOffset++] = (block11 >>> 9) & 8388607L;
      values[valuesOffset++] = (block11 >>> 32) & 8388607L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 16383L) << 9) | (block11 >>> 55);
      values[valuesOffset++] = (block12 >>> 14) & 8388607L;
      values[valuesOffset++] = (block12 >>> 37) & 8388607L;
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block13 & 524287L) << 4) | (block12 >>> 60);
      values[valuesOffset++] = (block13 >>> 19) & 8388607L;
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block14 & 1L) << 22) | (block13 >>> 42);
      values[valuesOffset++] = (block14 >>> 1) & 8388607L;
      values[valuesOffset++] = (block14 >>> 24) & 8388607L;
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block15 & 63L) << 17) | (block14 >>> 47);
      values[valuesOffset++] = (block15 >>> 6) & 8388607L;
      values[valuesOffset++] = (block15 >>> 29) & 8388607L;
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block16 & 2047L) << 12) | (block15 >>> 52);
      values[valuesOffset++] = (block16 >>> 11) & 8388607L;
      values[valuesOffset++] = (block16 >>> 34) & 8388607L;
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block17 & 65535L) << 7) | (block16 >>> 57);
      values[valuesOffset++] = (block17 >>> 16) & 8388607L;
      values[valuesOffset++] = (block17 >>> 39) & 8388607L;
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block18 & 2097151L) << 2) | (block17 >>> 62);
      values[valuesOffset++] = (block18 >>> 21) & 8388607L;
      final long block19 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block19 & 7L) << 20) | (block18 >>> 44);
      values[valuesOffset++] = (block19 >>> 3) & 8388607L;
      values[valuesOffset++] = (block19 >>> 26) & 8388607L;
      final long block20 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block20 & 255L) << 15) | (block19 >>> 49);
      values[valuesOffset++] = (block20 >>> 8) & 8388607L;
      values[valuesOffset++] = (block20 >>> 31) & 8388607L;
      final long block21 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block21 & 8191L) << 10) | (block20 >>> 54);
      values[valuesOffset++] = (block21 >>> 13) & 8388607L;
      values[valuesOffset++] = (block21 >>> 36) & 8388607L;
      final long block22 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block22 & 262143L) << 5) | (block21 >>> 59);
      values[valuesOffset++] = (block22 >>> 18) & 8388607L;
      values[valuesOffset++] = block22 >>> 41;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 127) << 16) | (byte1 << 8) | byte0;
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 63) << 17) | (byte4 << 9) | (byte3 << 1) | (byte2 >>> 7);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 31) << 18) | (byte7 << 10) | (byte6 << 2) | (byte5 >>> 6);
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 15) << 19) | (byte10 << 11) | (byte9 << 3) | (byte8 >>> 5);
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      final long byte13 = blocks[blocksOffset++] & 0xFF;
      final long byte14 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte14 & 7) << 20) | (byte13 << 12) | (byte12 << 4) | (byte11 >>> 4);
      final long byte15 = blocks[blocksOffset++] & 0xFF;
      final long byte16 = blocks[blocksOffset++] & 0xFF;
      final long byte17 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte17 & 3) << 21) | (byte16 << 13) | (byte15 << 5) | (byte14 >>> 3);
      final long byte18 = blocks[blocksOffset++] & 0xFF;
      final long byte19 = blocks[blocksOffset++] & 0xFF;
      final long byte20 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte20 & 1) << 22) | (byte19 << 14) | (byte18 << 6) | (byte17 >>> 2);
      final long byte21 = blocks[blocksOffset++] & 0xFF;
      final long byte22 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte22 << 15) | (byte21 << 7) | (byte20 >>> 1);
    }
  }

}
