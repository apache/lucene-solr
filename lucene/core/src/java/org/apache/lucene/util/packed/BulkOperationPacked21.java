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
final class BulkOperationPacked21 extends BulkOperationPacked {

  public BulkOperationPacked21() {
    super(21);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 2097151L);
      values[valuesOffset++] = (int) ((block0 >>> 21) & 2097151L);
      values[valuesOffset++] = (int) ((block0 >>> 42) & 2097151L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 1048575L) << 1) | (block0 >>> 63));
      values[valuesOffset++] = (int) ((block1 >>> 20) & 2097151L);
      values[valuesOffset++] = (int) ((block1 >>> 41) & 2097151L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 524287L) << 2) | (block1 >>> 62));
      values[valuesOffset++] = (int) ((block2 >>> 19) & 2097151L);
      values[valuesOffset++] = (int) ((block2 >>> 40) & 2097151L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 262143L) << 3) | (block2 >>> 61));
      values[valuesOffset++] = (int) ((block3 >>> 18) & 2097151L);
      values[valuesOffset++] = (int) ((block3 >>> 39) & 2097151L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 131071L) << 4) | (block3 >>> 60));
      values[valuesOffset++] = (int) ((block4 >>> 17) & 2097151L);
      values[valuesOffset++] = (int) ((block4 >>> 38) & 2097151L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 65535L) << 5) | (block4 >>> 59));
      values[valuesOffset++] = (int) ((block5 >>> 16) & 2097151L);
      values[valuesOffset++] = (int) ((block5 >>> 37) & 2097151L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 32767L) << 6) | (block5 >>> 58));
      values[valuesOffset++] = (int) ((block6 >>> 15) & 2097151L);
      values[valuesOffset++] = (int) ((block6 >>> 36) & 2097151L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 16383L) << 7) | (block6 >>> 57));
      values[valuesOffset++] = (int) ((block7 >>> 14) & 2097151L);
      values[valuesOffset++] = (int) ((block7 >>> 35) & 2097151L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 8191L) << 8) | (block7 >>> 56));
      values[valuesOffset++] = (int) ((block8 >>> 13) & 2097151L);
      values[valuesOffset++] = (int) ((block8 >>> 34) & 2097151L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 4095L) << 9) | (block8 >>> 55));
      values[valuesOffset++] = (int) ((block9 >>> 12) & 2097151L);
      values[valuesOffset++] = (int) ((block9 >>> 33) & 2097151L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 2047L) << 10) | (block9 >>> 54));
      values[valuesOffset++] = (int) ((block10 >>> 11) & 2097151L);
      values[valuesOffset++] = (int) ((block10 >>> 32) & 2097151L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 1023L) << 11) | (block10 >>> 53));
      values[valuesOffset++] = (int) ((block11 >>> 10) & 2097151L);
      values[valuesOffset++] = (int) ((block11 >>> 31) & 2097151L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 511L) << 12) | (block11 >>> 52));
      values[valuesOffset++] = (int) ((block12 >>> 9) & 2097151L);
      values[valuesOffset++] = (int) ((block12 >>> 30) & 2097151L);
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block13 & 255L) << 13) | (block12 >>> 51));
      values[valuesOffset++] = (int) ((block13 >>> 8) & 2097151L);
      values[valuesOffset++] = (int) ((block13 >>> 29) & 2097151L);
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block14 & 127L) << 14) | (block13 >>> 50));
      values[valuesOffset++] = (int) ((block14 >>> 7) & 2097151L);
      values[valuesOffset++] = (int) ((block14 >>> 28) & 2097151L);
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block15 & 63L) << 15) | (block14 >>> 49));
      values[valuesOffset++] = (int) ((block15 >>> 6) & 2097151L);
      values[valuesOffset++] = (int) ((block15 >>> 27) & 2097151L);
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block16 & 31L) << 16) | (block15 >>> 48));
      values[valuesOffset++] = (int) ((block16 >>> 5) & 2097151L);
      values[valuesOffset++] = (int) ((block16 >>> 26) & 2097151L);
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block17 & 15L) << 17) | (block16 >>> 47));
      values[valuesOffset++] = (int) ((block17 >>> 4) & 2097151L);
      values[valuesOffset++] = (int) ((block17 >>> 25) & 2097151L);
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block18 & 7L) << 18) | (block17 >>> 46));
      values[valuesOffset++] = (int) ((block18 >>> 3) & 2097151L);
      values[valuesOffset++] = (int) ((block18 >>> 24) & 2097151L);
      final long block19 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block19 & 3L) << 19) | (block18 >>> 45));
      values[valuesOffset++] = (int) ((block19 >>> 2) & 2097151L);
      values[valuesOffset++] = (int) ((block19 >>> 23) & 2097151L);
      final long block20 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block20 & 1L) << 20) | (block19 >>> 44));
      values[valuesOffset++] = (int) ((block20 >>> 1) & 2097151L);
      values[valuesOffset++] = (int) ((block20 >>> 22) & 2097151L);
      values[valuesOffset++] = (int) (block20 >>> 43);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 31) << 16) | (byte1 << 8) | byte0;
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 3) << 19) | (byte4 << 11) | (byte3 << 3) | (byte2 >>> 5);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 127) << 14) | (byte6 << 6) | (byte5 >>> 2);
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte10 & 15) << 17) | (byte9 << 9) | (byte8 << 1) | (byte7 >>> 7);
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      final int byte13 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte13 & 1) << 20) | (byte12 << 12) | (byte11 << 4) | (byte10 >>> 4);
      final int byte14 = blocks[blocksOffset++] & 0xFF;
      final int byte15 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte15 & 63) << 15) | (byte14 << 7) | (byte13 >>> 1);
      final int byte16 = blocks[blocksOffset++] & 0xFF;
      final int byte17 = blocks[blocksOffset++] & 0xFF;
      final int byte18 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte18 & 7) << 18) | (byte17 << 10) | (byte16 << 2) | (byte15 >>> 6);
      final int byte19 = blocks[blocksOffset++] & 0xFF;
      final int byte20 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte20 << 13) | (byte19 << 5) | (byte18 >>> 3);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 & 2097151L;
      values[valuesOffset++] = (block0 >>> 21) & 2097151L;
      values[valuesOffset++] = (block0 >>> 42) & 2097151L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 1048575L) << 1) | (block0 >>> 63);
      values[valuesOffset++] = (block1 >>> 20) & 2097151L;
      values[valuesOffset++] = (block1 >>> 41) & 2097151L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 524287L) << 2) | (block1 >>> 62);
      values[valuesOffset++] = (block2 >>> 19) & 2097151L;
      values[valuesOffset++] = (block2 >>> 40) & 2097151L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 262143L) << 3) | (block2 >>> 61);
      values[valuesOffset++] = (block3 >>> 18) & 2097151L;
      values[valuesOffset++] = (block3 >>> 39) & 2097151L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 131071L) << 4) | (block3 >>> 60);
      values[valuesOffset++] = (block4 >>> 17) & 2097151L;
      values[valuesOffset++] = (block4 >>> 38) & 2097151L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 65535L) << 5) | (block4 >>> 59);
      values[valuesOffset++] = (block5 >>> 16) & 2097151L;
      values[valuesOffset++] = (block5 >>> 37) & 2097151L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 32767L) << 6) | (block5 >>> 58);
      values[valuesOffset++] = (block6 >>> 15) & 2097151L;
      values[valuesOffset++] = (block6 >>> 36) & 2097151L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 16383L) << 7) | (block6 >>> 57);
      values[valuesOffset++] = (block7 >>> 14) & 2097151L;
      values[valuesOffset++] = (block7 >>> 35) & 2097151L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 8191L) << 8) | (block7 >>> 56);
      values[valuesOffset++] = (block8 >>> 13) & 2097151L;
      values[valuesOffset++] = (block8 >>> 34) & 2097151L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 4095L) << 9) | (block8 >>> 55);
      values[valuesOffset++] = (block9 >>> 12) & 2097151L;
      values[valuesOffset++] = (block9 >>> 33) & 2097151L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 2047L) << 10) | (block9 >>> 54);
      values[valuesOffset++] = (block10 >>> 11) & 2097151L;
      values[valuesOffset++] = (block10 >>> 32) & 2097151L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 1023L) << 11) | (block10 >>> 53);
      values[valuesOffset++] = (block11 >>> 10) & 2097151L;
      values[valuesOffset++] = (block11 >>> 31) & 2097151L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 511L) << 12) | (block11 >>> 52);
      values[valuesOffset++] = (block12 >>> 9) & 2097151L;
      values[valuesOffset++] = (block12 >>> 30) & 2097151L;
      final long block13 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block13 & 255L) << 13) | (block12 >>> 51);
      values[valuesOffset++] = (block13 >>> 8) & 2097151L;
      values[valuesOffset++] = (block13 >>> 29) & 2097151L;
      final long block14 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block14 & 127L) << 14) | (block13 >>> 50);
      values[valuesOffset++] = (block14 >>> 7) & 2097151L;
      values[valuesOffset++] = (block14 >>> 28) & 2097151L;
      final long block15 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block15 & 63L) << 15) | (block14 >>> 49);
      values[valuesOffset++] = (block15 >>> 6) & 2097151L;
      values[valuesOffset++] = (block15 >>> 27) & 2097151L;
      final long block16 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block16 & 31L) << 16) | (block15 >>> 48);
      values[valuesOffset++] = (block16 >>> 5) & 2097151L;
      values[valuesOffset++] = (block16 >>> 26) & 2097151L;
      final long block17 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block17 & 15L) << 17) | (block16 >>> 47);
      values[valuesOffset++] = (block17 >>> 4) & 2097151L;
      values[valuesOffset++] = (block17 >>> 25) & 2097151L;
      final long block18 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block18 & 7L) << 18) | (block17 >>> 46);
      values[valuesOffset++] = (block18 >>> 3) & 2097151L;
      values[valuesOffset++] = (block18 >>> 24) & 2097151L;
      final long block19 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block19 & 3L) << 19) | (block18 >>> 45);
      values[valuesOffset++] = (block19 >>> 2) & 2097151L;
      values[valuesOffset++] = (block19 >>> 23) & 2097151L;
      final long block20 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block20 & 1L) << 20) | (block19 >>> 44);
      values[valuesOffset++] = (block20 >>> 1) & 2097151L;
      values[valuesOffset++] = (block20 >>> 22) & 2097151L;
      values[valuesOffset++] = block20 >>> 43;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 31) << 16) | (byte1 << 8) | byte0;
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 3) << 19) | (byte4 << 11) | (byte3 << 3) | (byte2 >>> 5);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 127) << 14) | (byte6 << 6) | (byte5 >>> 2);
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte10 & 15) << 17) | (byte9 << 9) | (byte8 << 1) | (byte7 >>> 7);
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      final long byte13 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte13 & 1) << 20) | (byte12 << 12) | (byte11 << 4) | (byte10 >>> 4);
      final long byte14 = blocks[blocksOffset++] & 0xFF;
      final long byte15 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte15 & 63) << 15) | (byte14 << 7) | (byte13 >>> 1);
      final long byte16 = blocks[blocksOffset++] & 0xFF;
      final long byte17 = blocks[blocksOffset++] & 0xFF;
      final long byte18 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte18 & 7) << 18) | (byte17 << 10) | (byte16 << 2) | (byte15 >>> 6);
      final long byte19 = blocks[blocksOffset++] & 0xFF;
      final long byte20 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte20 << 13) | (byte19 << 5) | (byte18 >>> 3);
    }
  }

}
