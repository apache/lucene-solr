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
final class BulkOperationPacked11 extends BulkOperationPacked {

  public BulkOperationPacked11() {
    super(11);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 2047L);
      values[valuesOffset++] = (int) ((block0 >>> 11) & 2047L);
      values[valuesOffset++] = (int) ((block0 >>> 22) & 2047L);
      values[valuesOffset++] = (int) ((block0 >>> 33) & 2047L);
      values[valuesOffset++] = (int) ((block0 >>> 44) & 2047L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 3L) << 9) | (block0 >>> 55));
      values[valuesOffset++] = (int) ((block1 >>> 2) & 2047L);
      values[valuesOffset++] = (int) ((block1 >>> 13) & 2047L);
      values[valuesOffset++] = (int) ((block1 >>> 24) & 2047L);
      values[valuesOffset++] = (int) ((block1 >>> 35) & 2047L);
      values[valuesOffset++] = (int) ((block1 >>> 46) & 2047L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 15L) << 7) | (block1 >>> 57));
      values[valuesOffset++] = (int) ((block2 >>> 4) & 2047L);
      values[valuesOffset++] = (int) ((block2 >>> 15) & 2047L);
      values[valuesOffset++] = (int) ((block2 >>> 26) & 2047L);
      values[valuesOffset++] = (int) ((block2 >>> 37) & 2047L);
      values[valuesOffset++] = (int) ((block2 >>> 48) & 2047L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 63L) << 5) | (block2 >>> 59));
      values[valuesOffset++] = (int) ((block3 >>> 6) & 2047L);
      values[valuesOffset++] = (int) ((block3 >>> 17) & 2047L);
      values[valuesOffset++] = (int) ((block3 >>> 28) & 2047L);
      values[valuesOffset++] = (int) ((block3 >>> 39) & 2047L);
      values[valuesOffset++] = (int) ((block3 >>> 50) & 2047L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 255L) << 3) | (block3 >>> 61));
      values[valuesOffset++] = (int) ((block4 >>> 8) & 2047L);
      values[valuesOffset++] = (int) ((block4 >>> 19) & 2047L);
      values[valuesOffset++] = (int) ((block4 >>> 30) & 2047L);
      values[valuesOffset++] = (int) ((block4 >>> 41) & 2047L);
      values[valuesOffset++] = (int) ((block4 >>> 52) & 2047L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 1023L) << 1) | (block4 >>> 63));
      values[valuesOffset++] = (int) ((block5 >>> 10) & 2047L);
      values[valuesOffset++] = (int) ((block5 >>> 21) & 2047L);
      values[valuesOffset++] = (int) ((block5 >>> 32) & 2047L);
      values[valuesOffset++] = (int) ((block5 >>> 43) & 2047L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 1L) << 10) | (block5 >>> 54));
      values[valuesOffset++] = (int) ((block6 >>> 1) & 2047L);
      values[valuesOffset++] = (int) ((block6 >>> 12) & 2047L);
      values[valuesOffset++] = (int) ((block6 >>> 23) & 2047L);
      values[valuesOffset++] = (int) ((block6 >>> 34) & 2047L);
      values[valuesOffset++] = (int) ((block6 >>> 45) & 2047L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 7L) << 8) | (block6 >>> 56));
      values[valuesOffset++] = (int) ((block7 >>> 3) & 2047L);
      values[valuesOffset++] = (int) ((block7 >>> 14) & 2047L);
      values[valuesOffset++] = (int) ((block7 >>> 25) & 2047L);
      values[valuesOffset++] = (int) ((block7 >>> 36) & 2047L);
      values[valuesOffset++] = (int) ((block7 >>> 47) & 2047L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 31L) << 6) | (block7 >>> 58));
      values[valuesOffset++] = (int) ((block8 >>> 5) & 2047L);
      values[valuesOffset++] = (int) ((block8 >>> 16) & 2047L);
      values[valuesOffset++] = (int) ((block8 >>> 27) & 2047L);
      values[valuesOffset++] = (int) ((block8 >>> 38) & 2047L);
      values[valuesOffset++] = (int) ((block8 >>> 49) & 2047L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 127L) << 4) | (block8 >>> 60));
      values[valuesOffset++] = (int) ((block9 >>> 7) & 2047L);
      values[valuesOffset++] = (int) ((block9 >>> 18) & 2047L);
      values[valuesOffset++] = (int) ((block9 >>> 29) & 2047L);
      values[valuesOffset++] = (int) ((block9 >>> 40) & 2047L);
      values[valuesOffset++] = (int) ((block9 >>> 51) & 2047L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 511L) << 2) | (block9 >>> 62));
      values[valuesOffset++] = (int) ((block10 >>> 9) & 2047L);
      values[valuesOffset++] = (int) ((block10 >>> 20) & 2047L);
      values[valuesOffset++] = (int) ((block10 >>> 31) & 2047L);
      values[valuesOffset++] = (int) ((block10 >>> 42) & 2047L);
      values[valuesOffset++] = (int) ((block10 >>> 53) & 2047L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 | ((byte1 & 7) << 8);
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 63) << 5) | (byte1 >>> 3);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 1) << 10) | (byte3 << 2) | (byte2 >>> 6);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 15) << 7) | (byte4 >>> 1);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 127) << 4) | (byte5 >>> 4);
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 3) << 9) | (byte7 << 1) | (byte6 >>> 7);
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 31) << 6) | (byte8 >>> 2);
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte10 << 3) |  (byte9 >>> 5);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (block0 & 2047L);
      values[valuesOffset++] = ((block0 >>> 11) & 2047L);
      values[valuesOffset++] = ((block0 >>> 22) & 2047L);
      values[valuesOffset++] = ((block0 >>> 33) & 2047L);
      values[valuesOffset++] = ((block0 >>> 44) & 2047L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block1 & 3L) << 9) | (block0 >>> 55));
      values[valuesOffset++] = ((block1 >>> 2) & 2047L);
      values[valuesOffset++] = ((block1 >>> 13) & 2047L);
      values[valuesOffset++] = ((block1 >>> 24) & 2047L);
      values[valuesOffset++] = ((block1 >>> 35) & 2047L);
      values[valuesOffset++] = ((block1 >>> 46) & 2047L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block2 & 15L) << 7) | (block1 >>> 57));
      values[valuesOffset++] = ((block2 >>> 4) & 2047L);
      values[valuesOffset++] = ((block2 >>> 15) & 2047L);
      values[valuesOffset++] = ((block2 >>> 26) & 2047L);
      values[valuesOffset++] = ((block2 >>> 37) & 2047L);
      values[valuesOffset++] = ((block2 >>> 48) & 2047L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block3 & 63L) << 5) | (block2 >>> 59));
      values[valuesOffset++] = ((block3 >>> 6) & 2047L);
      values[valuesOffset++] = ((block3 >>> 17) & 2047L);
      values[valuesOffset++] = ((block3 >>> 28) & 2047L);
      values[valuesOffset++] = ((block3 >>> 39) & 2047L);
      values[valuesOffset++] = ((block3 >>> 50) & 2047L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block4 & 255L) << 3) | (block3 >>> 61));
      values[valuesOffset++] = ((block4 >>> 8) & 2047L);
      values[valuesOffset++] = ((block4 >>> 19) & 2047L);
      values[valuesOffset++] = ((block4 >>> 30) & 2047L);
      values[valuesOffset++] = ((block4 >>> 41) & 2047L);
      values[valuesOffset++] = ((block4 >>> 52) & 2047L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block5 & 1023L) << 1) | (block4 >>> 63));
      values[valuesOffset++] = ((block5 >>> 10) & 2047L);
      values[valuesOffset++] = ((block5 >>> 21) & 2047L);
      values[valuesOffset++] = ((block5 >>> 32) & 2047L);
      values[valuesOffset++] = ((block5 >>> 43) & 2047L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block6 & 1L) << 10) | (block5 >>> 54));
      values[valuesOffset++] = ((block6 >>> 1) & 2047L);
      values[valuesOffset++] = ((block6 >>> 12) & 2047L);
      values[valuesOffset++] = ((block6 >>> 23) & 2047L);
      values[valuesOffset++] = ((block6 >>> 34) & 2047L);
      values[valuesOffset++] = ((block6 >>> 45) & 2047L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block7 & 7L) << 8) | (block6 >>> 56));
      values[valuesOffset++] = ((block7 >>> 3) & 2047L);
      values[valuesOffset++] = ((block7 >>> 14) & 2047L);
      values[valuesOffset++] = ((block7 >>> 25) & 2047L);
      values[valuesOffset++] = ((block7 >>> 36) & 2047L);
      values[valuesOffset++] = ((block7 >>> 47) & 2047L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block8 & 31L) << 6) | (block7 >>> 58));
      values[valuesOffset++] = ((block8 >>> 5) & 2047L);
      values[valuesOffset++] = ((block8 >>> 16) & 2047L);
      values[valuesOffset++] = ((block8 >>> 27) & 2047L);
      values[valuesOffset++] = ((block8 >>> 38) & 2047L);
      values[valuesOffset++] = ((block8 >>> 49) & 2047L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block9 & 127L) << 4) | (block8 >>> 60));
      values[valuesOffset++] = ((block9 >>> 7) & 2047L);
      values[valuesOffset++] = ((block9 >>> 18) & 2047L);
      values[valuesOffset++] = ((block9 >>> 29) & 2047L);
      values[valuesOffset++] = ((block9 >>> 40) & 2047L);
      values[valuesOffset++] = ((block9 >>> 51) & 2047L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block10 & 511L) << 2) | (block9 >>> 62));
      values[valuesOffset++] = ((block10 >>> 9) & 2047L);
      values[valuesOffset++] = ((block10 >>> 20) & 2047L);
      values[valuesOffset++] = ((block10 >>> 31) & 2047L);
      values[valuesOffset++] = ((block10 >>> 42) & 2047L);
      values[valuesOffset++] = ((block10 >>> 53) & 2047L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 | ((byte1 & 7) << 8);
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 63) << 5) | (byte1 >>> 3);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 1) << 10) | (byte3 << 2) | (byte2 >>> 6);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 15) << 7) | (byte4 >>> 1);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 127) << 4) | (byte5 >>> 4);
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 3) << 9) | (byte7 << 1) | (byte6 >>> 7);
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 31) << 6) | (byte8 >>> 2);
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte10 << 3) |  (byte9 >>> 5);
    }
  }

}
