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
final class BulkOperationPacked9 extends BulkOperationPacked {

  public BulkOperationPacked9() {
    super(9);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 9) & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 18) & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 27) & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 36) & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 45) & 511L);
      values[valuesOffset++] = (int) ((block0 >>> 54) & 511L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 255L) << 1) | (block0 >>> 63));
      values[valuesOffset++] = (int) ((block1 >>> 8) & 511L);
      values[valuesOffset++] = (int) ((block1 >>> 17) & 511L);
      values[valuesOffset++] = (int) ((block1 >>> 26) & 511L);
      values[valuesOffset++] = (int) ((block1 >>> 35) & 511L);
      values[valuesOffset++] = (int) ((block1 >>> 44) & 511L);
      values[valuesOffset++] = (int) ((block1 >>> 53) & 511L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 127L) << 2) | (block1 >>> 62));
      values[valuesOffset++] = (int) ((block2 >>> 7) & 511L);
      values[valuesOffset++] = (int) ((block2 >>> 16) & 511L);
      values[valuesOffset++] = (int) ((block2 >>> 25) & 511L);
      values[valuesOffset++] = (int) ((block2 >>> 34) & 511L);
      values[valuesOffset++] = (int) ((block2 >>> 43) & 511L);
      values[valuesOffset++] = (int) ((block2 >>> 52) & 511L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 63L) << 3) | (block2 >>> 61));
      values[valuesOffset++] = (int) ((block3 >>> 6) & 511L);
      values[valuesOffset++] = (int) ((block3 >>> 15) & 511L);
      values[valuesOffset++] = (int) ((block3 >>> 24) & 511L);
      values[valuesOffset++] = (int) ((block3 >>> 33) & 511L);
      values[valuesOffset++] = (int) ((block3 >>> 42) & 511L);
      values[valuesOffset++] = (int) ((block3 >>> 51) & 511L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 31L) << 4) | (block3 >>> 60));
      values[valuesOffset++] = (int) ((block4 >>> 5) & 511L);
      values[valuesOffset++] = (int) ((block4 >>> 14) & 511L);
      values[valuesOffset++] = (int) ((block4 >>> 23) & 511L);
      values[valuesOffset++] = (int) ((block4 >>> 32) & 511L);
      values[valuesOffset++] = (int) ((block4 >>> 41) & 511L);
      values[valuesOffset++] = (int) ((block4 >>> 50) & 511L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 15L) << 5) | (block4 >>> 59));
      values[valuesOffset++] = (int) ((block5 >>> 4) & 511L);
      values[valuesOffset++] = (int) ((block5 >>> 13) & 511L);
      values[valuesOffset++] = (int) ((block5 >>> 22) & 511L);
      values[valuesOffset++] = (int) ((block5 >>> 31) & 511L);
      values[valuesOffset++] = (int) ((block5 >>> 40) & 511L);
      values[valuesOffset++] = (int) ((block5 >>> 49) & 511L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 7L) << 6) | (block5 >>> 58));
      values[valuesOffset++] = (int) ((block6 >>> 3) & 511L);
      values[valuesOffset++] = (int) ((block6 >>> 12) & 511L);
      values[valuesOffset++] = (int) ((block6 >>> 21) & 511L);
      values[valuesOffset++] = (int) ((block6 >>> 30) & 511L);
      values[valuesOffset++] = (int) ((block6 >>> 39) & 511L);
      values[valuesOffset++] = (int) ((block6 >>> 48) & 511L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 3L) << 7) | (block6 >>> 57));
      values[valuesOffset++] = (int) ((block7 >>> 2) & 511L);
      values[valuesOffset++] = (int) ((block7 >>> 11) & 511L);
      values[valuesOffset++] = (int) ((block7 >>> 20) & 511L);
      values[valuesOffset++] = (int) ((block7 >>> 29) & 511L);
      values[valuesOffset++] = (int) ((block7 >>> 38) & 511L);
      values[valuesOffset++] = (int) ((block7 >>> 47) & 511L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 1L) << 8) | (block7 >>> 56));
      values[valuesOffset++] = (int) ((block8 >>> 1) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 10) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 19) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 28) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 37) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 46) & 511L);
      values[valuesOffset++] = (int) ((block8 >>> 55) & 511L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 | ((byte1 & 1) << 8);
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 3) << 7) | (byte1 >>> 1);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 7) << 6) | (byte2 >>> 2);
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 15) << 5) | (byte3 >>> 3);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 31) << 4) | (byte4 >>> 4);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 63) << 3) | (byte5 >>> 5);
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 127) << 2) | (byte6 >>> 6);
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte8 << 1) | (byte7 >>> 7);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (block0 & 511L);
      values[valuesOffset++] = ((block0 >>> 9) & 511L);
      values[valuesOffset++] = ((block0 >>> 18) & 511L);
      values[valuesOffset++] = ((block0 >>> 27) & 511L);
      values[valuesOffset++] = ((block0 >>> 36) & 511L);
      values[valuesOffset++] = ((block0 >>> 45) & 511L);
      values[valuesOffset++] = ((block0 >>> 54) & 511L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block1 & 255L) << 1) | (block0 >>> 63));
      values[valuesOffset++] = ((block1 >>> 8) & 511L);
      values[valuesOffset++] = ((block1 >>> 17) & 511L);
      values[valuesOffset++] = ((block1 >>> 26) & 511L);
      values[valuesOffset++] = ((block1 >>> 35) & 511L);
      values[valuesOffset++] = ((block1 >>> 44) & 511L);
      values[valuesOffset++] = ((block1 >>> 53) & 511L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block2 & 127L) << 2) | (block1 >>> 62));
      values[valuesOffset++] = ((block2 >>> 7) & 511L);
      values[valuesOffset++] = ((block2 >>> 16) & 511L);
      values[valuesOffset++] = ((block2 >>> 25) & 511L);
      values[valuesOffset++] = ((block2 >>> 34) & 511L);
      values[valuesOffset++] = ((block2 >>> 43) & 511L);
      values[valuesOffset++] = ((block2 >>> 52) & 511L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block3 & 63L) << 3) | (block2 >>> 61));
      values[valuesOffset++] = ((block3 >>> 6) & 511L);
      values[valuesOffset++] = ((block3 >>> 15) & 511L);
      values[valuesOffset++] = ((block3 >>> 24) & 511L);
      values[valuesOffset++] = ((block3 >>> 33) & 511L);
      values[valuesOffset++] = ((block3 >>> 42) & 511L);
      values[valuesOffset++] = ((block3 >>> 51) & 511L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block4 & 31L) << 4) | (block3 >>> 60));
      values[valuesOffset++] = ((block4 >>> 5) & 511L);
      values[valuesOffset++] = ((block4 >>> 14) & 511L);
      values[valuesOffset++] = ((block4 >>> 23) & 511L);
      values[valuesOffset++] = ((block4 >>> 32) & 511L);
      values[valuesOffset++] = ((block4 >>> 41) & 511L);
      values[valuesOffset++] = ((block4 >>> 50) & 511L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block5 & 15L) << 5) | (block4 >>> 59));
      values[valuesOffset++] = ((block5 >>> 4) & 511L);
      values[valuesOffset++] = ((block5 >>> 13) & 511L);
      values[valuesOffset++] = ((block5 >>> 22) & 511L);
      values[valuesOffset++] = ((block5 >>> 31) & 511L);
      values[valuesOffset++] = ((block5 >>> 40) & 511L);
      values[valuesOffset++] = ((block5 >>> 49) & 511L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block6 & 7L) << 6) | (block5 >>> 58));
      values[valuesOffset++] = ((block6 >>> 3) & 511L);
      values[valuesOffset++] = ((block6 >>> 12) & 511L);
      values[valuesOffset++] = ((block6 >>> 21) & 511L);
      values[valuesOffset++] = ((block6 >>> 30) & 511L);
      values[valuesOffset++] = ((block6 >>> 39) & 511L);
      values[valuesOffset++] = ((block6 >>> 48) & 511L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block7 & 3L) << 7) | (block6 >>> 57));
      values[valuesOffset++] = ((block7 >>> 2) & 511L);
      values[valuesOffset++] = ((block7 >>> 11) & 511L);
      values[valuesOffset++] = ((block7 >>> 20) & 511L);
      values[valuesOffset++] = ((block7 >>> 29) & 511L);
      values[valuesOffset++] = ((block7 >>> 38) & 511L);
      values[valuesOffset++] = ((block7 >>> 47) & 511L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block8 & 1L) << 8) | (block7 >>> 56));
      values[valuesOffset++] = ((block8 >>> 1) & 511L);
      values[valuesOffset++] = ((block8 >>> 10) & 511L);
      values[valuesOffset++] = ((block8 >>> 19) & 511L);
      values[valuesOffset++] = ((block8 >>> 28) & 511L);
      values[valuesOffset++] = ((block8 >>> 37) & 511L);
      values[valuesOffset++] = ((block8 >>> 46) & 511L);
      values[valuesOffset++] = ((block8 >>> 55) & 511L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 | ((byte1 & 1) << 8);
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 3) << 7) | (byte1 >>> 1);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 7) << 6) | (byte2 >>> 2);
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 15) << 5) | (byte3 >>> 3);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 31) << 4) | (byte4 >>> 4);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 63) << 3) | (byte5 >>> 5);
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte7 & 127) << 2) | (byte6 >>> 6);
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte8 << 1) | (byte7 >>> 7);
    }
  }

}
