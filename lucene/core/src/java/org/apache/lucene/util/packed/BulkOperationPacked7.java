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
final class BulkOperationPacked7 extends BulkOperationPacked {

  public BulkOperationPacked7() {
    super(7);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 7) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 14) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 21) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 28) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 35) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 42) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 49) & 127L);
      values[valuesOffset++] = (int) ((block0 >>> 56) & 127L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 63L) << 1) | (block0 >>> 63));
      values[valuesOffset++] = (int) ((block1 >>> 6) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 13) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 20) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 27) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 34) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 41) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 48) & 127L);
      values[valuesOffset++] = (int) ((block1 >>> 55) & 127L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 31L) << 2) | (block1 >>> 62));
      values[valuesOffset++] = (int) ((block2 >>> 5) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 12) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 19) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 26) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 33) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 40) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 47) & 127L);
      values[valuesOffset++] = (int) ((block2 >>> 54) & 127L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 15L) << 3) | (block2 >>> 61));
      values[valuesOffset++] = (int) ((block3 >>> 4) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 11) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 18) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 25) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 32) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 39) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 46) & 127L);
      values[valuesOffset++] = (int) ((block3 >>> 53) & 127L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 7L) << 4) | (block3 >>> 60));
      values[valuesOffset++] = (int) ((block4 >>> 3) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 10) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 17) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 24) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 31) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 38) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 45) & 127L);
      values[valuesOffset++] = (int) ((block4 >>> 52) & 127L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 3L) << 5) | (block4 >>> 59));
      values[valuesOffset++] = (int) ((block5 >>> 2) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 9) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 16) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 23) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 30) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 37) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 44) & 127L);
      values[valuesOffset++] = (int) ((block5 >>> 51) & 127L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 1L) << 6) | (block5 >>> 58));
      values[valuesOffset++] = (int) ((block6 >>> 1) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 8) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 15) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 22) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 29) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 36) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 43) & 127L);
      values[valuesOffset++] = (int) ((block6 >>> 50) & 127L);
      values[valuesOffset++] = (int) (block6 >>> 57);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 & 127;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 63) << 1) | (byte0 >>> 7);
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 31) << 2) | (byte1 >>> 6);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 15) << 3) | (byte2 >>> 5);
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 7) << 4) | (byte3 >>> 4);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 3) << 5) | (byte4 >>> 3);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 1) << 6) | (byte5 >>> 2);
      values[valuesOffset++] = (byte6 >>> 1);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 & 127L;
      values[valuesOffset++] = (block0 >>> 7) & 127L;
      values[valuesOffset++] = (block0 >>> 14) & 127L;
      values[valuesOffset++] = (block0 >>> 21) & 127L;
      values[valuesOffset++] = (block0 >>> 28) & 127L;
      values[valuesOffset++] = (block0 >>> 35) & 127L;
      values[valuesOffset++] = (block0 >>> 42) & 127L;
      values[valuesOffset++] = (block0 >>> 49) & 127L;
      values[valuesOffset++] = (block0 >>> 56) & 127L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 63L) << 1) | (block0 >>> 63);
      values[valuesOffset++] = (block1 >>> 6) & 127L;
      values[valuesOffset++] = (block1 >>> 13) & 127L;
      values[valuesOffset++] = (block1 >>> 20) & 127L;
      values[valuesOffset++] = (block1 >>> 27) & 127L;
      values[valuesOffset++] = (block1 >>> 34) & 127L;
      values[valuesOffset++] = (block1 >>> 41) & 127L;
      values[valuesOffset++] = (block1 >>> 48) & 127L;
      values[valuesOffset++] = (block1 >>> 55) & 127L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 31L) << 2) | (block1 >>> 62);
      values[valuesOffset++] = (block2 >>> 5) & 127L;
      values[valuesOffset++] = (block2 >>> 12) & 127L;
      values[valuesOffset++] = (block2 >>> 19) & 127L;
      values[valuesOffset++] = (block2 >>> 26) & 127L;
      values[valuesOffset++] = (block2 >>> 33) & 127L;
      values[valuesOffset++] = (block2 >>> 40) & 127L;
      values[valuesOffset++] = (block2 >>> 47) & 127L;
      values[valuesOffset++] = (block2 >>> 54) & 127L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 15L) << 3) | (block2 >>> 61);
      values[valuesOffset++] = (block3 >>> 4) & 127L;
      values[valuesOffset++] = (block3 >>> 11) & 127L;
      values[valuesOffset++] = (block3 >>> 18) & 127L;
      values[valuesOffset++] = (block3 >>> 25) & 127L;
      values[valuesOffset++] = (block3 >>> 32) & 127L;
      values[valuesOffset++] = (block3 >>> 39) & 127L;
      values[valuesOffset++] = (block3 >>> 46) & 127L;
      values[valuesOffset++] = (block3 >>> 53) & 127L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 7L) << 4) | (block3 >>> 60);
      values[valuesOffset++] = (block4 >>> 3) & 127L;
      values[valuesOffset++] = (block4 >>> 10) & 127L;
      values[valuesOffset++] = (block4 >>> 17) & 127L;
      values[valuesOffset++] = (block4 >>> 24) & 127L;
      values[valuesOffset++] = (block4 >>> 31) & 127L;
      values[valuesOffset++] = (block4 >>> 38) & 127L;
      values[valuesOffset++] = (block4 >>> 45) & 127L;
      values[valuesOffset++] = (block4 >>> 52) & 127L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 3L) << 5) | (block4 >>> 59);
      values[valuesOffset++] = (block5 >>> 2) & 127L;
      values[valuesOffset++] = (block5 >>> 9) & 127L;
      values[valuesOffset++] = (block5 >>> 16) & 127L;
      values[valuesOffset++] = (block5 >>> 23) & 127L;
      values[valuesOffset++] = (block5 >>> 30) & 127L;
      values[valuesOffset++] = (block5 >>> 37) & 127L;
      values[valuesOffset++] = (block5 >>> 44) & 127L;
      values[valuesOffset++] = (block5 >>> 51) & 127L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 1L) << 6) | (block5 >>> 58);
      values[valuesOffset++] = (block6 >>> 1) & 127L;
      values[valuesOffset++] = (block6 >>> 8) & 127L;
      values[valuesOffset++] = (block6 >>> 15) & 127L;
      values[valuesOffset++] = (block6 >>> 22) & 127L;
      values[valuesOffset++] = (block6 >>> 29) & 127L;
      values[valuesOffset++] = (block6 >>> 36) & 127L;
      values[valuesOffset++] = (block6 >>> 43) & 127L;
      values[valuesOffset++] = (block6 >>> 50) & 127L;
      values[valuesOffset++] = block6 >>> 57;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 & 127;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 63) << 1) | (byte0 >>> 7);
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 31) << 2) | (byte1 >>> 6);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 15) << 3) | (byte2 >>> 5);
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 7) << 4) | (byte3 >>> 4);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 3) << 5) | (byte4 >>> 3);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 1) << 6) | (byte5 >>> 2);
      values[valuesOffset++] = (byte6 >>> 1);
    }
  }

}
