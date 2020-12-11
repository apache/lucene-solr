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
final class BulkOperationPacked5 extends BulkOperationPacked {

  public BulkOperationPacked5() {
    super(5);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 5) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 10) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 15) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 20) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 25) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 30) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 35) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 40) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 45) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 50) & 31L);
      values[valuesOffset++] = (int) ((block0 >>> 55) & 31L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 1L) << 4) | (block0 >>> 60));
      values[valuesOffset++] = (int) ((block1 >>> 1) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 6) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 11) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 16) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 21) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 26) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 31) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 36) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 41) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 46) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 51) & 31L);
      values[valuesOffset++] = (int) ((block1 >>> 56) & 31L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 3L) << 3) | (block1 >>> 61));
      values[valuesOffset++] = (int) ((block2 >>> 2) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 7) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 12) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 17) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 22) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 27) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 32) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 37) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 42) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 47) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 52) & 31L);
      values[valuesOffset++] = (int) ((block2 >>> 57) & 31L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 7L) << 2) | (block2 >>> 62));
      values[valuesOffset++] = (int) ((block3 >>> 3) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 8) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 13) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 18) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 23) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 28) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 33) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 38) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 43) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 48) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 53) & 31L);
      values[valuesOffset++] = (int) ((block3 >>> 58) & 31L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 15L) << 1) | (block3 >>> 63));
      values[valuesOffset++] = (int) ((block4 >>> 4) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 9) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 14) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 19) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 24) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 29) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 34) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 39) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 44) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 49) & 31L);
      values[valuesOffset++] = (int) ((block4 >>> 54) & 31L);
      values[valuesOffset++] = (int) (block4 >>> 59);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 & 31;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 3) << 3) | (byte0 >>> 5);
      values[valuesOffset++] = (byte1 >>> 2) & 31;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 15) << 1) | (byte1 >>> 7);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 1) << 4) | (byte2 >>> 4);
      values[valuesOffset++] = (byte3 >>> 1) & 31;
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 7) << 2) | (byte3 >>> 6);
      values[valuesOffset++] = (byte4 >>> 3);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 & 31L;
      values[valuesOffset++] = (block0 >>> 5) & 31L;
      values[valuesOffset++] = (block0 >>> 10) & 31L;
      values[valuesOffset++] = (block0 >>> 15) & 31L;
      values[valuesOffset++] = (block0 >>> 20) & 31L;
      values[valuesOffset++] = (block0 >>> 25) & 31L;
      values[valuesOffset++] = (block0 >>> 30) & 31L;
      values[valuesOffset++] = (block0 >>> 35) & 31L;
      values[valuesOffset++] = (block0 >>> 40) & 31L;
      values[valuesOffset++] = (block0 >>> 45) & 31L;
      values[valuesOffset++] = (block0 >>> 50) & 31L;
      values[valuesOffset++] = (block0 >>> 55) & 31L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 1L) << 4) | (block0 >>> 60);
      values[valuesOffset++] = (block1 >>> 1) & 31L;
      values[valuesOffset++] = (block1 >>> 6) & 31L;
      values[valuesOffset++] = (block1 >>> 11) & 31L;
      values[valuesOffset++] = (block1 >>> 16) & 31L;
      values[valuesOffset++] = (block1 >>> 21) & 31L;
      values[valuesOffset++] = (block1 >>> 26) & 31L;
      values[valuesOffset++] = (block1 >>> 31) & 31L;
      values[valuesOffset++] = (block1 >>> 36) & 31L;
      values[valuesOffset++] = (block1 >>> 41) & 31L;
      values[valuesOffset++] = (block1 >>> 46) & 31L;
      values[valuesOffset++] = (block1 >>> 51) & 31L;
      values[valuesOffset++] = (block1 >>> 56) & 31L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 3L) << 3) | (block1 >>> 61);
      values[valuesOffset++] = (block2 >>> 2) & 31L;
      values[valuesOffset++] = (block2 >>> 7) & 31L;
      values[valuesOffset++] = (block2 >>> 12) & 31L;
      values[valuesOffset++] = (block2 >>> 17) & 31L;
      values[valuesOffset++] = (block2 >>> 22) & 31L;
      values[valuesOffset++] = (block2 >>> 27) & 31L;
      values[valuesOffset++] = (block2 >>> 32) & 31L;
      values[valuesOffset++] = (block2 >>> 37) & 31L;
      values[valuesOffset++] = (block2 >>> 42) & 31L;
      values[valuesOffset++] = (block2 >>> 47) & 31L;
      values[valuesOffset++] = (block2 >>> 52) & 31L;
      values[valuesOffset++] = (block2 >>> 57) & 31L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 7L) << 2) | (block2 >>> 62);
      values[valuesOffset++] = (block3 >>> 3) & 31L;
      values[valuesOffset++] = (block3 >>> 8) & 31L;
      values[valuesOffset++] = (block3 >>> 13) & 31L;
      values[valuesOffset++] = (block3 >>> 18) & 31L;
      values[valuesOffset++] = (block3 >>> 23) & 31L;
      values[valuesOffset++] = (block3 >>> 28) & 31L;
      values[valuesOffset++] = (block3 >>> 33) & 31L;
      values[valuesOffset++] = (block3 >>> 38) & 31L;
      values[valuesOffset++] = (block3 >>> 43) & 31L;
      values[valuesOffset++] = (block3 >>> 48) & 31L;
      values[valuesOffset++] = (block3 >>> 53) & 31L;
      values[valuesOffset++] = (block3 >>> 58) & 31L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 15L) << 1) | (block3 >>> 63);
      values[valuesOffset++] = (block4 >>> 4) & 31L;
      values[valuesOffset++] = (block4 >>> 9) & 31L;
      values[valuesOffset++] = (block4 >>> 14) & 31L;
      values[valuesOffset++] = (block4 >>> 19) & 31L;
      values[valuesOffset++] = (block4 >>> 24) & 31L;
      values[valuesOffset++] = (block4 >>> 29) & 31L;
      values[valuesOffset++] = (block4 >>> 34) & 31L;
      values[valuesOffset++] = (block4 >>> 39) & 31L;
      values[valuesOffset++] = (block4 >>> 44) & 31L;
      values[valuesOffset++] = (block4 >>> 49) & 31L;
      values[valuesOffset++] = (block4 >>> 54) & 31L;
      values[valuesOffset++] = block4 >>> 59;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 & 31;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 3) << 3) | (byte0 >>> 5);
      values[valuesOffset++] = (byte1 >>> 2) & 31;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 15) << 1) | (byte1 >>> 7);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 1) << 4) | (byte2 >>> 4);
      values[valuesOffset++] = (byte3 >>> 1) & 31;
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 7) << 2) | (byte3 >>> 6);
      values[valuesOffset++] = (byte4 >>> 3);
    }
  }

}
