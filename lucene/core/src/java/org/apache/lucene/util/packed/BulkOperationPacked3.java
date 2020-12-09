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
final class BulkOperationPacked3 extends BulkOperationPacked {

  public BulkOperationPacked3() {
    super(3);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 3) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 6) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 9) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 12) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 15) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 18) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 21) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 24) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 27) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 30) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 33) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 36) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 39) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 42) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 45) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 48) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 51) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 54) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 57) & 7L);
      values[valuesOffset++] = (int) ((block0 >>> 60) & 7L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) ((block1 & 3L) << 1 | block0 >>> 63);
      values[valuesOffset++] = (int) ((block1 >>> 2) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 5) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 8) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 11) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 14) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 17) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 20) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 23) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 26) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 29) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 32) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 35) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 38) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 41) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 44) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 47) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 50) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 53) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 56) & 7L);
      values[valuesOffset++] = (int) ((block1 >>> 59) & 7L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) ((block2 & 1L) << 2 | block1 >>> 62);
      values[valuesOffset++] = (int) ((block2 >>> 1) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 4) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 7) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 10) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 13) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 16) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 19) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 22) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 25) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 28) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 31) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 34) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 37) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 40) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 43) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 46) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 49) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 52) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 55) & 7L);
      values[valuesOffset++] = (int) ((block2 >>> 58) & 7L);
      values[valuesOffset++] = (int) (block2 >>> 61);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = byte0 & 7;
      values[valuesOffset++] = (byte0 >>> 3) & 7;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 1) << 2) | (byte0 >>> 6);
      values[valuesOffset++] = (byte1 >>> 1) & 7;
      values[valuesOffset++] = (byte1 >>> 4) & 7;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 3) << 1) | (byte1 >>> 7);
      values[valuesOffset++] = (byte2 >>> 2) & 7;
      values[valuesOffset++] = (byte2 >>> 5) & 7;
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 & 7L;
      values[valuesOffset++] = ((block0 >>> 3) & 7L);
      values[valuesOffset++] = ((block0 >>> 6) & 7L);
      values[valuesOffset++] = ((block0 >>> 9) & 7L);
      values[valuesOffset++] = ((block0 >>> 12) & 7L);
      values[valuesOffset++] = ((block0 >>> 15) & 7L);
      values[valuesOffset++] = ((block0 >>> 18) & 7L);
      values[valuesOffset++] = ((block0 >>> 21) & 7L);
      values[valuesOffset++] = ((block0 >>> 24) & 7L);
      values[valuesOffset++] = ((block0 >>> 27) & 7L);
      values[valuesOffset++] = ((block0 >>> 30) & 7L);
      values[valuesOffset++] = ((block0 >>> 33) & 7L);
      values[valuesOffset++] = ((block0 >>> 36) & 7L);
      values[valuesOffset++] = ((block0 >>> 39) & 7L);
      values[valuesOffset++] = ((block0 >>> 42) & 7L);
      values[valuesOffset++] = ((block0 >>> 45) & 7L);
      values[valuesOffset++] = ((block0 >>> 48) & 7L);
      values[valuesOffset++] = ((block0 >>> 51) & 7L);
      values[valuesOffset++] = ((block0 >>> 54) & 7L);
      values[valuesOffset++] = ((block0 >>> 57) & 7L);
      values[valuesOffset++] = ((block0 >>> 60) & 7L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 3L) << 1 | block0 >>> 63);
      values[valuesOffset++] = ((block1 >>> 2) & 7L);
      values[valuesOffset++] = ((block1 >>> 5) & 7L);
      values[valuesOffset++] = ((block1 >>> 8) & 7L);
      values[valuesOffset++] = ((block1 >>> 11) & 7L);
      values[valuesOffset++] = ((block1 >>> 14) & 7L);
      values[valuesOffset++] = ((block1 >>> 17) & 7L);
      values[valuesOffset++] = ((block1 >>> 20) & 7L);
      values[valuesOffset++] = ((block1 >>> 23) & 7L);
      values[valuesOffset++] = ((block1 >>> 26) & 7L);
      values[valuesOffset++] = ((block1 >>> 29) & 7L);
      values[valuesOffset++] = ((block1 >>> 32) & 7L);
      values[valuesOffset++] = ((block1 >>> 35) & 7L);
      values[valuesOffset++] = ((block1 >>> 38) & 7L);
      values[valuesOffset++] = ((block1 >>> 41) & 7L);
      values[valuesOffset++] = ((block1 >>> 44) & 7L);
      values[valuesOffset++] = ((block1 >>> 47) & 7L);
      values[valuesOffset++] = ((block1 >>> 50) & 7L);
      values[valuesOffset++] = ((block1 >>> 53) & 7L);
      values[valuesOffset++] = ((block1 >>> 56) & 7L);
      values[valuesOffset++] = ((block1 >>> 59) & 7L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 1L) << 2 | block1 >>> 62);
      values[valuesOffset++] = ((block2 >>> 1) & 7L);
      values[valuesOffset++] = ((block2 >>> 4) & 7L);
      values[valuesOffset++] = ((block2 >>> 7) & 7L);
      values[valuesOffset++] = ((block2 >>> 10) & 7L);
      values[valuesOffset++] = ((block2 >>> 13) & 7L);
      values[valuesOffset++] = ((block2 >>> 16) & 7L);
      values[valuesOffset++] = ((block2 >>> 19) & 7L);
      values[valuesOffset++] = ((block2 >>> 22) & 7L);
      values[valuesOffset++] = ((block2 >>> 25) & 7L);
      values[valuesOffset++] = ((block2 >>> 28) & 7L);
      values[valuesOffset++] = ((block2 >>> 31) & 7L);
      values[valuesOffset++] = ((block2 >>> 34) & 7L);
      values[valuesOffset++] = ((block2 >>> 37) & 7L);
      values[valuesOffset++] = ((block2 >>> 40) & 7L);
      values[valuesOffset++] = ((block2 >>> 43) & 7L);
      values[valuesOffset++] = ((block2 >>> 46) & 7L);
      values[valuesOffset++] = ((block2 >>> 49) & 7L);
      values[valuesOffset++] = ((block2 >>> 52) & 7L);
      values[valuesOffset++] = ((block2 >>> 55) & 7L);
      values[valuesOffset++] = ((block2 >>> 58) & 7L);
      values[valuesOffset++] = (block2 >>> 61);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte0 ) & 7;
      values[valuesOffset++] = (byte0 >>> 3) & 7;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 1) << 2) | (byte0 >>> 6);
      values[valuesOffset++] = (byte1 >>> 1) & 7;
      values[valuesOffset++] = (byte1 >>> 4) & 7;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 3) << 1) | (byte1 >>> 7);
      values[valuesOffset++] = (byte2 >>> 2) & 7;
      values[valuesOffset++] = (byte2 >>> 5) & 7;
    }
  }
}
