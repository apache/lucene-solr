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
final class BulkOperationPacked10 extends BulkOperationPacked {

  public BulkOperationPacked10() {
    super(10);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 1023L);
      values[valuesOffset++] = (int) ((block0 >>> 10) & 1023L);
      values[valuesOffset++] = (int) ((block0 >>> 20) & 1023L);
      values[valuesOffset++] = (int) ((block0 >>> 30) & 1023L);
      values[valuesOffset++] = (int) ((block0 >>> 40) & 1023L);
      values[valuesOffset++] = (int) ((block0 >>> 50) & 1023L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 63L) << 4) | (block0 >>> 60));
      values[valuesOffset++] = (int) ((block1 >>> 6) & 1023L);
      values[valuesOffset++] = (int) ((block1 >>> 16) & 1023L);
      values[valuesOffset++] = (int) ((block1 >>> 26) & 1023L);
      values[valuesOffset++] = (int) ((block1 >>> 36) & 1023L);
      values[valuesOffset++] = (int) ((block1 >>> 46) & 1023L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 3L) << 8) | (block1 >>> 56));
      values[valuesOffset++] = (int) ((block2 >>> 2) & 1023L);
      values[valuesOffset++] = (int) ((block2 >>> 12) & 1023L);
      values[valuesOffset++] = (int) ((block2 >>> 22) & 1023L);
      values[valuesOffset++] = (int) ((block2 >>> 32) & 1023L);
      values[valuesOffset++] = (int) ((block2 >>> 42) & 1023L);
      values[valuesOffset++] = (int) ((block2 >>> 52) & 1023L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 255L) << 2) | (block2 >>> 62));
      values[valuesOffset++] = (int) ((block3 >>> 8) & 1023L);
      values[valuesOffset++] = (int) ((block3 >>> 18) & 1023L);
      values[valuesOffset++] = (int) ((block3 >>> 28) & 1023L);
      values[valuesOffset++] = (int) ((block3 >>> 38) & 1023L);
      values[valuesOffset++] = (int) ((block3 >>> 48) & 1023L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 15L) << 6) | (block3 >>> 58));
      values[valuesOffset++] = (int) ((block4 >>> 4) & 1023L);
      values[valuesOffset++] = (int) ((block4 >>> 14) & 1023L);
      values[valuesOffset++] = (int) ((block4 >>> 24) & 1023L);
      values[valuesOffset++] = (int) ((block4 >>> 34) & 1023L);
      values[valuesOffset++] = (int) ((block4 >>> 44) & 1023L);
      values[valuesOffset++] = (int) ((block4 >>> 54) & 1023L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 3) << 8) | byte0;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 15) << 6) | (byte1 >>> 2);
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 63) << 4) | (byte2 >>> 4);
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte4 << 2) | (byte3 >>> 6);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (block0 & 1023L);
      values[valuesOffset++] = ((block0 >>> 10) & 1023L);
      values[valuesOffset++] = ((block0 >>> 20) & 1023L);
      values[valuesOffset++] = ((block0 >>> 30) & 1023L);
      values[valuesOffset++] = ((block0 >>> 40) & 1023L);
      values[valuesOffset++] = ((block0 >>> 50) & 1023L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block1 & 63L) << 4) | (block0 >>> 60));
      values[valuesOffset++] = ((block1 >>> 6) & 1023L);
      values[valuesOffset++] = ((block1 >>> 16) & 1023L);
      values[valuesOffset++] = ((block1 >>> 26) & 1023L);
      values[valuesOffset++] = ((block1 >>> 36) & 1023L);
      values[valuesOffset++] = ((block1 >>> 46) & 1023L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block2 & 3L) << 8) | (block1 >>> 56));
      values[valuesOffset++] = ((block2 >>> 2) & 1023L);
      values[valuesOffset++] = ((block2 >>> 12) & 1023L);
      values[valuesOffset++] = ((block2 >>> 22) & 1023L);
      values[valuesOffset++] = ((block2 >>> 32) & 1023L);
      values[valuesOffset++] = ((block2 >>> 42) & 1023L);
      values[valuesOffset++] = ((block2 >>> 52) & 1023L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block3 & 255L) << 2) | (block2 >>> 62));
      values[valuesOffset++] = ((block3 >>> 8) & 1023L);
      values[valuesOffset++] = ((block3 >>> 18) & 1023L);
      values[valuesOffset++] = ((block3 >>> 28) & 1023L);
      values[valuesOffset++] = ((block3 >>> 38) & 1023L);
      values[valuesOffset++] = ((block3 >>> 48) & 1023L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (((block4 & 15L) << 6) | (block3 >>> 58));
      values[valuesOffset++] = ((block4 >>> 4) & 1023L);
      values[valuesOffset++] = ((block4 >>> 14) & 1023L);
      values[valuesOffset++] = ((block4 >>> 24) & 1023L);
      values[valuesOffset++] = ((block4 >>> 34) & 1023L);
      values[valuesOffset++] = ((block4 >>> 44) & 1023L);
      values[valuesOffset++] = ((block4 >>> 54) & 1023L);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 3) << 8) | byte0;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte2 & 15) << 6) | (byte1 >>> 2);
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 63) << 4) | (byte2 >>> 4);
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte4 << 2) | (byte3 >>> 6);
    }
  }

}
