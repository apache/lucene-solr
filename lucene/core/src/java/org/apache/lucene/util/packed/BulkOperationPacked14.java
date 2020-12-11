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
final class BulkOperationPacked14 extends BulkOperationPacked {

  public BulkOperationPacked14() {
    super(14);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 16383L);
      values[valuesOffset++] = (int) ((block0 >>> 14) & 16383L);
      values[valuesOffset++] = (int) ((block0 >>> 28) & 16383L);
      values[valuesOffset++] = (int) ((block0 >>> 42) & 16383L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 63L) << 8) | (block0 >>> 56));
      values[valuesOffset++] = (int) ((block1 >>> 6) & 16383L);
      values[valuesOffset++] = (int) ((block1 >>> 20) & 16383L);
      values[valuesOffset++] = (int) ((block1 >>> 34) & 16383L);
      values[valuesOffset++] = (int) ((block1 >>> 48) & 16383L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 4095L) << 2) | (block1 >>> 62));
      values[valuesOffset++] = (int) ((block2 >>> 12) & 16383L);
      values[valuesOffset++] = (int) ((block2 >>> 26) & 16383L);
      values[valuesOffset++] = (int) ((block2 >>> 40) & 16383L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 15L) << 10) | (block2 >>> 54));
      values[valuesOffset++] = (int) ((block3 >>> 4) & 16383L);
      values[valuesOffset++] = (int) ((block3 >>> 18) & 16383L);
      values[valuesOffset++] = (int) ((block3 >>> 32) & 16383L);
      values[valuesOffset++] = (int) ((block3 >>> 46) & 16383L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 1023L) << 4) | (block3 >>> 60));
      values[valuesOffset++] = (int) ((block4 >>> 10) & 16383L);
      values[valuesOffset++] = (int) ((block4 >>> 24) & 16383L);
      values[valuesOffset++] = (int) ((block4 >>> 38) & 16383L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 3L) << 12) | (block4 >>> 52));
      values[valuesOffset++] = (int) ((block5 >>> 2) & 16383L);
      values[valuesOffset++] = (int) ((block5 >>> 16) & 16383L);
      values[valuesOffset++] = (int) ((block5 >>> 30) & 16383L);
      values[valuesOffset++] = (int) ((block5 >>> 44) & 16383L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 255L) << 6) | (block5 >>> 58));
      values[valuesOffset++] = (int) ((block6 >>> 8) & 16383L);
      values[valuesOffset++] = (int) ((block6 >>> 22) & 16383L);
      values[valuesOffset++] = (int) ((block6 >>> 36) & 16383L);
      values[valuesOffset++] = (int) (block6 >>> 50);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 63) << 8) | byte0;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 15) << 10) | (byte2 << 2) | (byte1 >>> 6);
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 3) << 12) | (byte4 << 4) | (byte3 >>> 4);
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte6 << 6) | (byte5 >>> 2);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 & 16383L;
      values[valuesOffset++] = (block0 >>> 14) & 16383L;
      values[valuesOffset++] = (block0 >>> 28) & 16383L;
      values[valuesOffset++] = (block0 >>> 42) & 16383L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 63L) << 8) | (block0 >>> 56);
      values[valuesOffset++] = (block1 >>> 6) & 16383L;
      values[valuesOffset++] = (block1 >>> 20) & 16383L;
      values[valuesOffset++] = (block1 >>> 34) & 16383L;
      values[valuesOffset++] = (block1 >>> 48) & 16383L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 4095L) << 2) | (block1 >>> 62);
      values[valuesOffset++] = (block2 >>> 12) & 16383L;
      values[valuesOffset++] = (block2 >>> 26) & 16383L;
      values[valuesOffset++] = (block2 >>> 40) & 16383L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 15L) << 10) | (block2 >>> 54);
      values[valuesOffset++] = (block3 >>> 4) & 16383L;
      values[valuesOffset++] = (block3 >>> 18) & 16383L;
      values[valuesOffset++] = (block3 >>> 32) & 16383L;
      values[valuesOffset++] = (block3 >>> 46) & 16383L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 1023L) << 4) | (block3 >>> 60);
      values[valuesOffset++] = (block4 >>> 10) & 16383L;
      values[valuesOffset++] = (block4 >>> 24) & 16383L;
      values[valuesOffset++] = (block4 >>> 38) & 16383L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 3L) << 12) | (block4 >>> 52);
      values[valuesOffset++] = (block5 >>> 2) & 16383L;
      values[valuesOffset++] = (block5 >>> 16) & 16383L;
      values[valuesOffset++] = (block5 >>> 30) & 16383L;
      values[valuesOffset++] = (block5 >>> 44) & 16383L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 255L) << 6) | (block5 >>> 58);
      values[valuesOffset++] = (block6 >>> 8) & 16383L;
      values[valuesOffset++] = (block6 >>> 22) & 16383L;
      values[valuesOffset++] = (block6 >>> 36) & 16383L;
      values[valuesOffset++] = block6 >>> 50;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 63) << 8) | byte0;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 15) << 10) | (byte2 << 2) | (byte1 >>> 6);
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte5 & 3) << 12) | (byte4 << 4) | (byte3 >>> 4);
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte6 << 6) | (byte5 >>> 2);
    }
  }

}
