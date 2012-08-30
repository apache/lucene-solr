// This file has been automatically generated, DO NOT EDIT

package org.apache.lucene.util.packed;

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

/**
 * Efficient sequential read/write of packed integers.
 */
final class BulkOperationPacked2 extends BulkOperationPacked {

    public BulkOperationPacked2() {
      super(2);
      assert blockCount() == 1;
      assert valueCount() == 32;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 62);
        values[valuesOffset++] = (int) ((block0 >>> 60) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 58) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 56) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 54) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 52) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 50) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 48) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 46) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 44) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 42) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 40) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 38) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 36) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 34) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 32) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 30) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 28) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 26) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 24) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 22) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 20) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 18) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 16) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 14) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 12) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 10) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 8) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 6) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 4) & 3L);
        values[valuesOffset++] = (int) ((block0 >>> 2) & 3L);
        values[valuesOffset++] = (int) (block0 & 3L);
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 >>> 6;
        values[valuesOffset++] = (byte0 >>> 4) & 3;
        values[valuesOffset++] = (byte0 >>> 2) & 3;
        values[valuesOffset++] = byte0 & 3;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte1 >>> 6;
        values[valuesOffset++] = (byte1 >>> 4) & 3;
        values[valuesOffset++] = (byte1 >>> 2) & 3;
        values[valuesOffset++] = byte1 & 3;
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte2 >>> 6;
        values[valuesOffset++] = (byte2 >>> 4) & 3;
        values[valuesOffset++] = (byte2 >>> 2) & 3;
        values[valuesOffset++] = byte2 & 3;
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte3 >>> 6;
        values[valuesOffset++] = (byte3 >>> 4) & 3;
        values[valuesOffset++] = (byte3 >>> 2) & 3;
        values[valuesOffset++] = byte3 & 3;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte4 >>> 6;
        values[valuesOffset++] = (byte4 >>> 4) & 3;
        values[valuesOffset++] = (byte4 >>> 2) & 3;
        values[valuesOffset++] = byte4 & 3;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte5 >>> 6;
        values[valuesOffset++] = (byte5 >>> 4) & 3;
        values[valuesOffset++] = (byte5 >>> 2) & 3;
        values[valuesOffset++] = byte5 & 3;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte6 >>> 6;
        values[valuesOffset++] = (byte6 >>> 4) & 3;
        values[valuesOffset++] = (byte6 >>> 2) & 3;
        values[valuesOffset++] = byte6 & 3;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte7 >>> 6;
        values[valuesOffset++] = (byte7 >>> 4) & 3;
        values[valuesOffset++] = (byte7 >>> 2) & 3;
        values[valuesOffset++] = byte7 & 3;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 62;
        values[valuesOffset++] = (block0 >>> 60) & 3L;
        values[valuesOffset++] = (block0 >>> 58) & 3L;
        values[valuesOffset++] = (block0 >>> 56) & 3L;
        values[valuesOffset++] = (block0 >>> 54) & 3L;
        values[valuesOffset++] = (block0 >>> 52) & 3L;
        values[valuesOffset++] = (block0 >>> 50) & 3L;
        values[valuesOffset++] = (block0 >>> 48) & 3L;
        values[valuesOffset++] = (block0 >>> 46) & 3L;
        values[valuesOffset++] = (block0 >>> 44) & 3L;
        values[valuesOffset++] = (block0 >>> 42) & 3L;
        values[valuesOffset++] = (block0 >>> 40) & 3L;
        values[valuesOffset++] = (block0 >>> 38) & 3L;
        values[valuesOffset++] = (block0 >>> 36) & 3L;
        values[valuesOffset++] = (block0 >>> 34) & 3L;
        values[valuesOffset++] = (block0 >>> 32) & 3L;
        values[valuesOffset++] = (block0 >>> 30) & 3L;
        values[valuesOffset++] = (block0 >>> 28) & 3L;
        values[valuesOffset++] = (block0 >>> 26) & 3L;
        values[valuesOffset++] = (block0 >>> 24) & 3L;
        values[valuesOffset++] = (block0 >>> 22) & 3L;
        values[valuesOffset++] = (block0 >>> 20) & 3L;
        values[valuesOffset++] = (block0 >>> 18) & 3L;
        values[valuesOffset++] = (block0 >>> 16) & 3L;
        values[valuesOffset++] = (block0 >>> 14) & 3L;
        values[valuesOffset++] = (block0 >>> 12) & 3L;
        values[valuesOffset++] = (block0 >>> 10) & 3L;
        values[valuesOffset++] = (block0 >>> 8) & 3L;
        values[valuesOffset++] = (block0 >>> 6) & 3L;
        values[valuesOffset++] = (block0 >>> 4) & 3L;
        values[valuesOffset++] = (block0 >>> 2) & 3L;
        values[valuesOffset++] = block0 & 3L;
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 >>> 6;
        values[valuesOffset++] = (byte0 >>> 4) & 3;
        values[valuesOffset++] = (byte0 >>> 2) & 3;
        values[valuesOffset++] = byte0 & 3;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte1 >>> 6;
        values[valuesOffset++] = (byte1 >>> 4) & 3;
        values[valuesOffset++] = (byte1 >>> 2) & 3;
        values[valuesOffset++] = byte1 & 3;
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte2 >>> 6;
        values[valuesOffset++] = (byte2 >>> 4) & 3;
        values[valuesOffset++] = (byte2 >>> 2) & 3;
        values[valuesOffset++] = byte2 & 3;
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte3 >>> 6;
        values[valuesOffset++] = (byte3 >>> 4) & 3;
        values[valuesOffset++] = (byte3 >>> 2) & 3;
        values[valuesOffset++] = byte3 & 3;
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte4 >>> 6;
        values[valuesOffset++] = (byte4 >>> 4) & 3;
        values[valuesOffset++] = (byte4 >>> 2) & 3;
        values[valuesOffset++] = byte4 & 3;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte5 >>> 6;
        values[valuesOffset++] = (byte5 >>> 4) & 3;
        values[valuesOffset++] = (byte5 >>> 2) & 3;
        values[valuesOffset++] = byte5 & 3;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte6 >>> 6;
        values[valuesOffset++] = (byte6 >>> 4) & 3;
        values[valuesOffset++] = (byte6 >>> 2) & 3;
        values[valuesOffset++] = byte6 & 3;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte7 >>> 6;
        values[valuesOffset++] = (byte7 >>> 4) & 3;
        values[valuesOffset++] = (byte7 >>> 2) & 3;
        values[valuesOffset++] = byte7 & 3;
      }
    }

}
