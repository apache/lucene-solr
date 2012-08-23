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
final class BulkOperationPackedSingleBlock7 extends BulkOperation {
    @Override
    public int blockCount() {
      return 1;
     }

    @Override
    public int valueCount() {
      return 9;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block & 127L);
        values[valuesOffset++] = (int) ((block >>> 7) & 127L);
        values[valuesOffset++] = (int) ((block >>> 14) & 127L);
        values[valuesOffset++] = (int) ((block >>> 21) & 127L);
        values[valuesOffset++] = (int) ((block >>> 28) & 127L);
        values[valuesOffset++] = (int) ((block >>> 35) & 127L);
        values[valuesOffset++] = (int) ((block >>> 42) & 127L);
        values[valuesOffset++] = (int) ((block >>> 49) & 127L);
        values[valuesOffset++] = (int) (block >>> 56);
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 & 127;
        values[valuesOffset++] = (byte0 >>> 7) | ((byte1 & 63) << 1);
        values[valuesOffset++] = (byte1 >>> 6) | ((byte2 & 31) << 2);
        values[valuesOffset++] = (byte2 >>> 5) | ((byte3 & 15) << 3);
        values[valuesOffset++] = (byte3 >>> 4) | ((byte4 & 7) << 4);
        values[valuesOffset++] = (byte4 >>> 3) | ((byte5 & 3) << 5);
        values[valuesOffset++] = (byte5 >>> 2) | ((byte6 & 1) << 6);
        values[valuesOffset++] = byte6 >>> 1;
        values[valuesOffset++] = byte7 & 127;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = block & 127L;
        values[valuesOffset++] = (block >>> 7) & 127L;
        values[valuesOffset++] = (block >>> 14) & 127L;
        values[valuesOffset++] = (block >>> 21) & 127L;
        values[valuesOffset++] = (block >>> 28) & 127L;
        values[valuesOffset++] = (block >>> 35) & 127L;
        values[valuesOffset++] = (block >>> 42) & 127L;
        values[valuesOffset++] = (block >>> 49) & 127L;
        values[valuesOffset++] = block >>> 56;
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 & 127;
        values[valuesOffset++] = (byte0 >>> 7) | ((byte1 & 63) << 1);
        values[valuesOffset++] = (byte1 >>> 6) | ((byte2 & 31) << 2);
        values[valuesOffset++] = (byte2 >>> 5) | ((byte3 & 15) << 3);
        values[valuesOffset++] = (byte3 >>> 4) | ((byte4 & 7) << 4);
        values[valuesOffset++] = (byte4 >>> 3) | ((byte5 & 3) << 5);
        values[valuesOffset++] = (byte5 >>> 2) | ((byte6 & 1) << 6);
        values[valuesOffset++] = byte6 >>> 1;
        values[valuesOffset++] = byte7 & 127;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] & 0xffffffffL) | ((values[valuesOffset++] & 0xffffffffL) << 7) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset++] & 0xffffffffL) << 21) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 35) | ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 49) | ((values[valuesOffset++] & 0xffffffffL) << 56);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = values[valuesOffset++] | (values[valuesOffset++] << 7) | (values[valuesOffset++] << 14) | (values[valuesOffset++] << 21) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 35) | (values[valuesOffset++] << 42) | (values[valuesOffset++] << 49) | (values[valuesOffset++] << 56);
      }
    }

}
