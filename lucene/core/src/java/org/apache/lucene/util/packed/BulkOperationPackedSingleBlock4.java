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
final class BulkOperationPackedSingleBlock4 extends BulkOperation {
    @Override
    public int blockCount() {
      return 1;
     }

    @Override
    public int valueCount() {
      return 16;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block & 15L);
        values[valuesOffset++] = (int) ((block >>> 4) & 15L);
        values[valuesOffset++] = (int) ((block >>> 8) & 15L);
        values[valuesOffset++] = (int) ((block >>> 12) & 15L);
        values[valuesOffset++] = (int) ((block >>> 16) & 15L);
        values[valuesOffset++] = (int) ((block >>> 20) & 15L);
        values[valuesOffset++] = (int) ((block >>> 24) & 15L);
        values[valuesOffset++] = (int) ((block >>> 28) & 15L);
        values[valuesOffset++] = (int) ((block >>> 32) & 15L);
        values[valuesOffset++] = (int) ((block >>> 36) & 15L);
        values[valuesOffset++] = (int) ((block >>> 40) & 15L);
        values[valuesOffset++] = (int) ((block >>> 44) & 15L);
        values[valuesOffset++] = (int) ((block >>> 48) & 15L);
        values[valuesOffset++] = (int) ((block >>> 52) & 15L);
        values[valuesOffset++] = (int) ((block >>> 56) & 15L);
        values[valuesOffset++] = (int) (block >>> 60);
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
        values[valuesOffset++] = byte0 & 15;
        values[valuesOffset++] = byte0 >>> 4;
        values[valuesOffset++] = byte1 & 15;
        values[valuesOffset++] = byte1 >>> 4;
        values[valuesOffset++] = byte2 & 15;
        values[valuesOffset++] = byte2 >>> 4;
        values[valuesOffset++] = byte3 & 15;
        values[valuesOffset++] = byte3 >>> 4;
        values[valuesOffset++] = byte4 & 15;
        values[valuesOffset++] = byte4 >>> 4;
        values[valuesOffset++] = byte5 & 15;
        values[valuesOffset++] = byte5 >>> 4;
        values[valuesOffset++] = byte6 & 15;
        values[valuesOffset++] = byte6 >>> 4;
        values[valuesOffset++] = byte7 & 15;
        values[valuesOffset++] = byte7 >>> 4;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = block & 15L;
        values[valuesOffset++] = (block >>> 4) & 15L;
        values[valuesOffset++] = (block >>> 8) & 15L;
        values[valuesOffset++] = (block >>> 12) & 15L;
        values[valuesOffset++] = (block >>> 16) & 15L;
        values[valuesOffset++] = (block >>> 20) & 15L;
        values[valuesOffset++] = (block >>> 24) & 15L;
        values[valuesOffset++] = (block >>> 28) & 15L;
        values[valuesOffset++] = (block >>> 32) & 15L;
        values[valuesOffset++] = (block >>> 36) & 15L;
        values[valuesOffset++] = (block >>> 40) & 15L;
        values[valuesOffset++] = (block >>> 44) & 15L;
        values[valuesOffset++] = (block >>> 48) & 15L;
        values[valuesOffset++] = (block >>> 52) & 15L;
        values[valuesOffset++] = (block >>> 56) & 15L;
        values[valuesOffset++] = block >>> 60;
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
        values[valuesOffset++] = byte0 & 15;
        values[valuesOffset++] = byte0 >>> 4;
        values[valuesOffset++] = byte1 & 15;
        values[valuesOffset++] = byte1 >>> 4;
        values[valuesOffset++] = byte2 & 15;
        values[valuesOffset++] = byte2 >>> 4;
        values[valuesOffset++] = byte3 & 15;
        values[valuesOffset++] = byte3 >>> 4;
        values[valuesOffset++] = byte4 & 15;
        values[valuesOffset++] = byte4 >>> 4;
        values[valuesOffset++] = byte5 & 15;
        values[valuesOffset++] = byte5 >>> 4;
        values[valuesOffset++] = byte6 & 15;
        values[valuesOffset++] = byte6 >>> 4;
        values[valuesOffset++] = byte7 & 15;
        values[valuesOffset++] = byte7 >>> 4;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] & 0xffffffffL) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 60);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = values[valuesOffset++] | (values[valuesOffset++] << 4) | (values[valuesOffset++] << 8) | (values[valuesOffset++] << 12) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 44) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 52) | (values[valuesOffset++] << 56) | (values[valuesOffset++] << 60);
      }
    }

}
