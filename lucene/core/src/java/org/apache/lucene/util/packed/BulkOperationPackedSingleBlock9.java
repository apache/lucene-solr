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
final class BulkOperationPackedSingleBlock9 extends BulkOperation {
    @Override
    public int blockCount() {
      return 1;
     }

    @Override
    public int valueCount() {
      return 7;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block & 511L);
        values[valuesOffset++] = (int) ((block >>> 9) & 511L);
        values[valuesOffset++] = (int) ((block >>> 18) & 511L);
        values[valuesOffset++] = (int) ((block >>> 27) & 511L);
        values[valuesOffset++] = (int) ((block >>> 36) & 511L);
        values[valuesOffset++] = (int) ((block >>> 45) & 511L);
        values[valuesOffset++] = (int) (block >>> 54);
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
        values[valuesOffset++] = byte0 | ((byte1 & 1) << 8);
        values[valuesOffset++] = (byte1 >>> 1) | ((byte2 & 3) << 7);
        values[valuesOffset++] = (byte2 >>> 2) | ((byte3 & 7) << 6);
        values[valuesOffset++] = (byte3 >>> 3) | ((byte4 & 15) << 5);
        values[valuesOffset++] = (byte4 >>> 4) | ((byte5 & 31) << 4);
        values[valuesOffset++] = (byte5 >>> 5) | ((byte6 & 63) << 3);
        values[valuesOffset++] = (byte6 >>> 6) | ((byte7 & 127) << 2);
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = block & 511L;
        values[valuesOffset++] = (block >>> 9) & 511L;
        values[valuesOffset++] = (block >>> 18) & 511L;
        values[valuesOffset++] = (block >>> 27) & 511L;
        values[valuesOffset++] = (block >>> 36) & 511L;
        values[valuesOffset++] = (block >>> 45) & 511L;
        values[valuesOffset++] = block >>> 54;
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
        values[valuesOffset++] = byte0 | ((byte1 & 1) << 8);
        values[valuesOffset++] = (byte1 >>> 1) | ((byte2 & 3) << 7);
        values[valuesOffset++] = (byte2 >>> 2) | ((byte3 & 7) << 6);
        values[valuesOffset++] = (byte3 >>> 3) | ((byte4 & 15) << 5);
        values[valuesOffset++] = (byte4 >>> 4) | ((byte5 & 31) << 4);
        values[valuesOffset++] = (byte5 >>> 5) | ((byte6 & 63) << 3);
        values[valuesOffset++] = (byte6 >>> 6) | ((byte7 & 127) << 2);
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] & 0xffffffffL) | ((values[valuesOffset++] & 0xffffffffL) << 9) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset++] & 0xffffffffL) << 27) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 45) | ((values[valuesOffset++] & 0xffffffffL) << 54);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = values[valuesOffset++] | (values[valuesOffset++] << 9) | (values[valuesOffset++] << 18) | (values[valuesOffset++] << 27) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 45) | (values[valuesOffset++] << 54);
      }
    }

}
