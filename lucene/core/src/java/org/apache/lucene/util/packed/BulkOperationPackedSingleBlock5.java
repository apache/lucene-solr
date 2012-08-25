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
final class BulkOperationPackedSingleBlock5 extends BulkOperation {
    @Override
    public int blockCount() {
      return 1;
     }

    @Override
    public int valueCount() {
      return 12;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block & 31L);
        values[valuesOffset++] = (int) ((block >>> 5) & 31L);
        values[valuesOffset++] = (int) ((block >>> 10) & 31L);
        values[valuesOffset++] = (int) ((block >>> 15) & 31L);
        values[valuesOffset++] = (int) ((block >>> 20) & 31L);
        values[valuesOffset++] = (int) ((block >>> 25) & 31L);
        values[valuesOffset++] = (int) ((block >>> 30) & 31L);
        values[valuesOffset++] = (int) ((block >>> 35) & 31L);
        values[valuesOffset++] = (int) ((block >>> 40) & 31L);
        values[valuesOffset++] = (int) ((block >>> 45) & 31L);
        values[valuesOffset++] = (int) ((block >>> 50) & 31L);
        values[valuesOffset++] = (int) (block >>> 55);
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
        values[valuesOffset++] = byte0 & 31;
        values[valuesOffset++] = (byte0 >>> 5) | ((byte1 & 3) << 3);
        values[valuesOffset++] = (byte1 >>> 2) & 31;
        values[valuesOffset++] = (byte1 >>> 7) | ((byte2 & 15) << 1);
        values[valuesOffset++] = (byte2 >>> 4) | ((byte3 & 1) << 4);
        values[valuesOffset++] = (byte3 >>> 1) & 31;
        values[valuesOffset++] = (byte3 >>> 6) | ((byte4 & 7) << 2);
        values[valuesOffset++] = byte4 >>> 3;
        values[valuesOffset++] = byte5 & 31;
        values[valuesOffset++] = (byte5 >>> 5) | ((byte6 & 3) << 3);
        values[valuesOffset++] = (byte6 >>> 2) & 31;
        values[valuesOffset++] = (byte6 >>> 7) | ((byte7 & 15) << 1);
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = block & 31L;
        values[valuesOffset++] = (block >>> 5) & 31L;
        values[valuesOffset++] = (block >>> 10) & 31L;
        values[valuesOffset++] = (block >>> 15) & 31L;
        values[valuesOffset++] = (block >>> 20) & 31L;
        values[valuesOffset++] = (block >>> 25) & 31L;
        values[valuesOffset++] = (block >>> 30) & 31L;
        values[valuesOffset++] = (block >>> 35) & 31L;
        values[valuesOffset++] = (block >>> 40) & 31L;
        values[valuesOffset++] = (block >>> 45) & 31L;
        values[valuesOffset++] = (block >>> 50) & 31L;
        values[valuesOffset++] = block >>> 55;
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
        values[valuesOffset++] = byte0 & 31;
        values[valuesOffset++] = (byte0 >>> 5) | ((byte1 & 3) << 3);
        values[valuesOffset++] = (byte1 >>> 2) & 31;
        values[valuesOffset++] = (byte1 >>> 7) | ((byte2 & 15) << 1);
        values[valuesOffset++] = (byte2 >>> 4) | ((byte3 & 1) << 4);
        values[valuesOffset++] = (byte3 >>> 1) & 31;
        values[valuesOffset++] = (byte3 >>> 6) | ((byte4 & 7) << 2);
        values[valuesOffset++] = byte4 >>> 3;
        values[valuesOffset++] = byte5 & 31;
        values[valuesOffset++] = (byte5 >>> 5) | ((byte6 & 3) << 3);
        values[valuesOffset++] = (byte6 >>> 2) & 31;
        values[valuesOffset++] = (byte6 >>> 7) | ((byte7 & 15) << 1);
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] & 0xffffffffL) | ((values[valuesOffset++] & 0xffffffffL) << 5) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset++] & 0xffffffffL) << 15) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 25) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 35) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 45) | ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 55);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = values[valuesOffset++] | (values[valuesOffset++] << 5) | (values[valuesOffset++] << 10) | (values[valuesOffset++] << 15) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 25) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 35) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 45) | (values[valuesOffset++] << 50) | (values[valuesOffset++] << 55);
      }
    }

}
