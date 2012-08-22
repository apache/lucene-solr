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
final class BulkOperationPacked8 extends BulkOperation {
    public int blockCount() {
      return 1;
    }

    public int valueCount() {
      return 8;
    }

    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 56);
        values[valuesOffset++] = (int) ((block0 >>> 48) & 255L);
        values[valuesOffset++] = (int) ((block0 >>> 40) & 255L);
        values[valuesOffset++] = (int) ((block0 >>> 32) & 255L);
        values[valuesOffset++] = (int) ((block0 >>> 24) & 255L);
        values[valuesOffset++] = (int) ((block0 >>> 16) & 255L);
        values[valuesOffset++] = (int) ((block0 >>> 8) & 255L);
        values[valuesOffset++] = (int) (block0 & 255L);
      }
    }

    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte1;
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte2;
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte3;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte4;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte5;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte6;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte7;
      }
    }

    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 56;
        values[valuesOffset++] = (block0 >>> 48) & 255L;
        values[valuesOffset++] = (block0 >>> 40) & 255L;
        values[valuesOffset++] = (block0 >>> 32) & 255L;
        values[valuesOffset++] = (block0 >>> 24) & 255L;
        values[valuesOffset++] = (block0 >>> 16) & 255L;
        values[valuesOffset++] = (block0 >>> 8) & 255L;
        values[valuesOffset++] = block0 & 255L;
      }
    }

    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte1;
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte2;
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte3;
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte4;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte5;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte6;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte7;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 8) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 8) | values[valuesOffset++];
      }
    }

}
