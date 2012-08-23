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
final class BulkOperationPackedSingleBlock3 extends BulkOperation {
    @Override
    public int blockCount() {
      return 1;
     }

    @Override
    public int valueCount() {
      return 21;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block & 7L);
        values[valuesOffset++] = (int) ((block >>> 3) & 7L);
        values[valuesOffset++] = (int) ((block >>> 6) & 7L);
        values[valuesOffset++] = (int) ((block >>> 9) & 7L);
        values[valuesOffset++] = (int) ((block >>> 12) & 7L);
        values[valuesOffset++] = (int) ((block >>> 15) & 7L);
        values[valuesOffset++] = (int) ((block >>> 18) & 7L);
        values[valuesOffset++] = (int) ((block >>> 21) & 7L);
        values[valuesOffset++] = (int) ((block >>> 24) & 7L);
        values[valuesOffset++] = (int) ((block >>> 27) & 7L);
        values[valuesOffset++] = (int) ((block >>> 30) & 7L);
        values[valuesOffset++] = (int) ((block >>> 33) & 7L);
        values[valuesOffset++] = (int) ((block >>> 36) & 7L);
        values[valuesOffset++] = (int) ((block >>> 39) & 7L);
        values[valuesOffset++] = (int) ((block >>> 42) & 7L);
        values[valuesOffset++] = (int) ((block >>> 45) & 7L);
        values[valuesOffset++] = (int) ((block >>> 48) & 7L);
        values[valuesOffset++] = (int) ((block >>> 51) & 7L);
        values[valuesOffset++] = (int) ((block >>> 54) & 7L);
        values[valuesOffset++] = (int) ((block >>> 57) & 7L);
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
        values[valuesOffset++] = byte0 & 7;
        values[valuesOffset++] = (byte0 >>> 3) & 7;
        values[valuesOffset++] = (byte0 >>> 6) | ((byte1 & 1) << 2);
        values[valuesOffset++] = (byte1 >>> 1) & 7;
        values[valuesOffset++] = (byte1 >>> 4) & 7;
        values[valuesOffset++] = (byte1 >>> 7) | ((byte2 & 3) << 1);
        values[valuesOffset++] = (byte2 >>> 2) & 7;
        values[valuesOffset++] = byte2 >>> 5;
        values[valuesOffset++] = byte3 & 7;
        values[valuesOffset++] = (byte3 >>> 3) & 7;
        values[valuesOffset++] = (byte3 >>> 6) | ((byte4 & 1) << 2);
        values[valuesOffset++] = (byte4 >>> 1) & 7;
        values[valuesOffset++] = (byte4 >>> 4) & 7;
        values[valuesOffset++] = (byte4 >>> 7) | ((byte5 & 3) << 1);
        values[valuesOffset++] = (byte5 >>> 2) & 7;
        values[valuesOffset++] = byte5 >>> 5;
        values[valuesOffset++] = byte6 & 7;
        values[valuesOffset++] = (byte6 >>> 3) & 7;
        values[valuesOffset++] = (byte6 >>> 6) | ((byte7 & 1) << 2);
        values[valuesOffset++] = (byte7 >>> 1) & 7;
        values[valuesOffset++] = (byte7 >>> 4) & 7;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = block & 7L;
        values[valuesOffset++] = (block >>> 3) & 7L;
        values[valuesOffset++] = (block >>> 6) & 7L;
        values[valuesOffset++] = (block >>> 9) & 7L;
        values[valuesOffset++] = (block >>> 12) & 7L;
        values[valuesOffset++] = (block >>> 15) & 7L;
        values[valuesOffset++] = (block >>> 18) & 7L;
        values[valuesOffset++] = (block >>> 21) & 7L;
        values[valuesOffset++] = (block >>> 24) & 7L;
        values[valuesOffset++] = (block >>> 27) & 7L;
        values[valuesOffset++] = (block >>> 30) & 7L;
        values[valuesOffset++] = (block >>> 33) & 7L;
        values[valuesOffset++] = (block >>> 36) & 7L;
        values[valuesOffset++] = (block >>> 39) & 7L;
        values[valuesOffset++] = (block >>> 42) & 7L;
        values[valuesOffset++] = (block >>> 45) & 7L;
        values[valuesOffset++] = (block >>> 48) & 7L;
        values[valuesOffset++] = (block >>> 51) & 7L;
        values[valuesOffset++] = (block >>> 54) & 7L;
        values[valuesOffset++] = (block >>> 57) & 7L;
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
        values[valuesOffset++] = byte0 & 7;
        values[valuesOffset++] = (byte0 >>> 3) & 7;
        values[valuesOffset++] = (byte0 >>> 6) | ((byte1 & 1) << 2);
        values[valuesOffset++] = (byte1 >>> 1) & 7;
        values[valuesOffset++] = (byte1 >>> 4) & 7;
        values[valuesOffset++] = (byte1 >>> 7) | ((byte2 & 3) << 1);
        values[valuesOffset++] = (byte2 >>> 2) & 7;
        values[valuesOffset++] = byte2 >>> 5;
        values[valuesOffset++] = byte3 & 7;
        values[valuesOffset++] = (byte3 >>> 3) & 7;
        values[valuesOffset++] = (byte3 >>> 6) | ((byte4 & 1) << 2);
        values[valuesOffset++] = (byte4 >>> 1) & 7;
        values[valuesOffset++] = (byte4 >>> 4) & 7;
        values[valuesOffset++] = (byte4 >>> 7) | ((byte5 & 3) << 1);
        values[valuesOffset++] = (byte5 >>> 2) & 7;
        values[valuesOffset++] = byte5 >>> 5;
        values[valuesOffset++] = byte6 & 7;
        values[valuesOffset++] = (byte6 >>> 3) & 7;
        values[valuesOffset++] = (byte6 >>> 6) | ((byte7 & 1) << 2);
        values[valuesOffset++] = (byte7 >>> 1) & 7;
        values[valuesOffset++] = (byte7 >>> 4) & 7;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] & 0xffffffffL) | ((values[valuesOffset++] & 0xffffffffL) << 3) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset++] & 0xffffffffL) << 9) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset++] & 0xffffffffL) << 15) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset++] & 0xffffffffL) << 21) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 27) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 33) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 39) | ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 45) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 51) | ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 57) | ((values[valuesOffset++] & 0xffffffffL) << 60);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = values[valuesOffset++] | (values[valuesOffset++] << 3) | (values[valuesOffset++] << 6) | (values[valuesOffset++] << 9) | (values[valuesOffset++] << 12) | (values[valuesOffset++] << 15) | (values[valuesOffset++] << 18) | (values[valuesOffset++] << 21) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 27) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 33) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 39) | (values[valuesOffset++] << 42) | (values[valuesOffset++] << 45) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 51) | (values[valuesOffset++] << 54) | (values[valuesOffset++] << 57) | (values[valuesOffset++] << 60);
      }
    }

}
