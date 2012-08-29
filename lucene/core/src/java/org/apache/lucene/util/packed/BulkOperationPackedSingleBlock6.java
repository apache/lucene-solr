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
final class BulkOperationPackedSingleBlock6 extends BulkOperationPackedSingleBlock {

    public BulkOperationPackedSingleBlock6() {
      super(6);
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block & 63L);
        values[valuesOffset++] = (int) ((block >>> 6) & 63L);
        values[valuesOffset++] = (int) ((block >>> 12) & 63L);
        values[valuesOffset++] = (int) ((block >>> 18) & 63L);
        values[valuesOffset++] = (int) ((block >>> 24) & 63L);
        values[valuesOffset++] = (int) ((block >>> 30) & 63L);
        values[valuesOffset++] = (int) ((block >>> 36) & 63L);
        values[valuesOffset++] = (int) ((block >>> 42) & 63L);
        values[valuesOffset++] = (int) ((block >>> 48) & 63L);
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
        values[valuesOffset++] = byte0 & 63;
        values[valuesOffset++] = (byte0 >>> 6) | ((byte1 & 15) << 2);
        values[valuesOffset++] = (byte1 >>> 4) | ((byte2 & 3) << 4);
        values[valuesOffset++] = byte2 >>> 2;
        values[valuesOffset++] = byte3 & 63;
        values[valuesOffset++] = (byte3 >>> 6) | ((byte4 & 15) << 2);
        values[valuesOffset++] = (byte4 >>> 4) | ((byte5 & 3) << 4);
        values[valuesOffset++] = byte5 >>> 2;
        values[valuesOffset++] = byte6 & 63;
        values[valuesOffset++] = (byte6 >>> 6) | ((byte7 & 15) << 2);
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block = blocks[blocksOffset++];
        values[valuesOffset++] = block & 63L;
        values[valuesOffset++] = (block >>> 6) & 63L;
        values[valuesOffset++] = (block >>> 12) & 63L;
        values[valuesOffset++] = (block >>> 18) & 63L;
        values[valuesOffset++] = (block >>> 24) & 63L;
        values[valuesOffset++] = (block >>> 30) & 63L;
        values[valuesOffset++] = (block >>> 36) & 63L;
        values[valuesOffset++] = (block >>> 42) & 63L;
        values[valuesOffset++] = (block >>> 48) & 63L;
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
        values[valuesOffset++] = byte0 & 63;
        values[valuesOffset++] = (byte0 >>> 6) | ((byte1 & 15) << 2);
        values[valuesOffset++] = (byte1 >>> 4) | ((byte2 & 3) << 4);
        values[valuesOffset++] = byte2 >>> 2;
        values[valuesOffset++] = byte3 & 63;
        values[valuesOffset++] = (byte3 >>> 6) | ((byte4 & 15) << 2);
        values[valuesOffset++] = (byte4 >>> 4) | ((byte5 & 3) << 4);
        values[valuesOffset++] = byte5 >>> 2;
        values[valuesOffset++] = byte6 & 63;
        values[valuesOffset++] = (byte6 >>> 6) | ((byte7 & 15) << 2);
      }
    }

}
