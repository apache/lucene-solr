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
final class BulkOperationPacked12 extends BulkOperation {
    public int blockCount() {
      return 3;
    }

    public int valueCount() {
      return 16;
    }

    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 52);
        values[valuesOffset++] = (int) ((block0 >>> 40) & 4095L);
        values[valuesOffset++] = (int) ((block0 >>> 28) & 4095L);
        values[valuesOffset++] = (int) ((block0 >>> 16) & 4095L);
        values[valuesOffset++] = (int) ((block0 >>> 4) & 4095L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 15L) << 8) | (block1 >>> 56));
        values[valuesOffset++] = (int) ((block1 >>> 44) & 4095L);
        values[valuesOffset++] = (int) ((block1 >>> 32) & 4095L);
        values[valuesOffset++] = (int) ((block1 >>> 20) & 4095L);
        values[valuesOffset++] = (int) ((block1 >>> 8) & 4095L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 255L) << 4) | (block2 >>> 60));
        values[valuesOffset++] = (int) ((block2 >>> 48) & 4095L);
        values[valuesOffset++] = (int) ((block2 >>> 36) & 4095L);
        values[valuesOffset++] = (int) ((block2 >>> 24) & 4095L);
        values[valuesOffset++] = (int) ((block2 >>> 12) & 4095L);
        values[valuesOffset++] = (int) (block2 & 4095L);
      }
    }

    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 4) | (byte1 >>> 4);
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 15) << 8) | byte2;
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte3 << 4) | (byte4 >>> 4);
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 15) << 8) | byte5;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte6 << 4) | (byte7 >>> 4);
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 15) << 8) | byte8;
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte9 << 4) | (byte10 >>> 4);
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 15) << 8) | byte11;
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte12 << 4) | (byte13 >>> 4);
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 15) << 8) | byte14;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte15 << 4) | (byte16 >>> 4);
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 15) << 8) | byte17;
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte18 << 4) | (byte19 >>> 4);
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 15) << 8) | byte20;
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte21 << 4) | (byte22 >>> 4);
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 8) | byte23;
      }
    }

    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 52;
        values[valuesOffset++] = (block0 >>> 40) & 4095L;
        values[valuesOffset++] = (block0 >>> 28) & 4095L;
        values[valuesOffset++] = (block0 >>> 16) & 4095L;
        values[valuesOffset++] = (block0 >>> 4) & 4095L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 15L) << 8) | (block1 >>> 56);
        values[valuesOffset++] = (block1 >>> 44) & 4095L;
        values[valuesOffset++] = (block1 >>> 32) & 4095L;
        values[valuesOffset++] = (block1 >>> 20) & 4095L;
        values[valuesOffset++] = (block1 >>> 8) & 4095L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 255L) << 4) | (block2 >>> 60);
        values[valuesOffset++] = (block2 >>> 48) & 4095L;
        values[valuesOffset++] = (block2 >>> 36) & 4095L;
        values[valuesOffset++] = (block2 >>> 24) & 4095L;
        values[valuesOffset++] = (block2 >>> 12) & 4095L;
        values[valuesOffset++] = block2 & 4095L;
      }
    }

    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 4) | (byte1 >>> 4);
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 15) << 8) | byte2;
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte3 << 4) | (byte4 >>> 4);
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 15) << 8) | byte5;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte6 << 4) | (byte7 >>> 4);
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 15) << 8) | byte8;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte9 << 4) | (byte10 >>> 4);
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 15) << 8) | byte11;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte12 << 4) | (byte13 >>> 4);
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 15) << 8) | byte14;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte15 << 4) | (byte16 >>> 4);
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 15) << 8) | byte17;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte18 << 4) | (byte19 >>> 4);
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 15) << 8) | byte20;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte21 << 4) | (byte22 >>> 4);
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 8) | byte23;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 12) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 44) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 12) | values[valuesOffset++];
      }
    }

}
