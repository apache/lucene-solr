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
final class BulkOperationPacked10 extends BulkOperation {
    @Override
    public int blockCount() {
      return 5;
    }

    @Override
    public int valueCount() {
      return 32;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 54);
        values[valuesOffset++] = (int) ((block0 >>> 44) & 1023L);
        values[valuesOffset++] = (int) ((block0 >>> 34) & 1023L);
        values[valuesOffset++] = (int) ((block0 >>> 24) & 1023L);
        values[valuesOffset++] = (int) ((block0 >>> 14) & 1023L);
        values[valuesOffset++] = (int) ((block0 >>> 4) & 1023L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 15L) << 6) | (block1 >>> 58));
        values[valuesOffset++] = (int) ((block1 >>> 48) & 1023L);
        values[valuesOffset++] = (int) ((block1 >>> 38) & 1023L);
        values[valuesOffset++] = (int) ((block1 >>> 28) & 1023L);
        values[valuesOffset++] = (int) ((block1 >>> 18) & 1023L);
        values[valuesOffset++] = (int) ((block1 >>> 8) & 1023L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 255L) << 2) | (block2 >>> 62));
        values[valuesOffset++] = (int) ((block2 >>> 52) & 1023L);
        values[valuesOffset++] = (int) ((block2 >>> 42) & 1023L);
        values[valuesOffset++] = (int) ((block2 >>> 32) & 1023L);
        values[valuesOffset++] = (int) ((block2 >>> 22) & 1023L);
        values[valuesOffset++] = (int) ((block2 >>> 12) & 1023L);
        values[valuesOffset++] = (int) ((block2 >>> 2) & 1023L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 3L) << 8) | (block3 >>> 56));
        values[valuesOffset++] = (int) ((block3 >>> 46) & 1023L);
        values[valuesOffset++] = (int) ((block3 >>> 36) & 1023L);
        values[valuesOffset++] = (int) ((block3 >>> 26) & 1023L);
        values[valuesOffset++] = (int) ((block3 >>> 16) & 1023L);
        values[valuesOffset++] = (int) ((block3 >>> 6) & 1023L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 63L) << 4) | (block4 >>> 60));
        values[valuesOffset++] = (int) ((block4 >>> 50) & 1023L);
        values[valuesOffset++] = (int) ((block4 >>> 40) & 1023L);
        values[valuesOffset++] = (int) ((block4 >>> 30) & 1023L);
        values[valuesOffset++] = (int) ((block4 >>> 20) & 1023L);
        values[valuesOffset++] = (int) ((block4 >>> 10) & 1023L);
        values[valuesOffset++] = (int) (block4 & 1023L);
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 2) | (byte1 >>> 6);
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 63) << 4) | (byte2 >>> 4);
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 15) << 6) | (byte3 >>> 2);
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 3) << 8) | byte4;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte5 << 2) | (byte6 >>> 6);
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 63) << 4) | (byte7 >>> 4);
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 15) << 6) | (byte8 >>> 2);
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 3) << 8) | byte9;
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte10 << 2) | (byte11 >>> 6);
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 63) << 4) | (byte12 >>> 4);
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 15) << 6) | (byte13 >>> 2);
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 3) << 8) | byte14;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte15 << 2) | (byte16 >>> 6);
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 63) << 4) | (byte17 >>> 4);
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 15) << 6) | (byte18 >>> 2);
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 3) << 8) | byte19;
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte20 << 2) | (byte21 >>> 6);
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 63) << 4) | (byte22 >>> 4);
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 6) | (byte23 >>> 2);
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 3) << 8) | byte24;
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte25 << 2) | (byte26 >>> 6);
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 63) << 4) | (byte27 >>> 4);
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte27 & 15) << 6) | (byte28 >>> 2);
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 3) << 8) | byte29;
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte30 << 2) | (byte31 >>> 6);
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 63) << 4) | (byte32 >>> 4);
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 15) << 6) | (byte33 >>> 2);
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 8) | byte34;
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte35 << 2) | (byte36 >>> 6);
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte36 & 63) << 4) | (byte37 >>> 4);
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 15) << 6) | (byte38 >>> 2);
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 3) << 8) | byte39;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 54;
        values[valuesOffset++] = (block0 >>> 44) & 1023L;
        values[valuesOffset++] = (block0 >>> 34) & 1023L;
        values[valuesOffset++] = (block0 >>> 24) & 1023L;
        values[valuesOffset++] = (block0 >>> 14) & 1023L;
        values[valuesOffset++] = (block0 >>> 4) & 1023L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 15L) << 6) | (block1 >>> 58);
        values[valuesOffset++] = (block1 >>> 48) & 1023L;
        values[valuesOffset++] = (block1 >>> 38) & 1023L;
        values[valuesOffset++] = (block1 >>> 28) & 1023L;
        values[valuesOffset++] = (block1 >>> 18) & 1023L;
        values[valuesOffset++] = (block1 >>> 8) & 1023L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 255L) << 2) | (block2 >>> 62);
        values[valuesOffset++] = (block2 >>> 52) & 1023L;
        values[valuesOffset++] = (block2 >>> 42) & 1023L;
        values[valuesOffset++] = (block2 >>> 32) & 1023L;
        values[valuesOffset++] = (block2 >>> 22) & 1023L;
        values[valuesOffset++] = (block2 >>> 12) & 1023L;
        values[valuesOffset++] = (block2 >>> 2) & 1023L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 3L) << 8) | (block3 >>> 56);
        values[valuesOffset++] = (block3 >>> 46) & 1023L;
        values[valuesOffset++] = (block3 >>> 36) & 1023L;
        values[valuesOffset++] = (block3 >>> 26) & 1023L;
        values[valuesOffset++] = (block3 >>> 16) & 1023L;
        values[valuesOffset++] = (block3 >>> 6) & 1023L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 63L) << 4) | (block4 >>> 60);
        values[valuesOffset++] = (block4 >>> 50) & 1023L;
        values[valuesOffset++] = (block4 >>> 40) & 1023L;
        values[valuesOffset++] = (block4 >>> 30) & 1023L;
        values[valuesOffset++] = (block4 >>> 20) & 1023L;
        values[valuesOffset++] = (block4 >>> 10) & 1023L;
        values[valuesOffset++] = block4 & 1023L;
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 2) | (byte1 >>> 6);
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 63) << 4) | (byte2 >>> 4);
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 15) << 6) | (byte3 >>> 2);
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 3) << 8) | byte4;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte5 << 2) | (byte6 >>> 6);
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 63) << 4) | (byte7 >>> 4);
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 15) << 6) | (byte8 >>> 2);
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 3) << 8) | byte9;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte10 << 2) | (byte11 >>> 6);
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 63) << 4) | (byte12 >>> 4);
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 15) << 6) | (byte13 >>> 2);
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 3) << 8) | byte14;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte15 << 2) | (byte16 >>> 6);
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 63) << 4) | (byte17 >>> 4);
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 15) << 6) | (byte18 >>> 2);
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 3) << 8) | byte19;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte20 << 2) | (byte21 >>> 6);
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 63) << 4) | (byte22 >>> 4);
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 6) | (byte23 >>> 2);
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 3) << 8) | byte24;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte25 << 2) | (byte26 >>> 6);
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 63) << 4) | (byte27 >>> 4);
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte27 & 15) << 6) | (byte28 >>> 2);
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 3) << 8) | byte29;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte30 << 2) | (byte31 >>> 6);
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 63) << 4) | (byte32 >>> 4);
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 15) << 6) | (byte33 >>> 2);
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 8) | byte34;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte35 << 2) | (byte36 >>> 6);
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte36 & 63) << 4) | (byte37 >>> 4);
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 15) << 6) | (byte38 >>> 2);
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 3) << 8) | byte39;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 10) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 44) | (values[valuesOffset++] << 34) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 14) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 38) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 18) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 52) | (values[valuesOffset++] << 42) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 22) | (values[valuesOffset++] << 12) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 46) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 26) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 50) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 10) | values[valuesOffset++];
      }
    }

}
