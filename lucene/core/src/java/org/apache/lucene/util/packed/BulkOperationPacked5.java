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
final class BulkOperationPacked5 extends BulkOperation {
    @Override
    public int blockCount() {
      return 5;
    }

    @Override
    public int valueCount() {
      return 64;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 59);
        values[valuesOffset++] = (int) ((block0 >>> 54) & 31L);
        values[valuesOffset++] = (int) ((block0 >>> 49) & 31L);
        values[valuesOffset++] = (int) ((block0 >>> 44) & 31L);
        values[valuesOffset++] = (int) ((block0 >>> 39) & 31L);
        values[valuesOffset++] = (int) ((block0 >>> 34) & 31L);
        values[valuesOffset++] = (int) ((block0 >>> 29) & 31L);
        values[valuesOffset++] = (int) ((block0 >>> 24) & 31L);
        values[valuesOffset++] = (int) ((block0 >>> 19) & 31L);
        values[valuesOffset++] = (int) ((block0 >>> 14) & 31L);
        values[valuesOffset++] = (int) ((block0 >>> 9) & 31L);
        values[valuesOffset++] = (int) ((block0 >>> 4) & 31L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 15L) << 1) | (block1 >>> 63));
        values[valuesOffset++] = (int) ((block1 >>> 58) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 53) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 48) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 43) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 38) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 33) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 28) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 23) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 18) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 13) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 8) & 31L);
        values[valuesOffset++] = (int) ((block1 >>> 3) & 31L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 7L) << 2) | (block2 >>> 62));
        values[valuesOffset++] = (int) ((block2 >>> 57) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 52) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 47) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 42) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 37) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 32) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 27) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 22) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 17) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 12) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 7) & 31L);
        values[valuesOffset++] = (int) ((block2 >>> 2) & 31L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 3L) << 3) | (block3 >>> 61));
        values[valuesOffset++] = (int) ((block3 >>> 56) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 51) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 46) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 41) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 36) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 31) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 26) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 21) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 16) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 11) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 6) & 31L);
        values[valuesOffset++] = (int) ((block3 >>> 1) & 31L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 1L) << 4) | (block4 >>> 60));
        values[valuesOffset++] = (int) ((block4 >>> 55) & 31L);
        values[valuesOffset++] = (int) ((block4 >>> 50) & 31L);
        values[valuesOffset++] = (int) ((block4 >>> 45) & 31L);
        values[valuesOffset++] = (int) ((block4 >>> 40) & 31L);
        values[valuesOffset++] = (int) ((block4 >>> 35) & 31L);
        values[valuesOffset++] = (int) ((block4 >>> 30) & 31L);
        values[valuesOffset++] = (int) ((block4 >>> 25) & 31L);
        values[valuesOffset++] = (int) ((block4 >>> 20) & 31L);
        values[valuesOffset++] = (int) ((block4 >>> 15) & 31L);
        values[valuesOffset++] = (int) ((block4 >>> 10) & 31L);
        values[valuesOffset++] = (int) ((block4 >>> 5) & 31L);
        values[valuesOffset++] = (int) (block4 & 31L);
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 >>> 3;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte0 & 7) << 2) | (byte1 >>> 6);
        values[valuesOffset++] = (byte1 >>> 1) & 31;
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 1) << 4) | (byte2 >>> 4);
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 15) << 1) | (byte3 >>> 7);
        values[valuesOffset++] = (byte3 >>> 2) & 31;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 3) << 3) | (byte4 >>> 5);
        values[valuesOffset++] = byte4 & 31;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte5 >>> 3;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 7) << 2) | (byte6 >>> 6);
        values[valuesOffset++] = (byte6 >>> 1) & 31;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 1) << 4) | (byte7 >>> 4);
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 15) << 1) | (byte8 >>> 7);
        values[valuesOffset++] = (byte8 >>> 2) & 31;
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 3) << 3) | (byte9 >>> 5);
        values[valuesOffset++] = byte9 & 31;
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte10 >>> 3;
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 7) << 2) | (byte11 >>> 6);
        values[valuesOffset++] = (byte11 >>> 1) & 31;
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 1) << 4) | (byte12 >>> 4);
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 15) << 1) | (byte13 >>> 7);
        values[valuesOffset++] = (byte13 >>> 2) & 31;
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 3) << 3) | (byte14 >>> 5);
        values[valuesOffset++] = byte14 & 31;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte15 >>> 3;
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 7) << 2) | (byte16 >>> 6);
        values[valuesOffset++] = (byte16 >>> 1) & 31;
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 1) << 4) | (byte17 >>> 4);
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 15) << 1) | (byte18 >>> 7);
        values[valuesOffset++] = (byte18 >>> 2) & 31;
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 3) << 3) | (byte19 >>> 5);
        values[valuesOffset++] = byte19 & 31;
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte20 >>> 3;
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte20 & 7) << 2) | (byte21 >>> 6);
        values[valuesOffset++] = (byte21 >>> 1) & 31;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 1) << 4) | (byte22 >>> 4);
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 1) | (byte23 >>> 7);
        values[valuesOffset++] = (byte23 >>> 2) & 31;
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 3) << 3) | (byte24 >>> 5);
        values[valuesOffset++] = byte24 & 31;
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte25 >>> 3;
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte25 & 7) << 2) | (byte26 >>> 6);
        values[valuesOffset++] = (byte26 >>> 1) & 31;
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 1) << 4) | (byte27 >>> 4);
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte27 & 15) << 1) | (byte28 >>> 7);
        values[valuesOffset++] = (byte28 >>> 2) & 31;
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 3) << 3) | (byte29 >>> 5);
        values[valuesOffset++] = byte29 & 31;
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte30 >>> 3;
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte30 & 7) << 2) | (byte31 >>> 6);
        values[valuesOffset++] = (byte31 >>> 1) & 31;
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 1) << 4) | (byte32 >>> 4);
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 15) << 1) | (byte33 >>> 7);
        values[valuesOffset++] = (byte33 >>> 2) & 31;
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 3) | (byte34 >>> 5);
        values[valuesOffset++] = byte34 & 31;
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte35 >>> 3;
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte35 & 7) << 2) | (byte36 >>> 6);
        values[valuesOffset++] = (byte36 >>> 1) & 31;
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte36 & 1) << 4) | (byte37 >>> 4);
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 15) << 1) | (byte38 >>> 7);
        values[valuesOffset++] = (byte38 >>> 2) & 31;
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 3) << 3) | (byte39 >>> 5);
        values[valuesOffset++] = byte39 & 31;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 59;
        values[valuesOffset++] = (block0 >>> 54) & 31L;
        values[valuesOffset++] = (block0 >>> 49) & 31L;
        values[valuesOffset++] = (block0 >>> 44) & 31L;
        values[valuesOffset++] = (block0 >>> 39) & 31L;
        values[valuesOffset++] = (block0 >>> 34) & 31L;
        values[valuesOffset++] = (block0 >>> 29) & 31L;
        values[valuesOffset++] = (block0 >>> 24) & 31L;
        values[valuesOffset++] = (block0 >>> 19) & 31L;
        values[valuesOffset++] = (block0 >>> 14) & 31L;
        values[valuesOffset++] = (block0 >>> 9) & 31L;
        values[valuesOffset++] = (block0 >>> 4) & 31L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 15L) << 1) | (block1 >>> 63);
        values[valuesOffset++] = (block1 >>> 58) & 31L;
        values[valuesOffset++] = (block1 >>> 53) & 31L;
        values[valuesOffset++] = (block1 >>> 48) & 31L;
        values[valuesOffset++] = (block1 >>> 43) & 31L;
        values[valuesOffset++] = (block1 >>> 38) & 31L;
        values[valuesOffset++] = (block1 >>> 33) & 31L;
        values[valuesOffset++] = (block1 >>> 28) & 31L;
        values[valuesOffset++] = (block1 >>> 23) & 31L;
        values[valuesOffset++] = (block1 >>> 18) & 31L;
        values[valuesOffset++] = (block1 >>> 13) & 31L;
        values[valuesOffset++] = (block1 >>> 8) & 31L;
        values[valuesOffset++] = (block1 >>> 3) & 31L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 7L) << 2) | (block2 >>> 62);
        values[valuesOffset++] = (block2 >>> 57) & 31L;
        values[valuesOffset++] = (block2 >>> 52) & 31L;
        values[valuesOffset++] = (block2 >>> 47) & 31L;
        values[valuesOffset++] = (block2 >>> 42) & 31L;
        values[valuesOffset++] = (block2 >>> 37) & 31L;
        values[valuesOffset++] = (block2 >>> 32) & 31L;
        values[valuesOffset++] = (block2 >>> 27) & 31L;
        values[valuesOffset++] = (block2 >>> 22) & 31L;
        values[valuesOffset++] = (block2 >>> 17) & 31L;
        values[valuesOffset++] = (block2 >>> 12) & 31L;
        values[valuesOffset++] = (block2 >>> 7) & 31L;
        values[valuesOffset++] = (block2 >>> 2) & 31L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 3L) << 3) | (block3 >>> 61);
        values[valuesOffset++] = (block3 >>> 56) & 31L;
        values[valuesOffset++] = (block3 >>> 51) & 31L;
        values[valuesOffset++] = (block3 >>> 46) & 31L;
        values[valuesOffset++] = (block3 >>> 41) & 31L;
        values[valuesOffset++] = (block3 >>> 36) & 31L;
        values[valuesOffset++] = (block3 >>> 31) & 31L;
        values[valuesOffset++] = (block3 >>> 26) & 31L;
        values[valuesOffset++] = (block3 >>> 21) & 31L;
        values[valuesOffset++] = (block3 >>> 16) & 31L;
        values[valuesOffset++] = (block3 >>> 11) & 31L;
        values[valuesOffset++] = (block3 >>> 6) & 31L;
        values[valuesOffset++] = (block3 >>> 1) & 31L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 1L) << 4) | (block4 >>> 60);
        values[valuesOffset++] = (block4 >>> 55) & 31L;
        values[valuesOffset++] = (block4 >>> 50) & 31L;
        values[valuesOffset++] = (block4 >>> 45) & 31L;
        values[valuesOffset++] = (block4 >>> 40) & 31L;
        values[valuesOffset++] = (block4 >>> 35) & 31L;
        values[valuesOffset++] = (block4 >>> 30) & 31L;
        values[valuesOffset++] = (block4 >>> 25) & 31L;
        values[valuesOffset++] = (block4 >>> 20) & 31L;
        values[valuesOffset++] = (block4 >>> 15) & 31L;
        values[valuesOffset++] = (block4 >>> 10) & 31L;
        values[valuesOffset++] = (block4 >>> 5) & 31L;
        values[valuesOffset++] = block4 & 31L;
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 >>> 3;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte0 & 7) << 2) | (byte1 >>> 6);
        values[valuesOffset++] = (byte1 >>> 1) & 31;
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 1) << 4) | (byte2 >>> 4);
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 15) << 1) | (byte3 >>> 7);
        values[valuesOffset++] = (byte3 >>> 2) & 31;
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 3) << 3) | (byte4 >>> 5);
        values[valuesOffset++] = byte4 & 31;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte5 >>> 3;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 7) << 2) | (byte6 >>> 6);
        values[valuesOffset++] = (byte6 >>> 1) & 31;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 1) << 4) | (byte7 >>> 4);
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 15) << 1) | (byte8 >>> 7);
        values[valuesOffset++] = (byte8 >>> 2) & 31;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 3) << 3) | (byte9 >>> 5);
        values[valuesOffset++] = byte9 & 31;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte10 >>> 3;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 7) << 2) | (byte11 >>> 6);
        values[valuesOffset++] = (byte11 >>> 1) & 31;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 1) << 4) | (byte12 >>> 4);
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 15) << 1) | (byte13 >>> 7);
        values[valuesOffset++] = (byte13 >>> 2) & 31;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 3) << 3) | (byte14 >>> 5);
        values[valuesOffset++] = byte14 & 31;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte15 >>> 3;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 7) << 2) | (byte16 >>> 6);
        values[valuesOffset++] = (byte16 >>> 1) & 31;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 1) << 4) | (byte17 >>> 4);
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 15) << 1) | (byte18 >>> 7);
        values[valuesOffset++] = (byte18 >>> 2) & 31;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 3) << 3) | (byte19 >>> 5);
        values[valuesOffset++] = byte19 & 31;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte20 >>> 3;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte20 & 7) << 2) | (byte21 >>> 6);
        values[valuesOffset++] = (byte21 >>> 1) & 31;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 1) << 4) | (byte22 >>> 4);
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 1) | (byte23 >>> 7);
        values[valuesOffset++] = (byte23 >>> 2) & 31;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 3) << 3) | (byte24 >>> 5);
        values[valuesOffset++] = byte24 & 31;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte25 >>> 3;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte25 & 7) << 2) | (byte26 >>> 6);
        values[valuesOffset++] = (byte26 >>> 1) & 31;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 1) << 4) | (byte27 >>> 4);
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte27 & 15) << 1) | (byte28 >>> 7);
        values[valuesOffset++] = (byte28 >>> 2) & 31;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 3) << 3) | (byte29 >>> 5);
        values[valuesOffset++] = byte29 & 31;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte30 >>> 3;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte30 & 7) << 2) | (byte31 >>> 6);
        values[valuesOffset++] = (byte31 >>> 1) & 31;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 1) << 4) | (byte32 >>> 4);
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 15) << 1) | (byte33 >>> 7);
        values[valuesOffset++] = (byte33 >>> 2) & 31;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 3) | (byte34 >>> 5);
        values[valuesOffset++] = byte34 & 31;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte35 >>> 3;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte35 & 7) << 2) | (byte36 >>> 6);
        values[valuesOffset++] = (byte36 >>> 1) & 31;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte36 & 1) << 4) | (byte37 >>> 4);
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 15) << 1) | (byte38 >>> 7);
        values[valuesOffset++] = (byte38 >>> 2) & 31;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 3) << 3) | (byte39 >>> 5);
        values[valuesOffset++] = byte39 & 31;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 59) | ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 49) | ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 39) | ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 29) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 19) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset++] & 0xffffffffL) << 9) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 1);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 63) | ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 53) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 43) | ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 33) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 23) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset++] & 0xffffffffL) << 13) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset++] & 0xffffffffL) << 3) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 57) | ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 47) | ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 37) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 27) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset++] & 0xffffffffL) << 17) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset++] & 0xffffffffL) << 7) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 3);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 61) | ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 51) | ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 41) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 31) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset++] & 0xffffffffL) << 21) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 11) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset++] & 0xffffffffL) << 1) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 55) | ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 45) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 35) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 25) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 15) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset++] & 0xffffffffL) << 5) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 59) | (values[valuesOffset++] << 54) | (values[valuesOffset++] << 49) | (values[valuesOffset++] << 44) | (values[valuesOffset++] << 39) | (values[valuesOffset++] << 34) | (values[valuesOffset++] << 29) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 19) | (values[valuesOffset++] << 14) | (values[valuesOffset++] << 9) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 1);
        blocks[blocksOffset++] = (values[valuesOffset++] << 63) | (values[valuesOffset++] << 58) | (values[valuesOffset++] << 53) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 43) | (values[valuesOffset++] << 38) | (values[valuesOffset++] << 33) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 23) | (values[valuesOffset++] << 18) | (values[valuesOffset++] << 13) | (values[valuesOffset++] << 8) | (values[valuesOffset++] << 3) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 57) | (values[valuesOffset++] << 52) | (values[valuesOffset++] << 47) | (values[valuesOffset++] << 42) | (values[valuesOffset++] << 37) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 27) | (values[valuesOffset++] << 22) | (values[valuesOffset++] << 17) | (values[valuesOffset++] << 12) | (values[valuesOffset++] << 7) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 3);
        blocks[blocksOffset++] = (values[valuesOffset++] << 61) | (values[valuesOffset++] << 56) | (values[valuesOffset++] << 51) | (values[valuesOffset++] << 46) | (values[valuesOffset++] << 41) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 31) | (values[valuesOffset++] << 26) | (values[valuesOffset++] << 21) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 11) | (values[valuesOffset++] << 6) | (values[valuesOffset++] << 1) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 55) | (values[valuesOffset++] << 50) | (values[valuesOffset++] << 45) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 35) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 25) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 15) | (values[valuesOffset++] << 10) | (values[valuesOffset++] << 5) | values[valuesOffset++];
      }
    }

}
