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
final class BulkOperationPacked7 extends BulkOperation {
    public int blockCount() {
      return 7;
    }

    public int valueCount() {
      return 64;
    }

    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 57);
        values[valuesOffset++] = (int) ((block0 >>> 50) & 127L);
        values[valuesOffset++] = (int) ((block0 >>> 43) & 127L);
        values[valuesOffset++] = (int) ((block0 >>> 36) & 127L);
        values[valuesOffset++] = (int) ((block0 >>> 29) & 127L);
        values[valuesOffset++] = (int) ((block0 >>> 22) & 127L);
        values[valuesOffset++] = (int) ((block0 >>> 15) & 127L);
        values[valuesOffset++] = (int) ((block0 >>> 8) & 127L);
        values[valuesOffset++] = (int) ((block0 >>> 1) & 127L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 1L) << 6) | (block1 >>> 58));
        values[valuesOffset++] = (int) ((block1 >>> 51) & 127L);
        values[valuesOffset++] = (int) ((block1 >>> 44) & 127L);
        values[valuesOffset++] = (int) ((block1 >>> 37) & 127L);
        values[valuesOffset++] = (int) ((block1 >>> 30) & 127L);
        values[valuesOffset++] = (int) ((block1 >>> 23) & 127L);
        values[valuesOffset++] = (int) ((block1 >>> 16) & 127L);
        values[valuesOffset++] = (int) ((block1 >>> 9) & 127L);
        values[valuesOffset++] = (int) ((block1 >>> 2) & 127L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 3L) << 5) | (block2 >>> 59));
        values[valuesOffset++] = (int) ((block2 >>> 52) & 127L);
        values[valuesOffset++] = (int) ((block2 >>> 45) & 127L);
        values[valuesOffset++] = (int) ((block2 >>> 38) & 127L);
        values[valuesOffset++] = (int) ((block2 >>> 31) & 127L);
        values[valuesOffset++] = (int) ((block2 >>> 24) & 127L);
        values[valuesOffset++] = (int) ((block2 >>> 17) & 127L);
        values[valuesOffset++] = (int) ((block2 >>> 10) & 127L);
        values[valuesOffset++] = (int) ((block2 >>> 3) & 127L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 7L) << 4) | (block3 >>> 60));
        values[valuesOffset++] = (int) ((block3 >>> 53) & 127L);
        values[valuesOffset++] = (int) ((block3 >>> 46) & 127L);
        values[valuesOffset++] = (int) ((block3 >>> 39) & 127L);
        values[valuesOffset++] = (int) ((block3 >>> 32) & 127L);
        values[valuesOffset++] = (int) ((block3 >>> 25) & 127L);
        values[valuesOffset++] = (int) ((block3 >>> 18) & 127L);
        values[valuesOffset++] = (int) ((block3 >>> 11) & 127L);
        values[valuesOffset++] = (int) ((block3 >>> 4) & 127L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 15L) << 3) | (block4 >>> 61));
        values[valuesOffset++] = (int) ((block4 >>> 54) & 127L);
        values[valuesOffset++] = (int) ((block4 >>> 47) & 127L);
        values[valuesOffset++] = (int) ((block4 >>> 40) & 127L);
        values[valuesOffset++] = (int) ((block4 >>> 33) & 127L);
        values[valuesOffset++] = (int) ((block4 >>> 26) & 127L);
        values[valuesOffset++] = (int) ((block4 >>> 19) & 127L);
        values[valuesOffset++] = (int) ((block4 >>> 12) & 127L);
        values[valuesOffset++] = (int) ((block4 >>> 5) & 127L);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block4 & 31L) << 2) | (block5 >>> 62));
        values[valuesOffset++] = (int) ((block5 >>> 55) & 127L);
        values[valuesOffset++] = (int) ((block5 >>> 48) & 127L);
        values[valuesOffset++] = (int) ((block5 >>> 41) & 127L);
        values[valuesOffset++] = (int) ((block5 >>> 34) & 127L);
        values[valuesOffset++] = (int) ((block5 >>> 27) & 127L);
        values[valuesOffset++] = (int) ((block5 >>> 20) & 127L);
        values[valuesOffset++] = (int) ((block5 >>> 13) & 127L);
        values[valuesOffset++] = (int) ((block5 >>> 6) & 127L);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block5 & 63L) << 1) | (block6 >>> 63));
        values[valuesOffset++] = (int) ((block6 >>> 56) & 127L);
        values[valuesOffset++] = (int) ((block6 >>> 49) & 127L);
        values[valuesOffset++] = (int) ((block6 >>> 42) & 127L);
        values[valuesOffset++] = (int) ((block6 >>> 35) & 127L);
        values[valuesOffset++] = (int) ((block6 >>> 28) & 127L);
        values[valuesOffset++] = (int) ((block6 >>> 21) & 127L);
        values[valuesOffset++] = (int) ((block6 >>> 14) & 127L);
        values[valuesOffset++] = (int) ((block6 >>> 7) & 127L);
        values[valuesOffset++] = (int) (block6 & 127L);
      }
    }

    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 >>> 1;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte0 & 1) << 6) | (byte1 >>> 2);
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 3) << 5) | (byte2 >>> 3);
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 7) << 4) | (byte3 >>> 4);
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 15) << 3) | (byte4 >>> 5);
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 31) << 2) | (byte5 >>> 6);
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 63) << 1) | (byte6 >>> 7);
        values[valuesOffset++] = byte6 & 127;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte7 >>> 1;
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 1) << 6) | (byte8 >>> 2);
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 3) << 5) | (byte9 >>> 3);
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte9 & 7) << 4) | (byte10 >>> 4);
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 15) << 3) | (byte11 >>> 5);
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 31) << 2) | (byte12 >>> 6);
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 63) << 1) | (byte13 >>> 7);
        values[valuesOffset++] = byte13 & 127;
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte14 >>> 1;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte14 & 1) << 6) | (byte15 >>> 2);
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 5) | (byte16 >>> 3);
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 7) << 4) | (byte17 >>> 4);
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 15) << 3) | (byte18 >>> 5);
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 31) << 2) | (byte19 >>> 6);
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 63) << 1) | (byte20 >>> 7);
        values[valuesOffset++] = byte20 & 127;
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte21 >>> 1;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 1) << 6) | (byte22 >>> 2);
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 3) << 5) | (byte23 >>> 3);
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 7) << 4) | (byte24 >>> 4);
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 15) << 3) | (byte25 >>> 5);
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte25 & 31) << 2) | (byte26 >>> 6);
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 63) << 1) | (byte27 >>> 7);
        values[valuesOffset++] = byte27 & 127;
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte28 >>> 1;
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 1) << 6) | (byte29 >>> 2);
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 3) << 5) | (byte30 >>> 3);
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte30 & 7) << 4) | (byte31 >>> 4);
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 3) | (byte32 >>> 5);
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 31) << 2) | (byte33 >>> 6);
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 63) << 1) | (byte34 >>> 7);
        values[valuesOffset++] = byte34 & 127;
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte35 >>> 1;
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte35 & 1) << 6) | (byte36 >>> 2);
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte36 & 3) << 5) | (byte37 >>> 3);
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 7) << 4) | (byte38 >>> 4);
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 15) << 3) | (byte39 >>> 5);
        final int byte40 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte39 & 31) << 2) | (byte40 >>> 6);
        final int byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte40 & 63) << 1) | (byte41 >>> 7);
        values[valuesOffset++] = byte41 & 127;
        final int byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte42 >>> 1;
        final int byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 1) << 6) | (byte43 >>> 2);
        final int byte44 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte43 & 3) << 5) | (byte44 >>> 3);
        final int byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte44 & 7) << 4) | (byte45 >>> 4);
        final int byte46 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 15) << 3) | (byte46 >>> 5);
        final int byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte46 & 31) << 2) | (byte47 >>> 6);
        final int byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 63) << 1) | (byte48 >>> 7);
        values[valuesOffset++] = byte48 & 127;
        final int byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte49 >>> 1;
        final int byte50 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte49 & 1) << 6) | (byte50 >>> 2);
        final int byte51 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte50 & 3) << 5) | (byte51 >>> 3);
        final int byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte51 & 7) << 4) | (byte52 >>> 4);
        final int byte53 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 15) << 3) | (byte53 >>> 5);
        final int byte54 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte53 & 31) << 2) | (byte54 >>> 6);
        final int byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte54 & 63) << 1) | (byte55 >>> 7);
        values[valuesOffset++] = byte55 & 127;
      }
    }

    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 57;
        values[valuesOffset++] = (block0 >>> 50) & 127L;
        values[valuesOffset++] = (block0 >>> 43) & 127L;
        values[valuesOffset++] = (block0 >>> 36) & 127L;
        values[valuesOffset++] = (block0 >>> 29) & 127L;
        values[valuesOffset++] = (block0 >>> 22) & 127L;
        values[valuesOffset++] = (block0 >>> 15) & 127L;
        values[valuesOffset++] = (block0 >>> 8) & 127L;
        values[valuesOffset++] = (block0 >>> 1) & 127L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 1L) << 6) | (block1 >>> 58);
        values[valuesOffset++] = (block1 >>> 51) & 127L;
        values[valuesOffset++] = (block1 >>> 44) & 127L;
        values[valuesOffset++] = (block1 >>> 37) & 127L;
        values[valuesOffset++] = (block1 >>> 30) & 127L;
        values[valuesOffset++] = (block1 >>> 23) & 127L;
        values[valuesOffset++] = (block1 >>> 16) & 127L;
        values[valuesOffset++] = (block1 >>> 9) & 127L;
        values[valuesOffset++] = (block1 >>> 2) & 127L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 3L) << 5) | (block2 >>> 59);
        values[valuesOffset++] = (block2 >>> 52) & 127L;
        values[valuesOffset++] = (block2 >>> 45) & 127L;
        values[valuesOffset++] = (block2 >>> 38) & 127L;
        values[valuesOffset++] = (block2 >>> 31) & 127L;
        values[valuesOffset++] = (block2 >>> 24) & 127L;
        values[valuesOffset++] = (block2 >>> 17) & 127L;
        values[valuesOffset++] = (block2 >>> 10) & 127L;
        values[valuesOffset++] = (block2 >>> 3) & 127L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 7L) << 4) | (block3 >>> 60);
        values[valuesOffset++] = (block3 >>> 53) & 127L;
        values[valuesOffset++] = (block3 >>> 46) & 127L;
        values[valuesOffset++] = (block3 >>> 39) & 127L;
        values[valuesOffset++] = (block3 >>> 32) & 127L;
        values[valuesOffset++] = (block3 >>> 25) & 127L;
        values[valuesOffset++] = (block3 >>> 18) & 127L;
        values[valuesOffset++] = (block3 >>> 11) & 127L;
        values[valuesOffset++] = (block3 >>> 4) & 127L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 15L) << 3) | (block4 >>> 61);
        values[valuesOffset++] = (block4 >>> 54) & 127L;
        values[valuesOffset++] = (block4 >>> 47) & 127L;
        values[valuesOffset++] = (block4 >>> 40) & 127L;
        values[valuesOffset++] = (block4 >>> 33) & 127L;
        values[valuesOffset++] = (block4 >>> 26) & 127L;
        values[valuesOffset++] = (block4 >>> 19) & 127L;
        values[valuesOffset++] = (block4 >>> 12) & 127L;
        values[valuesOffset++] = (block4 >>> 5) & 127L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 31L) << 2) | (block5 >>> 62);
        values[valuesOffset++] = (block5 >>> 55) & 127L;
        values[valuesOffset++] = (block5 >>> 48) & 127L;
        values[valuesOffset++] = (block5 >>> 41) & 127L;
        values[valuesOffset++] = (block5 >>> 34) & 127L;
        values[valuesOffset++] = (block5 >>> 27) & 127L;
        values[valuesOffset++] = (block5 >>> 20) & 127L;
        values[valuesOffset++] = (block5 >>> 13) & 127L;
        values[valuesOffset++] = (block5 >>> 6) & 127L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 63L) << 1) | (block6 >>> 63);
        values[valuesOffset++] = (block6 >>> 56) & 127L;
        values[valuesOffset++] = (block6 >>> 49) & 127L;
        values[valuesOffset++] = (block6 >>> 42) & 127L;
        values[valuesOffset++] = (block6 >>> 35) & 127L;
        values[valuesOffset++] = (block6 >>> 28) & 127L;
        values[valuesOffset++] = (block6 >>> 21) & 127L;
        values[valuesOffset++] = (block6 >>> 14) & 127L;
        values[valuesOffset++] = (block6 >>> 7) & 127L;
        values[valuesOffset++] = block6 & 127L;
      }
    }

    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 >>> 1;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte0 & 1) << 6) | (byte1 >>> 2);
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 3) << 5) | (byte2 >>> 3);
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 7) << 4) | (byte3 >>> 4);
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 15) << 3) | (byte4 >>> 5);
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 31) << 2) | (byte5 >>> 6);
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 63) << 1) | (byte6 >>> 7);
        values[valuesOffset++] = byte6 & 127;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte7 >>> 1;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 1) << 6) | (byte8 >>> 2);
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 3) << 5) | (byte9 >>> 3);
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte9 & 7) << 4) | (byte10 >>> 4);
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 15) << 3) | (byte11 >>> 5);
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 31) << 2) | (byte12 >>> 6);
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 63) << 1) | (byte13 >>> 7);
        values[valuesOffset++] = byte13 & 127;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte14 >>> 1;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte14 & 1) << 6) | (byte15 >>> 2);
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 5) | (byte16 >>> 3);
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 7) << 4) | (byte17 >>> 4);
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 15) << 3) | (byte18 >>> 5);
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 31) << 2) | (byte19 >>> 6);
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 63) << 1) | (byte20 >>> 7);
        values[valuesOffset++] = byte20 & 127;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte21 >>> 1;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 1) << 6) | (byte22 >>> 2);
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 3) << 5) | (byte23 >>> 3);
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 7) << 4) | (byte24 >>> 4);
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 15) << 3) | (byte25 >>> 5);
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte25 & 31) << 2) | (byte26 >>> 6);
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 63) << 1) | (byte27 >>> 7);
        values[valuesOffset++] = byte27 & 127;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte28 >>> 1;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 1) << 6) | (byte29 >>> 2);
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 3) << 5) | (byte30 >>> 3);
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte30 & 7) << 4) | (byte31 >>> 4);
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 3) | (byte32 >>> 5);
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 31) << 2) | (byte33 >>> 6);
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 63) << 1) | (byte34 >>> 7);
        values[valuesOffset++] = byte34 & 127;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte35 >>> 1;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte35 & 1) << 6) | (byte36 >>> 2);
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte36 & 3) << 5) | (byte37 >>> 3);
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 7) << 4) | (byte38 >>> 4);
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 15) << 3) | (byte39 >>> 5);
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte39 & 31) << 2) | (byte40 >>> 6);
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte40 & 63) << 1) | (byte41 >>> 7);
        values[valuesOffset++] = byte41 & 127;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte42 >>> 1;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 1) << 6) | (byte43 >>> 2);
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte43 & 3) << 5) | (byte44 >>> 3);
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte44 & 7) << 4) | (byte45 >>> 4);
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 15) << 3) | (byte46 >>> 5);
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte46 & 31) << 2) | (byte47 >>> 6);
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 63) << 1) | (byte48 >>> 7);
        values[valuesOffset++] = byte48 & 127;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte49 >>> 1;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte49 & 1) << 6) | (byte50 >>> 2);
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte50 & 3) << 5) | (byte51 >>> 3);
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte51 & 7) << 4) | (byte52 >>> 4);
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 15) << 3) | (byte53 >>> 5);
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte53 & 31) << 2) | (byte54 >>> 6);
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte54 & 63) << 1) | (byte55 >>> 7);
        values[valuesOffset++] = byte55 & 127;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 57) | ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 43) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 29) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset++] & 0xffffffffL) << 15) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset++] & 0xffffffffL) << 1) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 51) | ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 37) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 23) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 9) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 5);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 59) | ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 45) | ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 31) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 17) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset++] & 0xffffffffL) << 3) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 53) | ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 39) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 25) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset++] & 0xffffffffL) << 11) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 3);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 61) | ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 47) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 33) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset++] & 0xffffffffL) << 19) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset++] & 0xffffffffL) << 5) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 55) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 41) | ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 27) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 13) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 1);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 63) | ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 49) | ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 35) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 21) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset++] & 0xffffffffL) << 7) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 57) | (values[valuesOffset++] << 50) | (values[valuesOffset++] << 43) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 29) | (values[valuesOffset++] << 22) | (values[valuesOffset++] << 15) | (values[valuesOffset++] << 8) | (values[valuesOffset++] << 1) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 51) | (values[valuesOffset++] << 44) | (values[valuesOffset++] << 37) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 23) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 9) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 5);
        blocks[blocksOffset++] = (values[valuesOffset++] << 59) | (values[valuesOffset++] << 52) | (values[valuesOffset++] << 45) | (values[valuesOffset++] << 38) | (values[valuesOffset++] << 31) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 17) | (values[valuesOffset++] << 10) | (values[valuesOffset++] << 3) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 53) | (values[valuesOffset++] << 46) | (values[valuesOffset++] << 39) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 25) | (values[valuesOffset++] << 18) | (values[valuesOffset++] << 11) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 3);
        blocks[blocksOffset++] = (values[valuesOffset++] << 61) | (values[valuesOffset++] << 54) | (values[valuesOffset++] << 47) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 33) | (values[valuesOffset++] << 26) | (values[valuesOffset++] << 19) | (values[valuesOffset++] << 12) | (values[valuesOffset++] << 5) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 55) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 41) | (values[valuesOffset++] << 34) | (values[valuesOffset++] << 27) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 13) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 1);
        blocks[blocksOffset++] = (values[valuesOffset++] << 63) | (values[valuesOffset++] << 56) | (values[valuesOffset++] << 49) | (values[valuesOffset++] << 42) | (values[valuesOffset++] << 35) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 21) | (values[valuesOffset++] << 14) | (values[valuesOffset++] << 7) | values[valuesOffset++];
      }
    }

}
