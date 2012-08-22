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
final class BulkOperationPacked9 extends BulkOperation {
    public int blockCount() {
      return 9;
    }

    public int valueCount() {
      return 64;
    }

    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 55);
        values[valuesOffset++] = (int) ((block0 >>> 46) & 511L);
        values[valuesOffset++] = (int) ((block0 >>> 37) & 511L);
        values[valuesOffset++] = (int) ((block0 >>> 28) & 511L);
        values[valuesOffset++] = (int) ((block0 >>> 19) & 511L);
        values[valuesOffset++] = (int) ((block0 >>> 10) & 511L);
        values[valuesOffset++] = (int) ((block0 >>> 1) & 511L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 1L) << 8) | (block1 >>> 56));
        values[valuesOffset++] = (int) ((block1 >>> 47) & 511L);
        values[valuesOffset++] = (int) ((block1 >>> 38) & 511L);
        values[valuesOffset++] = (int) ((block1 >>> 29) & 511L);
        values[valuesOffset++] = (int) ((block1 >>> 20) & 511L);
        values[valuesOffset++] = (int) ((block1 >>> 11) & 511L);
        values[valuesOffset++] = (int) ((block1 >>> 2) & 511L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 3L) << 7) | (block2 >>> 57));
        values[valuesOffset++] = (int) ((block2 >>> 48) & 511L);
        values[valuesOffset++] = (int) ((block2 >>> 39) & 511L);
        values[valuesOffset++] = (int) ((block2 >>> 30) & 511L);
        values[valuesOffset++] = (int) ((block2 >>> 21) & 511L);
        values[valuesOffset++] = (int) ((block2 >>> 12) & 511L);
        values[valuesOffset++] = (int) ((block2 >>> 3) & 511L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 7L) << 6) | (block3 >>> 58));
        values[valuesOffset++] = (int) ((block3 >>> 49) & 511L);
        values[valuesOffset++] = (int) ((block3 >>> 40) & 511L);
        values[valuesOffset++] = (int) ((block3 >>> 31) & 511L);
        values[valuesOffset++] = (int) ((block3 >>> 22) & 511L);
        values[valuesOffset++] = (int) ((block3 >>> 13) & 511L);
        values[valuesOffset++] = (int) ((block3 >>> 4) & 511L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 15L) << 5) | (block4 >>> 59));
        values[valuesOffset++] = (int) ((block4 >>> 50) & 511L);
        values[valuesOffset++] = (int) ((block4 >>> 41) & 511L);
        values[valuesOffset++] = (int) ((block4 >>> 32) & 511L);
        values[valuesOffset++] = (int) ((block4 >>> 23) & 511L);
        values[valuesOffset++] = (int) ((block4 >>> 14) & 511L);
        values[valuesOffset++] = (int) ((block4 >>> 5) & 511L);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block4 & 31L) << 4) | (block5 >>> 60));
        values[valuesOffset++] = (int) ((block5 >>> 51) & 511L);
        values[valuesOffset++] = (int) ((block5 >>> 42) & 511L);
        values[valuesOffset++] = (int) ((block5 >>> 33) & 511L);
        values[valuesOffset++] = (int) ((block5 >>> 24) & 511L);
        values[valuesOffset++] = (int) ((block5 >>> 15) & 511L);
        values[valuesOffset++] = (int) ((block5 >>> 6) & 511L);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block5 & 63L) << 3) | (block6 >>> 61));
        values[valuesOffset++] = (int) ((block6 >>> 52) & 511L);
        values[valuesOffset++] = (int) ((block6 >>> 43) & 511L);
        values[valuesOffset++] = (int) ((block6 >>> 34) & 511L);
        values[valuesOffset++] = (int) ((block6 >>> 25) & 511L);
        values[valuesOffset++] = (int) ((block6 >>> 16) & 511L);
        values[valuesOffset++] = (int) ((block6 >>> 7) & 511L);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block6 & 127L) << 2) | (block7 >>> 62));
        values[valuesOffset++] = (int) ((block7 >>> 53) & 511L);
        values[valuesOffset++] = (int) ((block7 >>> 44) & 511L);
        values[valuesOffset++] = (int) ((block7 >>> 35) & 511L);
        values[valuesOffset++] = (int) ((block7 >>> 26) & 511L);
        values[valuesOffset++] = (int) ((block7 >>> 17) & 511L);
        values[valuesOffset++] = (int) ((block7 >>> 8) & 511L);
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block7 & 255L) << 1) | (block8 >>> 63));
        values[valuesOffset++] = (int) ((block8 >>> 54) & 511L);
        values[valuesOffset++] = (int) ((block8 >>> 45) & 511L);
        values[valuesOffset++] = (int) ((block8 >>> 36) & 511L);
        values[valuesOffset++] = (int) ((block8 >>> 27) & 511L);
        values[valuesOffset++] = (int) ((block8 >>> 18) & 511L);
        values[valuesOffset++] = (int) ((block8 >>> 9) & 511L);
        values[valuesOffset++] = (int) (block8 & 511L);
      }
    }

    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 1) | (byte1 >>> 7);
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 127) << 2) | (byte2 >>> 6);
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 63) << 3) | (byte3 >>> 5);
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 31) << 4) | (byte4 >>> 4);
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 15) << 5) | (byte5 >>> 3);
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 7) << 6) | (byte6 >>> 2);
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 3) << 7) | (byte7 >>> 1);
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 1) << 8) | byte8;
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte9 << 1) | (byte10 >>> 7);
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 127) << 2) | (byte11 >>> 6);
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 63) << 3) | (byte12 >>> 5);
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 31) << 4) | (byte13 >>> 4);
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 15) << 5) | (byte14 >>> 3);
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte14 & 7) << 6) | (byte15 >>> 2);
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 7) | (byte16 >>> 1);
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 1) << 8) | byte17;
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte18 << 1) | (byte19 >>> 7);
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 127) << 2) | (byte20 >>> 6);
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte20 & 63) << 3) | (byte21 >>> 5);
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 31) << 4) | (byte22 >>> 4);
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 5) | (byte23 >>> 3);
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 7) << 6) | (byte24 >>> 2);
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 3) << 7) | (byte25 >>> 1);
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte25 & 1) << 8) | byte26;
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte27 << 1) | (byte28 >>> 7);
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 127) << 2) | (byte29 >>> 6);
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 63) << 3) | (byte30 >>> 5);
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte30 & 31) << 4) | (byte31 >>> 4);
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 5) | (byte32 >>> 3);
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 7) << 6) | (byte33 >>> 2);
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 7) | (byte34 >>> 1);
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte34 & 1) << 8) | byte35;
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte36 << 1) | (byte37 >>> 7);
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 127) << 2) | (byte38 >>> 6);
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 63) << 3) | (byte39 >>> 5);
        final int byte40 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte39 & 31) << 4) | (byte40 >>> 4);
        final int byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte40 & 15) << 5) | (byte41 >>> 3);
        final int byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte41 & 7) << 6) | (byte42 >>> 2);
        final int byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 3) << 7) | (byte43 >>> 1);
        final int byte44 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte43 & 1) << 8) | byte44;
        final int byte45 = blocks[blocksOffset++] & 0xFF;
        final int byte46 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte45 << 1) | (byte46 >>> 7);
        final int byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte46 & 127) << 2) | (byte47 >>> 6);
        final int byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 63) << 3) | (byte48 >>> 5);
        final int byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte48 & 31) << 4) | (byte49 >>> 4);
        final int byte50 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte49 & 15) << 5) | (byte50 >>> 3);
        final int byte51 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte50 & 7) << 6) | (byte51 >>> 2);
        final int byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte51 & 3) << 7) | (byte52 >>> 1);
        final int byte53 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 1) << 8) | byte53;
        final int byte54 = blocks[blocksOffset++] & 0xFF;
        final int byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte54 << 1) | (byte55 >>> 7);
        final int byte56 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte55 & 127) << 2) | (byte56 >>> 6);
        final int byte57 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte56 & 63) << 3) | (byte57 >>> 5);
        final int byte58 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte57 & 31) << 4) | (byte58 >>> 4);
        final int byte59 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte58 & 15) << 5) | (byte59 >>> 3);
        final int byte60 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte59 & 7) << 6) | (byte60 >>> 2);
        final int byte61 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte60 & 3) << 7) | (byte61 >>> 1);
        final int byte62 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte61 & 1) << 8) | byte62;
        final int byte63 = blocks[blocksOffset++] & 0xFF;
        final int byte64 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte63 << 1) | (byte64 >>> 7);
        final int byte65 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte64 & 127) << 2) | (byte65 >>> 6);
        final int byte66 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte65 & 63) << 3) | (byte66 >>> 5);
        final int byte67 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte66 & 31) << 4) | (byte67 >>> 4);
        final int byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte67 & 15) << 5) | (byte68 >>> 3);
        final int byte69 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte68 & 7) << 6) | (byte69 >>> 2);
        final int byte70 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte69 & 3) << 7) | (byte70 >>> 1);
        final int byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte70 & 1) << 8) | byte71;
      }
    }

    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 55;
        values[valuesOffset++] = (block0 >>> 46) & 511L;
        values[valuesOffset++] = (block0 >>> 37) & 511L;
        values[valuesOffset++] = (block0 >>> 28) & 511L;
        values[valuesOffset++] = (block0 >>> 19) & 511L;
        values[valuesOffset++] = (block0 >>> 10) & 511L;
        values[valuesOffset++] = (block0 >>> 1) & 511L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 1L) << 8) | (block1 >>> 56);
        values[valuesOffset++] = (block1 >>> 47) & 511L;
        values[valuesOffset++] = (block1 >>> 38) & 511L;
        values[valuesOffset++] = (block1 >>> 29) & 511L;
        values[valuesOffset++] = (block1 >>> 20) & 511L;
        values[valuesOffset++] = (block1 >>> 11) & 511L;
        values[valuesOffset++] = (block1 >>> 2) & 511L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 3L) << 7) | (block2 >>> 57);
        values[valuesOffset++] = (block2 >>> 48) & 511L;
        values[valuesOffset++] = (block2 >>> 39) & 511L;
        values[valuesOffset++] = (block2 >>> 30) & 511L;
        values[valuesOffset++] = (block2 >>> 21) & 511L;
        values[valuesOffset++] = (block2 >>> 12) & 511L;
        values[valuesOffset++] = (block2 >>> 3) & 511L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 7L) << 6) | (block3 >>> 58);
        values[valuesOffset++] = (block3 >>> 49) & 511L;
        values[valuesOffset++] = (block3 >>> 40) & 511L;
        values[valuesOffset++] = (block3 >>> 31) & 511L;
        values[valuesOffset++] = (block3 >>> 22) & 511L;
        values[valuesOffset++] = (block3 >>> 13) & 511L;
        values[valuesOffset++] = (block3 >>> 4) & 511L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 15L) << 5) | (block4 >>> 59);
        values[valuesOffset++] = (block4 >>> 50) & 511L;
        values[valuesOffset++] = (block4 >>> 41) & 511L;
        values[valuesOffset++] = (block4 >>> 32) & 511L;
        values[valuesOffset++] = (block4 >>> 23) & 511L;
        values[valuesOffset++] = (block4 >>> 14) & 511L;
        values[valuesOffset++] = (block4 >>> 5) & 511L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 31L) << 4) | (block5 >>> 60);
        values[valuesOffset++] = (block5 >>> 51) & 511L;
        values[valuesOffset++] = (block5 >>> 42) & 511L;
        values[valuesOffset++] = (block5 >>> 33) & 511L;
        values[valuesOffset++] = (block5 >>> 24) & 511L;
        values[valuesOffset++] = (block5 >>> 15) & 511L;
        values[valuesOffset++] = (block5 >>> 6) & 511L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 63L) << 3) | (block6 >>> 61);
        values[valuesOffset++] = (block6 >>> 52) & 511L;
        values[valuesOffset++] = (block6 >>> 43) & 511L;
        values[valuesOffset++] = (block6 >>> 34) & 511L;
        values[valuesOffset++] = (block6 >>> 25) & 511L;
        values[valuesOffset++] = (block6 >>> 16) & 511L;
        values[valuesOffset++] = (block6 >>> 7) & 511L;
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 127L) << 2) | (block7 >>> 62);
        values[valuesOffset++] = (block7 >>> 53) & 511L;
        values[valuesOffset++] = (block7 >>> 44) & 511L;
        values[valuesOffset++] = (block7 >>> 35) & 511L;
        values[valuesOffset++] = (block7 >>> 26) & 511L;
        values[valuesOffset++] = (block7 >>> 17) & 511L;
        values[valuesOffset++] = (block7 >>> 8) & 511L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 255L) << 1) | (block8 >>> 63);
        values[valuesOffset++] = (block8 >>> 54) & 511L;
        values[valuesOffset++] = (block8 >>> 45) & 511L;
        values[valuesOffset++] = (block8 >>> 36) & 511L;
        values[valuesOffset++] = (block8 >>> 27) & 511L;
        values[valuesOffset++] = (block8 >>> 18) & 511L;
        values[valuesOffset++] = (block8 >>> 9) & 511L;
        values[valuesOffset++] = block8 & 511L;
      }
    }

    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 1) | (byte1 >>> 7);
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 127) << 2) | (byte2 >>> 6);
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 63) << 3) | (byte3 >>> 5);
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 31) << 4) | (byte4 >>> 4);
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 15) << 5) | (byte5 >>> 3);
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 7) << 6) | (byte6 >>> 2);
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 3) << 7) | (byte7 >>> 1);
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 1) << 8) | byte8;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte9 << 1) | (byte10 >>> 7);
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 127) << 2) | (byte11 >>> 6);
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 63) << 3) | (byte12 >>> 5);
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 31) << 4) | (byte13 >>> 4);
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 15) << 5) | (byte14 >>> 3);
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte14 & 7) << 6) | (byte15 >>> 2);
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 7) | (byte16 >>> 1);
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 1) << 8) | byte17;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte18 << 1) | (byte19 >>> 7);
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 127) << 2) | (byte20 >>> 6);
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte20 & 63) << 3) | (byte21 >>> 5);
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 31) << 4) | (byte22 >>> 4);
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 5) | (byte23 >>> 3);
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 7) << 6) | (byte24 >>> 2);
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 3) << 7) | (byte25 >>> 1);
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte25 & 1) << 8) | byte26;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte27 << 1) | (byte28 >>> 7);
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 127) << 2) | (byte29 >>> 6);
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 63) << 3) | (byte30 >>> 5);
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte30 & 31) << 4) | (byte31 >>> 4);
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 5) | (byte32 >>> 3);
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 7) << 6) | (byte33 >>> 2);
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 7) | (byte34 >>> 1);
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte34 & 1) << 8) | byte35;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte36 << 1) | (byte37 >>> 7);
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 127) << 2) | (byte38 >>> 6);
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 63) << 3) | (byte39 >>> 5);
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte39 & 31) << 4) | (byte40 >>> 4);
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte40 & 15) << 5) | (byte41 >>> 3);
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte41 & 7) << 6) | (byte42 >>> 2);
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 3) << 7) | (byte43 >>> 1);
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte43 & 1) << 8) | byte44;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte45 << 1) | (byte46 >>> 7);
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte46 & 127) << 2) | (byte47 >>> 6);
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 63) << 3) | (byte48 >>> 5);
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte48 & 31) << 4) | (byte49 >>> 4);
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte49 & 15) << 5) | (byte50 >>> 3);
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte50 & 7) << 6) | (byte51 >>> 2);
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte51 & 3) << 7) | (byte52 >>> 1);
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 1) << 8) | byte53;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte54 << 1) | (byte55 >>> 7);
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte55 & 127) << 2) | (byte56 >>> 6);
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte56 & 63) << 3) | (byte57 >>> 5);
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte57 & 31) << 4) | (byte58 >>> 4);
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte58 & 15) << 5) | (byte59 >>> 3);
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte59 & 7) << 6) | (byte60 >>> 2);
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte60 & 3) << 7) | (byte61 >>> 1);
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte61 & 1) << 8) | byte62;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte63 << 1) | (byte64 >>> 7);
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte64 & 127) << 2) | (byte65 >>> 6);
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte65 & 63) << 3) | (byte66 >>> 5);
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte66 & 31) << 4) | (byte67 >>> 4);
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte67 & 15) << 5) | (byte68 >>> 3);
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte68 & 7) << 6) | (byte69 >>> 2);
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte69 & 3) << 7) | (byte70 >>> 1);
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte70 & 1) << 8) | byte71;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 55) | ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 37) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 19) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset++] & 0xffffffffL) << 1) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 47) | ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 29) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 11) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 7);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 57) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 39) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 21) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset++] & 0xffffffffL) << 3) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 49) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 31) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset++] & 0xffffffffL) << 13) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 5);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 59) | ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 41) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 23) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset++] & 0xffffffffL) << 5) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 51) | ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 33) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 15) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 3);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 61) | ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 43) | ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 25) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 7) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 53) | ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 35) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset++] & 0xffffffffL) << 17) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 1);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 63) | ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 45) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 27) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset++] & 0xffffffffL) << 9) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 55) | (values[valuesOffset++] << 46) | (values[valuesOffset++] << 37) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 19) | (values[valuesOffset++] << 10) | (values[valuesOffset++] << 1) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 47) | (values[valuesOffset++] << 38) | (values[valuesOffset++] << 29) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 11) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 7);
        blocks[blocksOffset++] = (values[valuesOffset++] << 57) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 39) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 21) | (values[valuesOffset++] << 12) | (values[valuesOffset++] << 3) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 49) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 31) | (values[valuesOffset++] << 22) | (values[valuesOffset++] << 13) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 5);
        blocks[blocksOffset++] = (values[valuesOffset++] << 59) | (values[valuesOffset++] << 50) | (values[valuesOffset++] << 41) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 23) | (values[valuesOffset++] << 14) | (values[valuesOffset++] << 5) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 51) | (values[valuesOffset++] << 42) | (values[valuesOffset++] << 33) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 15) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 3);
        blocks[blocksOffset++] = (values[valuesOffset++] << 61) | (values[valuesOffset++] << 52) | (values[valuesOffset++] << 43) | (values[valuesOffset++] << 34) | (values[valuesOffset++] << 25) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 7) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 53) | (values[valuesOffset++] << 44) | (values[valuesOffset++] << 35) | (values[valuesOffset++] << 26) | (values[valuesOffset++] << 17) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 1);
        blocks[blocksOffset++] = (values[valuesOffset++] << 63) | (values[valuesOffset++] << 54) | (values[valuesOffset++] << 45) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 27) | (values[valuesOffset++] << 18) | (values[valuesOffset++] << 9) | values[valuesOffset++];
      }
    }

}
