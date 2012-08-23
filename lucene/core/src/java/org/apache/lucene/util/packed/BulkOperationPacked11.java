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
final class BulkOperationPacked11 extends BulkOperation {
    @Override
    public int blockCount() {
      return 11;
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
        values[valuesOffset++] = (int) (block0 >>> 53);
        values[valuesOffset++] = (int) ((block0 >>> 42) & 2047L);
        values[valuesOffset++] = (int) ((block0 >>> 31) & 2047L);
        values[valuesOffset++] = (int) ((block0 >>> 20) & 2047L);
        values[valuesOffset++] = (int) ((block0 >>> 9) & 2047L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 511L) << 2) | (block1 >>> 62));
        values[valuesOffset++] = (int) ((block1 >>> 51) & 2047L);
        values[valuesOffset++] = (int) ((block1 >>> 40) & 2047L);
        values[valuesOffset++] = (int) ((block1 >>> 29) & 2047L);
        values[valuesOffset++] = (int) ((block1 >>> 18) & 2047L);
        values[valuesOffset++] = (int) ((block1 >>> 7) & 2047L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 127L) << 4) | (block2 >>> 60));
        values[valuesOffset++] = (int) ((block2 >>> 49) & 2047L);
        values[valuesOffset++] = (int) ((block2 >>> 38) & 2047L);
        values[valuesOffset++] = (int) ((block2 >>> 27) & 2047L);
        values[valuesOffset++] = (int) ((block2 >>> 16) & 2047L);
        values[valuesOffset++] = (int) ((block2 >>> 5) & 2047L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 31L) << 6) | (block3 >>> 58));
        values[valuesOffset++] = (int) ((block3 >>> 47) & 2047L);
        values[valuesOffset++] = (int) ((block3 >>> 36) & 2047L);
        values[valuesOffset++] = (int) ((block3 >>> 25) & 2047L);
        values[valuesOffset++] = (int) ((block3 >>> 14) & 2047L);
        values[valuesOffset++] = (int) ((block3 >>> 3) & 2047L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 7L) << 8) | (block4 >>> 56));
        values[valuesOffset++] = (int) ((block4 >>> 45) & 2047L);
        values[valuesOffset++] = (int) ((block4 >>> 34) & 2047L);
        values[valuesOffset++] = (int) ((block4 >>> 23) & 2047L);
        values[valuesOffset++] = (int) ((block4 >>> 12) & 2047L);
        values[valuesOffset++] = (int) ((block4 >>> 1) & 2047L);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block4 & 1L) << 10) | (block5 >>> 54));
        values[valuesOffset++] = (int) ((block5 >>> 43) & 2047L);
        values[valuesOffset++] = (int) ((block5 >>> 32) & 2047L);
        values[valuesOffset++] = (int) ((block5 >>> 21) & 2047L);
        values[valuesOffset++] = (int) ((block5 >>> 10) & 2047L);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block5 & 1023L) << 1) | (block6 >>> 63));
        values[valuesOffset++] = (int) ((block6 >>> 52) & 2047L);
        values[valuesOffset++] = (int) ((block6 >>> 41) & 2047L);
        values[valuesOffset++] = (int) ((block6 >>> 30) & 2047L);
        values[valuesOffset++] = (int) ((block6 >>> 19) & 2047L);
        values[valuesOffset++] = (int) ((block6 >>> 8) & 2047L);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block6 & 255L) << 3) | (block7 >>> 61));
        values[valuesOffset++] = (int) ((block7 >>> 50) & 2047L);
        values[valuesOffset++] = (int) ((block7 >>> 39) & 2047L);
        values[valuesOffset++] = (int) ((block7 >>> 28) & 2047L);
        values[valuesOffset++] = (int) ((block7 >>> 17) & 2047L);
        values[valuesOffset++] = (int) ((block7 >>> 6) & 2047L);
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block7 & 63L) << 5) | (block8 >>> 59));
        values[valuesOffset++] = (int) ((block8 >>> 48) & 2047L);
        values[valuesOffset++] = (int) ((block8 >>> 37) & 2047L);
        values[valuesOffset++] = (int) ((block8 >>> 26) & 2047L);
        values[valuesOffset++] = (int) ((block8 >>> 15) & 2047L);
        values[valuesOffset++] = (int) ((block8 >>> 4) & 2047L);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block8 & 15L) << 7) | (block9 >>> 57));
        values[valuesOffset++] = (int) ((block9 >>> 46) & 2047L);
        values[valuesOffset++] = (int) ((block9 >>> 35) & 2047L);
        values[valuesOffset++] = (int) ((block9 >>> 24) & 2047L);
        values[valuesOffset++] = (int) ((block9 >>> 13) & 2047L);
        values[valuesOffset++] = (int) ((block9 >>> 2) & 2047L);
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block9 & 3L) << 9) | (block10 >>> 55));
        values[valuesOffset++] = (int) ((block10 >>> 44) & 2047L);
        values[valuesOffset++] = (int) ((block10 >>> 33) & 2047L);
        values[valuesOffset++] = (int) ((block10 >>> 22) & 2047L);
        values[valuesOffset++] = (int) ((block10 >>> 11) & 2047L);
        values[valuesOffset++] = (int) (block10 & 2047L);
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 3) | (byte1 >>> 5);
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 31) << 6) | (byte2 >>> 2);
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 3) << 9) | (byte3 << 1) | (byte4 >>> 7);
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 127) << 4) | (byte5 >>> 4);
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 15) << 7) | (byte6 >>> 1);
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 1) << 10) | (byte7 << 2) | (byte8 >>> 6);
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 63) << 5) | (byte9 >>> 3);
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte9 & 7) << 8) | byte10;
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte11 << 3) | (byte12 >>> 5);
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 31) << 6) | (byte13 >>> 2);
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 3) << 9) | (byte14 << 1) | (byte15 >>> 7);
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 127) << 4) | (byte16 >>> 4);
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 15) << 7) | (byte17 >>> 1);
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 1) << 10) | (byte18 << 2) | (byte19 >>> 6);
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 63) << 5) | (byte20 >>> 3);
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte20 & 7) << 8) | byte21;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte22 << 3) | (byte23 >>> 5);
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 31) << 6) | (byte24 >>> 2);
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 3) << 9) | (byte25 << 1) | (byte26 >>> 7);
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 127) << 4) | (byte27 >>> 4);
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte27 & 15) << 7) | (byte28 >>> 1);
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 1) << 10) | (byte29 << 2) | (byte30 >>> 6);
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte30 & 63) << 5) | (byte31 >>> 3);
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 7) << 8) | byte32;
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte33 << 3) | (byte34 >>> 5);
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte34 & 31) << 6) | (byte35 >>> 2);
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte35 & 3) << 9) | (byte36 << 1) | (byte37 >>> 7);
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 127) << 4) | (byte38 >>> 4);
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 15) << 7) | (byte39 >>> 1);
        final int byte40 = blocks[blocksOffset++] & 0xFF;
        final int byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte39 & 1) << 10) | (byte40 << 2) | (byte41 >>> 6);
        final int byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte41 & 63) << 5) | (byte42 >>> 3);
        final int byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 7) << 8) | byte43;
        final int byte44 = blocks[blocksOffset++] & 0xFF;
        final int byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte44 << 3) | (byte45 >>> 5);
        final int byte46 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 31) << 6) | (byte46 >>> 2);
        final int byte47 = blocks[blocksOffset++] & 0xFF;
        final int byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte46 & 3) << 9) | (byte47 << 1) | (byte48 >>> 7);
        final int byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte48 & 127) << 4) | (byte49 >>> 4);
        final int byte50 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte49 & 15) << 7) | (byte50 >>> 1);
        final int byte51 = blocks[blocksOffset++] & 0xFF;
        final int byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte50 & 1) << 10) | (byte51 << 2) | (byte52 >>> 6);
        final int byte53 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 63) << 5) | (byte53 >>> 3);
        final int byte54 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte53 & 7) << 8) | byte54;
        final int byte55 = blocks[blocksOffset++] & 0xFF;
        final int byte56 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte55 << 3) | (byte56 >>> 5);
        final int byte57 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte56 & 31) << 6) | (byte57 >>> 2);
        final int byte58 = blocks[blocksOffset++] & 0xFF;
        final int byte59 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte57 & 3) << 9) | (byte58 << 1) | (byte59 >>> 7);
        final int byte60 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte59 & 127) << 4) | (byte60 >>> 4);
        final int byte61 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte60 & 15) << 7) | (byte61 >>> 1);
        final int byte62 = blocks[blocksOffset++] & 0xFF;
        final int byte63 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte61 & 1) << 10) | (byte62 << 2) | (byte63 >>> 6);
        final int byte64 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte63 & 63) << 5) | (byte64 >>> 3);
        final int byte65 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte64 & 7) << 8) | byte65;
        final int byte66 = blocks[blocksOffset++] & 0xFF;
        final int byte67 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte66 << 3) | (byte67 >>> 5);
        final int byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte67 & 31) << 6) | (byte68 >>> 2);
        final int byte69 = blocks[blocksOffset++] & 0xFF;
        final int byte70 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte68 & 3) << 9) | (byte69 << 1) | (byte70 >>> 7);
        final int byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte70 & 127) << 4) | (byte71 >>> 4);
        final int byte72 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte71 & 15) << 7) | (byte72 >>> 1);
        final int byte73 = blocks[blocksOffset++] & 0xFF;
        final int byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte72 & 1) << 10) | (byte73 << 2) | (byte74 >>> 6);
        final int byte75 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte74 & 63) << 5) | (byte75 >>> 3);
        final int byte76 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte75 & 7) << 8) | byte76;
        final int byte77 = blocks[blocksOffset++] & 0xFF;
        final int byte78 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte77 << 3) | (byte78 >>> 5);
        final int byte79 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte78 & 31) << 6) | (byte79 >>> 2);
        final int byte80 = blocks[blocksOffset++] & 0xFF;
        final int byte81 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte79 & 3) << 9) | (byte80 << 1) | (byte81 >>> 7);
        final int byte82 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte81 & 127) << 4) | (byte82 >>> 4);
        final int byte83 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte82 & 15) << 7) | (byte83 >>> 1);
        final int byte84 = blocks[blocksOffset++] & 0xFF;
        final int byte85 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte83 & 1) << 10) | (byte84 << 2) | (byte85 >>> 6);
        final int byte86 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte85 & 63) << 5) | (byte86 >>> 3);
        final int byte87 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte86 & 7) << 8) | byte87;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 53;
        values[valuesOffset++] = (block0 >>> 42) & 2047L;
        values[valuesOffset++] = (block0 >>> 31) & 2047L;
        values[valuesOffset++] = (block0 >>> 20) & 2047L;
        values[valuesOffset++] = (block0 >>> 9) & 2047L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 511L) << 2) | (block1 >>> 62);
        values[valuesOffset++] = (block1 >>> 51) & 2047L;
        values[valuesOffset++] = (block1 >>> 40) & 2047L;
        values[valuesOffset++] = (block1 >>> 29) & 2047L;
        values[valuesOffset++] = (block1 >>> 18) & 2047L;
        values[valuesOffset++] = (block1 >>> 7) & 2047L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 127L) << 4) | (block2 >>> 60);
        values[valuesOffset++] = (block2 >>> 49) & 2047L;
        values[valuesOffset++] = (block2 >>> 38) & 2047L;
        values[valuesOffset++] = (block2 >>> 27) & 2047L;
        values[valuesOffset++] = (block2 >>> 16) & 2047L;
        values[valuesOffset++] = (block2 >>> 5) & 2047L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 31L) << 6) | (block3 >>> 58);
        values[valuesOffset++] = (block3 >>> 47) & 2047L;
        values[valuesOffset++] = (block3 >>> 36) & 2047L;
        values[valuesOffset++] = (block3 >>> 25) & 2047L;
        values[valuesOffset++] = (block3 >>> 14) & 2047L;
        values[valuesOffset++] = (block3 >>> 3) & 2047L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 7L) << 8) | (block4 >>> 56);
        values[valuesOffset++] = (block4 >>> 45) & 2047L;
        values[valuesOffset++] = (block4 >>> 34) & 2047L;
        values[valuesOffset++] = (block4 >>> 23) & 2047L;
        values[valuesOffset++] = (block4 >>> 12) & 2047L;
        values[valuesOffset++] = (block4 >>> 1) & 2047L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 1L) << 10) | (block5 >>> 54);
        values[valuesOffset++] = (block5 >>> 43) & 2047L;
        values[valuesOffset++] = (block5 >>> 32) & 2047L;
        values[valuesOffset++] = (block5 >>> 21) & 2047L;
        values[valuesOffset++] = (block5 >>> 10) & 2047L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 1023L) << 1) | (block6 >>> 63);
        values[valuesOffset++] = (block6 >>> 52) & 2047L;
        values[valuesOffset++] = (block6 >>> 41) & 2047L;
        values[valuesOffset++] = (block6 >>> 30) & 2047L;
        values[valuesOffset++] = (block6 >>> 19) & 2047L;
        values[valuesOffset++] = (block6 >>> 8) & 2047L;
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 255L) << 3) | (block7 >>> 61);
        values[valuesOffset++] = (block7 >>> 50) & 2047L;
        values[valuesOffset++] = (block7 >>> 39) & 2047L;
        values[valuesOffset++] = (block7 >>> 28) & 2047L;
        values[valuesOffset++] = (block7 >>> 17) & 2047L;
        values[valuesOffset++] = (block7 >>> 6) & 2047L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 63L) << 5) | (block8 >>> 59);
        values[valuesOffset++] = (block8 >>> 48) & 2047L;
        values[valuesOffset++] = (block8 >>> 37) & 2047L;
        values[valuesOffset++] = (block8 >>> 26) & 2047L;
        values[valuesOffset++] = (block8 >>> 15) & 2047L;
        values[valuesOffset++] = (block8 >>> 4) & 2047L;
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 15L) << 7) | (block9 >>> 57);
        values[valuesOffset++] = (block9 >>> 46) & 2047L;
        values[valuesOffset++] = (block9 >>> 35) & 2047L;
        values[valuesOffset++] = (block9 >>> 24) & 2047L;
        values[valuesOffset++] = (block9 >>> 13) & 2047L;
        values[valuesOffset++] = (block9 >>> 2) & 2047L;
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 3L) << 9) | (block10 >>> 55);
        values[valuesOffset++] = (block10 >>> 44) & 2047L;
        values[valuesOffset++] = (block10 >>> 33) & 2047L;
        values[valuesOffset++] = (block10 >>> 22) & 2047L;
        values[valuesOffset++] = (block10 >>> 11) & 2047L;
        values[valuesOffset++] = block10 & 2047L;
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 3) | (byte1 >>> 5);
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 31) << 6) | (byte2 >>> 2);
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 3) << 9) | (byte3 << 1) | (byte4 >>> 7);
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 127) << 4) | (byte5 >>> 4);
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 15) << 7) | (byte6 >>> 1);
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 1) << 10) | (byte7 << 2) | (byte8 >>> 6);
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 63) << 5) | (byte9 >>> 3);
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte9 & 7) << 8) | byte10;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte11 << 3) | (byte12 >>> 5);
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 31) << 6) | (byte13 >>> 2);
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 3) << 9) | (byte14 << 1) | (byte15 >>> 7);
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 127) << 4) | (byte16 >>> 4);
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 15) << 7) | (byte17 >>> 1);
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 1) << 10) | (byte18 << 2) | (byte19 >>> 6);
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 63) << 5) | (byte20 >>> 3);
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte20 & 7) << 8) | byte21;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte22 << 3) | (byte23 >>> 5);
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 31) << 6) | (byte24 >>> 2);
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 3) << 9) | (byte25 << 1) | (byte26 >>> 7);
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 127) << 4) | (byte27 >>> 4);
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte27 & 15) << 7) | (byte28 >>> 1);
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 1) << 10) | (byte29 << 2) | (byte30 >>> 6);
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte30 & 63) << 5) | (byte31 >>> 3);
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 7) << 8) | byte32;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte33 << 3) | (byte34 >>> 5);
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte34 & 31) << 6) | (byte35 >>> 2);
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte35 & 3) << 9) | (byte36 << 1) | (byte37 >>> 7);
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 127) << 4) | (byte38 >>> 4);
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 15) << 7) | (byte39 >>> 1);
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte39 & 1) << 10) | (byte40 << 2) | (byte41 >>> 6);
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte41 & 63) << 5) | (byte42 >>> 3);
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 7) << 8) | byte43;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte44 << 3) | (byte45 >>> 5);
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 31) << 6) | (byte46 >>> 2);
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte46 & 3) << 9) | (byte47 << 1) | (byte48 >>> 7);
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte48 & 127) << 4) | (byte49 >>> 4);
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte49 & 15) << 7) | (byte50 >>> 1);
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte50 & 1) << 10) | (byte51 << 2) | (byte52 >>> 6);
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 63) << 5) | (byte53 >>> 3);
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte53 & 7) << 8) | byte54;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte55 << 3) | (byte56 >>> 5);
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte56 & 31) << 6) | (byte57 >>> 2);
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte57 & 3) << 9) | (byte58 << 1) | (byte59 >>> 7);
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte59 & 127) << 4) | (byte60 >>> 4);
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte60 & 15) << 7) | (byte61 >>> 1);
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte61 & 1) << 10) | (byte62 << 2) | (byte63 >>> 6);
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte63 & 63) << 5) | (byte64 >>> 3);
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte64 & 7) << 8) | byte65;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte66 << 3) | (byte67 >>> 5);
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte67 & 31) << 6) | (byte68 >>> 2);
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte68 & 3) << 9) | (byte69 << 1) | (byte70 >>> 7);
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte70 & 127) << 4) | (byte71 >>> 4);
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte71 & 15) << 7) | (byte72 >>> 1);
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte72 & 1) << 10) | (byte73 << 2) | (byte74 >>> 6);
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte74 & 63) << 5) | (byte75 >>> 3);
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte75 & 7) << 8) | byte76;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte77 << 3) | (byte78 >>> 5);
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte78 & 31) << 6) | (byte79 >>> 2);
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte79 & 3) << 9) | (byte80 << 1) | (byte81 >>> 7);
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte81 & 127) << 4) | (byte82 >>> 4);
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte82 & 15) << 7) | (byte83 >>> 1);
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte83 & 1) << 10) | (byte84 << 2) | (byte85 >>> 6);
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte85 & 63) << 5) | (byte86 >>> 3);
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte86 & 7) << 8) | byte87;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 53) | ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 31) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 9) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 51) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 29) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset++] & 0xffffffffL) << 7) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 49) | ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 27) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 5) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 47) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 25) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset++] & 0xffffffffL) << 3) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 45) | ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 23) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset++] & 0xffffffffL) << 1) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 43) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 21) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 1);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 63) | ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 41) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 19) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 3);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 61) | ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 39) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 17) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 5);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 59) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 37) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset++] & 0xffffffffL) << 15) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 7);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 57) | ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 35) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 13) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 9);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 55) | ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 33) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset++] & 0xffffffffL) << 11) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 53) | (values[valuesOffset++] << 42) | (values[valuesOffset++] << 31) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 9) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 51) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 29) | (values[valuesOffset++] << 18) | (values[valuesOffset++] << 7) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 49) | (values[valuesOffset++] << 38) | (values[valuesOffset++] << 27) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 5) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 47) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 25) | (values[valuesOffset++] << 14) | (values[valuesOffset++] << 3) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 45) | (values[valuesOffset++] << 34) | (values[valuesOffset++] << 23) | (values[valuesOffset++] << 12) | (values[valuesOffset++] << 1) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 43) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 21) | (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 1);
        blocks[blocksOffset++] = (values[valuesOffset++] << 63) | (values[valuesOffset++] << 52) | (values[valuesOffset++] << 41) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 19) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 3);
        blocks[blocksOffset++] = (values[valuesOffset++] << 61) | (values[valuesOffset++] << 50) | (values[valuesOffset++] << 39) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 17) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 5);
        blocks[blocksOffset++] = (values[valuesOffset++] << 59) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 37) | (values[valuesOffset++] << 26) | (values[valuesOffset++] << 15) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 7);
        blocks[blocksOffset++] = (values[valuesOffset++] << 57) | (values[valuesOffset++] << 46) | (values[valuesOffset++] << 35) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 13) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 9);
        blocks[blocksOffset++] = (values[valuesOffset++] << 55) | (values[valuesOffset++] << 44) | (values[valuesOffset++] << 33) | (values[valuesOffset++] << 22) | (values[valuesOffset++] << 11) | values[valuesOffset++];
      }
    }

}
