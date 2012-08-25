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
final class BulkOperationPacked22 extends BulkOperation {
    @Override
    public int blockCount() {
      return 11;
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
        values[valuesOffset++] = (int) (block0 >>> 42);
        values[valuesOffset++] = (int) ((block0 >>> 20) & 4194303L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 1048575L) << 2) | (block1 >>> 62));
        values[valuesOffset++] = (int) ((block1 >>> 40) & 4194303L);
        values[valuesOffset++] = (int) ((block1 >>> 18) & 4194303L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 262143L) << 4) | (block2 >>> 60));
        values[valuesOffset++] = (int) ((block2 >>> 38) & 4194303L);
        values[valuesOffset++] = (int) ((block2 >>> 16) & 4194303L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 65535L) << 6) | (block3 >>> 58));
        values[valuesOffset++] = (int) ((block3 >>> 36) & 4194303L);
        values[valuesOffset++] = (int) ((block3 >>> 14) & 4194303L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 16383L) << 8) | (block4 >>> 56));
        values[valuesOffset++] = (int) ((block4 >>> 34) & 4194303L);
        values[valuesOffset++] = (int) ((block4 >>> 12) & 4194303L);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block4 & 4095L) << 10) | (block5 >>> 54));
        values[valuesOffset++] = (int) ((block5 >>> 32) & 4194303L);
        values[valuesOffset++] = (int) ((block5 >>> 10) & 4194303L);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block5 & 1023L) << 12) | (block6 >>> 52));
        values[valuesOffset++] = (int) ((block6 >>> 30) & 4194303L);
        values[valuesOffset++] = (int) ((block6 >>> 8) & 4194303L);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block6 & 255L) << 14) | (block7 >>> 50));
        values[valuesOffset++] = (int) ((block7 >>> 28) & 4194303L);
        values[valuesOffset++] = (int) ((block7 >>> 6) & 4194303L);
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block7 & 63L) << 16) | (block8 >>> 48));
        values[valuesOffset++] = (int) ((block8 >>> 26) & 4194303L);
        values[valuesOffset++] = (int) ((block8 >>> 4) & 4194303L);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block8 & 15L) << 18) | (block9 >>> 46));
        values[valuesOffset++] = (int) ((block9 >>> 24) & 4194303L);
        values[valuesOffset++] = (int) ((block9 >>> 2) & 4194303L);
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block9 & 3L) << 20) | (block10 >>> 44));
        values[valuesOffset++] = (int) ((block10 >>> 22) & 4194303L);
        values[valuesOffset++] = (int) (block10 & 4194303L);
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 14) | (byte1 << 6) | (byte2 >>> 2);
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 3) << 20) | (byte3 << 12) | (byte4 << 4) | (byte5 >>> 4);
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 15) << 18) | (byte6 << 10) | (byte7 << 2) | (byte8 >>> 6);
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 63) << 16) | (byte9 << 8) | byte10;
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte11 << 14) | (byte12 << 6) | (byte13 >>> 2);
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 3) << 20) | (byte14 << 12) | (byte15 << 4) | (byte16 >>> 4);
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 15) << 18) | (byte17 << 10) | (byte18 << 2) | (byte19 >>> 6);
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 63) << 16) | (byte20 << 8) | byte21;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte22 << 14) | (byte23 << 6) | (byte24 >>> 2);
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 3) << 20) | (byte25 << 12) | (byte26 << 4) | (byte27 >>> 4);
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte27 & 15) << 18) | (byte28 << 10) | (byte29 << 2) | (byte30 >>> 6);
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte30 & 63) << 16) | (byte31 << 8) | byte32;
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte33 << 14) | (byte34 << 6) | (byte35 >>> 2);
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte35 & 3) << 20) | (byte36 << 12) | (byte37 << 4) | (byte38 >>> 4);
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        final int byte40 = blocks[blocksOffset++] & 0xFF;
        final int byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 15) << 18) | (byte39 << 10) | (byte40 << 2) | (byte41 >>> 6);
        final int byte42 = blocks[blocksOffset++] & 0xFF;
        final int byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte41 & 63) << 16) | (byte42 << 8) | byte43;
        final int byte44 = blocks[blocksOffset++] & 0xFF;
        final int byte45 = blocks[blocksOffset++] & 0xFF;
        final int byte46 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte44 << 14) | (byte45 << 6) | (byte46 >>> 2);
        final int byte47 = blocks[blocksOffset++] & 0xFF;
        final int byte48 = blocks[blocksOffset++] & 0xFF;
        final int byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte46 & 3) << 20) | (byte47 << 12) | (byte48 << 4) | (byte49 >>> 4);
        final int byte50 = blocks[blocksOffset++] & 0xFF;
        final int byte51 = blocks[blocksOffset++] & 0xFF;
        final int byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte49 & 15) << 18) | (byte50 << 10) | (byte51 << 2) | (byte52 >>> 6);
        final int byte53 = blocks[blocksOffset++] & 0xFF;
        final int byte54 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 63) << 16) | (byte53 << 8) | byte54;
        final int byte55 = blocks[blocksOffset++] & 0xFF;
        final int byte56 = blocks[blocksOffset++] & 0xFF;
        final int byte57 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte55 << 14) | (byte56 << 6) | (byte57 >>> 2);
        final int byte58 = blocks[blocksOffset++] & 0xFF;
        final int byte59 = blocks[blocksOffset++] & 0xFF;
        final int byte60 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte57 & 3) << 20) | (byte58 << 12) | (byte59 << 4) | (byte60 >>> 4);
        final int byte61 = blocks[blocksOffset++] & 0xFF;
        final int byte62 = blocks[blocksOffset++] & 0xFF;
        final int byte63 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte60 & 15) << 18) | (byte61 << 10) | (byte62 << 2) | (byte63 >>> 6);
        final int byte64 = blocks[blocksOffset++] & 0xFF;
        final int byte65 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte63 & 63) << 16) | (byte64 << 8) | byte65;
        final int byte66 = blocks[blocksOffset++] & 0xFF;
        final int byte67 = blocks[blocksOffset++] & 0xFF;
        final int byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte66 << 14) | (byte67 << 6) | (byte68 >>> 2);
        final int byte69 = blocks[blocksOffset++] & 0xFF;
        final int byte70 = blocks[blocksOffset++] & 0xFF;
        final int byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte68 & 3) << 20) | (byte69 << 12) | (byte70 << 4) | (byte71 >>> 4);
        final int byte72 = blocks[blocksOffset++] & 0xFF;
        final int byte73 = blocks[blocksOffset++] & 0xFF;
        final int byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte71 & 15) << 18) | (byte72 << 10) | (byte73 << 2) | (byte74 >>> 6);
        final int byte75 = blocks[blocksOffset++] & 0xFF;
        final int byte76 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte74 & 63) << 16) | (byte75 << 8) | byte76;
        final int byte77 = blocks[blocksOffset++] & 0xFF;
        final int byte78 = blocks[blocksOffset++] & 0xFF;
        final int byte79 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte77 << 14) | (byte78 << 6) | (byte79 >>> 2);
        final int byte80 = blocks[blocksOffset++] & 0xFF;
        final int byte81 = blocks[blocksOffset++] & 0xFF;
        final int byte82 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte79 & 3) << 20) | (byte80 << 12) | (byte81 << 4) | (byte82 >>> 4);
        final int byte83 = blocks[blocksOffset++] & 0xFF;
        final int byte84 = blocks[blocksOffset++] & 0xFF;
        final int byte85 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte82 & 15) << 18) | (byte83 << 10) | (byte84 << 2) | (byte85 >>> 6);
        final int byte86 = blocks[blocksOffset++] & 0xFF;
        final int byte87 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte85 & 63) << 16) | (byte86 << 8) | byte87;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 42;
        values[valuesOffset++] = (block0 >>> 20) & 4194303L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 1048575L) << 2) | (block1 >>> 62);
        values[valuesOffset++] = (block1 >>> 40) & 4194303L;
        values[valuesOffset++] = (block1 >>> 18) & 4194303L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 262143L) << 4) | (block2 >>> 60);
        values[valuesOffset++] = (block2 >>> 38) & 4194303L;
        values[valuesOffset++] = (block2 >>> 16) & 4194303L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 65535L) << 6) | (block3 >>> 58);
        values[valuesOffset++] = (block3 >>> 36) & 4194303L;
        values[valuesOffset++] = (block3 >>> 14) & 4194303L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 16383L) << 8) | (block4 >>> 56);
        values[valuesOffset++] = (block4 >>> 34) & 4194303L;
        values[valuesOffset++] = (block4 >>> 12) & 4194303L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 4095L) << 10) | (block5 >>> 54);
        values[valuesOffset++] = (block5 >>> 32) & 4194303L;
        values[valuesOffset++] = (block5 >>> 10) & 4194303L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 1023L) << 12) | (block6 >>> 52);
        values[valuesOffset++] = (block6 >>> 30) & 4194303L;
        values[valuesOffset++] = (block6 >>> 8) & 4194303L;
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 255L) << 14) | (block7 >>> 50);
        values[valuesOffset++] = (block7 >>> 28) & 4194303L;
        values[valuesOffset++] = (block7 >>> 6) & 4194303L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 63L) << 16) | (block8 >>> 48);
        values[valuesOffset++] = (block8 >>> 26) & 4194303L;
        values[valuesOffset++] = (block8 >>> 4) & 4194303L;
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 15L) << 18) | (block9 >>> 46);
        values[valuesOffset++] = (block9 >>> 24) & 4194303L;
        values[valuesOffset++] = (block9 >>> 2) & 4194303L;
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 3L) << 20) | (block10 >>> 44);
        values[valuesOffset++] = (block10 >>> 22) & 4194303L;
        values[valuesOffset++] = block10 & 4194303L;
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 14) | (byte1 << 6) | (byte2 >>> 2);
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 3) << 20) | (byte3 << 12) | (byte4 << 4) | (byte5 >>> 4);
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 15) << 18) | (byte6 << 10) | (byte7 << 2) | (byte8 >>> 6);
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 63) << 16) | (byte9 << 8) | byte10;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte11 << 14) | (byte12 << 6) | (byte13 >>> 2);
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 3) << 20) | (byte14 << 12) | (byte15 << 4) | (byte16 >>> 4);
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 15) << 18) | (byte17 << 10) | (byte18 << 2) | (byte19 >>> 6);
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 63) << 16) | (byte20 << 8) | byte21;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte22 << 14) | (byte23 << 6) | (byte24 >>> 2);
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 3) << 20) | (byte25 << 12) | (byte26 << 4) | (byte27 >>> 4);
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte27 & 15) << 18) | (byte28 << 10) | (byte29 << 2) | (byte30 >>> 6);
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte30 & 63) << 16) | (byte31 << 8) | byte32;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte33 << 14) | (byte34 << 6) | (byte35 >>> 2);
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte35 & 3) << 20) | (byte36 << 12) | (byte37 << 4) | (byte38 >>> 4);
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 15) << 18) | (byte39 << 10) | (byte40 << 2) | (byte41 >>> 6);
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte41 & 63) << 16) | (byte42 << 8) | byte43;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte44 << 14) | (byte45 << 6) | (byte46 >>> 2);
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte46 & 3) << 20) | (byte47 << 12) | (byte48 << 4) | (byte49 >>> 4);
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte49 & 15) << 18) | (byte50 << 10) | (byte51 << 2) | (byte52 >>> 6);
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 63) << 16) | (byte53 << 8) | byte54;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte55 << 14) | (byte56 << 6) | (byte57 >>> 2);
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte57 & 3) << 20) | (byte58 << 12) | (byte59 << 4) | (byte60 >>> 4);
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte60 & 15) << 18) | (byte61 << 10) | (byte62 << 2) | (byte63 >>> 6);
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte63 & 63) << 16) | (byte64 << 8) | byte65;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte66 << 14) | (byte67 << 6) | (byte68 >>> 2);
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte68 & 3) << 20) | (byte69 << 12) | (byte70 << 4) | (byte71 >>> 4);
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte71 & 15) << 18) | (byte72 << 10) | (byte73 << 2) | (byte74 >>> 6);
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte74 & 63) << 16) | (byte75 << 8) | byte76;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte77 << 14) | (byte78 << 6) | (byte79 >>> 2);
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte79 & 3) << 20) | (byte80 << 12) | (byte81 << 4) | (byte82 >>> 4);
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte82 & 15) << 18) | (byte83 << 10) | (byte84 << 2) | (byte85 >>> 6);
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte85 & 63) << 16) | (byte86 << 8) | byte87;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 22) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 38) | (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 34) | (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset++] << 26) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset++] << 22) | values[valuesOffset++];
      }
    }

}
