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
final class BulkOperationPacked26 extends BulkOperation {
    @Override
    public int blockCount() {
      return 13;
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
        values[valuesOffset++] = (int) (block0 >>> 38);
        values[valuesOffset++] = (int) ((block0 >>> 12) & 67108863L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 4095L) << 14) | (block1 >>> 50));
        values[valuesOffset++] = (int) ((block1 >>> 24) & 67108863L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 16777215L) << 2) | (block2 >>> 62));
        values[valuesOffset++] = (int) ((block2 >>> 36) & 67108863L);
        values[valuesOffset++] = (int) ((block2 >>> 10) & 67108863L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 1023L) << 16) | (block3 >>> 48));
        values[valuesOffset++] = (int) ((block3 >>> 22) & 67108863L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 4194303L) << 4) | (block4 >>> 60));
        values[valuesOffset++] = (int) ((block4 >>> 34) & 67108863L);
        values[valuesOffset++] = (int) ((block4 >>> 8) & 67108863L);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block4 & 255L) << 18) | (block5 >>> 46));
        values[valuesOffset++] = (int) ((block5 >>> 20) & 67108863L);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block5 & 1048575L) << 6) | (block6 >>> 58));
        values[valuesOffset++] = (int) ((block6 >>> 32) & 67108863L);
        values[valuesOffset++] = (int) ((block6 >>> 6) & 67108863L);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block6 & 63L) << 20) | (block7 >>> 44));
        values[valuesOffset++] = (int) ((block7 >>> 18) & 67108863L);
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block7 & 262143L) << 8) | (block8 >>> 56));
        values[valuesOffset++] = (int) ((block8 >>> 30) & 67108863L);
        values[valuesOffset++] = (int) ((block8 >>> 4) & 67108863L);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block8 & 15L) << 22) | (block9 >>> 42));
        values[valuesOffset++] = (int) ((block9 >>> 16) & 67108863L);
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block9 & 65535L) << 10) | (block10 >>> 54));
        values[valuesOffset++] = (int) ((block10 >>> 28) & 67108863L);
        values[valuesOffset++] = (int) ((block10 >>> 2) & 67108863L);
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block10 & 3L) << 24) | (block11 >>> 40));
        values[valuesOffset++] = (int) ((block11 >>> 14) & 67108863L);
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block11 & 16383L) << 12) | (block12 >>> 52));
        values[valuesOffset++] = (int) ((block12 >>> 26) & 67108863L);
        values[valuesOffset++] = (int) (block12 & 67108863L);
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
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 18) | (byte1 << 10) | (byte2 << 2) | (byte3 >>> 6);
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 63) << 20) | (byte4 << 12) | (byte5 << 4) | (byte6 >>> 4);
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 15) << 22) | (byte7 << 14) | (byte8 << 6) | (byte9 >>> 2);
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte9 & 3) << 24) | (byte10 << 16) | (byte11 << 8) | byte12;
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte13 << 18) | (byte14 << 10) | (byte15 << 2) | (byte16 >>> 6);
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 63) << 20) | (byte17 << 12) | (byte18 << 4) | (byte19 >>> 4);
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 15) << 22) | (byte20 << 14) | (byte21 << 6) | (byte22 >>> 2);
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 3) << 24) | (byte23 << 16) | (byte24 << 8) | byte25;
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte26 << 18) | (byte27 << 10) | (byte28 << 2) | (byte29 >>> 6);
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 63) << 20) | (byte30 << 12) | (byte31 << 4) | (byte32 >>> 4);
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 15) << 22) | (byte33 << 14) | (byte34 << 6) | (byte35 >>> 2);
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte35 & 3) << 24) | (byte36 << 16) | (byte37 << 8) | byte38;
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        final int byte40 = blocks[blocksOffset++] & 0xFF;
        final int byte41 = blocks[blocksOffset++] & 0xFF;
        final int byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte39 << 18) | (byte40 << 10) | (byte41 << 2) | (byte42 >>> 6);
        final int byte43 = blocks[blocksOffset++] & 0xFF;
        final int byte44 = blocks[blocksOffset++] & 0xFF;
        final int byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 63) << 20) | (byte43 << 12) | (byte44 << 4) | (byte45 >>> 4);
        final int byte46 = blocks[blocksOffset++] & 0xFF;
        final int byte47 = blocks[blocksOffset++] & 0xFF;
        final int byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 15) << 22) | (byte46 << 14) | (byte47 << 6) | (byte48 >>> 2);
        final int byte49 = blocks[blocksOffset++] & 0xFF;
        final int byte50 = blocks[blocksOffset++] & 0xFF;
        final int byte51 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte48 & 3) << 24) | (byte49 << 16) | (byte50 << 8) | byte51;
        final int byte52 = blocks[blocksOffset++] & 0xFF;
        final int byte53 = blocks[blocksOffset++] & 0xFF;
        final int byte54 = blocks[blocksOffset++] & 0xFF;
        final int byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte52 << 18) | (byte53 << 10) | (byte54 << 2) | (byte55 >>> 6);
        final int byte56 = blocks[blocksOffset++] & 0xFF;
        final int byte57 = blocks[blocksOffset++] & 0xFF;
        final int byte58 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte55 & 63) << 20) | (byte56 << 12) | (byte57 << 4) | (byte58 >>> 4);
        final int byte59 = blocks[blocksOffset++] & 0xFF;
        final int byte60 = blocks[blocksOffset++] & 0xFF;
        final int byte61 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte58 & 15) << 22) | (byte59 << 14) | (byte60 << 6) | (byte61 >>> 2);
        final int byte62 = blocks[blocksOffset++] & 0xFF;
        final int byte63 = blocks[blocksOffset++] & 0xFF;
        final int byte64 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte61 & 3) << 24) | (byte62 << 16) | (byte63 << 8) | byte64;
        final int byte65 = blocks[blocksOffset++] & 0xFF;
        final int byte66 = blocks[blocksOffset++] & 0xFF;
        final int byte67 = blocks[blocksOffset++] & 0xFF;
        final int byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte65 << 18) | (byte66 << 10) | (byte67 << 2) | (byte68 >>> 6);
        final int byte69 = blocks[blocksOffset++] & 0xFF;
        final int byte70 = blocks[blocksOffset++] & 0xFF;
        final int byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte68 & 63) << 20) | (byte69 << 12) | (byte70 << 4) | (byte71 >>> 4);
        final int byte72 = blocks[blocksOffset++] & 0xFF;
        final int byte73 = blocks[blocksOffset++] & 0xFF;
        final int byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte71 & 15) << 22) | (byte72 << 14) | (byte73 << 6) | (byte74 >>> 2);
        final int byte75 = blocks[blocksOffset++] & 0xFF;
        final int byte76 = blocks[blocksOffset++] & 0xFF;
        final int byte77 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte74 & 3) << 24) | (byte75 << 16) | (byte76 << 8) | byte77;
        final int byte78 = blocks[blocksOffset++] & 0xFF;
        final int byte79 = blocks[blocksOffset++] & 0xFF;
        final int byte80 = blocks[blocksOffset++] & 0xFF;
        final int byte81 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte78 << 18) | (byte79 << 10) | (byte80 << 2) | (byte81 >>> 6);
        final int byte82 = blocks[blocksOffset++] & 0xFF;
        final int byte83 = blocks[blocksOffset++] & 0xFF;
        final int byte84 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte81 & 63) << 20) | (byte82 << 12) | (byte83 << 4) | (byte84 >>> 4);
        final int byte85 = blocks[blocksOffset++] & 0xFF;
        final int byte86 = blocks[blocksOffset++] & 0xFF;
        final int byte87 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte84 & 15) << 22) | (byte85 << 14) | (byte86 << 6) | (byte87 >>> 2);
        final int byte88 = blocks[blocksOffset++] & 0xFF;
        final int byte89 = blocks[blocksOffset++] & 0xFF;
        final int byte90 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte87 & 3) << 24) | (byte88 << 16) | (byte89 << 8) | byte90;
        final int byte91 = blocks[blocksOffset++] & 0xFF;
        final int byte92 = blocks[blocksOffset++] & 0xFF;
        final int byte93 = blocks[blocksOffset++] & 0xFF;
        final int byte94 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte91 << 18) | (byte92 << 10) | (byte93 << 2) | (byte94 >>> 6);
        final int byte95 = blocks[blocksOffset++] & 0xFF;
        final int byte96 = blocks[blocksOffset++] & 0xFF;
        final int byte97 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte94 & 63) << 20) | (byte95 << 12) | (byte96 << 4) | (byte97 >>> 4);
        final int byte98 = blocks[blocksOffset++] & 0xFF;
        final int byte99 = blocks[blocksOffset++] & 0xFF;
        final int byte100 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte97 & 15) << 22) | (byte98 << 14) | (byte99 << 6) | (byte100 >>> 2);
        final int byte101 = blocks[blocksOffset++] & 0xFF;
        final int byte102 = blocks[blocksOffset++] & 0xFF;
        final int byte103 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte100 & 3) << 24) | (byte101 << 16) | (byte102 << 8) | byte103;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 38;
        values[valuesOffset++] = (block0 >>> 12) & 67108863L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 4095L) << 14) | (block1 >>> 50);
        values[valuesOffset++] = (block1 >>> 24) & 67108863L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 16777215L) << 2) | (block2 >>> 62);
        values[valuesOffset++] = (block2 >>> 36) & 67108863L;
        values[valuesOffset++] = (block2 >>> 10) & 67108863L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 1023L) << 16) | (block3 >>> 48);
        values[valuesOffset++] = (block3 >>> 22) & 67108863L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 4194303L) << 4) | (block4 >>> 60);
        values[valuesOffset++] = (block4 >>> 34) & 67108863L;
        values[valuesOffset++] = (block4 >>> 8) & 67108863L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 255L) << 18) | (block5 >>> 46);
        values[valuesOffset++] = (block5 >>> 20) & 67108863L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 1048575L) << 6) | (block6 >>> 58);
        values[valuesOffset++] = (block6 >>> 32) & 67108863L;
        values[valuesOffset++] = (block6 >>> 6) & 67108863L;
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 63L) << 20) | (block7 >>> 44);
        values[valuesOffset++] = (block7 >>> 18) & 67108863L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 262143L) << 8) | (block8 >>> 56);
        values[valuesOffset++] = (block8 >>> 30) & 67108863L;
        values[valuesOffset++] = (block8 >>> 4) & 67108863L;
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 15L) << 22) | (block9 >>> 42);
        values[valuesOffset++] = (block9 >>> 16) & 67108863L;
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 65535L) << 10) | (block10 >>> 54);
        values[valuesOffset++] = (block10 >>> 28) & 67108863L;
        values[valuesOffset++] = (block10 >>> 2) & 67108863L;
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 3L) << 24) | (block11 >>> 40);
        values[valuesOffset++] = (block11 >>> 14) & 67108863L;
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 16383L) << 12) | (block12 >>> 52);
        values[valuesOffset++] = (block12 >>> 26) & 67108863L;
        values[valuesOffset++] = block12 & 67108863L;
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
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 18) | (byte1 << 10) | (byte2 << 2) | (byte3 >>> 6);
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 63) << 20) | (byte4 << 12) | (byte5 << 4) | (byte6 >>> 4);
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 15) << 22) | (byte7 << 14) | (byte8 << 6) | (byte9 >>> 2);
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte9 & 3) << 24) | (byte10 << 16) | (byte11 << 8) | byte12;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte13 << 18) | (byte14 << 10) | (byte15 << 2) | (byte16 >>> 6);
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 63) << 20) | (byte17 << 12) | (byte18 << 4) | (byte19 >>> 4);
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 15) << 22) | (byte20 << 14) | (byte21 << 6) | (byte22 >>> 2);
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 3) << 24) | (byte23 << 16) | (byte24 << 8) | byte25;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte26 << 18) | (byte27 << 10) | (byte28 << 2) | (byte29 >>> 6);
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 63) << 20) | (byte30 << 12) | (byte31 << 4) | (byte32 >>> 4);
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 15) << 22) | (byte33 << 14) | (byte34 << 6) | (byte35 >>> 2);
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte35 & 3) << 24) | (byte36 << 16) | (byte37 << 8) | byte38;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte39 << 18) | (byte40 << 10) | (byte41 << 2) | (byte42 >>> 6);
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 63) << 20) | (byte43 << 12) | (byte44 << 4) | (byte45 >>> 4);
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 15) << 22) | (byte46 << 14) | (byte47 << 6) | (byte48 >>> 2);
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte48 & 3) << 24) | (byte49 << 16) | (byte50 << 8) | byte51;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte52 << 18) | (byte53 << 10) | (byte54 << 2) | (byte55 >>> 6);
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte55 & 63) << 20) | (byte56 << 12) | (byte57 << 4) | (byte58 >>> 4);
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte58 & 15) << 22) | (byte59 << 14) | (byte60 << 6) | (byte61 >>> 2);
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte61 & 3) << 24) | (byte62 << 16) | (byte63 << 8) | byte64;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte65 << 18) | (byte66 << 10) | (byte67 << 2) | (byte68 >>> 6);
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte68 & 63) << 20) | (byte69 << 12) | (byte70 << 4) | (byte71 >>> 4);
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte71 & 15) << 22) | (byte72 << 14) | (byte73 << 6) | (byte74 >>> 2);
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte74 & 3) << 24) | (byte75 << 16) | (byte76 << 8) | byte77;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte78 << 18) | (byte79 << 10) | (byte80 << 2) | (byte81 >>> 6);
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte81 & 63) << 20) | (byte82 << 12) | (byte83 << 4) | (byte84 >>> 4);
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte84 & 15) << 22) | (byte85 << 14) | (byte86 << 6) | (byte87 >>> 2);
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte87 & 3) << 24) | (byte88 << 16) | (byte89 << 8) | byte90;
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte91 << 18) | (byte92 << 10) | (byte93 << 2) | (byte94 >>> 6);
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte94 & 63) << 20) | (byte95 << 12) | (byte96 << 4) | (byte97 >>> 4);
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte97 & 15) << 22) | (byte98 << 14) | (byte99 << 6) | (byte100 >>> 2);
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte100 & 3) << 24) | (byte101 << 16) | (byte102 << 8) | byte103;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 22);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 26) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 38) | (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset++] << 22) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 34) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 22);
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 26) | values[valuesOffset++];
      }
    }

}
