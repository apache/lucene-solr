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
final class BulkOperationPacked30 extends BulkOperation {
    @Override
    public int blockCount() {
      return 15;
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
        values[valuesOffset++] = (int) (block0 >>> 34);
        values[valuesOffset++] = (int) ((block0 >>> 4) & 1073741823L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 15L) << 26) | (block1 >>> 38));
        values[valuesOffset++] = (int) ((block1 >>> 8) & 1073741823L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 255L) << 22) | (block2 >>> 42));
        values[valuesOffset++] = (int) ((block2 >>> 12) & 1073741823L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 4095L) << 18) | (block3 >>> 46));
        values[valuesOffset++] = (int) ((block3 >>> 16) & 1073741823L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 65535L) << 14) | (block4 >>> 50));
        values[valuesOffset++] = (int) ((block4 >>> 20) & 1073741823L);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block4 & 1048575L) << 10) | (block5 >>> 54));
        values[valuesOffset++] = (int) ((block5 >>> 24) & 1073741823L);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block5 & 16777215L) << 6) | (block6 >>> 58));
        values[valuesOffset++] = (int) ((block6 >>> 28) & 1073741823L);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block6 & 268435455L) << 2) | (block7 >>> 62));
        values[valuesOffset++] = (int) ((block7 >>> 32) & 1073741823L);
        values[valuesOffset++] = (int) ((block7 >>> 2) & 1073741823L);
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block7 & 3L) << 28) | (block8 >>> 36));
        values[valuesOffset++] = (int) ((block8 >>> 6) & 1073741823L);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block8 & 63L) << 24) | (block9 >>> 40));
        values[valuesOffset++] = (int) ((block9 >>> 10) & 1073741823L);
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block9 & 1023L) << 20) | (block10 >>> 44));
        values[valuesOffset++] = (int) ((block10 >>> 14) & 1073741823L);
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block10 & 16383L) << 16) | (block11 >>> 48));
        values[valuesOffset++] = (int) ((block11 >>> 18) & 1073741823L);
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block11 & 262143L) << 12) | (block12 >>> 52));
        values[valuesOffset++] = (int) ((block12 >>> 22) & 1073741823L);
        final long block13 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block12 & 4194303L) << 8) | (block13 >>> 56));
        values[valuesOffset++] = (int) ((block13 >>> 26) & 1073741823L);
        final long block14 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block13 & 67108863L) << 4) | (block14 >>> 60));
        values[valuesOffset++] = (int) ((block14 >>> 30) & 1073741823L);
        values[valuesOffset++] = (int) (block14 & 1073741823L);
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
        values[valuesOffset++] = (byte0 << 22) | (byte1 << 14) | (byte2 << 6) | (byte3 >>> 2);
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 3) << 28) | (byte4 << 20) | (byte5 << 12) | (byte6 << 4) | (byte7 >>> 4);
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 15) << 26) | (byte8 << 18) | (byte9 << 10) | (byte10 << 2) | (byte11 >>> 6);
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 63) << 24) | (byte12 << 16) | (byte13 << 8) | byte14;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte15 << 22) | (byte16 << 14) | (byte17 << 6) | (byte18 >>> 2);
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 3) << 28) | (byte19 << 20) | (byte20 << 12) | (byte21 << 4) | (byte22 >>> 4);
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 26) | (byte23 << 18) | (byte24 << 10) | (byte25 << 2) | (byte26 >>> 6);
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 63) << 24) | (byte27 << 16) | (byte28 << 8) | byte29;
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte30 << 22) | (byte31 << 14) | (byte32 << 6) | (byte33 >>> 2);
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 28) | (byte34 << 20) | (byte35 << 12) | (byte36 << 4) | (byte37 >>> 4);
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        final int byte40 = blocks[blocksOffset++] & 0xFF;
        final int byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 15) << 26) | (byte38 << 18) | (byte39 << 10) | (byte40 << 2) | (byte41 >>> 6);
        final int byte42 = blocks[blocksOffset++] & 0xFF;
        final int byte43 = blocks[blocksOffset++] & 0xFF;
        final int byte44 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte41 & 63) << 24) | (byte42 << 16) | (byte43 << 8) | byte44;
        final int byte45 = blocks[blocksOffset++] & 0xFF;
        final int byte46 = blocks[blocksOffset++] & 0xFF;
        final int byte47 = blocks[blocksOffset++] & 0xFF;
        final int byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte45 << 22) | (byte46 << 14) | (byte47 << 6) | (byte48 >>> 2);
        final int byte49 = blocks[blocksOffset++] & 0xFF;
        final int byte50 = blocks[blocksOffset++] & 0xFF;
        final int byte51 = blocks[blocksOffset++] & 0xFF;
        final int byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte48 & 3) << 28) | (byte49 << 20) | (byte50 << 12) | (byte51 << 4) | (byte52 >>> 4);
        final int byte53 = blocks[blocksOffset++] & 0xFF;
        final int byte54 = blocks[blocksOffset++] & 0xFF;
        final int byte55 = blocks[blocksOffset++] & 0xFF;
        final int byte56 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 15) << 26) | (byte53 << 18) | (byte54 << 10) | (byte55 << 2) | (byte56 >>> 6);
        final int byte57 = blocks[blocksOffset++] & 0xFF;
        final int byte58 = blocks[blocksOffset++] & 0xFF;
        final int byte59 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte56 & 63) << 24) | (byte57 << 16) | (byte58 << 8) | byte59;
        final int byte60 = blocks[blocksOffset++] & 0xFF;
        final int byte61 = blocks[blocksOffset++] & 0xFF;
        final int byte62 = blocks[blocksOffset++] & 0xFF;
        final int byte63 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte60 << 22) | (byte61 << 14) | (byte62 << 6) | (byte63 >>> 2);
        final int byte64 = blocks[blocksOffset++] & 0xFF;
        final int byte65 = blocks[blocksOffset++] & 0xFF;
        final int byte66 = blocks[blocksOffset++] & 0xFF;
        final int byte67 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte63 & 3) << 28) | (byte64 << 20) | (byte65 << 12) | (byte66 << 4) | (byte67 >>> 4);
        final int byte68 = blocks[blocksOffset++] & 0xFF;
        final int byte69 = blocks[blocksOffset++] & 0xFF;
        final int byte70 = blocks[blocksOffset++] & 0xFF;
        final int byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte67 & 15) << 26) | (byte68 << 18) | (byte69 << 10) | (byte70 << 2) | (byte71 >>> 6);
        final int byte72 = blocks[blocksOffset++] & 0xFF;
        final int byte73 = blocks[blocksOffset++] & 0xFF;
        final int byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte71 & 63) << 24) | (byte72 << 16) | (byte73 << 8) | byte74;
        final int byte75 = blocks[blocksOffset++] & 0xFF;
        final int byte76 = blocks[blocksOffset++] & 0xFF;
        final int byte77 = blocks[blocksOffset++] & 0xFF;
        final int byte78 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte75 << 22) | (byte76 << 14) | (byte77 << 6) | (byte78 >>> 2);
        final int byte79 = blocks[blocksOffset++] & 0xFF;
        final int byte80 = blocks[blocksOffset++] & 0xFF;
        final int byte81 = blocks[blocksOffset++] & 0xFF;
        final int byte82 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte78 & 3) << 28) | (byte79 << 20) | (byte80 << 12) | (byte81 << 4) | (byte82 >>> 4);
        final int byte83 = blocks[blocksOffset++] & 0xFF;
        final int byte84 = blocks[blocksOffset++] & 0xFF;
        final int byte85 = blocks[blocksOffset++] & 0xFF;
        final int byte86 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte82 & 15) << 26) | (byte83 << 18) | (byte84 << 10) | (byte85 << 2) | (byte86 >>> 6);
        final int byte87 = blocks[blocksOffset++] & 0xFF;
        final int byte88 = blocks[blocksOffset++] & 0xFF;
        final int byte89 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte86 & 63) << 24) | (byte87 << 16) | (byte88 << 8) | byte89;
        final int byte90 = blocks[blocksOffset++] & 0xFF;
        final int byte91 = blocks[blocksOffset++] & 0xFF;
        final int byte92 = blocks[blocksOffset++] & 0xFF;
        final int byte93 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte90 << 22) | (byte91 << 14) | (byte92 << 6) | (byte93 >>> 2);
        final int byte94 = blocks[blocksOffset++] & 0xFF;
        final int byte95 = blocks[blocksOffset++] & 0xFF;
        final int byte96 = blocks[blocksOffset++] & 0xFF;
        final int byte97 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte93 & 3) << 28) | (byte94 << 20) | (byte95 << 12) | (byte96 << 4) | (byte97 >>> 4);
        final int byte98 = blocks[blocksOffset++] & 0xFF;
        final int byte99 = blocks[blocksOffset++] & 0xFF;
        final int byte100 = blocks[blocksOffset++] & 0xFF;
        final int byte101 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte97 & 15) << 26) | (byte98 << 18) | (byte99 << 10) | (byte100 << 2) | (byte101 >>> 6);
        final int byte102 = blocks[blocksOffset++] & 0xFF;
        final int byte103 = blocks[blocksOffset++] & 0xFF;
        final int byte104 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte101 & 63) << 24) | (byte102 << 16) | (byte103 << 8) | byte104;
        final int byte105 = blocks[blocksOffset++] & 0xFF;
        final int byte106 = blocks[blocksOffset++] & 0xFF;
        final int byte107 = blocks[blocksOffset++] & 0xFF;
        final int byte108 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte105 << 22) | (byte106 << 14) | (byte107 << 6) | (byte108 >>> 2);
        final int byte109 = blocks[blocksOffset++] & 0xFF;
        final int byte110 = blocks[blocksOffset++] & 0xFF;
        final int byte111 = blocks[blocksOffset++] & 0xFF;
        final int byte112 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte108 & 3) << 28) | (byte109 << 20) | (byte110 << 12) | (byte111 << 4) | (byte112 >>> 4);
        final int byte113 = blocks[blocksOffset++] & 0xFF;
        final int byte114 = blocks[blocksOffset++] & 0xFF;
        final int byte115 = blocks[blocksOffset++] & 0xFF;
        final int byte116 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte112 & 15) << 26) | (byte113 << 18) | (byte114 << 10) | (byte115 << 2) | (byte116 >>> 6);
        final int byte117 = blocks[blocksOffset++] & 0xFF;
        final int byte118 = blocks[blocksOffset++] & 0xFF;
        final int byte119 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte116 & 63) << 24) | (byte117 << 16) | (byte118 << 8) | byte119;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 34;
        values[valuesOffset++] = (block0 >>> 4) & 1073741823L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 15L) << 26) | (block1 >>> 38);
        values[valuesOffset++] = (block1 >>> 8) & 1073741823L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 255L) << 22) | (block2 >>> 42);
        values[valuesOffset++] = (block2 >>> 12) & 1073741823L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 4095L) << 18) | (block3 >>> 46);
        values[valuesOffset++] = (block3 >>> 16) & 1073741823L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 65535L) << 14) | (block4 >>> 50);
        values[valuesOffset++] = (block4 >>> 20) & 1073741823L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 1048575L) << 10) | (block5 >>> 54);
        values[valuesOffset++] = (block5 >>> 24) & 1073741823L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 16777215L) << 6) | (block6 >>> 58);
        values[valuesOffset++] = (block6 >>> 28) & 1073741823L;
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 268435455L) << 2) | (block7 >>> 62);
        values[valuesOffset++] = (block7 >>> 32) & 1073741823L;
        values[valuesOffset++] = (block7 >>> 2) & 1073741823L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 3L) << 28) | (block8 >>> 36);
        values[valuesOffset++] = (block8 >>> 6) & 1073741823L;
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 63L) << 24) | (block9 >>> 40);
        values[valuesOffset++] = (block9 >>> 10) & 1073741823L;
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 1023L) << 20) | (block10 >>> 44);
        values[valuesOffset++] = (block10 >>> 14) & 1073741823L;
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 16383L) << 16) | (block11 >>> 48);
        values[valuesOffset++] = (block11 >>> 18) & 1073741823L;
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 262143L) << 12) | (block12 >>> 52);
        values[valuesOffset++] = (block12 >>> 22) & 1073741823L;
        final long block13 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block12 & 4194303L) << 8) | (block13 >>> 56);
        values[valuesOffset++] = (block13 >>> 26) & 1073741823L;
        final long block14 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block13 & 67108863L) << 4) | (block14 >>> 60);
        values[valuesOffset++] = (block14 >>> 30) & 1073741823L;
        values[valuesOffset++] = block14 & 1073741823L;
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
        values[valuesOffset++] = (byte0 << 22) | (byte1 << 14) | (byte2 << 6) | (byte3 >>> 2);
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 3) << 28) | (byte4 << 20) | (byte5 << 12) | (byte6 << 4) | (byte7 >>> 4);
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 15) << 26) | (byte8 << 18) | (byte9 << 10) | (byte10 << 2) | (byte11 >>> 6);
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 63) << 24) | (byte12 << 16) | (byte13 << 8) | byte14;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte15 << 22) | (byte16 << 14) | (byte17 << 6) | (byte18 >>> 2);
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 3) << 28) | (byte19 << 20) | (byte20 << 12) | (byte21 << 4) | (byte22 >>> 4);
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 26) | (byte23 << 18) | (byte24 << 10) | (byte25 << 2) | (byte26 >>> 6);
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 63) << 24) | (byte27 << 16) | (byte28 << 8) | byte29;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte30 << 22) | (byte31 << 14) | (byte32 << 6) | (byte33 >>> 2);
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 28) | (byte34 << 20) | (byte35 << 12) | (byte36 << 4) | (byte37 >>> 4);
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 15) << 26) | (byte38 << 18) | (byte39 << 10) | (byte40 << 2) | (byte41 >>> 6);
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte41 & 63) << 24) | (byte42 << 16) | (byte43 << 8) | byte44;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte45 << 22) | (byte46 << 14) | (byte47 << 6) | (byte48 >>> 2);
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte48 & 3) << 28) | (byte49 << 20) | (byte50 << 12) | (byte51 << 4) | (byte52 >>> 4);
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 15) << 26) | (byte53 << 18) | (byte54 << 10) | (byte55 << 2) | (byte56 >>> 6);
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte56 & 63) << 24) | (byte57 << 16) | (byte58 << 8) | byte59;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte60 << 22) | (byte61 << 14) | (byte62 << 6) | (byte63 >>> 2);
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte63 & 3) << 28) | (byte64 << 20) | (byte65 << 12) | (byte66 << 4) | (byte67 >>> 4);
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte67 & 15) << 26) | (byte68 << 18) | (byte69 << 10) | (byte70 << 2) | (byte71 >>> 6);
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte71 & 63) << 24) | (byte72 << 16) | (byte73 << 8) | byte74;
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte75 << 22) | (byte76 << 14) | (byte77 << 6) | (byte78 >>> 2);
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte78 & 3) << 28) | (byte79 << 20) | (byte80 << 12) | (byte81 << 4) | (byte82 >>> 4);
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte82 & 15) << 26) | (byte83 << 18) | (byte84 << 10) | (byte85 << 2) | (byte86 >>> 6);
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte86 & 63) << 24) | (byte87 << 16) | (byte88 << 8) | byte89;
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte90 << 22) | (byte91 << 14) | (byte92 << 6) | (byte93 >>> 2);
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte93 & 3) << 28) | (byte94 << 20) | (byte95 << 12) | (byte96 << 4) | (byte97 >>> 4);
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte97 & 15) << 26) | (byte98 << 18) | (byte99 << 10) | (byte100 << 2) | (byte101 >>> 6);
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        final long byte104 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte101 & 63) << 24) | (byte102 << 16) | (byte103 << 8) | byte104;
        final long byte105 = blocks[blocksOffset++] & 0xFF;
        final long byte106 = blocks[blocksOffset++] & 0xFF;
        final long byte107 = blocks[blocksOffset++] & 0xFF;
        final long byte108 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte105 << 22) | (byte106 << 14) | (byte107 << 6) | (byte108 >>> 2);
        final long byte109 = blocks[blocksOffset++] & 0xFF;
        final long byte110 = blocks[blocksOffset++] & 0xFF;
        final long byte111 = blocks[blocksOffset++] & 0xFF;
        final long byte112 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte108 & 3) << 28) | (byte109 << 20) | (byte110 << 12) | (byte111 << 4) | (byte112 >>> 4);
        final long byte113 = blocks[blocksOffset++] & 0xFF;
        final long byte114 = blocks[blocksOffset++] & 0xFF;
        final long byte115 = blocks[blocksOffset++] & 0xFF;
        final long byte116 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte112 & 15) << 26) | (byte113 << 18) | (byte114 << 10) | (byte115 << 2) | (byte116 >>> 6);
        final long byte117 = blocks[blocksOffset++] & 0xFF;
        final long byte118 = blocks[blocksOffset++] & 0xFF;
        final long byte119 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte116 & 63) << 24) | (byte117 << 16) | (byte118 << 8) | byte119;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 26);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 22);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 28);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 30) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 34) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 26);
        blocks[blocksOffset++] = (values[valuesOffset++] << 38) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 22);
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 28) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 28);
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 22) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 26) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 30) | values[valuesOffset++];
      }
    }

}
