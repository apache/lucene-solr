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
final class BulkOperationPacked38 extends BulkOperation {
    public int blockCount() {
      return 19;
    }

    public int valueCount() {
      return 32;
    }

    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      throw new UnsupportedOperationException();
    }

    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 26;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 67108863L) << 12) | (block1 >>> 52);
        values[valuesOffset++] = (block1 >>> 14) & 274877906943L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 16383L) << 24) | (block2 >>> 40);
        values[valuesOffset++] = (block2 >>> 2) & 274877906943L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 3L) << 36) | (block3 >>> 28);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 268435455L) << 10) | (block4 >>> 54);
        values[valuesOffset++] = (block4 >>> 16) & 274877906943L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 65535L) << 22) | (block5 >>> 42);
        values[valuesOffset++] = (block5 >>> 4) & 274877906943L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 15L) << 34) | (block6 >>> 30);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 1073741823L) << 8) | (block7 >>> 56);
        values[valuesOffset++] = (block7 >>> 18) & 274877906943L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 262143L) << 20) | (block8 >>> 44);
        values[valuesOffset++] = (block8 >>> 6) & 274877906943L;
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 63L) << 32) | (block9 >>> 32);
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 4294967295L) << 6) | (block10 >>> 58);
        values[valuesOffset++] = (block10 >>> 20) & 274877906943L;
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 1048575L) << 18) | (block11 >>> 46);
        values[valuesOffset++] = (block11 >>> 8) & 274877906943L;
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 255L) << 30) | (block12 >>> 34);
        final long block13 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block12 & 17179869183L) << 4) | (block13 >>> 60);
        values[valuesOffset++] = (block13 >>> 22) & 274877906943L;
        final long block14 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block13 & 4194303L) << 16) | (block14 >>> 48);
        values[valuesOffset++] = (block14 >>> 10) & 274877906943L;
        final long block15 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block14 & 1023L) << 28) | (block15 >>> 36);
        final long block16 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block15 & 68719476735L) << 2) | (block16 >>> 62);
        values[valuesOffset++] = (block16 >>> 24) & 274877906943L;
        final long block17 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block16 & 16777215L) << 14) | (block17 >>> 50);
        values[valuesOffset++] = (block17 >>> 12) & 274877906943L;
        final long block18 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block17 & 4095L) << 26) | (block18 >>> 38);
        values[valuesOffset++] = block18 & 274877906943L;
      }
    }

    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 30) | (byte1 << 22) | (byte2 << 14) | (byte3 << 6) | (byte4 >>> 2);
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 3) << 36) | (byte5 << 28) | (byte6 << 20) | (byte7 << 12) | (byte8 << 4) | (byte9 >>> 4);
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte9 & 15) << 34) | (byte10 << 26) | (byte11 << 18) | (byte12 << 10) | (byte13 << 2) | (byte14 >>> 6);
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte14 & 63) << 32) | (byte15 << 24) | (byte16 << 16) | (byte17 << 8) | byte18;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte19 << 30) | (byte20 << 22) | (byte21 << 14) | (byte22 << 6) | (byte23 >>> 2);
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 3) << 36) | (byte24 << 28) | (byte25 << 20) | (byte26 << 12) | (byte27 << 4) | (byte28 >>> 4);
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 15) << 34) | (byte29 << 26) | (byte30 << 18) | (byte31 << 10) | (byte32 << 2) | (byte33 >>> 6);
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 63) << 32) | (byte34 << 24) | (byte35 << 16) | (byte36 << 8) | byte37;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte38 << 30) | (byte39 << 22) | (byte40 << 14) | (byte41 << 6) | (byte42 >>> 2);
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 3) << 36) | (byte43 << 28) | (byte44 << 20) | (byte45 << 12) | (byte46 << 4) | (byte47 >>> 4);
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 15) << 34) | (byte48 << 26) | (byte49 << 18) | (byte50 << 10) | (byte51 << 2) | (byte52 >>> 6);
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 63) << 32) | (byte53 << 24) | (byte54 << 16) | (byte55 << 8) | byte56;
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte57 << 30) | (byte58 << 22) | (byte59 << 14) | (byte60 << 6) | (byte61 >>> 2);
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte61 & 3) << 36) | (byte62 << 28) | (byte63 << 20) | (byte64 << 12) | (byte65 << 4) | (byte66 >>> 4);
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte66 & 15) << 34) | (byte67 << 26) | (byte68 << 18) | (byte69 << 10) | (byte70 << 2) | (byte71 >>> 6);
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte71 & 63) << 32) | (byte72 << 24) | (byte73 << 16) | (byte74 << 8) | byte75;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte76 << 30) | (byte77 << 22) | (byte78 << 14) | (byte79 << 6) | (byte80 >>> 2);
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte80 & 3) << 36) | (byte81 << 28) | (byte82 << 20) | (byte83 << 12) | (byte84 << 4) | (byte85 >>> 4);
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte85 & 15) << 34) | (byte86 << 26) | (byte87 << 18) | (byte88 << 10) | (byte89 << 2) | (byte90 >>> 6);
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte90 & 63) << 32) | (byte91 << 24) | (byte92 << 16) | (byte93 << 8) | byte94;
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte95 << 30) | (byte96 << 22) | (byte97 << 14) | (byte98 << 6) | (byte99 >>> 2);
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        final long byte104 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte99 & 3) << 36) | (byte100 << 28) | (byte101 << 20) | (byte102 << 12) | (byte103 << 4) | (byte104 >>> 4);
        final long byte105 = blocks[blocksOffset++] & 0xFF;
        final long byte106 = blocks[blocksOffset++] & 0xFF;
        final long byte107 = blocks[blocksOffset++] & 0xFF;
        final long byte108 = blocks[blocksOffset++] & 0xFF;
        final long byte109 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte104 & 15) << 34) | (byte105 << 26) | (byte106 << 18) | (byte107 << 10) | (byte108 << 2) | (byte109 >>> 6);
        final long byte110 = blocks[blocksOffset++] & 0xFF;
        final long byte111 = blocks[blocksOffset++] & 0xFF;
        final long byte112 = blocks[blocksOffset++] & 0xFF;
        final long byte113 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte109 & 63) << 32) | (byte110 << 24) | (byte111 << 16) | (byte112 << 8) | byte113;
        final long byte114 = blocks[blocksOffset++] & 0xFF;
        final long byte115 = blocks[blocksOffset++] & 0xFF;
        final long byte116 = blocks[blocksOffset++] & 0xFF;
        final long byte117 = blocks[blocksOffset++] & 0xFF;
        final long byte118 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte114 << 30) | (byte115 << 22) | (byte116 << 14) | (byte117 << 6) | (byte118 >>> 2);
        final long byte119 = blocks[blocksOffset++] & 0xFF;
        final long byte120 = blocks[blocksOffset++] & 0xFF;
        final long byte121 = blocks[blocksOffset++] & 0xFF;
        final long byte122 = blocks[blocksOffset++] & 0xFF;
        final long byte123 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte118 & 3) << 36) | (byte119 << 28) | (byte120 << 20) | (byte121 << 12) | (byte122 << 4) | (byte123 >>> 4);
        final long byte124 = blocks[blocksOffset++] & 0xFF;
        final long byte125 = blocks[blocksOffset++] & 0xFF;
        final long byte126 = blocks[blocksOffset++] & 0xFF;
        final long byte127 = blocks[blocksOffset++] & 0xFF;
        final long byte128 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte123 & 15) << 34) | (byte124 << 26) | (byte125 << 18) | (byte126 << 10) | (byte127 << 2) | (byte128 >>> 6);
        final long byte129 = blocks[blocksOffset++] & 0xFF;
        final long byte130 = blocks[blocksOffset++] & 0xFF;
        final long byte131 = blocks[blocksOffset++] & 0xFF;
        final long byte132 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte128 & 63) << 32) | (byte129 << 24) | (byte130 << 16) | (byte131 << 8) | byte132;
        final long byte133 = blocks[blocksOffset++] & 0xFF;
        final long byte134 = blocks[blocksOffset++] & 0xFF;
        final long byte135 = blocks[blocksOffset++] & 0xFF;
        final long byte136 = blocks[blocksOffset++] & 0xFF;
        final long byte137 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte133 << 30) | (byte134 << 22) | (byte135 << 14) | (byte136 << 6) | (byte137 >>> 2);
        final long byte138 = blocks[blocksOffset++] & 0xFF;
        final long byte139 = blocks[blocksOffset++] & 0xFF;
        final long byte140 = blocks[blocksOffset++] & 0xFF;
        final long byte141 = blocks[blocksOffset++] & 0xFF;
        final long byte142 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte137 & 3) << 36) | (byte138 << 28) | (byte139 << 20) | (byte140 << 12) | (byte141 << 4) | (byte142 >>> 4);
        final long byte143 = blocks[blocksOffset++] & 0xFF;
        final long byte144 = blocks[blocksOffset++] & 0xFF;
        final long byte145 = blocks[blocksOffset++] & 0xFF;
        final long byte146 = blocks[blocksOffset++] & 0xFF;
        final long byte147 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte142 & 15) << 34) | (byte143 << 26) | (byte144 << 18) | (byte145 << 10) | (byte146 << 2) | (byte147 >>> 6);
        final long byte148 = blocks[blocksOffset++] & 0xFF;
        final long byte149 = blocks[blocksOffset++] & 0xFF;
        final long byte150 = blocks[blocksOffset++] & 0xFF;
        final long byte151 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte147 & 63) << 32) | (byte148 << 24) | (byte149 << 16) | (byte150 << 8) | byte151;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 36);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 22);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 34);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 32);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 30);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 28);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 26);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 38) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 26) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 36);
        blocks[blocksOffset++] = (values[valuesOffset++] << 28) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 22);
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 34);
        blocks[blocksOffset++] = (values[valuesOffset++] << 30) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 32);
        blocks[blocksOffset++] = (values[valuesOffset++] << 32) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 30);
        blocks[blocksOffset++] = (values[valuesOffset++] << 34) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 22) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 28);
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 26);
        blocks[blocksOffset++] = (values[valuesOffset++] << 38) | values[valuesOffset++];
      }
    }

}
