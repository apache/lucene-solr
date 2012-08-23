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
final class BulkOperationPacked46 extends BulkOperation {
    @Override
    public int blockCount() {
      return 23;
    }

    @Override
    public int valueCount() {
      return 32;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 18;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 262143L) << 28) | (block1 >>> 36);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 68719476735L) << 10) | (block2 >>> 54);
        values[valuesOffset++] = (block2 >>> 8) & 70368744177663L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 255L) << 38) | (block3 >>> 26);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 67108863L) << 20) | (block4 >>> 44);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 17592186044415L) << 2) | (block5 >>> 62);
        values[valuesOffset++] = (block5 >>> 16) & 70368744177663L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 65535L) << 30) | (block6 >>> 34);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 17179869183L) << 12) | (block7 >>> 52);
        values[valuesOffset++] = (block7 >>> 6) & 70368744177663L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 63L) << 40) | (block8 >>> 24);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 16777215L) << 22) | (block9 >>> 42);
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 4398046511103L) << 4) | (block10 >>> 60);
        values[valuesOffset++] = (block10 >>> 14) & 70368744177663L;
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 16383L) << 32) | (block11 >>> 32);
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 4294967295L) << 14) | (block12 >>> 50);
        values[valuesOffset++] = (block12 >>> 4) & 70368744177663L;
        final long block13 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block12 & 15L) << 42) | (block13 >>> 22);
        final long block14 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block13 & 4194303L) << 24) | (block14 >>> 40);
        final long block15 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block14 & 1099511627775L) << 6) | (block15 >>> 58);
        values[valuesOffset++] = (block15 >>> 12) & 70368744177663L;
        final long block16 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block15 & 4095L) << 34) | (block16 >>> 30);
        final long block17 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block16 & 1073741823L) << 16) | (block17 >>> 48);
        values[valuesOffset++] = (block17 >>> 2) & 70368744177663L;
        final long block18 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block17 & 3L) << 44) | (block18 >>> 20);
        final long block19 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block18 & 1048575L) << 26) | (block19 >>> 38);
        final long block20 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block19 & 274877906943L) << 8) | (block20 >>> 56);
        values[valuesOffset++] = (block20 >>> 10) & 70368744177663L;
        final long block21 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block20 & 1023L) << 36) | (block21 >>> 28);
        final long block22 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block21 & 268435455L) << 18) | (block22 >>> 46);
        values[valuesOffset++] = block22 & 70368744177663L;
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
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 38) | (byte1 << 30) | (byte2 << 22) | (byte3 << 14) | (byte4 << 6) | (byte5 >>> 2);
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 3) << 44) | (byte6 << 36) | (byte7 << 28) | (byte8 << 20) | (byte9 << 12) | (byte10 << 4) | (byte11 >>> 4);
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 15) << 42) | (byte12 << 34) | (byte13 << 26) | (byte14 << 18) | (byte15 << 10) | (byte16 << 2) | (byte17 >>> 6);
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 63) << 40) | (byte18 << 32) | (byte19 << 24) | (byte20 << 16) | (byte21 << 8) | byte22;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte23 << 38) | (byte24 << 30) | (byte25 << 22) | (byte26 << 14) | (byte27 << 6) | (byte28 >>> 2);
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte28 & 3) << 44) | (byte29 << 36) | (byte30 << 28) | (byte31 << 20) | (byte32 << 12) | (byte33 << 4) | (byte34 >>> 4);
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte34 & 15) << 42) | (byte35 << 34) | (byte36 << 26) | (byte37 << 18) | (byte38 << 10) | (byte39 << 2) | (byte40 >>> 6);
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte40 & 63) << 40) | (byte41 << 32) | (byte42 << 24) | (byte43 << 16) | (byte44 << 8) | byte45;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte46 << 38) | (byte47 << 30) | (byte48 << 22) | (byte49 << 14) | (byte50 << 6) | (byte51 >>> 2);
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte51 & 3) << 44) | (byte52 << 36) | (byte53 << 28) | (byte54 << 20) | (byte55 << 12) | (byte56 << 4) | (byte57 >>> 4);
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte57 & 15) << 42) | (byte58 << 34) | (byte59 << 26) | (byte60 << 18) | (byte61 << 10) | (byte62 << 2) | (byte63 >>> 6);
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte63 & 63) << 40) | (byte64 << 32) | (byte65 << 24) | (byte66 << 16) | (byte67 << 8) | byte68;
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte69 << 38) | (byte70 << 30) | (byte71 << 22) | (byte72 << 14) | (byte73 << 6) | (byte74 >>> 2);
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte74 & 3) << 44) | (byte75 << 36) | (byte76 << 28) | (byte77 << 20) | (byte78 << 12) | (byte79 << 4) | (byte80 >>> 4);
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte80 & 15) << 42) | (byte81 << 34) | (byte82 << 26) | (byte83 << 18) | (byte84 << 10) | (byte85 << 2) | (byte86 >>> 6);
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte86 & 63) << 40) | (byte87 << 32) | (byte88 << 24) | (byte89 << 16) | (byte90 << 8) | byte91;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte92 << 38) | (byte93 << 30) | (byte94 << 22) | (byte95 << 14) | (byte96 << 6) | (byte97 >>> 2);
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte97 & 3) << 44) | (byte98 << 36) | (byte99 << 28) | (byte100 << 20) | (byte101 << 12) | (byte102 << 4) | (byte103 >>> 4);
        final long byte104 = blocks[blocksOffset++] & 0xFF;
        final long byte105 = blocks[blocksOffset++] & 0xFF;
        final long byte106 = blocks[blocksOffset++] & 0xFF;
        final long byte107 = blocks[blocksOffset++] & 0xFF;
        final long byte108 = blocks[blocksOffset++] & 0xFF;
        final long byte109 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte103 & 15) << 42) | (byte104 << 34) | (byte105 << 26) | (byte106 << 18) | (byte107 << 10) | (byte108 << 2) | (byte109 >>> 6);
        final long byte110 = blocks[blocksOffset++] & 0xFF;
        final long byte111 = blocks[blocksOffset++] & 0xFF;
        final long byte112 = blocks[blocksOffset++] & 0xFF;
        final long byte113 = blocks[blocksOffset++] & 0xFF;
        final long byte114 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte109 & 63) << 40) | (byte110 << 32) | (byte111 << 24) | (byte112 << 16) | (byte113 << 8) | byte114;
        final long byte115 = blocks[blocksOffset++] & 0xFF;
        final long byte116 = blocks[blocksOffset++] & 0xFF;
        final long byte117 = blocks[blocksOffset++] & 0xFF;
        final long byte118 = blocks[blocksOffset++] & 0xFF;
        final long byte119 = blocks[blocksOffset++] & 0xFF;
        final long byte120 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte115 << 38) | (byte116 << 30) | (byte117 << 22) | (byte118 << 14) | (byte119 << 6) | (byte120 >>> 2);
        final long byte121 = blocks[blocksOffset++] & 0xFF;
        final long byte122 = blocks[blocksOffset++] & 0xFF;
        final long byte123 = blocks[blocksOffset++] & 0xFF;
        final long byte124 = blocks[blocksOffset++] & 0xFF;
        final long byte125 = blocks[blocksOffset++] & 0xFF;
        final long byte126 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte120 & 3) << 44) | (byte121 << 36) | (byte122 << 28) | (byte123 << 20) | (byte124 << 12) | (byte125 << 4) | (byte126 >>> 4);
        final long byte127 = blocks[blocksOffset++] & 0xFF;
        final long byte128 = blocks[blocksOffset++] & 0xFF;
        final long byte129 = blocks[blocksOffset++] & 0xFF;
        final long byte130 = blocks[blocksOffset++] & 0xFF;
        final long byte131 = blocks[blocksOffset++] & 0xFF;
        final long byte132 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte126 & 15) << 42) | (byte127 << 34) | (byte128 << 26) | (byte129 << 18) | (byte130 << 10) | (byte131 << 2) | (byte132 >>> 6);
        final long byte133 = blocks[blocksOffset++] & 0xFF;
        final long byte134 = blocks[blocksOffset++] & 0xFF;
        final long byte135 = blocks[blocksOffset++] & 0xFF;
        final long byte136 = blocks[blocksOffset++] & 0xFF;
        final long byte137 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte132 & 63) << 40) | (byte133 << 32) | (byte134 << 24) | (byte135 << 16) | (byte136 << 8) | byte137;
        final long byte138 = blocks[blocksOffset++] & 0xFF;
        final long byte139 = blocks[blocksOffset++] & 0xFF;
        final long byte140 = blocks[blocksOffset++] & 0xFF;
        final long byte141 = blocks[blocksOffset++] & 0xFF;
        final long byte142 = blocks[blocksOffset++] & 0xFF;
        final long byte143 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte138 << 38) | (byte139 << 30) | (byte140 << 22) | (byte141 << 14) | (byte142 << 6) | (byte143 >>> 2);
        final long byte144 = blocks[blocksOffset++] & 0xFF;
        final long byte145 = blocks[blocksOffset++] & 0xFF;
        final long byte146 = blocks[blocksOffset++] & 0xFF;
        final long byte147 = blocks[blocksOffset++] & 0xFF;
        final long byte148 = blocks[blocksOffset++] & 0xFF;
        final long byte149 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte143 & 3) << 44) | (byte144 << 36) | (byte145 << 28) | (byte146 << 20) | (byte147 << 12) | (byte148 << 4) | (byte149 >>> 4);
        final long byte150 = blocks[blocksOffset++] & 0xFF;
        final long byte151 = blocks[blocksOffset++] & 0xFF;
        final long byte152 = blocks[blocksOffset++] & 0xFF;
        final long byte153 = blocks[blocksOffset++] & 0xFF;
        final long byte154 = blocks[blocksOffset++] & 0xFF;
        final long byte155 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte149 & 15) << 42) | (byte150 << 34) | (byte151 << 26) | (byte152 << 18) | (byte153 << 10) | (byte154 << 2) | (byte155 >>> 6);
        final long byte156 = blocks[blocksOffset++] & 0xFF;
        final long byte157 = blocks[blocksOffset++] & 0xFF;
        final long byte158 = blocks[blocksOffset++] & 0xFF;
        final long byte159 = blocks[blocksOffset++] & 0xFF;
        final long byte160 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte155 & 63) << 40) | (byte156 << 32) | (byte157 << 24) | (byte158 << 16) | (byte159 << 8) | byte160;
        final long byte161 = blocks[blocksOffset++] & 0xFF;
        final long byte162 = blocks[blocksOffset++] & 0xFF;
        final long byte163 = blocks[blocksOffset++] & 0xFF;
        final long byte164 = blocks[blocksOffset++] & 0xFF;
        final long byte165 = blocks[blocksOffset++] & 0xFF;
        final long byte166 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte161 << 38) | (byte162 << 30) | (byte163 << 22) | (byte164 << 14) | (byte165 << 6) | (byte166 >>> 2);
        final long byte167 = blocks[blocksOffset++] & 0xFF;
        final long byte168 = blocks[blocksOffset++] & 0xFF;
        final long byte169 = blocks[blocksOffset++] & 0xFF;
        final long byte170 = blocks[blocksOffset++] & 0xFF;
        final long byte171 = blocks[blocksOffset++] & 0xFF;
        final long byte172 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte166 & 3) << 44) | (byte167 << 36) | (byte168 << 28) | (byte169 << 20) | (byte170 << 12) | (byte171 << 4) | (byte172 >>> 4);
        final long byte173 = blocks[blocksOffset++] & 0xFF;
        final long byte174 = blocks[blocksOffset++] & 0xFF;
        final long byte175 = blocks[blocksOffset++] & 0xFF;
        final long byte176 = blocks[blocksOffset++] & 0xFF;
        final long byte177 = blocks[blocksOffset++] & 0xFF;
        final long byte178 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte172 & 15) << 42) | (byte173 << 34) | (byte174 << 26) | (byte175 << 18) | (byte176 << 10) | (byte177 << 2) | (byte178 >>> 6);
        final long byte179 = blocks[blocksOffset++] & 0xFF;
        final long byte180 = blocks[blocksOffset++] & 0xFF;
        final long byte181 = blocks[blocksOffset++] & 0xFF;
        final long byte182 = blocks[blocksOffset++] & 0xFF;
        final long byte183 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte178 & 63) << 40) | (byte179 << 32) | (byte180 << 24) | (byte181 << 16) | (byte182 << 8) | byte183;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 28);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 38);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 30);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 40);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 22);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 32);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 42);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 34);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 44);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 26);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 36);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 28);
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 38);
        blocks[blocksOffset++] = (values[valuesOffset++] << 26) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 30);
        blocks[blocksOffset++] = (values[valuesOffset++] << 34) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 40);
        blocks[blocksOffset++] = (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 22);
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 32);
        blocks[blocksOffset++] = (values[valuesOffset++] << 32) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 42);
        blocks[blocksOffset++] = (values[valuesOffset++] << 22) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 34);
        blocks[blocksOffset++] = (values[valuesOffset++] << 30) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 44);
        blocks[blocksOffset++] = (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 26);
        blocks[blocksOffset++] = (values[valuesOffset++] << 38) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 36);
        blocks[blocksOffset++] = (values[valuesOffset++] << 28) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | values[valuesOffset++];
      }
    }

}
