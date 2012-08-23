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
final class BulkOperationPacked50 extends BulkOperation {
    @Override
    public int blockCount() {
      return 25;
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
        values[valuesOffset++] = block0 >>> 14;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 16383L) << 36) | (block1 >>> 28);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 268435455L) << 22) | (block2 >>> 42);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 4398046511103L) << 8) | (block3 >>> 56);
        values[valuesOffset++] = (block3 >>> 6) & 1125899906842623L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 63L) << 44) | (block4 >>> 20);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 1048575L) << 30) | (block5 >>> 34);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 17179869183L) << 16) | (block6 >>> 48);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 281474976710655L) << 2) | (block7 >>> 62);
        values[valuesOffset++] = (block7 >>> 12) & 1125899906842623L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 4095L) << 38) | (block8 >>> 26);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 67108863L) << 24) | (block9 >>> 40);
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 1099511627775L) << 10) | (block10 >>> 54);
        values[valuesOffset++] = (block10 >>> 4) & 1125899906842623L;
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 15L) << 46) | (block11 >>> 18);
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 262143L) << 32) | (block12 >>> 32);
        final long block13 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block12 & 4294967295L) << 18) | (block13 >>> 46);
        final long block14 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block13 & 70368744177663L) << 4) | (block14 >>> 60);
        values[valuesOffset++] = (block14 >>> 10) & 1125899906842623L;
        final long block15 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block14 & 1023L) << 40) | (block15 >>> 24);
        final long block16 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block15 & 16777215L) << 26) | (block16 >>> 38);
        final long block17 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block16 & 274877906943L) << 12) | (block17 >>> 52);
        values[valuesOffset++] = (block17 >>> 2) & 1125899906842623L;
        final long block18 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block17 & 3L) << 48) | (block18 >>> 16);
        final long block19 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block18 & 65535L) << 34) | (block19 >>> 30);
        final long block20 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block19 & 1073741823L) << 20) | (block20 >>> 44);
        final long block21 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block20 & 17592186044415L) << 6) | (block21 >>> 58);
        values[valuesOffset++] = (block21 >>> 8) & 1125899906842623L;
        final long block22 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block21 & 255L) << 42) | (block22 >>> 22);
        final long block23 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block22 & 4194303L) << 28) | (block23 >>> 36);
        final long block24 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block23 & 68719476735L) << 14) | (block24 >>> 50);
        values[valuesOffset++] = block24 & 1125899906842623L;
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
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 42) | (byte1 << 34) | (byte2 << 26) | (byte3 << 18) | (byte4 << 10) | (byte5 << 2) | (byte6 >>> 6);
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 63) << 44) | (byte7 << 36) | (byte8 << 28) | (byte9 << 20) | (byte10 << 12) | (byte11 << 4) | (byte12 >>> 4);
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 15) << 46) | (byte13 << 38) | (byte14 << 30) | (byte15 << 22) | (byte16 << 14) | (byte17 << 6) | (byte18 >>> 2);
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 3) << 48) | (byte19 << 40) | (byte20 << 32) | (byte21 << 24) | (byte22 << 16) | (byte23 << 8) | byte24;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte25 << 42) | (byte26 << 34) | (byte27 << 26) | (byte28 << 18) | (byte29 << 10) | (byte30 << 2) | (byte31 >>> 6);
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 63) << 44) | (byte32 << 36) | (byte33 << 28) | (byte34 << 20) | (byte35 << 12) | (byte36 << 4) | (byte37 >>> 4);
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte37 & 15) << 46) | (byte38 << 38) | (byte39 << 30) | (byte40 << 22) | (byte41 << 14) | (byte42 << 6) | (byte43 >>> 2);
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte43 & 3) << 48) | (byte44 << 40) | (byte45 << 32) | (byte46 << 24) | (byte47 << 16) | (byte48 << 8) | byte49;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte50 << 42) | (byte51 << 34) | (byte52 << 26) | (byte53 << 18) | (byte54 << 10) | (byte55 << 2) | (byte56 >>> 6);
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte56 & 63) << 44) | (byte57 << 36) | (byte58 << 28) | (byte59 << 20) | (byte60 << 12) | (byte61 << 4) | (byte62 >>> 4);
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte62 & 15) << 46) | (byte63 << 38) | (byte64 << 30) | (byte65 << 22) | (byte66 << 14) | (byte67 << 6) | (byte68 >>> 2);
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte68 & 3) << 48) | (byte69 << 40) | (byte70 << 32) | (byte71 << 24) | (byte72 << 16) | (byte73 << 8) | byte74;
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte75 << 42) | (byte76 << 34) | (byte77 << 26) | (byte78 << 18) | (byte79 << 10) | (byte80 << 2) | (byte81 >>> 6);
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte81 & 63) << 44) | (byte82 << 36) | (byte83 << 28) | (byte84 << 20) | (byte85 << 12) | (byte86 << 4) | (byte87 >>> 4);
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte87 & 15) << 46) | (byte88 << 38) | (byte89 << 30) | (byte90 << 22) | (byte91 << 14) | (byte92 << 6) | (byte93 >>> 2);
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte93 & 3) << 48) | (byte94 << 40) | (byte95 << 32) | (byte96 << 24) | (byte97 << 16) | (byte98 << 8) | byte99;
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        final long byte104 = blocks[blocksOffset++] & 0xFF;
        final long byte105 = blocks[blocksOffset++] & 0xFF;
        final long byte106 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte100 << 42) | (byte101 << 34) | (byte102 << 26) | (byte103 << 18) | (byte104 << 10) | (byte105 << 2) | (byte106 >>> 6);
        final long byte107 = blocks[blocksOffset++] & 0xFF;
        final long byte108 = blocks[blocksOffset++] & 0xFF;
        final long byte109 = blocks[blocksOffset++] & 0xFF;
        final long byte110 = blocks[blocksOffset++] & 0xFF;
        final long byte111 = blocks[blocksOffset++] & 0xFF;
        final long byte112 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte106 & 63) << 44) | (byte107 << 36) | (byte108 << 28) | (byte109 << 20) | (byte110 << 12) | (byte111 << 4) | (byte112 >>> 4);
        final long byte113 = blocks[blocksOffset++] & 0xFF;
        final long byte114 = blocks[blocksOffset++] & 0xFF;
        final long byte115 = blocks[blocksOffset++] & 0xFF;
        final long byte116 = blocks[blocksOffset++] & 0xFF;
        final long byte117 = blocks[blocksOffset++] & 0xFF;
        final long byte118 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte112 & 15) << 46) | (byte113 << 38) | (byte114 << 30) | (byte115 << 22) | (byte116 << 14) | (byte117 << 6) | (byte118 >>> 2);
        final long byte119 = blocks[blocksOffset++] & 0xFF;
        final long byte120 = blocks[blocksOffset++] & 0xFF;
        final long byte121 = blocks[blocksOffset++] & 0xFF;
        final long byte122 = blocks[blocksOffset++] & 0xFF;
        final long byte123 = blocks[blocksOffset++] & 0xFF;
        final long byte124 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte118 & 3) << 48) | (byte119 << 40) | (byte120 << 32) | (byte121 << 24) | (byte122 << 16) | (byte123 << 8) | byte124;
        final long byte125 = blocks[blocksOffset++] & 0xFF;
        final long byte126 = blocks[blocksOffset++] & 0xFF;
        final long byte127 = blocks[blocksOffset++] & 0xFF;
        final long byte128 = blocks[blocksOffset++] & 0xFF;
        final long byte129 = blocks[blocksOffset++] & 0xFF;
        final long byte130 = blocks[blocksOffset++] & 0xFF;
        final long byte131 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte125 << 42) | (byte126 << 34) | (byte127 << 26) | (byte128 << 18) | (byte129 << 10) | (byte130 << 2) | (byte131 >>> 6);
        final long byte132 = blocks[blocksOffset++] & 0xFF;
        final long byte133 = blocks[blocksOffset++] & 0xFF;
        final long byte134 = blocks[blocksOffset++] & 0xFF;
        final long byte135 = blocks[blocksOffset++] & 0xFF;
        final long byte136 = blocks[blocksOffset++] & 0xFF;
        final long byte137 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte131 & 63) << 44) | (byte132 << 36) | (byte133 << 28) | (byte134 << 20) | (byte135 << 12) | (byte136 << 4) | (byte137 >>> 4);
        final long byte138 = blocks[blocksOffset++] & 0xFF;
        final long byte139 = blocks[blocksOffset++] & 0xFF;
        final long byte140 = blocks[blocksOffset++] & 0xFF;
        final long byte141 = blocks[blocksOffset++] & 0xFF;
        final long byte142 = blocks[blocksOffset++] & 0xFF;
        final long byte143 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte137 & 15) << 46) | (byte138 << 38) | (byte139 << 30) | (byte140 << 22) | (byte141 << 14) | (byte142 << 6) | (byte143 >>> 2);
        final long byte144 = blocks[blocksOffset++] & 0xFF;
        final long byte145 = blocks[blocksOffset++] & 0xFF;
        final long byte146 = blocks[blocksOffset++] & 0xFF;
        final long byte147 = blocks[blocksOffset++] & 0xFF;
        final long byte148 = blocks[blocksOffset++] & 0xFF;
        final long byte149 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte143 & 3) << 48) | (byte144 << 40) | (byte145 << 32) | (byte146 << 24) | (byte147 << 16) | (byte148 << 8) | byte149;
        final long byte150 = blocks[blocksOffset++] & 0xFF;
        final long byte151 = blocks[blocksOffset++] & 0xFF;
        final long byte152 = blocks[blocksOffset++] & 0xFF;
        final long byte153 = blocks[blocksOffset++] & 0xFF;
        final long byte154 = blocks[blocksOffset++] & 0xFF;
        final long byte155 = blocks[blocksOffset++] & 0xFF;
        final long byte156 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte150 << 42) | (byte151 << 34) | (byte152 << 26) | (byte153 << 18) | (byte154 << 10) | (byte155 << 2) | (byte156 >>> 6);
        final long byte157 = blocks[blocksOffset++] & 0xFF;
        final long byte158 = blocks[blocksOffset++] & 0xFF;
        final long byte159 = blocks[blocksOffset++] & 0xFF;
        final long byte160 = blocks[blocksOffset++] & 0xFF;
        final long byte161 = blocks[blocksOffset++] & 0xFF;
        final long byte162 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte156 & 63) << 44) | (byte157 << 36) | (byte158 << 28) | (byte159 << 20) | (byte160 << 12) | (byte161 << 4) | (byte162 >>> 4);
        final long byte163 = blocks[blocksOffset++] & 0xFF;
        final long byte164 = blocks[blocksOffset++] & 0xFF;
        final long byte165 = blocks[blocksOffset++] & 0xFF;
        final long byte166 = blocks[blocksOffset++] & 0xFF;
        final long byte167 = blocks[blocksOffset++] & 0xFF;
        final long byte168 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte162 & 15) << 46) | (byte163 << 38) | (byte164 << 30) | (byte165 << 22) | (byte166 << 14) | (byte167 << 6) | (byte168 >>> 2);
        final long byte169 = blocks[blocksOffset++] & 0xFF;
        final long byte170 = blocks[blocksOffset++] & 0xFF;
        final long byte171 = blocks[blocksOffset++] & 0xFF;
        final long byte172 = blocks[blocksOffset++] & 0xFF;
        final long byte173 = blocks[blocksOffset++] & 0xFF;
        final long byte174 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte168 & 3) << 48) | (byte169 << 40) | (byte170 << 32) | (byte171 << 24) | (byte172 << 16) | (byte173 << 8) | byte174;
        final long byte175 = blocks[blocksOffset++] & 0xFF;
        final long byte176 = blocks[blocksOffset++] & 0xFF;
        final long byte177 = blocks[blocksOffset++] & 0xFF;
        final long byte178 = blocks[blocksOffset++] & 0xFF;
        final long byte179 = blocks[blocksOffset++] & 0xFF;
        final long byte180 = blocks[blocksOffset++] & 0xFF;
        final long byte181 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte175 << 42) | (byte176 << 34) | (byte177 << 26) | (byte178 << 18) | (byte179 << 10) | (byte180 << 2) | (byte181 >>> 6);
        final long byte182 = blocks[blocksOffset++] & 0xFF;
        final long byte183 = blocks[blocksOffset++] & 0xFF;
        final long byte184 = blocks[blocksOffset++] & 0xFF;
        final long byte185 = blocks[blocksOffset++] & 0xFF;
        final long byte186 = blocks[blocksOffset++] & 0xFF;
        final long byte187 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte181 & 63) << 44) | (byte182 << 36) | (byte183 << 28) | (byte184 << 20) | (byte185 << 12) | (byte186 << 4) | (byte187 >>> 4);
        final long byte188 = blocks[blocksOffset++] & 0xFF;
        final long byte189 = blocks[blocksOffset++] & 0xFF;
        final long byte190 = blocks[blocksOffset++] & 0xFF;
        final long byte191 = blocks[blocksOffset++] & 0xFF;
        final long byte192 = blocks[blocksOffset++] & 0xFF;
        final long byte193 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte187 & 15) << 46) | (byte188 << 38) | (byte189 << 30) | (byte190 << 22) | (byte191 << 14) | (byte192 << 6) | (byte193 >>> 2);
        final long byte194 = blocks[blocksOffset++] & 0xFF;
        final long byte195 = blocks[blocksOffset++] & 0xFF;
        final long byte196 = blocks[blocksOffset++] & 0xFF;
        final long byte197 = blocks[blocksOffset++] & 0xFF;
        final long byte198 = blocks[blocksOffset++] & 0xFF;
        final long byte199 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte193 & 3) << 48) | (byte194 << 40) | (byte195 << 32) | (byte196 << 24) | (byte197 << 16) | (byte198 << 8) | byte199;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 36);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset] & 0xffffffffL) >>> 22);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 44);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 30);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 38);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 46);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 32);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 40);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 26);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 48);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 34);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 42);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset] & 0xffffffffL) >>> 28);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 36);
        blocks[blocksOffset++] = (values[valuesOffset++] << 28) | (values[valuesOffset] >>> 22);
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 44);
        blocks[blocksOffset++] = (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 30);
        blocks[blocksOffset++] = (values[valuesOffset++] << 34) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 38);
        blocks[blocksOffset++] = (values[valuesOffset++] << 26) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 46);
        blocks[blocksOffset++] = (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 32);
        blocks[blocksOffset++] = (values[valuesOffset++] << 32) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 40);
        blocks[blocksOffset++] = (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 26);
        blocks[blocksOffset++] = (values[valuesOffset++] << 38) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 48);
        blocks[blocksOffset++] = (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 34);
        blocks[blocksOffset++] = (values[valuesOffset++] << 30) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 42);
        blocks[blocksOffset++] = (values[valuesOffset++] << 22) | (values[valuesOffset] >>> 28);
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | values[valuesOffset++];
      }
    }

}
