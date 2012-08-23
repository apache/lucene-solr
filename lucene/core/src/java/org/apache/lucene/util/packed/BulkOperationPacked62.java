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
final class BulkOperationPacked62 extends BulkOperation {
    @Override
    public int blockCount() {
      return 31;
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
        values[valuesOffset++] = block0 >>> 2;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 3L) << 60) | (block1 >>> 4);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 15L) << 58) | (block2 >>> 6);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 63L) << 56) | (block3 >>> 8);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 255L) << 54) | (block4 >>> 10);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 1023L) << 52) | (block5 >>> 12);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 4095L) << 50) | (block6 >>> 14);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 16383L) << 48) | (block7 >>> 16);
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 65535L) << 46) | (block8 >>> 18);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 262143L) << 44) | (block9 >>> 20);
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 1048575L) << 42) | (block10 >>> 22);
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 4194303L) << 40) | (block11 >>> 24);
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 16777215L) << 38) | (block12 >>> 26);
        final long block13 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block12 & 67108863L) << 36) | (block13 >>> 28);
        final long block14 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block13 & 268435455L) << 34) | (block14 >>> 30);
        final long block15 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block14 & 1073741823L) << 32) | (block15 >>> 32);
        final long block16 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block15 & 4294967295L) << 30) | (block16 >>> 34);
        final long block17 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block16 & 17179869183L) << 28) | (block17 >>> 36);
        final long block18 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block17 & 68719476735L) << 26) | (block18 >>> 38);
        final long block19 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block18 & 274877906943L) << 24) | (block19 >>> 40);
        final long block20 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block19 & 1099511627775L) << 22) | (block20 >>> 42);
        final long block21 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block20 & 4398046511103L) << 20) | (block21 >>> 44);
        final long block22 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block21 & 17592186044415L) << 18) | (block22 >>> 46);
        final long block23 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block22 & 70368744177663L) << 16) | (block23 >>> 48);
        final long block24 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block23 & 281474976710655L) << 14) | (block24 >>> 50);
        final long block25 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block24 & 1125899906842623L) << 12) | (block25 >>> 52);
        final long block26 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block25 & 4503599627370495L) << 10) | (block26 >>> 54);
        final long block27 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block26 & 18014398509481983L) << 8) | (block27 >>> 56);
        final long block28 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block27 & 72057594037927935L) << 6) | (block28 >>> 58);
        final long block29 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block28 & 288230376151711743L) << 4) | (block29 >>> 60);
        final long block30 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block29 & 1152921504606846975L) << 2) | (block30 >>> 62);
        values[valuesOffset++] = block30 & 4611686018427387903L;
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
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 54) | (byte1 << 46) | (byte2 << 38) | (byte3 << 30) | (byte4 << 22) | (byte5 << 14) | (byte6 << 6) | (byte7 >>> 2);
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 3) << 60) | (byte8 << 52) | (byte9 << 44) | (byte10 << 36) | (byte11 << 28) | (byte12 << 20) | (byte13 << 12) | (byte14 << 4) | (byte15 >>> 4);
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 15) << 58) | (byte16 << 50) | (byte17 << 42) | (byte18 << 34) | (byte19 << 26) | (byte20 << 18) | (byte21 << 10) | (byte22 << 2) | (byte23 >>> 6);
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte23 & 63) << 56) | (byte24 << 48) | (byte25 << 40) | (byte26 << 32) | (byte27 << 24) | (byte28 << 16) | (byte29 << 8) | byte30;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte31 << 54) | (byte32 << 46) | (byte33 << 38) | (byte34 << 30) | (byte35 << 22) | (byte36 << 14) | (byte37 << 6) | (byte38 >>> 2);
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 3) << 60) | (byte39 << 52) | (byte40 << 44) | (byte41 << 36) | (byte42 << 28) | (byte43 << 20) | (byte44 << 12) | (byte45 << 4) | (byte46 >>> 4);
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte46 & 15) << 58) | (byte47 << 50) | (byte48 << 42) | (byte49 << 34) | (byte50 << 26) | (byte51 << 18) | (byte52 << 10) | (byte53 << 2) | (byte54 >>> 6);
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte54 & 63) << 56) | (byte55 << 48) | (byte56 << 40) | (byte57 << 32) | (byte58 << 24) | (byte59 << 16) | (byte60 << 8) | byte61;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte62 << 54) | (byte63 << 46) | (byte64 << 38) | (byte65 << 30) | (byte66 << 22) | (byte67 << 14) | (byte68 << 6) | (byte69 >>> 2);
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte69 & 3) << 60) | (byte70 << 52) | (byte71 << 44) | (byte72 << 36) | (byte73 << 28) | (byte74 << 20) | (byte75 << 12) | (byte76 << 4) | (byte77 >>> 4);
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte77 & 15) << 58) | (byte78 << 50) | (byte79 << 42) | (byte80 << 34) | (byte81 << 26) | (byte82 << 18) | (byte83 << 10) | (byte84 << 2) | (byte85 >>> 6);
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte85 & 63) << 56) | (byte86 << 48) | (byte87 << 40) | (byte88 << 32) | (byte89 << 24) | (byte90 << 16) | (byte91 << 8) | byte92;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte93 << 54) | (byte94 << 46) | (byte95 << 38) | (byte96 << 30) | (byte97 << 22) | (byte98 << 14) | (byte99 << 6) | (byte100 >>> 2);
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        final long byte104 = blocks[blocksOffset++] & 0xFF;
        final long byte105 = blocks[blocksOffset++] & 0xFF;
        final long byte106 = blocks[blocksOffset++] & 0xFF;
        final long byte107 = blocks[blocksOffset++] & 0xFF;
        final long byte108 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte100 & 3) << 60) | (byte101 << 52) | (byte102 << 44) | (byte103 << 36) | (byte104 << 28) | (byte105 << 20) | (byte106 << 12) | (byte107 << 4) | (byte108 >>> 4);
        final long byte109 = blocks[blocksOffset++] & 0xFF;
        final long byte110 = blocks[blocksOffset++] & 0xFF;
        final long byte111 = blocks[blocksOffset++] & 0xFF;
        final long byte112 = blocks[blocksOffset++] & 0xFF;
        final long byte113 = blocks[blocksOffset++] & 0xFF;
        final long byte114 = blocks[blocksOffset++] & 0xFF;
        final long byte115 = blocks[blocksOffset++] & 0xFF;
        final long byte116 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte108 & 15) << 58) | (byte109 << 50) | (byte110 << 42) | (byte111 << 34) | (byte112 << 26) | (byte113 << 18) | (byte114 << 10) | (byte115 << 2) | (byte116 >>> 6);
        final long byte117 = blocks[blocksOffset++] & 0xFF;
        final long byte118 = blocks[blocksOffset++] & 0xFF;
        final long byte119 = blocks[blocksOffset++] & 0xFF;
        final long byte120 = blocks[blocksOffset++] & 0xFF;
        final long byte121 = blocks[blocksOffset++] & 0xFF;
        final long byte122 = blocks[blocksOffset++] & 0xFF;
        final long byte123 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte116 & 63) << 56) | (byte117 << 48) | (byte118 << 40) | (byte119 << 32) | (byte120 << 24) | (byte121 << 16) | (byte122 << 8) | byte123;
        final long byte124 = blocks[blocksOffset++] & 0xFF;
        final long byte125 = blocks[blocksOffset++] & 0xFF;
        final long byte126 = blocks[blocksOffset++] & 0xFF;
        final long byte127 = blocks[blocksOffset++] & 0xFF;
        final long byte128 = blocks[blocksOffset++] & 0xFF;
        final long byte129 = blocks[blocksOffset++] & 0xFF;
        final long byte130 = blocks[blocksOffset++] & 0xFF;
        final long byte131 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte124 << 54) | (byte125 << 46) | (byte126 << 38) | (byte127 << 30) | (byte128 << 22) | (byte129 << 14) | (byte130 << 6) | (byte131 >>> 2);
        final long byte132 = blocks[blocksOffset++] & 0xFF;
        final long byte133 = blocks[blocksOffset++] & 0xFF;
        final long byte134 = blocks[blocksOffset++] & 0xFF;
        final long byte135 = blocks[blocksOffset++] & 0xFF;
        final long byte136 = blocks[blocksOffset++] & 0xFF;
        final long byte137 = blocks[blocksOffset++] & 0xFF;
        final long byte138 = blocks[blocksOffset++] & 0xFF;
        final long byte139 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte131 & 3) << 60) | (byte132 << 52) | (byte133 << 44) | (byte134 << 36) | (byte135 << 28) | (byte136 << 20) | (byte137 << 12) | (byte138 << 4) | (byte139 >>> 4);
        final long byte140 = blocks[blocksOffset++] & 0xFF;
        final long byte141 = blocks[blocksOffset++] & 0xFF;
        final long byte142 = blocks[blocksOffset++] & 0xFF;
        final long byte143 = blocks[blocksOffset++] & 0xFF;
        final long byte144 = blocks[blocksOffset++] & 0xFF;
        final long byte145 = blocks[blocksOffset++] & 0xFF;
        final long byte146 = blocks[blocksOffset++] & 0xFF;
        final long byte147 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte139 & 15) << 58) | (byte140 << 50) | (byte141 << 42) | (byte142 << 34) | (byte143 << 26) | (byte144 << 18) | (byte145 << 10) | (byte146 << 2) | (byte147 >>> 6);
        final long byte148 = blocks[blocksOffset++] & 0xFF;
        final long byte149 = blocks[blocksOffset++] & 0xFF;
        final long byte150 = blocks[blocksOffset++] & 0xFF;
        final long byte151 = blocks[blocksOffset++] & 0xFF;
        final long byte152 = blocks[blocksOffset++] & 0xFF;
        final long byte153 = blocks[blocksOffset++] & 0xFF;
        final long byte154 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte147 & 63) << 56) | (byte148 << 48) | (byte149 << 40) | (byte150 << 32) | (byte151 << 24) | (byte152 << 16) | (byte153 << 8) | byte154;
        final long byte155 = blocks[blocksOffset++] & 0xFF;
        final long byte156 = blocks[blocksOffset++] & 0xFF;
        final long byte157 = blocks[blocksOffset++] & 0xFF;
        final long byte158 = blocks[blocksOffset++] & 0xFF;
        final long byte159 = blocks[blocksOffset++] & 0xFF;
        final long byte160 = blocks[blocksOffset++] & 0xFF;
        final long byte161 = blocks[blocksOffset++] & 0xFF;
        final long byte162 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte155 << 54) | (byte156 << 46) | (byte157 << 38) | (byte158 << 30) | (byte159 << 22) | (byte160 << 14) | (byte161 << 6) | (byte162 >>> 2);
        final long byte163 = blocks[blocksOffset++] & 0xFF;
        final long byte164 = blocks[blocksOffset++] & 0xFF;
        final long byte165 = blocks[blocksOffset++] & 0xFF;
        final long byte166 = blocks[blocksOffset++] & 0xFF;
        final long byte167 = blocks[blocksOffset++] & 0xFF;
        final long byte168 = blocks[blocksOffset++] & 0xFF;
        final long byte169 = blocks[blocksOffset++] & 0xFF;
        final long byte170 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte162 & 3) << 60) | (byte163 << 52) | (byte164 << 44) | (byte165 << 36) | (byte166 << 28) | (byte167 << 20) | (byte168 << 12) | (byte169 << 4) | (byte170 >>> 4);
        final long byte171 = blocks[blocksOffset++] & 0xFF;
        final long byte172 = blocks[blocksOffset++] & 0xFF;
        final long byte173 = blocks[blocksOffset++] & 0xFF;
        final long byte174 = blocks[blocksOffset++] & 0xFF;
        final long byte175 = blocks[blocksOffset++] & 0xFF;
        final long byte176 = blocks[blocksOffset++] & 0xFF;
        final long byte177 = blocks[blocksOffset++] & 0xFF;
        final long byte178 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte170 & 15) << 58) | (byte171 << 50) | (byte172 << 42) | (byte173 << 34) | (byte174 << 26) | (byte175 << 18) | (byte176 << 10) | (byte177 << 2) | (byte178 >>> 6);
        final long byte179 = blocks[blocksOffset++] & 0xFF;
        final long byte180 = blocks[blocksOffset++] & 0xFF;
        final long byte181 = blocks[blocksOffset++] & 0xFF;
        final long byte182 = blocks[blocksOffset++] & 0xFF;
        final long byte183 = blocks[blocksOffset++] & 0xFF;
        final long byte184 = blocks[blocksOffset++] & 0xFF;
        final long byte185 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte178 & 63) << 56) | (byte179 << 48) | (byte180 << 40) | (byte181 << 32) | (byte182 << 24) | (byte183 << 16) | (byte184 << 8) | byte185;
        final long byte186 = blocks[blocksOffset++] & 0xFF;
        final long byte187 = blocks[blocksOffset++] & 0xFF;
        final long byte188 = blocks[blocksOffset++] & 0xFF;
        final long byte189 = blocks[blocksOffset++] & 0xFF;
        final long byte190 = blocks[blocksOffset++] & 0xFF;
        final long byte191 = blocks[blocksOffset++] & 0xFF;
        final long byte192 = blocks[blocksOffset++] & 0xFF;
        final long byte193 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte186 << 54) | (byte187 << 46) | (byte188 << 38) | (byte189 << 30) | (byte190 << 22) | (byte191 << 14) | (byte192 << 6) | (byte193 >>> 2);
        final long byte194 = blocks[blocksOffset++] & 0xFF;
        final long byte195 = blocks[blocksOffset++] & 0xFF;
        final long byte196 = blocks[blocksOffset++] & 0xFF;
        final long byte197 = blocks[blocksOffset++] & 0xFF;
        final long byte198 = blocks[blocksOffset++] & 0xFF;
        final long byte199 = blocks[blocksOffset++] & 0xFF;
        final long byte200 = blocks[blocksOffset++] & 0xFF;
        final long byte201 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte193 & 3) << 60) | (byte194 << 52) | (byte195 << 44) | (byte196 << 36) | (byte197 << 28) | (byte198 << 20) | (byte199 << 12) | (byte200 << 4) | (byte201 >>> 4);
        final long byte202 = blocks[blocksOffset++] & 0xFF;
        final long byte203 = blocks[blocksOffset++] & 0xFF;
        final long byte204 = blocks[blocksOffset++] & 0xFF;
        final long byte205 = blocks[blocksOffset++] & 0xFF;
        final long byte206 = blocks[blocksOffset++] & 0xFF;
        final long byte207 = blocks[blocksOffset++] & 0xFF;
        final long byte208 = blocks[blocksOffset++] & 0xFF;
        final long byte209 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte201 & 15) << 58) | (byte202 << 50) | (byte203 << 42) | (byte204 << 34) | (byte205 << 26) | (byte206 << 18) | (byte207 << 10) | (byte208 << 2) | (byte209 >>> 6);
        final long byte210 = blocks[blocksOffset++] & 0xFF;
        final long byte211 = blocks[blocksOffset++] & 0xFF;
        final long byte212 = blocks[blocksOffset++] & 0xFF;
        final long byte213 = blocks[blocksOffset++] & 0xFF;
        final long byte214 = blocks[blocksOffset++] & 0xFF;
        final long byte215 = blocks[blocksOffset++] & 0xFF;
        final long byte216 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte209 & 63) << 56) | (byte210 << 48) | (byte211 << 40) | (byte212 << 32) | (byte213 << 24) | (byte214 << 16) | (byte215 << 8) | byte216;
        final long byte217 = blocks[blocksOffset++] & 0xFF;
        final long byte218 = blocks[blocksOffset++] & 0xFF;
        final long byte219 = blocks[blocksOffset++] & 0xFF;
        final long byte220 = blocks[blocksOffset++] & 0xFF;
        final long byte221 = blocks[blocksOffset++] & 0xFF;
        final long byte222 = blocks[blocksOffset++] & 0xFF;
        final long byte223 = blocks[blocksOffset++] & 0xFF;
        final long byte224 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte217 << 54) | (byte218 << 46) | (byte219 << 38) | (byte220 << 30) | (byte221 << 22) | (byte222 << 14) | (byte223 << 6) | (byte224 >>> 2);
        final long byte225 = blocks[blocksOffset++] & 0xFF;
        final long byte226 = blocks[blocksOffset++] & 0xFF;
        final long byte227 = blocks[blocksOffset++] & 0xFF;
        final long byte228 = blocks[blocksOffset++] & 0xFF;
        final long byte229 = blocks[blocksOffset++] & 0xFF;
        final long byte230 = blocks[blocksOffset++] & 0xFF;
        final long byte231 = blocks[blocksOffset++] & 0xFF;
        final long byte232 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte224 & 3) << 60) | (byte225 << 52) | (byte226 << 44) | (byte227 << 36) | (byte228 << 28) | (byte229 << 20) | (byte230 << 12) | (byte231 << 4) | (byte232 >>> 4);
        final long byte233 = blocks[blocksOffset++] & 0xFF;
        final long byte234 = blocks[blocksOffset++] & 0xFF;
        final long byte235 = blocks[blocksOffset++] & 0xFF;
        final long byte236 = blocks[blocksOffset++] & 0xFF;
        final long byte237 = blocks[blocksOffset++] & 0xFF;
        final long byte238 = blocks[blocksOffset++] & 0xFF;
        final long byte239 = blocks[blocksOffset++] & 0xFF;
        final long byte240 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte232 & 15) << 58) | (byte233 << 50) | (byte234 << 42) | (byte235 << 34) | (byte236 << 26) | (byte237 << 18) | (byte238 << 10) | (byte239 << 2) | (byte240 >>> 6);
        final long byte241 = blocks[blocksOffset++] & 0xFF;
        final long byte242 = blocks[blocksOffset++] & 0xFF;
        final long byte243 = blocks[blocksOffset++] & 0xFF;
        final long byte244 = blocks[blocksOffset++] & 0xFF;
        final long byte245 = blocks[blocksOffset++] & 0xFF;
        final long byte246 = blocks[blocksOffset++] & 0xFF;
        final long byte247 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte240 & 63) << 56) | (byte241 << 48) | (byte242 << 40) | (byte243 << 32) | (byte244 << 24) | (byte245 << 16) | (byte246 << 8) | byte247;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 60);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 58);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 56);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 54);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 52);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 50);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 48);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 46);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 44);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 42);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset] & 0xffffffffL) >>> 40);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 38);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset] & 0xffffffffL) >>> 36);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset] & 0xffffffffL) >>> 34);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset] & 0xffffffffL) >>> 32);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset] & 0xffffffffL) >>> 30);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset] & 0xffffffffL) >>> 28);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset] & 0xffffffffL) >>> 26);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset] & 0xffffffffL) >>> 22);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 60);
        blocks[blocksOffset++] = (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 58);
        blocks[blocksOffset++] = (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 56);
        blocks[blocksOffset++] = (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 54);
        blocks[blocksOffset++] = (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 52);
        blocks[blocksOffset++] = (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 50);
        blocks[blocksOffset++] = (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 48);
        blocks[blocksOffset++] = (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 46);
        blocks[blocksOffset++] = (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 44);
        blocks[blocksOffset++] = (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 42);
        blocks[blocksOffset++] = (values[valuesOffset++] << 22) | (values[valuesOffset] >>> 40);
        blocks[blocksOffset++] = (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 38);
        blocks[blocksOffset++] = (values[valuesOffset++] << 26) | (values[valuesOffset] >>> 36);
        blocks[blocksOffset++] = (values[valuesOffset++] << 28) | (values[valuesOffset] >>> 34);
        blocks[blocksOffset++] = (values[valuesOffset++] << 30) | (values[valuesOffset] >>> 32);
        blocks[blocksOffset++] = (values[valuesOffset++] << 32) | (values[valuesOffset] >>> 30);
        blocks[blocksOffset++] = (values[valuesOffset++] << 34) | (values[valuesOffset] >>> 28);
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset] >>> 26);
        blocks[blocksOffset++] = (values[valuesOffset++] << 38) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset] >>> 22);
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | values[valuesOffset++];
      }
    }

}
