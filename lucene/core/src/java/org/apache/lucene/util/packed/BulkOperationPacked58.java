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
final class BulkOperationPacked58 extends BulkOperation {
    public int blockCount() {
      return 29;
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
        values[valuesOffset++] = block0 >>> 6;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 63L) << 52) | (block1 >>> 12);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 4095L) << 46) | (block2 >>> 18);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 262143L) << 40) | (block3 >>> 24);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 16777215L) << 34) | (block4 >>> 30);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 1073741823L) << 28) | (block5 >>> 36);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 68719476735L) << 22) | (block6 >>> 42);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 4398046511103L) << 16) | (block7 >>> 48);
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 281474976710655L) << 10) | (block8 >>> 54);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 18014398509481983L) << 4) | (block9 >>> 60);
        values[valuesOffset++] = (block9 >>> 2) & 288230376151711743L;
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 3L) << 56) | (block10 >>> 8);
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 255L) << 50) | (block11 >>> 14);
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 16383L) << 44) | (block12 >>> 20);
        final long block13 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block12 & 1048575L) << 38) | (block13 >>> 26);
        final long block14 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block13 & 67108863L) << 32) | (block14 >>> 32);
        final long block15 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block14 & 4294967295L) << 26) | (block15 >>> 38);
        final long block16 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block15 & 274877906943L) << 20) | (block16 >>> 44);
        final long block17 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block16 & 17592186044415L) << 14) | (block17 >>> 50);
        final long block18 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block17 & 1125899906842623L) << 8) | (block18 >>> 56);
        final long block19 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block18 & 72057594037927935L) << 2) | (block19 >>> 62);
        values[valuesOffset++] = (block19 >>> 4) & 288230376151711743L;
        final long block20 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block19 & 15L) << 54) | (block20 >>> 10);
        final long block21 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block20 & 1023L) << 48) | (block21 >>> 16);
        final long block22 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block21 & 65535L) << 42) | (block22 >>> 22);
        final long block23 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block22 & 4194303L) << 36) | (block23 >>> 28);
        final long block24 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block23 & 268435455L) << 30) | (block24 >>> 34);
        final long block25 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block24 & 17179869183L) << 24) | (block25 >>> 40);
        final long block26 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block25 & 1099511627775L) << 18) | (block26 >>> 46);
        final long block27 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block26 & 70368744177663L) << 12) | (block27 >>> 52);
        final long block28 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block27 & 4503599627370495L) << 6) | (block28 >>> 58);
        values[valuesOffset++] = block28 & 288230376151711743L;
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
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 50) | (byte1 << 42) | (byte2 << 34) | (byte3 << 26) | (byte4 << 18) | (byte5 << 10) | (byte6 << 2) | (byte7 >>> 6);
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 63) << 52) | (byte8 << 44) | (byte9 << 36) | (byte10 << 28) | (byte11 << 20) | (byte12 << 12) | (byte13 << 4) | (byte14 >>> 4);
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte14 & 15) << 54) | (byte15 << 46) | (byte16 << 38) | (byte17 << 30) | (byte18 << 22) | (byte19 << 14) | (byte20 << 6) | (byte21 >>> 2);
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 3) << 56) | (byte22 << 48) | (byte23 << 40) | (byte24 << 32) | (byte25 << 24) | (byte26 << 16) | (byte27 << 8) | byte28;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte29 << 50) | (byte30 << 42) | (byte31 << 34) | (byte32 << 26) | (byte33 << 18) | (byte34 << 10) | (byte35 << 2) | (byte36 >>> 6);
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte36 & 63) << 52) | (byte37 << 44) | (byte38 << 36) | (byte39 << 28) | (byte40 << 20) | (byte41 << 12) | (byte42 << 4) | (byte43 >>> 4);
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte43 & 15) << 54) | (byte44 << 46) | (byte45 << 38) | (byte46 << 30) | (byte47 << 22) | (byte48 << 14) | (byte49 << 6) | (byte50 >>> 2);
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte50 & 3) << 56) | (byte51 << 48) | (byte52 << 40) | (byte53 << 32) | (byte54 << 24) | (byte55 << 16) | (byte56 << 8) | byte57;
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte58 << 50) | (byte59 << 42) | (byte60 << 34) | (byte61 << 26) | (byte62 << 18) | (byte63 << 10) | (byte64 << 2) | (byte65 >>> 6);
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte65 & 63) << 52) | (byte66 << 44) | (byte67 << 36) | (byte68 << 28) | (byte69 << 20) | (byte70 << 12) | (byte71 << 4) | (byte72 >>> 4);
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte72 & 15) << 54) | (byte73 << 46) | (byte74 << 38) | (byte75 << 30) | (byte76 << 22) | (byte77 << 14) | (byte78 << 6) | (byte79 >>> 2);
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte79 & 3) << 56) | (byte80 << 48) | (byte81 << 40) | (byte82 << 32) | (byte83 << 24) | (byte84 << 16) | (byte85 << 8) | byte86;
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte87 << 50) | (byte88 << 42) | (byte89 << 34) | (byte90 << 26) | (byte91 << 18) | (byte92 << 10) | (byte93 << 2) | (byte94 >>> 6);
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte94 & 63) << 52) | (byte95 << 44) | (byte96 << 36) | (byte97 << 28) | (byte98 << 20) | (byte99 << 12) | (byte100 << 4) | (byte101 >>> 4);
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        final long byte104 = blocks[blocksOffset++] & 0xFF;
        final long byte105 = blocks[blocksOffset++] & 0xFF;
        final long byte106 = blocks[blocksOffset++] & 0xFF;
        final long byte107 = blocks[blocksOffset++] & 0xFF;
        final long byte108 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte101 & 15) << 54) | (byte102 << 46) | (byte103 << 38) | (byte104 << 30) | (byte105 << 22) | (byte106 << 14) | (byte107 << 6) | (byte108 >>> 2);
        final long byte109 = blocks[blocksOffset++] & 0xFF;
        final long byte110 = blocks[blocksOffset++] & 0xFF;
        final long byte111 = blocks[blocksOffset++] & 0xFF;
        final long byte112 = blocks[blocksOffset++] & 0xFF;
        final long byte113 = blocks[blocksOffset++] & 0xFF;
        final long byte114 = blocks[blocksOffset++] & 0xFF;
        final long byte115 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte108 & 3) << 56) | (byte109 << 48) | (byte110 << 40) | (byte111 << 32) | (byte112 << 24) | (byte113 << 16) | (byte114 << 8) | byte115;
        final long byte116 = blocks[blocksOffset++] & 0xFF;
        final long byte117 = blocks[blocksOffset++] & 0xFF;
        final long byte118 = blocks[blocksOffset++] & 0xFF;
        final long byte119 = blocks[blocksOffset++] & 0xFF;
        final long byte120 = blocks[blocksOffset++] & 0xFF;
        final long byte121 = blocks[blocksOffset++] & 0xFF;
        final long byte122 = blocks[blocksOffset++] & 0xFF;
        final long byte123 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte116 << 50) | (byte117 << 42) | (byte118 << 34) | (byte119 << 26) | (byte120 << 18) | (byte121 << 10) | (byte122 << 2) | (byte123 >>> 6);
        final long byte124 = blocks[blocksOffset++] & 0xFF;
        final long byte125 = blocks[blocksOffset++] & 0xFF;
        final long byte126 = blocks[blocksOffset++] & 0xFF;
        final long byte127 = blocks[blocksOffset++] & 0xFF;
        final long byte128 = blocks[blocksOffset++] & 0xFF;
        final long byte129 = blocks[blocksOffset++] & 0xFF;
        final long byte130 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte123 & 63) << 52) | (byte124 << 44) | (byte125 << 36) | (byte126 << 28) | (byte127 << 20) | (byte128 << 12) | (byte129 << 4) | (byte130 >>> 4);
        final long byte131 = blocks[blocksOffset++] & 0xFF;
        final long byte132 = blocks[blocksOffset++] & 0xFF;
        final long byte133 = blocks[blocksOffset++] & 0xFF;
        final long byte134 = blocks[blocksOffset++] & 0xFF;
        final long byte135 = blocks[blocksOffset++] & 0xFF;
        final long byte136 = blocks[blocksOffset++] & 0xFF;
        final long byte137 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte130 & 15) << 54) | (byte131 << 46) | (byte132 << 38) | (byte133 << 30) | (byte134 << 22) | (byte135 << 14) | (byte136 << 6) | (byte137 >>> 2);
        final long byte138 = blocks[blocksOffset++] & 0xFF;
        final long byte139 = blocks[blocksOffset++] & 0xFF;
        final long byte140 = blocks[blocksOffset++] & 0xFF;
        final long byte141 = blocks[blocksOffset++] & 0xFF;
        final long byte142 = blocks[blocksOffset++] & 0xFF;
        final long byte143 = blocks[blocksOffset++] & 0xFF;
        final long byte144 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte137 & 3) << 56) | (byte138 << 48) | (byte139 << 40) | (byte140 << 32) | (byte141 << 24) | (byte142 << 16) | (byte143 << 8) | byte144;
        final long byte145 = blocks[blocksOffset++] & 0xFF;
        final long byte146 = blocks[blocksOffset++] & 0xFF;
        final long byte147 = blocks[blocksOffset++] & 0xFF;
        final long byte148 = blocks[blocksOffset++] & 0xFF;
        final long byte149 = blocks[blocksOffset++] & 0xFF;
        final long byte150 = blocks[blocksOffset++] & 0xFF;
        final long byte151 = blocks[blocksOffset++] & 0xFF;
        final long byte152 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte145 << 50) | (byte146 << 42) | (byte147 << 34) | (byte148 << 26) | (byte149 << 18) | (byte150 << 10) | (byte151 << 2) | (byte152 >>> 6);
        final long byte153 = blocks[blocksOffset++] & 0xFF;
        final long byte154 = blocks[blocksOffset++] & 0xFF;
        final long byte155 = blocks[blocksOffset++] & 0xFF;
        final long byte156 = blocks[blocksOffset++] & 0xFF;
        final long byte157 = blocks[blocksOffset++] & 0xFF;
        final long byte158 = blocks[blocksOffset++] & 0xFF;
        final long byte159 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte152 & 63) << 52) | (byte153 << 44) | (byte154 << 36) | (byte155 << 28) | (byte156 << 20) | (byte157 << 12) | (byte158 << 4) | (byte159 >>> 4);
        final long byte160 = blocks[blocksOffset++] & 0xFF;
        final long byte161 = blocks[blocksOffset++] & 0xFF;
        final long byte162 = blocks[blocksOffset++] & 0xFF;
        final long byte163 = blocks[blocksOffset++] & 0xFF;
        final long byte164 = blocks[blocksOffset++] & 0xFF;
        final long byte165 = blocks[blocksOffset++] & 0xFF;
        final long byte166 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte159 & 15) << 54) | (byte160 << 46) | (byte161 << 38) | (byte162 << 30) | (byte163 << 22) | (byte164 << 14) | (byte165 << 6) | (byte166 >>> 2);
        final long byte167 = blocks[blocksOffset++] & 0xFF;
        final long byte168 = blocks[blocksOffset++] & 0xFF;
        final long byte169 = blocks[blocksOffset++] & 0xFF;
        final long byte170 = blocks[blocksOffset++] & 0xFF;
        final long byte171 = blocks[blocksOffset++] & 0xFF;
        final long byte172 = blocks[blocksOffset++] & 0xFF;
        final long byte173 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte166 & 3) << 56) | (byte167 << 48) | (byte168 << 40) | (byte169 << 32) | (byte170 << 24) | (byte171 << 16) | (byte172 << 8) | byte173;
        final long byte174 = blocks[blocksOffset++] & 0xFF;
        final long byte175 = blocks[blocksOffset++] & 0xFF;
        final long byte176 = blocks[blocksOffset++] & 0xFF;
        final long byte177 = blocks[blocksOffset++] & 0xFF;
        final long byte178 = blocks[blocksOffset++] & 0xFF;
        final long byte179 = blocks[blocksOffset++] & 0xFF;
        final long byte180 = blocks[blocksOffset++] & 0xFF;
        final long byte181 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte174 << 50) | (byte175 << 42) | (byte176 << 34) | (byte177 << 26) | (byte178 << 18) | (byte179 << 10) | (byte180 << 2) | (byte181 >>> 6);
        final long byte182 = blocks[blocksOffset++] & 0xFF;
        final long byte183 = blocks[blocksOffset++] & 0xFF;
        final long byte184 = blocks[blocksOffset++] & 0xFF;
        final long byte185 = blocks[blocksOffset++] & 0xFF;
        final long byte186 = blocks[blocksOffset++] & 0xFF;
        final long byte187 = blocks[blocksOffset++] & 0xFF;
        final long byte188 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte181 & 63) << 52) | (byte182 << 44) | (byte183 << 36) | (byte184 << 28) | (byte185 << 20) | (byte186 << 12) | (byte187 << 4) | (byte188 >>> 4);
        final long byte189 = blocks[blocksOffset++] & 0xFF;
        final long byte190 = blocks[blocksOffset++] & 0xFF;
        final long byte191 = blocks[blocksOffset++] & 0xFF;
        final long byte192 = blocks[blocksOffset++] & 0xFF;
        final long byte193 = blocks[blocksOffset++] & 0xFF;
        final long byte194 = blocks[blocksOffset++] & 0xFF;
        final long byte195 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte188 & 15) << 54) | (byte189 << 46) | (byte190 << 38) | (byte191 << 30) | (byte192 << 22) | (byte193 << 14) | (byte194 << 6) | (byte195 >>> 2);
        final long byte196 = blocks[blocksOffset++] & 0xFF;
        final long byte197 = blocks[blocksOffset++] & 0xFF;
        final long byte198 = blocks[blocksOffset++] & 0xFF;
        final long byte199 = blocks[blocksOffset++] & 0xFF;
        final long byte200 = blocks[blocksOffset++] & 0xFF;
        final long byte201 = blocks[blocksOffset++] & 0xFF;
        final long byte202 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte195 & 3) << 56) | (byte196 << 48) | (byte197 << 40) | (byte198 << 32) | (byte199 << 24) | (byte200 << 16) | (byte201 << 8) | byte202;
        final long byte203 = blocks[blocksOffset++] & 0xFF;
        final long byte204 = blocks[blocksOffset++] & 0xFF;
        final long byte205 = blocks[blocksOffset++] & 0xFF;
        final long byte206 = blocks[blocksOffset++] & 0xFF;
        final long byte207 = blocks[blocksOffset++] & 0xFF;
        final long byte208 = blocks[blocksOffset++] & 0xFF;
        final long byte209 = blocks[blocksOffset++] & 0xFF;
        final long byte210 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte203 << 50) | (byte204 << 42) | (byte205 << 34) | (byte206 << 26) | (byte207 << 18) | (byte208 << 10) | (byte209 << 2) | (byte210 >>> 6);
        final long byte211 = blocks[blocksOffset++] & 0xFF;
        final long byte212 = blocks[blocksOffset++] & 0xFF;
        final long byte213 = blocks[blocksOffset++] & 0xFF;
        final long byte214 = blocks[blocksOffset++] & 0xFF;
        final long byte215 = blocks[blocksOffset++] & 0xFF;
        final long byte216 = blocks[blocksOffset++] & 0xFF;
        final long byte217 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte210 & 63) << 52) | (byte211 << 44) | (byte212 << 36) | (byte213 << 28) | (byte214 << 20) | (byte215 << 12) | (byte216 << 4) | (byte217 >>> 4);
        final long byte218 = blocks[blocksOffset++] & 0xFF;
        final long byte219 = blocks[blocksOffset++] & 0xFF;
        final long byte220 = blocks[blocksOffset++] & 0xFF;
        final long byte221 = blocks[blocksOffset++] & 0xFF;
        final long byte222 = blocks[blocksOffset++] & 0xFF;
        final long byte223 = blocks[blocksOffset++] & 0xFF;
        final long byte224 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte217 & 15) << 54) | (byte218 << 46) | (byte219 << 38) | (byte220 << 30) | (byte221 << 22) | (byte222 << 14) | (byte223 << 6) | (byte224 >>> 2);
        final long byte225 = blocks[blocksOffset++] & 0xFF;
        final long byte226 = blocks[blocksOffset++] & 0xFF;
        final long byte227 = blocks[blocksOffset++] & 0xFF;
        final long byte228 = blocks[blocksOffset++] & 0xFF;
        final long byte229 = blocks[blocksOffset++] & 0xFF;
        final long byte230 = blocks[blocksOffset++] & 0xFF;
        final long byte231 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte224 & 3) << 56) | (byte225 << 48) | (byte226 << 40) | (byte227 << 32) | (byte228 << 24) | (byte229 << 16) | (byte230 << 8) | byte231;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 52);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 46);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 40);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 34);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset] & 0xffffffffL) >>> 28);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset] & 0xffffffffL) >>> 22);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 56);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 50);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 44);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 38);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset] & 0xffffffffL) >>> 32);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset] & 0xffffffffL) >>> 26);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 54);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 48);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 42);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset] & 0xffffffffL) >>> 36);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset] & 0xffffffffL) >>> 30);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 52);
        blocks[blocksOffset++] = (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 46);
        blocks[blocksOffset++] = (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 40);
        blocks[blocksOffset++] = (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 34);
        blocks[blocksOffset++] = (values[valuesOffset++] << 30) | (values[valuesOffset] >>> 28);
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset] >>> 22);
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 56);
        blocks[blocksOffset++] = (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 50);
        blocks[blocksOffset++] = (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 44);
        blocks[blocksOffset++] = (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 38);
        blocks[blocksOffset++] = (values[valuesOffset++] << 26) | (values[valuesOffset] >>> 32);
        blocks[blocksOffset++] = (values[valuesOffset++] << 32) | (values[valuesOffset] >>> 26);
        blocks[blocksOffset++] = (values[valuesOffset++] << 38) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 54);
        blocks[blocksOffset++] = (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 48);
        blocks[blocksOffset++] = (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 42);
        blocks[blocksOffset++] = (values[valuesOffset++] << 22) | (values[valuesOffset] >>> 36);
        blocks[blocksOffset++] = (values[valuesOffset++] << 28) | (values[valuesOffset] >>> 30);
        blocks[blocksOffset++] = (values[valuesOffset++] << 34) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | values[valuesOffset++];
      }
    }

}
