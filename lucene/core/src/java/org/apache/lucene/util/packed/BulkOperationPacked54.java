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
final class BulkOperationPacked54 extends BulkOperation {
    public int blockCount() {
      return 27;
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
        values[valuesOffset++] = block0 >>> 10;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 1023L) << 44) | (block1 >>> 20);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 1048575L) << 34) | (block2 >>> 30);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 1073741823L) << 24) | (block3 >>> 40);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 1099511627775L) << 14) | (block4 >>> 50);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 1125899906842623L) << 4) | (block5 >>> 60);
        values[valuesOffset++] = (block5 >>> 6) & 18014398509481983L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 63L) << 48) | (block6 >>> 16);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 65535L) << 38) | (block7 >>> 26);
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 67108863L) << 28) | (block8 >>> 36);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 68719476735L) << 18) | (block9 >>> 46);
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 70368744177663L) << 8) | (block10 >>> 56);
        values[valuesOffset++] = (block10 >>> 2) & 18014398509481983L;
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 3L) << 52) | (block11 >>> 12);
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 4095L) << 42) | (block12 >>> 22);
        final long block13 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block12 & 4194303L) << 32) | (block13 >>> 32);
        final long block14 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block13 & 4294967295L) << 22) | (block14 >>> 42);
        final long block15 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block14 & 4398046511103L) << 12) | (block15 >>> 52);
        final long block16 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block15 & 4503599627370495L) << 2) | (block16 >>> 62);
        values[valuesOffset++] = (block16 >>> 8) & 18014398509481983L;
        final long block17 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block16 & 255L) << 46) | (block17 >>> 18);
        final long block18 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block17 & 262143L) << 36) | (block18 >>> 28);
        final long block19 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block18 & 268435455L) << 26) | (block19 >>> 38);
        final long block20 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block19 & 274877906943L) << 16) | (block20 >>> 48);
        final long block21 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block20 & 281474976710655L) << 6) | (block21 >>> 58);
        values[valuesOffset++] = (block21 >>> 4) & 18014398509481983L;
        final long block22 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block21 & 15L) << 50) | (block22 >>> 14);
        final long block23 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block22 & 16383L) << 40) | (block23 >>> 24);
        final long block24 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block23 & 16777215L) << 30) | (block24 >>> 34);
        final long block25 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block24 & 17179869183L) << 20) | (block25 >>> 44);
        final long block26 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block25 & 17592186044415L) << 10) | (block26 >>> 54);
        values[valuesOffset++] = block26 & 18014398509481983L;
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
        values[valuesOffset++] = (byte0 << 46) | (byte1 << 38) | (byte2 << 30) | (byte3 << 22) | (byte4 << 14) | (byte5 << 6) | (byte6 >>> 2);
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 3) << 52) | (byte7 << 44) | (byte8 << 36) | (byte9 << 28) | (byte10 << 20) | (byte11 << 12) | (byte12 << 4) | (byte13 >>> 4);
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 15) << 50) | (byte14 << 42) | (byte15 << 34) | (byte16 << 26) | (byte17 << 18) | (byte18 << 10) | (byte19 << 2) | (byte20 >>> 6);
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte20 & 63) << 48) | (byte21 << 40) | (byte22 << 32) | (byte23 << 24) | (byte24 << 16) | (byte25 << 8) | byte26;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte27 << 46) | (byte28 << 38) | (byte29 << 30) | (byte30 << 22) | (byte31 << 14) | (byte32 << 6) | (byte33 >>> 2);
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 52) | (byte34 << 44) | (byte35 << 36) | (byte36 << 28) | (byte37 << 20) | (byte38 << 12) | (byte39 << 4) | (byte40 >>> 4);
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte40 & 15) << 50) | (byte41 << 42) | (byte42 << 34) | (byte43 << 26) | (byte44 << 18) | (byte45 << 10) | (byte46 << 2) | (byte47 >>> 6);
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 63) << 48) | (byte48 << 40) | (byte49 << 32) | (byte50 << 24) | (byte51 << 16) | (byte52 << 8) | byte53;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte54 << 46) | (byte55 << 38) | (byte56 << 30) | (byte57 << 22) | (byte58 << 14) | (byte59 << 6) | (byte60 >>> 2);
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte60 & 3) << 52) | (byte61 << 44) | (byte62 << 36) | (byte63 << 28) | (byte64 << 20) | (byte65 << 12) | (byte66 << 4) | (byte67 >>> 4);
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte67 & 15) << 50) | (byte68 << 42) | (byte69 << 34) | (byte70 << 26) | (byte71 << 18) | (byte72 << 10) | (byte73 << 2) | (byte74 >>> 6);
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte74 & 63) << 48) | (byte75 << 40) | (byte76 << 32) | (byte77 << 24) | (byte78 << 16) | (byte79 << 8) | byte80;
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte81 << 46) | (byte82 << 38) | (byte83 << 30) | (byte84 << 22) | (byte85 << 14) | (byte86 << 6) | (byte87 >>> 2);
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte87 & 3) << 52) | (byte88 << 44) | (byte89 << 36) | (byte90 << 28) | (byte91 << 20) | (byte92 << 12) | (byte93 << 4) | (byte94 >>> 4);
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte94 & 15) << 50) | (byte95 << 42) | (byte96 << 34) | (byte97 << 26) | (byte98 << 18) | (byte99 << 10) | (byte100 << 2) | (byte101 >>> 6);
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        final long byte104 = blocks[blocksOffset++] & 0xFF;
        final long byte105 = blocks[blocksOffset++] & 0xFF;
        final long byte106 = blocks[blocksOffset++] & 0xFF;
        final long byte107 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte101 & 63) << 48) | (byte102 << 40) | (byte103 << 32) | (byte104 << 24) | (byte105 << 16) | (byte106 << 8) | byte107;
        final long byte108 = blocks[blocksOffset++] & 0xFF;
        final long byte109 = blocks[blocksOffset++] & 0xFF;
        final long byte110 = blocks[blocksOffset++] & 0xFF;
        final long byte111 = blocks[blocksOffset++] & 0xFF;
        final long byte112 = blocks[blocksOffset++] & 0xFF;
        final long byte113 = blocks[blocksOffset++] & 0xFF;
        final long byte114 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte108 << 46) | (byte109 << 38) | (byte110 << 30) | (byte111 << 22) | (byte112 << 14) | (byte113 << 6) | (byte114 >>> 2);
        final long byte115 = blocks[blocksOffset++] & 0xFF;
        final long byte116 = blocks[blocksOffset++] & 0xFF;
        final long byte117 = blocks[blocksOffset++] & 0xFF;
        final long byte118 = blocks[blocksOffset++] & 0xFF;
        final long byte119 = blocks[blocksOffset++] & 0xFF;
        final long byte120 = blocks[blocksOffset++] & 0xFF;
        final long byte121 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte114 & 3) << 52) | (byte115 << 44) | (byte116 << 36) | (byte117 << 28) | (byte118 << 20) | (byte119 << 12) | (byte120 << 4) | (byte121 >>> 4);
        final long byte122 = blocks[blocksOffset++] & 0xFF;
        final long byte123 = blocks[blocksOffset++] & 0xFF;
        final long byte124 = blocks[blocksOffset++] & 0xFF;
        final long byte125 = blocks[blocksOffset++] & 0xFF;
        final long byte126 = blocks[blocksOffset++] & 0xFF;
        final long byte127 = blocks[blocksOffset++] & 0xFF;
        final long byte128 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte121 & 15) << 50) | (byte122 << 42) | (byte123 << 34) | (byte124 << 26) | (byte125 << 18) | (byte126 << 10) | (byte127 << 2) | (byte128 >>> 6);
        final long byte129 = blocks[blocksOffset++] & 0xFF;
        final long byte130 = blocks[blocksOffset++] & 0xFF;
        final long byte131 = blocks[blocksOffset++] & 0xFF;
        final long byte132 = blocks[blocksOffset++] & 0xFF;
        final long byte133 = blocks[blocksOffset++] & 0xFF;
        final long byte134 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte128 & 63) << 48) | (byte129 << 40) | (byte130 << 32) | (byte131 << 24) | (byte132 << 16) | (byte133 << 8) | byte134;
        final long byte135 = blocks[blocksOffset++] & 0xFF;
        final long byte136 = blocks[blocksOffset++] & 0xFF;
        final long byte137 = blocks[blocksOffset++] & 0xFF;
        final long byte138 = blocks[blocksOffset++] & 0xFF;
        final long byte139 = blocks[blocksOffset++] & 0xFF;
        final long byte140 = blocks[blocksOffset++] & 0xFF;
        final long byte141 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte135 << 46) | (byte136 << 38) | (byte137 << 30) | (byte138 << 22) | (byte139 << 14) | (byte140 << 6) | (byte141 >>> 2);
        final long byte142 = blocks[blocksOffset++] & 0xFF;
        final long byte143 = blocks[blocksOffset++] & 0xFF;
        final long byte144 = blocks[blocksOffset++] & 0xFF;
        final long byte145 = blocks[blocksOffset++] & 0xFF;
        final long byte146 = blocks[blocksOffset++] & 0xFF;
        final long byte147 = blocks[blocksOffset++] & 0xFF;
        final long byte148 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte141 & 3) << 52) | (byte142 << 44) | (byte143 << 36) | (byte144 << 28) | (byte145 << 20) | (byte146 << 12) | (byte147 << 4) | (byte148 >>> 4);
        final long byte149 = blocks[blocksOffset++] & 0xFF;
        final long byte150 = blocks[blocksOffset++] & 0xFF;
        final long byte151 = blocks[blocksOffset++] & 0xFF;
        final long byte152 = blocks[blocksOffset++] & 0xFF;
        final long byte153 = blocks[blocksOffset++] & 0xFF;
        final long byte154 = blocks[blocksOffset++] & 0xFF;
        final long byte155 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte148 & 15) << 50) | (byte149 << 42) | (byte150 << 34) | (byte151 << 26) | (byte152 << 18) | (byte153 << 10) | (byte154 << 2) | (byte155 >>> 6);
        final long byte156 = blocks[blocksOffset++] & 0xFF;
        final long byte157 = blocks[blocksOffset++] & 0xFF;
        final long byte158 = blocks[blocksOffset++] & 0xFF;
        final long byte159 = blocks[blocksOffset++] & 0xFF;
        final long byte160 = blocks[blocksOffset++] & 0xFF;
        final long byte161 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte155 & 63) << 48) | (byte156 << 40) | (byte157 << 32) | (byte158 << 24) | (byte159 << 16) | (byte160 << 8) | byte161;
        final long byte162 = blocks[blocksOffset++] & 0xFF;
        final long byte163 = blocks[blocksOffset++] & 0xFF;
        final long byte164 = blocks[blocksOffset++] & 0xFF;
        final long byte165 = blocks[blocksOffset++] & 0xFF;
        final long byte166 = blocks[blocksOffset++] & 0xFF;
        final long byte167 = blocks[blocksOffset++] & 0xFF;
        final long byte168 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte162 << 46) | (byte163 << 38) | (byte164 << 30) | (byte165 << 22) | (byte166 << 14) | (byte167 << 6) | (byte168 >>> 2);
        final long byte169 = blocks[blocksOffset++] & 0xFF;
        final long byte170 = blocks[blocksOffset++] & 0xFF;
        final long byte171 = blocks[blocksOffset++] & 0xFF;
        final long byte172 = blocks[blocksOffset++] & 0xFF;
        final long byte173 = blocks[blocksOffset++] & 0xFF;
        final long byte174 = blocks[blocksOffset++] & 0xFF;
        final long byte175 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte168 & 3) << 52) | (byte169 << 44) | (byte170 << 36) | (byte171 << 28) | (byte172 << 20) | (byte173 << 12) | (byte174 << 4) | (byte175 >>> 4);
        final long byte176 = blocks[blocksOffset++] & 0xFF;
        final long byte177 = blocks[blocksOffset++] & 0xFF;
        final long byte178 = blocks[blocksOffset++] & 0xFF;
        final long byte179 = blocks[blocksOffset++] & 0xFF;
        final long byte180 = blocks[blocksOffset++] & 0xFF;
        final long byte181 = blocks[blocksOffset++] & 0xFF;
        final long byte182 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte175 & 15) << 50) | (byte176 << 42) | (byte177 << 34) | (byte178 << 26) | (byte179 << 18) | (byte180 << 10) | (byte181 << 2) | (byte182 >>> 6);
        final long byte183 = blocks[blocksOffset++] & 0xFF;
        final long byte184 = blocks[blocksOffset++] & 0xFF;
        final long byte185 = blocks[blocksOffset++] & 0xFF;
        final long byte186 = blocks[blocksOffset++] & 0xFF;
        final long byte187 = blocks[blocksOffset++] & 0xFF;
        final long byte188 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte182 & 63) << 48) | (byte183 << 40) | (byte184 << 32) | (byte185 << 24) | (byte186 << 16) | (byte187 << 8) | byte188;
        final long byte189 = blocks[blocksOffset++] & 0xFF;
        final long byte190 = blocks[blocksOffset++] & 0xFF;
        final long byte191 = blocks[blocksOffset++] & 0xFF;
        final long byte192 = blocks[blocksOffset++] & 0xFF;
        final long byte193 = blocks[blocksOffset++] & 0xFF;
        final long byte194 = blocks[blocksOffset++] & 0xFF;
        final long byte195 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte189 << 46) | (byte190 << 38) | (byte191 << 30) | (byte192 << 22) | (byte193 << 14) | (byte194 << 6) | (byte195 >>> 2);
        final long byte196 = blocks[blocksOffset++] & 0xFF;
        final long byte197 = blocks[blocksOffset++] & 0xFF;
        final long byte198 = blocks[blocksOffset++] & 0xFF;
        final long byte199 = blocks[blocksOffset++] & 0xFF;
        final long byte200 = blocks[blocksOffset++] & 0xFF;
        final long byte201 = blocks[blocksOffset++] & 0xFF;
        final long byte202 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte195 & 3) << 52) | (byte196 << 44) | (byte197 << 36) | (byte198 << 28) | (byte199 << 20) | (byte200 << 12) | (byte201 << 4) | (byte202 >>> 4);
        final long byte203 = blocks[blocksOffset++] & 0xFF;
        final long byte204 = blocks[blocksOffset++] & 0xFF;
        final long byte205 = blocks[blocksOffset++] & 0xFF;
        final long byte206 = blocks[blocksOffset++] & 0xFF;
        final long byte207 = blocks[blocksOffset++] & 0xFF;
        final long byte208 = blocks[blocksOffset++] & 0xFF;
        final long byte209 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte202 & 15) << 50) | (byte203 << 42) | (byte204 << 34) | (byte205 << 26) | (byte206 << 18) | (byte207 << 10) | (byte208 << 2) | (byte209 >>> 6);
        final long byte210 = blocks[blocksOffset++] & 0xFF;
        final long byte211 = blocks[blocksOffset++] & 0xFF;
        final long byte212 = blocks[blocksOffset++] & 0xFF;
        final long byte213 = blocks[blocksOffset++] & 0xFF;
        final long byte214 = blocks[blocksOffset++] & 0xFF;
        final long byte215 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte209 & 63) << 48) | (byte210 << 40) | (byte211 << 32) | (byte212 << 24) | (byte213 << 16) | (byte214 << 8) | byte215;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 44);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 34);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 48);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 38);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset] & 0xffffffffL) >>> 28);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 52);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 42);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset] & 0xffffffffL) >>> 32);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset] & 0xffffffffL) >>> 22);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 46);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 36);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset] & 0xffffffffL) >>> 26);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 50);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 40);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 30);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 44);
        blocks[blocksOffset++] = (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 34);
        blocks[blocksOffset++] = (values[valuesOffset++] << 30) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 48);
        blocks[blocksOffset++] = (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 38);
        blocks[blocksOffset++] = (values[valuesOffset++] << 26) | (values[valuesOffset] >>> 28);
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 52);
        blocks[blocksOffset++] = (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 42);
        blocks[blocksOffset++] = (values[valuesOffset++] << 22) | (values[valuesOffset] >>> 32);
        blocks[blocksOffset++] = (values[valuesOffset++] << 32) | (values[valuesOffset] >>> 22);
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 46);
        blocks[blocksOffset++] = (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 36);
        blocks[blocksOffset++] = (values[valuesOffset++] << 28) | (values[valuesOffset] >>> 26);
        blocks[blocksOffset++] = (values[valuesOffset++] << 38) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 50);
        blocks[blocksOffset++] = (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 40);
        blocks[blocksOffset++] = (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 30);
        blocks[blocksOffset++] = (values[valuesOffset++] << 34) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | values[valuesOffset++];
      }
    }

}
