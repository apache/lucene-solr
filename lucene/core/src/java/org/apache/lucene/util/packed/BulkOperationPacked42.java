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
final class BulkOperationPacked42 extends BulkOperation {
    @Override
    public int blockCount() {
      return 21;
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
        values[valuesOffset++] = block0 >>> 22;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 4194303L) << 20) | (block1 >>> 44);
        values[valuesOffset++] = (block1 >>> 2) & 4398046511103L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 3L) << 40) | (block2 >>> 24);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 16777215L) << 18) | (block3 >>> 46);
        values[valuesOffset++] = (block3 >>> 4) & 4398046511103L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 15L) << 38) | (block4 >>> 26);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 67108863L) << 16) | (block5 >>> 48);
        values[valuesOffset++] = (block5 >>> 6) & 4398046511103L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 63L) << 36) | (block6 >>> 28);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 268435455L) << 14) | (block7 >>> 50);
        values[valuesOffset++] = (block7 >>> 8) & 4398046511103L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 255L) << 34) | (block8 >>> 30);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 1073741823L) << 12) | (block9 >>> 52);
        values[valuesOffset++] = (block9 >>> 10) & 4398046511103L;
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 1023L) << 32) | (block10 >>> 32);
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 4294967295L) << 10) | (block11 >>> 54);
        values[valuesOffset++] = (block11 >>> 12) & 4398046511103L;
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 4095L) << 30) | (block12 >>> 34);
        final long block13 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block12 & 17179869183L) << 8) | (block13 >>> 56);
        values[valuesOffset++] = (block13 >>> 14) & 4398046511103L;
        final long block14 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block13 & 16383L) << 28) | (block14 >>> 36);
        final long block15 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block14 & 68719476735L) << 6) | (block15 >>> 58);
        values[valuesOffset++] = (block15 >>> 16) & 4398046511103L;
        final long block16 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block15 & 65535L) << 26) | (block16 >>> 38);
        final long block17 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block16 & 274877906943L) << 4) | (block17 >>> 60);
        values[valuesOffset++] = (block17 >>> 18) & 4398046511103L;
        final long block18 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block17 & 262143L) << 24) | (block18 >>> 40);
        final long block19 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block18 & 1099511627775L) << 2) | (block19 >>> 62);
        values[valuesOffset++] = (block19 >>> 20) & 4398046511103L;
        final long block20 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block19 & 1048575L) << 22) | (block20 >>> 42);
        values[valuesOffset++] = block20 & 4398046511103L;
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
        values[valuesOffset++] = (byte0 << 34) | (byte1 << 26) | (byte2 << 18) | (byte3 << 10) | (byte4 << 2) | (byte5 >>> 6);
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 63) << 36) | (byte6 << 28) | (byte7 << 20) | (byte8 << 12) | (byte9 << 4) | (byte10 >>> 4);
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 15) << 38) | (byte11 << 30) | (byte12 << 22) | (byte13 << 14) | (byte14 << 6) | (byte15 >>> 2);
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 40) | (byte16 << 32) | (byte17 << 24) | (byte18 << 16) | (byte19 << 8) | byte20;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte21 << 34) | (byte22 << 26) | (byte23 << 18) | (byte24 << 10) | (byte25 << 2) | (byte26 >>> 6);
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 63) << 36) | (byte27 << 28) | (byte28 << 20) | (byte29 << 12) | (byte30 << 4) | (byte31 >>> 4);
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 38) | (byte32 << 30) | (byte33 << 22) | (byte34 << 14) | (byte35 << 6) | (byte36 >>> 2);
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte36 & 3) << 40) | (byte37 << 32) | (byte38 << 24) | (byte39 << 16) | (byte40 << 8) | byte41;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte42 << 34) | (byte43 << 26) | (byte44 << 18) | (byte45 << 10) | (byte46 << 2) | (byte47 >>> 6);
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 63) << 36) | (byte48 << 28) | (byte49 << 20) | (byte50 << 12) | (byte51 << 4) | (byte52 >>> 4);
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 15) << 38) | (byte53 << 30) | (byte54 << 22) | (byte55 << 14) | (byte56 << 6) | (byte57 >>> 2);
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte57 & 3) << 40) | (byte58 << 32) | (byte59 << 24) | (byte60 << 16) | (byte61 << 8) | byte62;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte63 << 34) | (byte64 << 26) | (byte65 << 18) | (byte66 << 10) | (byte67 << 2) | (byte68 >>> 6);
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte68 & 63) << 36) | (byte69 << 28) | (byte70 << 20) | (byte71 << 12) | (byte72 << 4) | (byte73 >>> 4);
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte73 & 15) << 38) | (byte74 << 30) | (byte75 << 22) | (byte76 << 14) | (byte77 << 6) | (byte78 >>> 2);
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte78 & 3) << 40) | (byte79 << 32) | (byte80 << 24) | (byte81 << 16) | (byte82 << 8) | byte83;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte84 << 34) | (byte85 << 26) | (byte86 << 18) | (byte87 << 10) | (byte88 << 2) | (byte89 >>> 6);
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte89 & 63) << 36) | (byte90 << 28) | (byte91 << 20) | (byte92 << 12) | (byte93 << 4) | (byte94 >>> 4);
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte94 & 15) << 38) | (byte95 << 30) | (byte96 << 22) | (byte97 << 14) | (byte98 << 6) | (byte99 >>> 2);
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        final long byte104 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte99 & 3) << 40) | (byte100 << 32) | (byte101 << 24) | (byte102 << 16) | (byte103 << 8) | byte104;
        final long byte105 = blocks[blocksOffset++] & 0xFF;
        final long byte106 = blocks[blocksOffset++] & 0xFF;
        final long byte107 = blocks[blocksOffset++] & 0xFF;
        final long byte108 = blocks[blocksOffset++] & 0xFF;
        final long byte109 = blocks[blocksOffset++] & 0xFF;
        final long byte110 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte105 << 34) | (byte106 << 26) | (byte107 << 18) | (byte108 << 10) | (byte109 << 2) | (byte110 >>> 6);
        final long byte111 = blocks[blocksOffset++] & 0xFF;
        final long byte112 = blocks[blocksOffset++] & 0xFF;
        final long byte113 = blocks[blocksOffset++] & 0xFF;
        final long byte114 = blocks[blocksOffset++] & 0xFF;
        final long byte115 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte110 & 63) << 36) | (byte111 << 28) | (byte112 << 20) | (byte113 << 12) | (byte114 << 4) | (byte115 >>> 4);
        final long byte116 = blocks[blocksOffset++] & 0xFF;
        final long byte117 = blocks[blocksOffset++] & 0xFF;
        final long byte118 = blocks[blocksOffset++] & 0xFF;
        final long byte119 = blocks[blocksOffset++] & 0xFF;
        final long byte120 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte115 & 15) << 38) | (byte116 << 30) | (byte117 << 22) | (byte118 << 14) | (byte119 << 6) | (byte120 >>> 2);
        final long byte121 = blocks[blocksOffset++] & 0xFF;
        final long byte122 = blocks[blocksOffset++] & 0xFF;
        final long byte123 = blocks[blocksOffset++] & 0xFF;
        final long byte124 = blocks[blocksOffset++] & 0xFF;
        final long byte125 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte120 & 3) << 40) | (byte121 << 32) | (byte122 << 24) | (byte123 << 16) | (byte124 << 8) | byte125;
        final long byte126 = blocks[blocksOffset++] & 0xFF;
        final long byte127 = blocks[blocksOffset++] & 0xFF;
        final long byte128 = blocks[blocksOffset++] & 0xFF;
        final long byte129 = blocks[blocksOffset++] & 0xFF;
        final long byte130 = blocks[blocksOffset++] & 0xFF;
        final long byte131 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte126 << 34) | (byte127 << 26) | (byte128 << 18) | (byte129 << 10) | (byte130 << 2) | (byte131 >>> 6);
        final long byte132 = blocks[blocksOffset++] & 0xFF;
        final long byte133 = blocks[blocksOffset++] & 0xFF;
        final long byte134 = blocks[blocksOffset++] & 0xFF;
        final long byte135 = blocks[blocksOffset++] & 0xFF;
        final long byte136 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte131 & 63) << 36) | (byte132 << 28) | (byte133 << 20) | (byte134 << 12) | (byte135 << 4) | (byte136 >>> 4);
        final long byte137 = blocks[blocksOffset++] & 0xFF;
        final long byte138 = blocks[blocksOffset++] & 0xFF;
        final long byte139 = blocks[blocksOffset++] & 0xFF;
        final long byte140 = blocks[blocksOffset++] & 0xFF;
        final long byte141 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte136 & 15) << 38) | (byte137 << 30) | (byte138 << 22) | (byte139 << 14) | (byte140 << 6) | (byte141 >>> 2);
        final long byte142 = blocks[blocksOffset++] & 0xFF;
        final long byte143 = blocks[blocksOffset++] & 0xFF;
        final long byte144 = blocks[blocksOffset++] & 0xFF;
        final long byte145 = blocks[blocksOffset++] & 0xFF;
        final long byte146 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte141 & 3) << 40) | (byte142 << 32) | (byte143 << 24) | (byte144 << 16) | (byte145 << 8) | byte146;
        final long byte147 = blocks[blocksOffset++] & 0xFF;
        final long byte148 = blocks[blocksOffset++] & 0xFF;
        final long byte149 = blocks[blocksOffset++] & 0xFF;
        final long byte150 = blocks[blocksOffset++] & 0xFF;
        final long byte151 = blocks[blocksOffset++] & 0xFF;
        final long byte152 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte147 << 34) | (byte148 << 26) | (byte149 << 18) | (byte150 << 10) | (byte151 << 2) | (byte152 >>> 6);
        final long byte153 = blocks[blocksOffset++] & 0xFF;
        final long byte154 = blocks[blocksOffset++] & 0xFF;
        final long byte155 = blocks[blocksOffset++] & 0xFF;
        final long byte156 = blocks[blocksOffset++] & 0xFF;
        final long byte157 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte152 & 63) << 36) | (byte153 << 28) | (byte154 << 20) | (byte155 << 12) | (byte156 << 4) | (byte157 >>> 4);
        final long byte158 = blocks[blocksOffset++] & 0xFF;
        final long byte159 = blocks[blocksOffset++] & 0xFF;
        final long byte160 = blocks[blocksOffset++] & 0xFF;
        final long byte161 = blocks[blocksOffset++] & 0xFF;
        final long byte162 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte157 & 15) << 38) | (byte158 << 30) | (byte159 << 22) | (byte160 << 14) | (byte161 << 6) | (byte162 >>> 2);
        final long byte163 = blocks[blocksOffset++] & 0xFF;
        final long byte164 = blocks[blocksOffset++] & 0xFF;
        final long byte165 = blocks[blocksOffset++] & 0xFF;
        final long byte166 = blocks[blocksOffset++] & 0xFF;
        final long byte167 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte162 & 3) << 40) | (byte163 << 32) | (byte164 << 24) | (byte165 << 16) | (byte166 << 8) | byte167;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 40);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 38);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 36);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 34);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 32);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 30);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 28);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 26);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 22);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 22) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 40);
        blocks[blocksOffset++] = (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 38);
        blocks[blocksOffset++] = (values[valuesOffset++] << 26) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 36);
        blocks[blocksOffset++] = (values[valuesOffset++] << 28) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 34);
        blocks[blocksOffset++] = (values[valuesOffset++] << 30) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 32);
        blocks[blocksOffset++] = (values[valuesOffset++] << 32) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 30);
        blocks[blocksOffset++] = (values[valuesOffset++] << 34) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 28);
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 26);
        blocks[blocksOffset++] = (values[valuesOffset++] << 38) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 22);
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | values[valuesOffset++];
      }
    }

}
