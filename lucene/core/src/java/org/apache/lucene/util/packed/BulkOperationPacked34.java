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
final class BulkOperationPacked34 extends BulkOperation {
    @Override
    public int blockCount() {
      return 17;
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
        values[valuesOffset++] = block0 >>> 30;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 1073741823L) << 4) | (block1 >>> 60);
        values[valuesOffset++] = (block1 >>> 26) & 17179869183L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 67108863L) << 8) | (block2 >>> 56);
        values[valuesOffset++] = (block2 >>> 22) & 17179869183L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 4194303L) << 12) | (block3 >>> 52);
        values[valuesOffset++] = (block3 >>> 18) & 17179869183L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 262143L) << 16) | (block4 >>> 48);
        values[valuesOffset++] = (block4 >>> 14) & 17179869183L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 16383L) << 20) | (block5 >>> 44);
        values[valuesOffset++] = (block5 >>> 10) & 17179869183L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 1023L) << 24) | (block6 >>> 40);
        values[valuesOffset++] = (block6 >>> 6) & 17179869183L;
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 63L) << 28) | (block7 >>> 36);
        values[valuesOffset++] = (block7 >>> 2) & 17179869183L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 3L) << 32) | (block8 >>> 32);
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 4294967295L) << 2) | (block9 >>> 62);
        values[valuesOffset++] = (block9 >>> 28) & 17179869183L;
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 268435455L) << 6) | (block10 >>> 58);
        values[valuesOffset++] = (block10 >>> 24) & 17179869183L;
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 16777215L) << 10) | (block11 >>> 54);
        values[valuesOffset++] = (block11 >>> 20) & 17179869183L;
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 1048575L) << 14) | (block12 >>> 50);
        values[valuesOffset++] = (block12 >>> 16) & 17179869183L;
        final long block13 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block12 & 65535L) << 18) | (block13 >>> 46);
        values[valuesOffset++] = (block13 >>> 12) & 17179869183L;
        final long block14 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block13 & 4095L) << 22) | (block14 >>> 42);
        values[valuesOffset++] = (block14 >>> 8) & 17179869183L;
        final long block15 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block14 & 255L) << 26) | (block15 >>> 38);
        values[valuesOffset++] = (block15 >>> 4) & 17179869183L;
        final long block16 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block15 & 15L) << 30) | (block16 >>> 34);
        values[valuesOffset++] = block16 & 17179869183L;
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
        values[valuesOffset++] = (byte0 << 26) | (byte1 << 18) | (byte2 << 10) | (byte3 << 2) | (byte4 >>> 6);
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 63) << 28) | (byte5 << 20) | (byte6 << 12) | (byte7 << 4) | (byte8 >>> 4);
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 15) << 30) | (byte9 << 22) | (byte10 << 14) | (byte11 << 6) | (byte12 >>> 2);
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 3) << 32) | (byte13 << 24) | (byte14 << 16) | (byte15 << 8) | byte16;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte17 << 26) | (byte18 << 18) | (byte19 << 10) | (byte20 << 2) | (byte21 >>> 6);
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 63) << 28) | (byte22 << 20) | (byte23 << 12) | (byte24 << 4) | (byte25 >>> 4);
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte25 & 15) << 30) | (byte26 << 22) | (byte27 << 14) | (byte28 << 6) | (byte29 >>> 2);
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 3) << 32) | (byte30 << 24) | (byte31 << 16) | (byte32 << 8) | byte33;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte34 << 26) | (byte35 << 18) | (byte36 << 10) | (byte37 << 2) | (byte38 >>> 6);
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 63) << 28) | (byte39 << 20) | (byte40 << 12) | (byte41 << 4) | (byte42 >>> 4);
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 15) << 30) | (byte43 << 22) | (byte44 << 14) | (byte45 << 6) | (byte46 >>> 2);
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte46 & 3) << 32) | (byte47 << 24) | (byte48 << 16) | (byte49 << 8) | byte50;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte51 << 26) | (byte52 << 18) | (byte53 << 10) | (byte54 << 2) | (byte55 >>> 6);
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte55 & 63) << 28) | (byte56 << 20) | (byte57 << 12) | (byte58 << 4) | (byte59 >>> 4);
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte59 & 15) << 30) | (byte60 << 22) | (byte61 << 14) | (byte62 << 6) | (byte63 >>> 2);
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte63 & 3) << 32) | (byte64 << 24) | (byte65 << 16) | (byte66 << 8) | byte67;
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte68 << 26) | (byte69 << 18) | (byte70 << 10) | (byte71 << 2) | (byte72 >>> 6);
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte72 & 63) << 28) | (byte73 << 20) | (byte74 << 12) | (byte75 << 4) | (byte76 >>> 4);
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte76 & 15) << 30) | (byte77 << 22) | (byte78 << 14) | (byte79 << 6) | (byte80 >>> 2);
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte80 & 3) << 32) | (byte81 << 24) | (byte82 << 16) | (byte83 << 8) | byte84;
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte85 << 26) | (byte86 << 18) | (byte87 << 10) | (byte88 << 2) | (byte89 >>> 6);
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte89 & 63) << 28) | (byte90 << 20) | (byte91 << 12) | (byte92 << 4) | (byte93 >>> 4);
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte93 & 15) << 30) | (byte94 << 22) | (byte95 << 14) | (byte96 << 6) | (byte97 >>> 2);
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte97 & 3) << 32) | (byte98 << 24) | (byte99 << 16) | (byte100 << 8) | byte101;
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        final long byte104 = blocks[blocksOffset++] & 0xFF;
        final long byte105 = blocks[blocksOffset++] & 0xFF;
        final long byte106 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte102 << 26) | (byte103 << 18) | (byte104 << 10) | (byte105 << 2) | (byte106 >>> 6);
        final long byte107 = blocks[blocksOffset++] & 0xFF;
        final long byte108 = blocks[blocksOffset++] & 0xFF;
        final long byte109 = blocks[blocksOffset++] & 0xFF;
        final long byte110 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte106 & 63) << 28) | (byte107 << 20) | (byte108 << 12) | (byte109 << 4) | (byte110 >>> 4);
        final long byte111 = blocks[blocksOffset++] & 0xFF;
        final long byte112 = blocks[blocksOffset++] & 0xFF;
        final long byte113 = blocks[blocksOffset++] & 0xFF;
        final long byte114 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte110 & 15) << 30) | (byte111 << 22) | (byte112 << 14) | (byte113 << 6) | (byte114 >>> 2);
        final long byte115 = blocks[blocksOffset++] & 0xFF;
        final long byte116 = blocks[blocksOffset++] & 0xFF;
        final long byte117 = blocks[blocksOffset++] & 0xFF;
        final long byte118 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte114 & 3) << 32) | (byte115 << 24) | (byte116 << 16) | (byte117 << 8) | byte118;
        final long byte119 = blocks[blocksOffset++] & 0xFF;
        final long byte120 = blocks[blocksOffset++] & 0xFF;
        final long byte121 = blocks[blocksOffset++] & 0xFF;
        final long byte122 = blocks[blocksOffset++] & 0xFF;
        final long byte123 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte119 << 26) | (byte120 << 18) | (byte121 << 10) | (byte122 << 2) | (byte123 >>> 6);
        final long byte124 = blocks[blocksOffset++] & 0xFF;
        final long byte125 = blocks[blocksOffset++] & 0xFF;
        final long byte126 = blocks[blocksOffset++] & 0xFF;
        final long byte127 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte123 & 63) << 28) | (byte124 << 20) | (byte125 << 12) | (byte126 << 4) | (byte127 >>> 4);
        final long byte128 = blocks[blocksOffset++] & 0xFF;
        final long byte129 = blocks[blocksOffset++] & 0xFF;
        final long byte130 = blocks[blocksOffset++] & 0xFF;
        final long byte131 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte127 & 15) << 30) | (byte128 << 22) | (byte129 << 14) | (byte130 << 6) | (byte131 >>> 2);
        final long byte132 = blocks[blocksOffset++] & 0xFF;
        final long byte133 = blocks[blocksOffset++] & 0xFF;
        final long byte134 = blocks[blocksOffset++] & 0xFF;
        final long byte135 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte131 & 3) << 32) | (byte132 << 24) | (byte133 << 16) | (byte134 << 8) | byte135;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 28);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 32);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 14);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 18);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 22);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 26);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 30);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 34) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 30) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 26) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 22) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 18) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset++] << 14) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 28);
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 32);
        blocks[blocksOffset++] = (values[valuesOffset++] << 32) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 28) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 14);
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 18);
        blocks[blocksOffset++] = (values[valuesOffset++] << 46) | (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 22);
        blocks[blocksOffset++] = (values[valuesOffset++] << 42) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 26);
        blocks[blocksOffset++] = (values[valuesOffset++] << 38) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 30);
        blocks[blocksOffset++] = (values[valuesOffset++] << 34) | values[valuesOffset++];
      }
    }

}
