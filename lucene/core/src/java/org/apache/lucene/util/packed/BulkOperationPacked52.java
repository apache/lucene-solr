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
final class BulkOperationPacked52 extends BulkOperation {
    @Override
    public int blockCount() {
      return 13;
    }

    @Override
    public int valueCount() {
      return 16;
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
        values[valuesOffset++] = block0 >>> 12;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 4095L) << 40) | (block1 >>> 24);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 16777215L) << 28) | (block2 >>> 36);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 68719476735L) << 16) | (block3 >>> 48);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 281474976710655L) << 4) | (block4 >>> 60);
        values[valuesOffset++] = (block4 >>> 8) & 4503599627370495L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 255L) << 44) | (block5 >>> 20);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 1048575L) << 32) | (block6 >>> 32);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 4294967295L) << 20) | (block7 >>> 44);
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 17592186044415L) << 8) | (block8 >>> 56);
        values[valuesOffset++] = (block8 >>> 4) & 4503599627370495L;
        final long block9 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block8 & 15L) << 48) | (block9 >>> 16);
        final long block10 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block9 & 65535L) << 36) | (block10 >>> 28);
        final long block11 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block10 & 268435455L) << 24) | (block11 >>> 40);
        final long block12 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block11 & 1099511627775L) << 12) | (block12 >>> 52);
        values[valuesOffset++] = block12 & 4503599627370495L;
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
        values[valuesOffset++] = (byte0 << 44) | (byte1 << 36) | (byte2 << 28) | (byte3 << 20) | (byte4 << 12) | (byte5 << 4) | (byte6 >>> 4);
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 15) << 48) | (byte7 << 40) | (byte8 << 32) | (byte9 << 24) | (byte10 << 16) | (byte11 << 8) | byte12;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte13 << 44) | (byte14 << 36) | (byte15 << 28) | (byte16 << 20) | (byte17 << 12) | (byte18 << 4) | (byte19 >>> 4);
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 15) << 48) | (byte20 << 40) | (byte21 << 32) | (byte22 << 24) | (byte23 << 16) | (byte24 << 8) | byte25;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte26 << 44) | (byte27 << 36) | (byte28 << 28) | (byte29 << 20) | (byte30 << 12) | (byte31 << 4) | (byte32 >>> 4);
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte32 & 15) << 48) | (byte33 << 40) | (byte34 << 32) | (byte35 << 24) | (byte36 << 16) | (byte37 << 8) | byte38;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte39 << 44) | (byte40 << 36) | (byte41 << 28) | (byte42 << 20) | (byte43 << 12) | (byte44 << 4) | (byte45 >>> 4);
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 15) << 48) | (byte46 << 40) | (byte47 << 32) | (byte48 << 24) | (byte49 << 16) | (byte50 << 8) | byte51;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte52 << 44) | (byte53 << 36) | (byte54 << 28) | (byte55 << 20) | (byte56 << 12) | (byte57 << 4) | (byte58 >>> 4);
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte58 & 15) << 48) | (byte59 << 40) | (byte60 << 32) | (byte61 << 24) | (byte62 << 16) | (byte63 << 8) | byte64;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte65 << 44) | (byte66 << 36) | (byte67 << 28) | (byte68 << 20) | (byte69 << 12) | (byte70 << 4) | (byte71 >>> 4);
        final long byte72 = blocks[blocksOffset++] & 0xFF;
        final long byte73 = blocks[blocksOffset++] & 0xFF;
        final long byte74 = blocks[blocksOffset++] & 0xFF;
        final long byte75 = blocks[blocksOffset++] & 0xFF;
        final long byte76 = blocks[blocksOffset++] & 0xFF;
        final long byte77 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte71 & 15) << 48) | (byte72 << 40) | (byte73 << 32) | (byte74 << 24) | (byte75 << 16) | (byte76 << 8) | byte77;
        final long byte78 = blocks[blocksOffset++] & 0xFF;
        final long byte79 = blocks[blocksOffset++] & 0xFF;
        final long byte80 = blocks[blocksOffset++] & 0xFF;
        final long byte81 = blocks[blocksOffset++] & 0xFF;
        final long byte82 = blocks[blocksOffset++] & 0xFF;
        final long byte83 = blocks[blocksOffset++] & 0xFF;
        final long byte84 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte78 << 44) | (byte79 << 36) | (byte80 << 28) | (byte81 << 20) | (byte82 << 12) | (byte83 << 4) | (byte84 >>> 4);
        final long byte85 = blocks[blocksOffset++] & 0xFF;
        final long byte86 = blocks[blocksOffset++] & 0xFF;
        final long byte87 = blocks[blocksOffset++] & 0xFF;
        final long byte88 = blocks[blocksOffset++] & 0xFF;
        final long byte89 = blocks[blocksOffset++] & 0xFF;
        final long byte90 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte84 & 15) << 48) | (byte85 << 40) | (byte86 << 32) | (byte87 << 24) | (byte88 << 16) | (byte89 << 8) | byte90;
        final long byte91 = blocks[blocksOffset++] & 0xFF;
        final long byte92 = blocks[blocksOffset++] & 0xFF;
        final long byte93 = blocks[blocksOffset++] & 0xFF;
        final long byte94 = blocks[blocksOffset++] & 0xFF;
        final long byte95 = blocks[blocksOffset++] & 0xFF;
        final long byte96 = blocks[blocksOffset++] & 0xFF;
        final long byte97 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte91 << 44) | (byte92 << 36) | (byte93 << 28) | (byte94 << 20) | (byte95 << 12) | (byte96 << 4) | (byte97 >>> 4);
        final long byte98 = blocks[blocksOffset++] & 0xFF;
        final long byte99 = blocks[blocksOffset++] & 0xFF;
        final long byte100 = blocks[blocksOffset++] & 0xFF;
        final long byte101 = blocks[blocksOffset++] & 0xFF;
        final long byte102 = blocks[blocksOffset++] & 0xFF;
        final long byte103 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte97 & 15) << 48) | (byte98 << 40) | (byte99 << 32) | (byte100 << 24) | (byte101 << 16) | (byte102 << 8) | byte103;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 40);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 28);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 44);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 32);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 48);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 36);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 40);
        blocks[blocksOffset++] = (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 28);
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 44);
        blocks[blocksOffset++] = (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 32);
        blocks[blocksOffset++] = (values[valuesOffset++] << 32) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 48);
        blocks[blocksOffset++] = (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 36);
        blocks[blocksOffset++] = (values[valuesOffset++] << 28) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | values[valuesOffset++];
      }
    }

}
