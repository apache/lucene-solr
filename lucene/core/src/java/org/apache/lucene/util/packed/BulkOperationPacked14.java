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
final class BulkOperationPacked14 extends BulkOperation {
    public int blockCount() {
      return 7;
    }

    public int valueCount() {
      return 32;
    }

    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 50);
        values[valuesOffset++] = (int) ((block0 >>> 36) & 16383L);
        values[valuesOffset++] = (int) ((block0 >>> 22) & 16383L);
        values[valuesOffset++] = (int) ((block0 >>> 8) & 16383L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 255L) << 6) | (block1 >>> 58));
        values[valuesOffset++] = (int) ((block1 >>> 44) & 16383L);
        values[valuesOffset++] = (int) ((block1 >>> 30) & 16383L);
        values[valuesOffset++] = (int) ((block1 >>> 16) & 16383L);
        values[valuesOffset++] = (int) ((block1 >>> 2) & 16383L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 3L) << 12) | (block2 >>> 52));
        values[valuesOffset++] = (int) ((block2 >>> 38) & 16383L);
        values[valuesOffset++] = (int) ((block2 >>> 24) & 16383L);
        values[valuesOffset++] = (int) ((block2 >>> 10) & 16383L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 1023L) << 4) | (block3 >>> 60));
        values[valuesOffset++] = (int) ((block3 >>> 46) & 16383L);
        values[valuesOffset++] = (int) ((block3 >>> 32) & 16383L);
        values[valuesOffset++] = (int) ((block3 >>> 18) & 16383L);
        values[valuesOffset++] = (int) ((block3 >>> 4) & 16383L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 15L) << 10) | (block4 >>> 54));
        values[valuesOffset++] = (int) ((block4 >>> 40) & 16383L);
        values[valuesOffset++] = (int) ((block4 >>> 26) & 16383L);
        values[valuesOffset++] = (int) ((block4 >>> 12) & 16383L);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block4 & 4095L) << 2) | (block5 >>> 62));
        values[valuesOffset++] = (int) ((block5 >>> 48) & 16383L);
        values[valuesOffset++] = (int) ((block5 >>> 34) & 16383L);
        values[valuesOffset++] = (int) ((block5 >>> 20) & 16383L);
        values[valuesOffset++] = (int) ((block5 >>> 6) & 16383L);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block5 & 63L) << 8) | (block6 >>> 56));
        values[valuesOffset++] = (int) ((block6 >>> 42) & 16383L);
        values[valuesOffset++] = (int) ((block6 >>> 28) & 16383L);
        values[valuesOffset++] = (int) ((block6 >>> 14) & 16383L);
        values[valuesOffset++] = (int) (block6 & 16383L);
      }
    }

    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 6) | (byte1 >>> 2);
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 3) << 12) | (byte2 << 4) | (byte3 >>> 4);
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 15) << 10) | (byte4 << 2) | (byte5 >>> 6);
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 63) << 8) | byte6;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte7 << 6) | (byte8 >>> 2);
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 3) << 12) | (byte9 << 4) | (byte10 >>> 4);
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 15) << 10) | (byte11 << 2) | (byte12 >>> 6);
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 63) << 8) | byte13;
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte14 << 6) | (byte15 >>> 2);
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 12) | (byte16 << 4) | (byte17 >>> 4);
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 15) << 10) | (byte18 << 2) | (byte19 >>> 6);
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 63) << 8) | byte20;
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte21 << 6) | (byte22 >>> 2);
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 3) << 12) | (byte23 << 4) | (byte24 >>> 4);
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 15) << 10) | (byte25 << 2) | (byte26 >>> 6);
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 63) << 8) | byte27;
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte28 << 6) | (byte29 >>> 2);
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 3) << 12) | (byte30 << 4) | (byte31 >>> 4);
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 10) | (byte32 << 2) | (byte33 >>> 6);
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 63) << 8) | byte34;
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte35 << 6) | (byte36 >>> 2);
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte36 & 3) << 12) | (byte37 << 4) | (byte38 >>> 4);
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        final int byte40 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 15) << 10) | (byte39 << 2) | (byte40 >>> 6);
        final int byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte40 & 63) << 8) | byte41;
        final int byte42 = blocks[blocksOffset++] & 0xFF;
        final int byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte42 << 6) | (byte43 >>> 2);
        final int byte44 = blocks[blocksOffset++] & 0xFF;
        final int byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte43 & 3) << 12) | (byte44 << 4) | (byte45 >>> 4);
        final int byte46 = blocks[blocksOffset++] & 0xFF;
        final int byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 15) << 10) | (byte46 << 2) | (byte47 >>> 6);
        final int byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 63) << 8) | byte48;
        final int byte49 = blocks[blocksOffset++] & 0xFF;
        final int byte50 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte49 << 6) | (byte50 >>> 2);
        final int byte51 = blocks[blocksOffset++] & 0xFF;
        final int byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte50 & 3) << 12) | (byte51 << 4) | (byte52 >>> 4);
        final int byte53 = blocks[blocksOffset++] & 0xFF;
        final int byte54 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 15) << 10) | (byte53 << 2) | (byte54 >>> 6);
        final int byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte54 & 63) << 8) | byte55;
      }
    }

    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 50;
        values[valuesOffset++] = (block0 >>> 36) & 16383L;
        values[valuesOffset++] = (block0 >>> 22) & 16383L;
        values[valuesOffset++] = (block0 >>> 8) & 16383L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 255L) << 6) | (block1 >>> 58);
        values[valuesOffset++] = (block1 >>> 44) & 16383L;
        values[valuesOffset++] = (block1 >>> 30) & 16383L;
        values[valuesOffset++] = (block1 >>> 16) & 16383L;
        values[valuesOffset++] = (block1 >>> 2) & 16383L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 3L) << 12) | (block2 >>> 52);
        values[valuesOffset++] = (block2 >>> 38) & 16383L;
        values[valuesOffset++] = (block2 >>> 24) & 16383L;
        values[valuesOffset++] = (block2 >>> 10) & 16383L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 1023L) << 4) | (block3 >>> 60);
        values[valuesOffset++] = (block3 >>> 46) & 16383L;
        values[valuesOffset++] = (block3 >>> 32) & 16383L;
        values[valuesOffset++] = (block3 >>> 18) & 16383L;
        values[valuesOffset++] = (block3 >>> 4) & 16383L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 15L) << 10) | (block4 >>> 54);
        values[valuesOffset++] = (block4 >>> 40) & 16383L;
        values[valuesOffset++] = (block4 >>> 26) & 16383L;
        values[valuesOffset++] = (block4 >>> 12) & 16383L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 4095L) << 2) | (block5 >>> 62);
        values[valuesOffset++] = (block5 >>> 48) & 16383L;
        values[valuesOffset++] = (block5 >>> 34) & 16383L;
        values[valuesOffset++] = (block5 >>> 20) & 16383L;
        values[valuesOffset++] = (block5 >>> 6) & 16383L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 63L) << 8) | (block6 >>> 56);
        values[valuesOffset++] = (block6 >>> 42) & 16383L;
        values[valuesOffset++] = (block6 >>> 28) & 16383L;
        values[valuesOffset++] = (block6 >>> 14) & 16383L;
        values[valuesOffset++] = block6 & 16383L;
      }
    }

    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 6) | (byte1 >>> 2);
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 3) << 12) | (byte2 << 4) | (byte3 >>> 4);
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 15) << 10) | (byte4 << 2) | (byte5 >>> 6);
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte5 & 63) << 8) | byte6;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte7 << 6) | (byte8 >>> 2);
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte8 & 3) << 12) | (byte9 << 4) | (byte10 >>> 4);
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 15) << 10) | (byte11 << 2) | (byte12 >>> 6);
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 63) << 8) | byte13;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte14 << 6) | (byte15 >>> 2);
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 12) | (byte16 << 4) | (byte17 >>> 4);
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 15) << 10) | (byte18 << 2) | (byte19 >>> 6);
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 63) << 8) | byte20;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte21 << 6) | (byte22 >>> 2);
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 3) << 12) | (byte23 << 4) | (byte24 >>> 4);
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 15) << 10) | (byte25 << 2) | (byte26 >>> 6);
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte26 & 63) << 8) | byte27;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte28 << 6) | (byte29 >>> 2);
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 3) << 12) | (byte30 << 4) | (byte31 >>> 4);
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 10) | (byte32 << 2) | (byte33 >>> 6);
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 63) << 8) | byte34;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte35 << 6) | (byte36 >>> 2);
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte36 & 3) << 12) | (byte37 << 4) | (byte38 >>> 4);
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 15) << 10) | (byte39 << 2) | (byte40 >>> 6);
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte40 & 63) << 8) | byte41;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte42 << 6) | (byte43 >>> 2);
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte43 & 3) << 12) | (byte44 << 4) | (byte45 >>> 4);
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 15) << 10) | (byte46 << 2) | (byte47 >>> 6);
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 63) << 8) | byte48;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte49 << 6) | (byte50 >>> 2);
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte50 & 3) << 12) | (byte51 << 4) | (byte52 >>> 4);
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 15) << 10) | (byte53 << 2) | (byte54 >>> 6);
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte54 & 63) << 8) | byte55;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 6);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 10);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 14) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 50) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 22) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 6);
        blocks[blocksOffset++] = (values[valuesOffset++] << 58) | (values[valuesOffset++] << 44) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 38) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 10) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 46) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 18) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 10);
        blocks[blocksOffset++] = (values[valuesOffset++] << 54) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 26) | (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 34) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 6) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 42) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 14) | values[valuesOffset++];
      }
    }

}
