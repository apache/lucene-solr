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
final class BulkOperationPacked3 extends BulkOperation {
    public int blockCount() {
      return 3;
    }

    public int valueCount() {
      return 64;
    }

    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 61);
        values[valuesOffset++] = (int) ((block0 >>> 58) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 55) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 52) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 49) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 46) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 43) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 40) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 37) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 34) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 31) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 28) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 25) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 22) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 19) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 16) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 13) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 10) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 7) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 4) & 7L);
        values[valuesOffset++] = (int) ((block0 >>> 1) & 7L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 1L) << 2) | (block1 >>> 62));
        values[valuesOffset++] = (int) ((block1 >>> 59) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 56) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 53) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 50) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 47) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 44) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 41) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 38) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 35) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 32) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 29) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 26) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 23) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 20) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 17) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 14) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 11) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 8) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 5) & 7L);
        values[valuesOffset++] = (int) ((block1 >>> 2) & 7L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 3L) << 1) | (block2 >>> 63));
        values[valuesOffset++] = (int) ((block2 >>> 60) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 57) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 54) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 51) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 48) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 45) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 42) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 39) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 36) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 33) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 30) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 27) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 24) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 21) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 18) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 15) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 12) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 9) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 6) & 7L);
        values[valuesOffset++] = (int) ((block2 >>> 3) & 7L);
        values[valuesOffset++] = (int) (block2 & 7L);
      }
    }

    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 >>> 5;
        values[valuesOffset++] = (byte0 >>> 2) & 7;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte0 & 3) << 1) | (byte1 >>> 7);
        values[valuesOffset++] = (byte1 >>> 4) & 7;
        values[valuesOffset++] = (byte1 >>> 1) & 7;
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 1) << 2) | (byte2 >>> 6);
        values[valuesOffset++] = (byte2 >>> 3) & 7;
        values[valuesOffset++] = byte2 & 7;
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte3 >>> 5;
        values[valuesOffset++] = (byte3 >>> 2) & 7;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 3) << 1) | (byte4 >>> 7);
        values[valuesOffset++] = (byte4 >>> 4) & 7;
        values[valuesOffset++] = (byte4 >>> 1) & 7;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 1) << 2) | (byte5 >>> 6);
        values[valuesOffset++] = (byte5 >>> 3) & 7;
        values[valuesOffset++] = byte5 & 7;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte6 >>> 5;
        values[valuesOffset++] = (byte6 >>> 2) & 7;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 3) << 1) | (byte7 >>> 7);
        values[valuesOffset++] = (byte7 >>> 4) & 7;
        values[valuesOffset++] = (byte7 >>> 1) & 7;
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 1) << 2) | (byte8 >>> 6);
        values[valuesOffset++] = (byte8 >>> 3) & 7;
        values[valuesOffset++] = byte8 & 7;
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte9 >>> 5;
        values[valuesOffset++] = (byte9 >>> 2) & 7;
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte9 & 3) << 1) | (byte10 >>> 7);
        values[valuesOffset++] = (byte10 >>> 4) & 7;
        values[valuesOffset++] = (byte10 >>> 1) & 7;
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 1) << 2) | (byte11 >>> 6);
        values[valuesOffset++] = (byte11 >>> 3) & 7;
        values[valuesOffset++] = byte11 & 7;
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte12 >>> 5;
        values[valuesOffset++] = (byte12 >>> 2) & 7;
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 3) << 1) | (byte13 >>> 7);
        values[valuesOffset++] = (byte13 >>> 4) & 7;
        values[valuesOffset++] = (byte13 >>> 1) & 7;
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 1) << 2) | (byte14 >>> 6);
        values[valuesOffset++] = (byte14 >>> 3) & 7;
        values[valuesOffset++] = byte14 & 7;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte15 >>> 5;
        values[valuesOffset++] = (byte15 >>> 2) & 7;
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 1) | (byte16 >>> 7);
        values[valuesOffset++] = (byte16 >>> 4) & 7;
        values[valuesOffset++] = (byte16 >>> 1) & 7;
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 1) << 2) | (byte17 >>> 6);
        values[valuesOffset++] = (byte17 >>> 3) & 7;
        values[valuesOffset++] = byte17 & 7;
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte18 >>> 5;
        values[valuesOffset++] = (byte18 >>> 2) & 7;
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 3) << 1) | (byte19 >>> 7);
        values[valuesOffset++] = (byte19 >>> 4) & 7;
        values[valuesOffset++] = (byte19 >>> 1) & 7;
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 1) << 2) | (byte20 >>> 6);
        values[valuesOffset++] = (byte20 >>> 3) & 7;
        values[valuesOffset++] = byte20 & 7;
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte21 >>> 5;
        values[valuesOffset++] = (byte21 >>> 2) & 7;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 3) << 1) | (byte22 >>> 7);
        values[valuesOffset++] = (byte22 >>> 4) & 7;
        values[valuesOffset++] = (byte22 >>> 1) & 7;
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 1) << 2) | (byte23 >>> 6);
        values[valuesOffset++] = (byte23 >>> 3) & 7;
        values[valuesOffset++] = byte23 & 7;
      }
    }

    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 61;
        values[valuesOffset++] = (block0 >>> 58) & 7L;
        values[valuesOffset++] = (block0 >>> 55) & 7L;
        values[valuesOffset++] = (block0 >>> 52) & 7L;
        values[valuesOffset++] = (block0 >>> 49) & 7L;
        values[valuesOffset++] = (block0 >>> 46) & 7L;
        values[valuesOffset++] = (block0 >>> 43) & 7L;
        values[valuesOffset++] = (block0 >>> 40) & 7L;
        values[valuesOffset++] = (block0 >>> 37) & 7L;
        values[valuesOffset++] = (block0 >>> 34) & 7L;
        values[valuesOffset++] = (block0 >>> 31) & 7L;
        values[valuesOffset++] = (block0 >>> 28) & 7L;
        values[valuesOffset++] = (block0 >>> 25) & 7L;
        values[valuesOffset++] = (block0 >>> 22) & 7L;
        values[valuesOffset++] = (block0 >>> 19) & 7L;
        values[valuesOffset++] = (block0 >>> 16) & 7L;
        values[valuesOffset++] = (block0 >>> 13) & 7L;
        values[valuesOffset++] = (block0 >>> 10) & 7L;
        values[valuesOffset++] = (block0 >>> 7) & 7L;
        values[valuesOffset++] = (block0 >>> 4) & 7L;
        values[valuesOffset++] = (block0 >>> 1) & 7L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 1L) << 2) | (block1 >>> 62);
        values[valuesOffset++] = (block1 >>> 59) & 7L;
        values[valuesOffset++] = (block1 >>> 56) & 7L;
        values[valuesOffset++] = (block1 >>> 53) & 7L;
        values[valuesOffset++] = (block1 >>> 50) & 7L;
        values[valuesOffset++] = (block1 >>> 47) & 7L;
        values[valuesOffset++] = (block1 >>> 44) & 7L;
        values[valuesOffset++] = (block1 >>> 41) & 7L;
        values[valuesOffset++] = (block1 >>> 38) & 7L;
        values[valuesOffset++] = (block1 >>> 35) & 7L;
        values[valuesOffset++] = (block1 >>> 32) & 7L;
        values[valuesOffset++] = (block1 >>> 29) & 7L;
        values[valuesOffset++] = (block1 >>> 26) & 7L;
        values[valuesOffset++] = (block1 >>> 23) & 7L;
        values[valuesOffset++] = (block1 >>> 20) & 7L;
        values[valuesOffset++] = (block1 >>> 17) & 7L;
        values[valuesOffset++] = (block1 >>> 14) & 7L;
        values[valuesOffset++] = (block1 >>> 11) & 7L;
        values[valuesOffset++] = (block1 >>> 8) & 7L;
        values[valuesOffset++] = (block1 >>> 5) & 7L;
        values[valuesOffset++] = (block1 >>> 2) & 7L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 3L) << 1) | (block2 >>> 63);
        values[valuesOffset++] = (block2 >>> 60) & 7L;
        values[valuesOffset++] = (block2 >>> 57) & 7L;
        values[valuesOffset++] = (block2 >>> 54) & 7L;
        values[valuesOffset++] = (block2 >>> 51) & 7L;
        values[valuesOffset++] = (block2 >>> 48) & 7L;
        values[valuesOffset++] = (block2 >>> 45) & 7L;
        values[valuesOffset++] = (block2 >>> 42) & 7L;
        values[valuesOffset++] = (block2 >>> 39) & 7L;
        values[valuesOffset++] = (block2 >>> 36) & 7L;
        values[valuesOffset++] = (block2 >>> 33) & 7L;
        values[valuesOffset++] = (block2 >>> 30) & 7L;
        values[valuesOffset++] = (block2 >>> 27) & 7L;
        values[valuesOffset++] = (block2 >>> 24) & 7L;
        values[valuesOffset++] = (block2 >>> 21) & 7L;
        values[valuesOffset++] = (block2 >>> 18) & 7L;
        values[valuesOffset++] = (block2 >>> 15) & 7L;
        values[valuesOffset++] = (block2 >>> 12) & 7L;
        values[valuesOffset++] = (block2 >>> 9) & 7L;
        values[valuesOffset++] = (block2 >>> 6) & 7L;
        values[valuesOffset++] = (block2 >>> 3) & 7L;
        values[valuesOffset++] = block2 & 7L;
      }
    }

    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 >>> 5;
        values[valuesOffset++] = (byte0 >>> 2) & 7;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte0 & 3) << 1) | (byte1 >>> 7);
        values[valuesOffset++] = (byte1 >>> 4) & 7;
        values[valuesOffset++] = (byte1 >>> 1) & 7;
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte1 & 1) << 2) | (byte2 >>> 6);
        values[valuesOffset++] = (byte2 >>> 3) & 7;
        values[valuesOffset++] = byte2 & 7;
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte3 >>> 5;
        values[valuesOffset++] = (byte3 >>> 2) & 7;
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 3) << 1) | (byte4 >>> 7);
        values[valuesOffset++] = (byte4 >>> 4) & 7;
        values[valuesOffset++] = (byte4 >>> 1) & 7;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 1) << 2) | (byte5 >>> 6);
        values[valuesOffset++] = (byte5 >>> 3) & 7;
        values[valuesOffset++] = byte5 & 7;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte6 >>> 5;
        values[valuesOffset++] = (byte6 >>> 2) & 7;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 3) << 1) | (byte7 >>> 7);
        values[valuesOffset++] = (byte7 >>> 4) & 7;
        values[valuesOffset++] = (byte7 >>> 1) & 7;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte7 & 1) << 2) | (byte8 >>> 6);
        values[valuesOffset++] = (byte8 >>> 3) & 7;
        values[valuesOffset++] = byte8 & 7;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte9 >>> 5;
        values[valuesOffset++] = (byte9 >>> 2) & 7;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte9 & 3) << 1) | (byte10 >>> 7);
        values[valuesOffset++] = (byte10 >>> 4) & 7;
        values[valuesOffset++] = (byte10 >>> 1) & 7;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 1) << 2) | (byte11 >>> 6);
        values[valuesOffset++] = (byte11 >>> 3) & 7;
        values[valuesOffset++] = byte11 & 7;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte12 >>> 5;
        values[valuesOffset++] = (byte12 >>> 2) & 7;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte12 & 3) << 1) | (byte13 >>> 7);
        values[valuesOffset++] = (byte13 >>> 4) & 7;
        values[valuesOffset++] = (byte13 >>> 1) & 7;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 1) << 2) | (byte14 >>> 6);
        values[valuesOffset++] = (byte14 >>> 3) & 7;
        values[valuesOffset++] = byte14 & 7;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte15 >>> 5;
        values[valuesOffset++] = (byte15 >>> 2) & 7;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 1) | (byte16 >>> 7);
        values[valuesOffset++] = (byte16 >>> 4) & 7;
        values[valuesOffset++] = (byte16 >>> 1) & 7;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte16 & 1) << 2) | (byte17 >>> 6);
        values[valuesOffset++] = (byte17 >>> 3) & 7;
        values[valuesOffset++] = byte17 & 7;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte18 >>> 5;
        values[valuesOffset++] = (byte18 >>> 2) & 7;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte18 & 3) << 1) | (byte19 >>> 7);
        values[valuesOffset++] = (byte19 >>> 4) & 7;
        values[valuesOffset++] = (byte19 >>> 1) & 7;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte19 & 1) << 2) | (byte20 >>> 6);
        values[valuesOffset++] = (byte20 >>> 3) & 7;
        values[valuesOffset++] = byte20 & 7;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte21 >>> 5;
        values[valuesOffset++] = (byte21 >>> 2) & 7;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte21 & 3) << 1) | (byte22 >>> 7);
        values[valuesOffset++] = (byte22 >>> 4) & 7;
        values[valuesOffset++] = (byte22 >>> 1) & 7;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 1) << 2) | (byte23 >>> 6);
        values[valuesOffset++] = (byte23 >>> 3) & 7;
        values[valuesOffset++] = byte23 & 7;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 61) | ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 55) | ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 49) | ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 43) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 37) | ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 31) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 25) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset++] & 0xffffffffL) << 19) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 13) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset++] & 0xffffffffL) << 7) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset++] & 0xffffffffL) << 1) | ((values[valuesOffset] & 0xffffffffL) >>> 2);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 59) | ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 53) | ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 47) | ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 41) | ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 35) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 29) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset++] & 0xffffffffL) << 23) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 17) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset++] & 0xffffffffL) << 11) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset++] & 0xffffffffL) << 5) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset] & 0xffffffffL) >>> 1);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 63) | ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 57) | ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 51) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 45) | ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 39) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 33) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 27) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 21) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset++] & 0xffffffffL) << 15) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset++] & 0xffffffffL) << 9) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset++] & 0xffffffffL) << 3) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 61) | (values[valuesOffset++] << 58) | (values[valuesOffset++] << 55) | (values[valuesOffset++] << 52) | (values[valuesOffset++] << 49) | (values[valuesOffset++] << 46) | (values[valuesOffset++] << 43) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 37) | (values[valuesOffset++] << 34) | (values[valuesOffset++] << 31) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 25) | (values[valuesOffset++] << 22) | (values[valuesOffset++] << 19) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 13) | (values[valuesOffset++] << 10) | (values[valuesOffset++] << 7) | (values[valuesOffset++] << 4) | (values[valuesOffset++] << 1) | (values[valuesOffset] >>> 2);
        blocks[blocksOffset++] = (values[valuesOffset++] << 62) | (values[valuesOffset++] << 59) | (values[valuesOffset++] << 56) | (values[valuesOffset++] << 53) | (values[valuesOffset++] << 50) | (values[valuesOffset++] << 47) | (values[valuesOffset++] << 44) | (values[valuesOffset++] << 41) | (values[valuesOffset++] << 38) | (values[valuesOffset++] << 35) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 29) | (values[valuesOffset++] << 26) | (values[valuesOffset++] << 23) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 17) | (values[valuesOffset++] << 14) | (values[valuesOffset++] << 11) | (values[valuesOffset++] << 8) | (values[valuesOffset++] << 5) | (values[valuesOffset++] << 2) | (values[valuesOffset] >>> 1);
        blocks[blocksOffset++] = (values[valuesOffset++] << 63) | (values[valuesOffset++] << 60) | (values[valuesOffset++] << 57) | (values[valuesOffset++] << 54) | (values[valuesOffset++] << 51) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 45) | (values[valuesOffset++] << 42) | (values[valuesOffset++] << 39) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 33) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 27) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 21) | (values[valuesOffset++] << 18) | (values[valuesOffset++] << 15) | (values[valuesOffset++] << 12) | (values[valuesOffset++] << 9) | (values[valuesOffset++] << 6) | (values[valuesOffset++] << 3) | values[valuesOffset++];
      }
    }

}
