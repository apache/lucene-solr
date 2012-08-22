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
final class BulkOperationPacked1 extends BulkOperation {
    public int blockCount() {
      return 1;
    }

    public int valueCount() {
      return 64;
    }

    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 63);
        values[valuesOffset++] = (int) ((block0 >>> 62) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 61) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 60) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 59) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 58) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 57) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 56) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 55) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 54) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 53) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 52) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 51) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 50) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 49) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 48) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 47) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 46) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 45) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 44) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 43) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 42) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 41) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 40) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 39) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 38) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 37) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 36) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 35) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 34) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 33) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 32) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 31) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 30) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 29) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 28) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 27) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 26) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 25) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 24) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 23) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 22) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 21) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 20) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 19) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 18) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 17) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 16) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 15) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 14) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 13) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 12) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 11) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 10) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 9) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 8) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 7) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 6) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 5) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 4) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 3) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 2) & 1L);
        values[valuesOffset++] = (int) ((block0 >>> 1) & 1L);
        values[valuesOffset++] = (int) (block0 & 1L);
      }
    }

    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 >>> 7;
        values[valuesOffset++] = (byte0 >>> 6) & 1;
        values[valuesOffset++] = (byte0 >>> 5) & 1;
        values[valuesOffset++] = (byte0 >>> 4) & 1;
        values[valuesOffset++] = (byte0 >>> 3) & 1;
        values[valuesOffset++] = (byte0 >>> 2) & 1;
        values[valuesOffset++] = (byte0 >>> 1) & 1;
        values[valuesOffset++] = byte0 & 1;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte1 >>> 7;
        values[valuesOffset++] = (byte1 >>> 6) & 1;
        values[valuesOffset++] = (byte1 >>> 5) & 1;
        values[valuesOffset++] = (byte1 >>> 4) & 1;
        values[valuesOffset++] = (byte1 >>> 3) & 1;
        values[valuesOffset++] = (byte1 >>> 2) & 1;
        values[valuesOffset++] = (byte1 >>> 1) & 1;
        values[valuesOffset++] = byte1 & 1;
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte2 >>> 7;
        values[valuesOffset++] = (byte2 >>> 6) & 1;
        values[valuesOffset++] = (byte2 >>> 5) & 1;
        values[valuesOffset++] = (byte2 >>> 4) & 1;
        values[valuesOffset++] = (byte2 >>> 3) & 1;
        values[valuesOffset++] = (byte2 >>> 2) & 1;
        values[valuesOffset++] = (byte2 >>> 1) & 1;
        values[valuesOffset++] = byte2 & 1;
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte3 >>> 7;
        values[valuesOffset++] = (byte3 >>> 6) & 1;
        values[valuesOffset++] = (byte3 >>> 5) & 1;
        values[valuesOffset++] = (byte3 >>> 4) & 1;
        values[valuesOffset++] = (byte3 >>> 3) & 1;
        values[valuesOffset++] = (byte3 >>> 2) & 1;
        values[valuesOffset++] = (byte3 >>> 1) & 1;
        values[valuesOffset++] = byte3 & 1;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte4 >>> 7;
        values[valuesOffset++] = (byte4 >>> 6) & 1;
        values[valuesOffset++] = (byte4 >>> 5) & 1;
        values[valuesOffset++] = (byte4 >>> 4) & 1;
        values[valuesOffset++] = (byte4 >>> 3) & 1;
        values[valuesOffset++] = (byte4 >>> 2) & 1;
        values[valuesOffset++] = (byte4 >>> 1) & 1;
        values[valuesOffset++] = byte4 & 1;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte5 >>> 7;
        values[valuesOffset++] = (byte5 >>> 6) & 1;
        values[valuesOffset++] = (byte5 >>> 5) & 1;
        values[valuesOffset++] = (byte5 >>> 4) & 1;
        values[valuesOffset++] = (byte5 >>> 3) & 1;
        values[valuesOffset++] = (byte5 >>> 2) & 1;
        values[valuesOffset++] = (byte5 >>> 1) & 1;
        values[valuesOffset++] = byte5 & 1;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte6 >>> 7;
        values[valuesOffset++] = (byte6 >>> 6) & 1;
        values[valuesOffset++] = (byte6 >>> 5) & 1;
        values[valuesOffset++] = (byte6 >>> 4) & 1;
        values[valuesOffset++] = (byte6 >>> 3) & 1;
        values[valuesOffset++] = (byte6 >>> 2) & 1;
        values[valuesOffset++] = (byte6 >>> 1) & 1;
        values[valuesOffset++] = byte6 & 1;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte7 >>> 7;
        values[valuesOffset++] = (byte7 >>> 6) & 1;
        values[valuesOffset++] = (byte7 >>> 5) & 1;
        values[valuesOffset++] = (byte7 >>> 4) & 1;
        values[valuesOffset++] = (byte7 >>> 3) & 1;
        values[valuesOffset++] = (byte7 >>> 2) & 1;
        values[valuesOffset++] = (byte7 >>> 1) & 1;
        values[valuesOffset++] = byte7 & 1;
      }
    }

    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 63;
        values[valuesOffset++] = (block0 >>> 62) & 1L;
        values[valuesOffset++] = (block0 >>> 61) & 1L;
        values[valuesOffset++] = (block0 >>> 60) & 1L;
        values[valuesOffset++] = (block0 >>> 59) & 1L;
        values[valuesOffset++] = (block0 >>> 58) & 1L;
        values[valuesOffset++] = (block0 >>> 57) & 1L;
        values[valuesOffset++] = (block0 >>> 56) & 1L;
        values[valuesOffset++] = (block0 >>> 55) & 1L;
        values[valuesOffset++] = (block0 >>> 54) & 1L;
        values[valuesOffset++] = (block0 >>> 53) & 1L;
        values[valuesOffset++] = (block0 >>> 52) & 1L;
        values[valuesOffset++] = (block0 >>> 51) & 1L;
        values[valuesOffset++] = (block0 >>> 50) & 1L;
        values[valuesOffset++] = (block0 >>> 49) & 1L;
        values[valuesOffset++] = (block0 >>> 48) & 1L;
        values[valuesOffset++] = (block0 >>> 47) & 1L;
        values[valuesOffset++] = (block0 >>> 46) & 1L;
        values[valuesOffset++] = (block0 >>> 45) & 1L;
        values[valuesOffset++] = (block0 >>> 44) & 1L;
        values[valuesOffset++] = (block0 >>> 43) & 1L;
        values[valuesOffset++] = (block0 >>> 42) & 1L;
        values[valuesOffset++] = (block0 >>> 41) & 1L;
        values[valuesOffset++] = (block0 >>> 40) & 1L;
        values[valuesOffset++] = (block0 >>> 39) & 1L;
        values[valuesOffset++] = (block0 >>> 38) & 1L;
        values[valuesOffset++] = (block0 >>> 37) & 1L;
        values[valuesOffset++] = (block0 >>> 36) & 1L;
        values[valuesOffset++] = (block0 >>> 35) & 1L;
        values[valuesOffset++] = (block0 >>> 34) & 1L;
        values[valuesOffset++] = (block0 >>> 33) & 1L;
        values[valuesOffset++] = (block0 >>> 32) & 1L;
        values[valuesOffset++] = (block0 >>> 31) & 1L;
        values[valuesOffset++] = (block0 >>> 30) & 1L;
        values[valuesOffset++] = (block0 >>> 29) & 1L;
        values[valuesOffset++] = (block0 >>> 28) & 1L;
        values[valuesOffset++] = (block0 >>> 27) & 1L;
        values[valuesOffset++] = (block0 >>> 26) & 1L;
        values[valuesOffset++] = (block0 >>> 25) & 1L;
        values[valuesOffset++] = (block0 >>> 24) & 1L;
        values[valuesOffset++] = (block0 >>> 23) & 1L;
        values[valuesOffset++] = (block0 >>> 22) & 1L;
        values[valuesOffset++] = (block0 >>> 21) & 1L;
        values[valuesOffset++] = (block0 >>> 20) & 1L;
        values[valuesOffset++] = (block0 >>> 19) & 1L;
        values[valuesOffset++] = (block0 >>> 18) & 1L;
        values[valuesOffset++] = (block0 >>> 17) & 1L;
        values[valuesOffset++] = (block0 >>> 16) & 1L;
        values[valuesOffset++] = (block0 >>> 15) & 1L;
        values[valuesOffset++] = (block0 >>> 14) & 1L;
        values[valuesOffset++] = (block0 >>> 13) & 1L;
        values[valuesOffset++] = (block0 >>> 12) & 1L;
        values[valuesOffset++] = (block0 >>> 11) & 1L;
        values[valuesOffset++] = (block0 >>> 10) & 1L;
        values[valuesOffset++] = (block0 >>> 9) & 1L;
        values[valuesOffset++] = (block0 >>> 8) & 1L;
        values[valuesOffset++] = (block0 >>> 7) & 1L;
        values[valuesOffset++] = (block0 >>> 6) & 1L;
        values[valuesOffset++] = (block0 >>> 5) & 1L;
        values[valuesOffset++] = (block0 >>> 4) & 1L;
        values[valuesOffset++] = (block0 >>> 3) & 1L;
        values[valuesOffset++] = (block0 >>> 2) & 1L;
        values[valuesOffset++] = (block0 >>> 1) & 1L;
        values[valuesOffset++] = block0 & 1L;
      }
    }

    public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long byte0 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte0 >>> 7;
        values[valuesOffset++] = (byte0 >>> 6) & 1;
        values[valuesOffset++] = (byte0 >>> 5) & 1;
        values[valuesOffset++] = (byte0 >>> 4) & 1;
        values[valuesOffset++] = (byte0 >>> 3) & 1;
        values[valuesOffset++] = (byte0 >>> 2) & 1;
        values[valuesOffset++] = (byte0 >>> 1) & 1;
        values[valuesOffset++] = byte0 & 1;
        final long byte1 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte1 >>> 7;
        values[valuesOffset++] = (byte1 >>> 6) & 1;
        values[valuesOffset++] = (byte1 >>> 5) & 1;
        values[valuesOffset++] = (byte1 >>> 4) & 1;
        values[valuesOffset++] = (byte1 >>> 3) & 1;
        values[valuesOffset++] = (byte1 >>> 2) & 1;
        values[valuesOffset++] = (byte1 >>> 1) & 1;
        values[valuesOffset++] = byte1 & 1;
        final long byte2 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte2 >>> 7;
        values[valuesOffset++] = (byte2 >>> 6) & 1;
        values[valuesOffset++] = (byte2 >>> 5) & 1;
        values[valuesOffset++] = (byte2 >>> 4) & 1;
        values[valuesOffset++] = (byte2 >>> 3) & 1;
        values[valuesOffset++] = (byte2 >>> 2) & 1;
        values[valuesOffset++] = (byte2 >>> 1) & 1;
        values[valuesOffset++] = byte2 & 1;
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte3 >>> 7;
        values[valuesOffset++] = (byte3 >>> 6) & 1;
        values[valuesOffset++] = (byte3 >>> 5) & 1;
        values[valuesOffset++] = (byte3 >>> 4) & 1;
        values[valuesOffset++] = (byte3 >>> 3) & 1;
        values[valuesOffset++] = (byte3 >>> 2) & 1;
        values[valuesOffset++] = (byte3 >>> 1) & 1;
        values[valuesOffset++] = byte3 & 1;
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte4 >>> 7;
        values[valuesOffset++] = (byte4 >>> 6) & 1;
        values[valuesOffset++] = (byte4 >>> 5) & 1;
        values[valuesOffset++] = (byte4 >>> 4) & 1;
        values[valuesOffset++] = (byte4 >>> 3) & 1;
        values[valuesOffset++] = (byte4 >>> 2) & 1;
        values[valuesOffset++] = (byte4 >>> 1) & 1;
        values[valuesOffset++] = byte4 & 1;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte5 >>> 7;
        values[valuesOffset++] = (byte5 >>> 6) & 1;
        values[valuesOffset++] = (byte5 >>> 5) & 1;
        values[valuesOffset++] = (byte5 >>> 4) & 1;
        values[valuesOffset++] = (byte5 >>> 3) & 1;
        values[valuesOffset++] = (byte5 >>> 2) & 1;
        values[valuesOffset++] = (byte5 >>> 1) & 1;
        values[valuesOffset++] = byte5 & 1;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte6 >>> 7;
        values[valuesOffset++] = (byte6 >>> 6) & 1;
        values[valuesOffset++] = (byte6 >>> 5) & 1;
        values[valuesOffset++] = (byte6 >>> 4) & 1;
        values[valuesOffset++] = (byte6 >>> 3) & 1;
        values[valuesOffset++] = (byte6 >>> 2) & 1;
        values[valuesOffset++] = (byte6 >>> 1) & 1;
        values[valuesOffset++] = byte6 & 1;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = byte7 >>> 7;
        values[valuesOffset++] = (byte7 >>> 6) & 1;
        values[valuesOffset++] = (byte7 >>> 5) & 1;
        values[valuesOffset++] = (byte7 >>> 4) & 1;
        values[valuesOffset++] = (byte7 >>> 3) & 1;
        values[valuesOffset++] = (byte7 >>> 2) & 1;
        values[valuesOffset++] = (byte7 >>> 1) & 1;
        values[valuesOffset++] = byte7 & 1;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 63) | ((values[valuesOffset++] & 0xffffffffL) << 62) | ((values[valuesOffset++] & 0xffffffffL) << 61) | ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 59) | ((values[valuesOffset++] & 0xffffffffL) << 58) | ((values[valuesOffset++] & 0xffffffffL) << 57) | ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 55) | ((values[valuesOffset++] & 0xffffffffL) << 54) | ((values[valuesOffset++] & 0xffffffffL) << 53) | ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 51) | ((values[valuesOffset++] & 0xffffffffL) << 50) | ((values[valuesOffset++] & 0xffffffffL) << 49) | ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 47) | ((values[valuesOffset++] & 0xffffffffL) << 46) | ((values[valuesOffset++] & 0xffffffffL) << 45) | ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 43) | ((values[valuesOffset++] & 0xffffffffL) << 42) | ((values[valuesOffset++] & 0xffffffffL) << 41) | ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 39) | ((values[valuesOffset++] & 0xffffffffL) << 38) | ((values[valuesOffset++] & 0xffffffffL) << 37) | ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 35) | ((values[valuesOffset++] & 0xffffffffL) << 34) | ((values[valuesOffset++] & 0xffffffffL) << 33) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 31) | ((values[valuesOffset++] & 0xffffffffL) << 30) | ((values[valuesOffset++] & 0xffffffffL) << 29) | ((values[valuesOffset++] & 0xffffffffL) << 28) | ((values[valuesOffset++] & 0xffffffffL) << 27) | ((values[valuesOffset++] & 0xffffffffL) << 26) | ((values[valuesOffset++] & 0xffffffffL) << 25) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset++] & 0xffffffffL) << 23) | ((values[valuesOffset++] & 0xffffffffL) << 22) | ((values[valuesOffset++] & 0xffffffffL) << 21) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset++] & 0xffffffffL) << 19) | ((values[valuesOffset++] & 0xffffffffL) << 18) | ((values[valuesOffset++] & 0xffffffffL) << 17) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset++] & 0xffffffffL) << 15) | ((values[valuesOffset++] & 0xffffffffL) << 14) | ((values[valuesOffset++] & 0xffffffffL) << 13) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset++] & 0xffffffffL) << 11) | ((values[valuesOffset++] & 0xffffffffL) << 10) | ((values[valuesOffset++] & 0xffffffffL) << 9) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset++] & 0xffffffffL) << 7) | ((values[valuesOffset++] & 0xffffffffL) << 6) | ((values[valuesOffset++] & 0xffffffffL) << 5) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset++] & 0xffffffffL) << 3) | ((values[valuesOffset++] & 0xffffffffL) << 2) | ((values[valuesOffset++] & 0xffffffffL) << 1) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 63) | (values[valuesOffset++] << 62) | (values[valuesOffset++] << 61) | (values[valuesOffset++] << 60) | (values[valuesOffset++] << 59) | (values[valuesOffset++] << 58) | (values[valuesOffset++] << 57) | (values[valuesOffset++] << 56) | (values[valuesOffset++] << 55) | (values[valuesOffset++] << 54) | (values[valuesOffset++] << 53) | (values[valuesOffset++] << 52) | (values[valuesOffset++] << 51) | (values[valuesOffset++] << 50) | (values[valuesOffset++] << 49) | (values[valuesOffset++] << 48) | (values[valuesOffset++] << 47) | (values[valuesOffset++] << 46) | (values[valuesOffset++] << 45) | (values[valuesOffset++] << 44) | (values[valuesOffset++] << 43) | (values[valuesOffset++] << 42) | (values[valuesOffset++] << 41) | (values[valuesOffset++] << 40) | (values[valuesOffset++] << 39) | (values[valuesOffset++] << 38) | (values[valuesOffset++] << 37) | (values[valuesOffset++] << 36) | (values[valuesOffset++] << 35) | (values[valuesOffset++] << 34) | (values[valuesOffset++] << 33) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 31) | (values[valuesOffset++] << 30) | (values[valuesOffset++] << 29) | (values[valuesOffset++] << 28) | (values[valuesOffset++] << 27) | (values[valuesOffset++] << 26) | (values[valuesOffset++] << 25) | (values[valuesOffset++] << 24) | (values[valuesOffset++] << 23) | (values[valuesOffset++] << 22) | (values[valuesOffset++] << 21) | (values[valuesOffset++] << 20) | (values[valuesOffset++] << 19) | (values[valuesOffset++] << 18) | (values[valuesOffset++] << 17) | (values[valuesOffset++] << 16) | (values[valuesOffset++] << 15) | (values[valuesOffset++] << 14) | (values[valuesOffset++] << 13) | (values[valuesOffset++] << 12) | (values[valuesOffset++] << 11) | (values[valuesOffset++] << 10) | (values[valuesOffset++] << 9) | (values[valuesOffset++] << 8) | (values[valuesOffset++] << 7) | (values[valuesOffset++] << 6) | (values[valuesOffset++] << 5) | (values[valuesOffset++] << 4) | (values[valuesOffset++] << 3) | (values[valuesOffset++] << 2) | (values[valuesOffset++] << 1) | values[valuesOffset++];
      }
    }

}
