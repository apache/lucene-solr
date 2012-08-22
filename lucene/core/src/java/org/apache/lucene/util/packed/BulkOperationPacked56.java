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
final class BulkOperationPacked56 extends BulkOperation {
    public int blockCount() {
      return 7;
    }

    public int valueCount() {
      return 8;
    }

    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      throw new UnsupportedOperationException();
    }

    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 8;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 255L) << 48) | (block1 >>> 16);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 65535L) << 40) | (block2 >>> 24);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 16777215L) << 32) | (block3 >>> 32);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 4294967295L) << 24) | (block4 >>> 40);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 1099511627775L) << 16) | (block5 >>> 48);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 281474976710655L) << 8) | (block6 >>> 56);
        values[valuesOffset++] = block6 & 72057594037927935L;
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
        values[valuesOffset++] = (byte0 << 48) | (byte1 << 40) | (byte2 << 32) | (byte3 << 24) | (byte4 << 16) | (byte5 << 8) | byte6;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte7 << 48) | (byte8 << 40) | (byte9 << 32) | (byte10 << 24) | (byte11 << 16) | (byte12 << 8) | byte13;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte14 << 48) | (byte15 << 40) | (byte16 << 32) | (byte17 << 24) | (byte18 << 16) | (byte19 << 8) | byte20;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte21 << 48) | (byte22 << 40) | (byte23 << 32) | (byte24 << 24) | (byte25 << 16) | (byte26 << 8) | byte27;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte28 << 48) | (byte29 << 40) | (byte30 << 32) | (byte31 << 24) | (byte32 << 16) | (byte33 << 8) | byte34;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte35 << 48) | (byte36 << 40) | (byte37 << 32) | (byte38 << 24) | (byte39 << 16) | (byte40 << 8) | byte41;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte42 << 48) | (byte43 << 40) | (byte44 << 32) | (byte45 << 24) | (byte46 << 16) | (byte47 << 8) | byte48;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte49 << 48) | (byte50 << 40) | (byte51 << 32) | (byte52 << 24) | (byte53 << 16) | (byte54 << 8) | byte55;
      }
    }

    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 48);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 40);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 32);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 48);
        blocks[blocksOffset++] = (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 40);
        blocks[blocksOffset++] = (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 32);
        blocks[blocksOffset++] = (values[valuesOffset++] << 32) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | values[valuesOffset++];
      }
    }

}
