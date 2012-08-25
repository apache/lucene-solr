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
final class BulkOperationPacked28 extends BulkOperation {
    @Override
    public int blockCount() {
      return 7;
    }

    @Override
    public int valueCount() {
      return 16;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 36);
        values[valuesOffset++] = (int) ((block0 >>> 8) & 268435455L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 255L) << 20) | (block1 >>> 44));
        values[valuesOffset++] = (int) ((block1 >>> 16) & 268435455L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 65535L) << 12) | (block2 >>> 52));
        values[valuesOffset++] = (int) ((block2 >>> 24) & 268435455L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 16777215L) << 4) | (block3 >>> 60));
        values[valuesOffset++] = (int) ((block3 >>> 32) & 268435455L);
        values[valuesOffset++] = (int) ((block3 >>> 4) & 268435455L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 15L) << 24) | (block4 >>> 40));
        values[valuesOffset++] = (int) ((block4 >>> 12) & 268435455L);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block4 & 4095L) << 16) | (block5 >>> 48));
        values[valuesOffset++] = (int) ((block5 >>> 20) & 268435455L);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block5 & 1048575L) << 8) | (block6 >>> 56));
        values[valuesOffset++] = (int) ((block6 >>> 28) & 268435455L);
        values[valuesOffset++] = (int) (block6 & 268435455L);
      }
    }

    @Override
    public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + 8 * iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final int byte0 = blocks[blocksOffset++] & 0xFF;
        final int byte1 = blocks[blocksOffset++] & 0xFF;
        final int byte2 = blocks[blocksOffset++] & 0xFF;
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte0 << 20) | (byte1 << 12) | (byte2 << 4) | (byte3 >>> 4);
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 15) << 24) | (byte4 << 16) | (byte5 << 8) | byte6;
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte7 << 20) | (byte8 << 12) | (byte9 << 4) | (byte10 >>> 4);
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 15) << 24) | (byte11 << 16) | (byte12 << 8) | byte13;
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte14 << 20) | (byte15 << 12) | (byte16 << 4) | (byte17 >>> 4);
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 15) << 24) | (byte18 << 16) | (byte19 << 8) | byte20;
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte21 << 20) | (byte22 << 12) | (byte23 << 4) | (byte24 >>> 4);
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 15) << 24) | (byte25 << 16) | (byte26 << 8) | byte27;
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte28 << 20) | (byte29 << 12) | (byte30 << 4) | (byte31 >>> 4);
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 24) | (byte32 << 16) | (byte33 << 8) | byte34;
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte35 << 20) | (byte36 << 12) | (byte37 << 4) | (byte38 >>> 4);
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        final int byte40 = blocks[blocksOffset++] & 0xFF;
        final int byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 15) << 24) | (byte39 << 16) | (byte40 << 8) | byte41;
        final int byte42 = blocks[blocksOffset++] & 0xFF;
        final int byte43 = blocks[blocksOffset++] & 0xFF;
        final int byte44 = blocks[blocksOffset++] & 0xFF;
        final int byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte42 << 20) | (byte43 << 12) | (byte44 << 4) | (byte45 >>> 4);
        final int byte46 = blocks[blocksOffset++] & 0xFF;
        final int byte47 = blocks[blocksOffset++] & 0xFF;
        final int byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 15) << 24) | (byte46 << 16) | (byte47 << 8) | byte48;
        final int byte49 = blocks[blocksOffset++] & 0xFF;
        final int byte50 = blocks[blocksOffset++] & 0xFF;
        final int byte51 = blocks[blocksOffset++] & 0xFF;
        final int byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte49 << 20) | (byte50 << 12) | (byte51 << 4) | (byte52 >>> 4);
        final int byte53 = blocks[blocksOffset++] & 0xFF;
        final int byte54 = blocks[blocksOffset++] & 0xFF;
        final int byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 15) << 24) | (byte53 << 16) | (byte54 << 8) | byte55;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 36;
        values[valuesOffset++] = (block0 >>> 8) & 268435455L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 255L) << 20) | (block1 >>> 44);
        values[valuesOffset++] = (block1 >>> 16) & 268435455L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 65535L) << 12) | (block2 >>> 52);
        values[valuesOffset++] = (block2 >>> 24) & 268435455L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 16777215L) << 4) | (block3 >>> 60);
        values[valuesOffset++] = (block3 >>> 32) & 268435455L;
        values[valuesOffset++] = (block3 >>> 4) & 268435455L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 15L) << 24) | (block4 >>> 40);
        values[valuesOffset++] = (block4 >>> 12) & 268435455L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 4095L) << 16) | (block5 >>> 48);
        values[valuesOffset++] = (block5 >>> 20) & 268435455L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 1048575L) << 8) | (block6 >>> 56);
        values[valuesOffset++] = (block6 >>> 28) & 268435455L;
        values[valuesOffset++] = block6 & 268435455L;
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
        values[valuesOffset++] = (byte0 << 20) | (byte1 << 12) | (byte2 << 4) | (byte3 >>> 4);
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte3 & 15) << 24) | (byte4 << 16) | (byte5 << 8) | byte6;
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte7 << 20) | (byte8 << 12) | (byte9 << 4) | (byte10 >>> 4);
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte10 & 15) << 24) | (byte11 << 16) | (byte12 << 8) | byte13;
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte14 << 20) | (byte15 << 12) | (byte16 << 4) | (byte17 >>> 4);
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte17 & 15) << 24) | (byte18 << 16) | (byte19 << 8) | byte20;
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte21 << 20) | (byte22 << 12) | (byte23 << 4) | (byte24 >>> 4);
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 15) << 24) | (byte25 << 16) | (byte26 << 8) | byte27;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte28 << 20) | (byte29 << 12) | (byte30 << 4) | (byte31 >>> 4);
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 24) | (byte32 << 16) | (byte33 << 8) | byte34;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte35 << 20) | (byte36 << 12) | (byte37 << 4) | (byte38 >>> 4);
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 15) << 24) | (byte39 << 16) | (byte40 << 8) | byte41;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte42 << 20) | (byte43 << 12) | (byte44 << 4) | (byte45 >>> 4);
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte45 & 15) << 24) | (byte46 << 16) | (byte47 << 8) | byte48;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte49 << 20) | (byte50 << 12) | (byte51 << 4) | (byte52 >>> 4);
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte52 & 15) << 24) | (byte53 << 16) | (byte54 << 8) | byte55;
      }
    }

    @Override
    public void encode(int[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 36) | ((values[valuesOffset++] & 0xffffffffL) << 8) | ((values[valuesOffset] & 0xffffffffL) >>> 20);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 44) | ((values[valuesOffset++] & 0xffffffffL) << 16) | ((values[valuesOffset] & 0xffffffffL) >>> 12);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 52) | ((values[valuesOffset++] & 0xffffffffL) << 24) | ((values[valuesOffset] & 0xffffffffL) >>> 4);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 60) | ((values[valuesOffset++] & 0xffffffffL) << 32) | ((values[valuesOffset++] & 0xffffffffL) << 4) | ((values[valuesOffset] & 0xffffffffL) >>> 24);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 40) | ((values[valuesOffset++] & 0xffffffffL) << 12) | ((values[valuesOffset] & 0xffffffffL) >>> 16);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 48) | ((values[valuesOffset++] & 0xffffffffL) << 20) | ((values[valuesOffset] & 0xffffffffL) >>> 8);
        blocks[blocksOffset++] = ((values[valuesOffset++] & 0xffffffffL) << 56) | ((values[valuesOffset++] & 0xffffffffL) << 28) | (values[valuesOffset++] & 0xffffffffL);
      }
    }

    @Override
    public void encode(long[] values, int valuesOffset, long[] blocks, int blocksOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        blocks[blocksOffset++] = (values[valuesOffset++] << 36) | (values[valuesOffset++] << 8) | (values[valuesOffset] >>> 20);
        blocks[blocksOffset++] = (values[valuesOffset++] << 44) | (values[valuesOffset++] << 16) | (values[valuesOffset] >>> 12);
        blocks[blocksOffset++] = (values[valuesOffset++] << 52) | (values[valuesOffset++] << 24) | (values[valuesOffset] >>> 4);
        blocks[blocksOffset++] = (values[valuesOffset++] << 60) | (values[valuesOffset++] << 32) | (values[valuesOffset++] << 4) | (values[valuesOffset] >>> 24);
        blocks[blocksOffset++] = (values[valuesOffset++] << 40) | (values[valuesOffset++] << 12) | (values[valuesOffset] >>> 16);
        blocks[blocksOffset++] = (values[valuesOffset++] << 48) | (values[valuesOffset++] << 20) | (values[valuesOffset] >>> 8);
        blocks[blocksOffset++] = (values[valuesOffset++] << 56) | (values[valuesOffset++] << 28) | values[valuesOffset++];
      }
    }

}
