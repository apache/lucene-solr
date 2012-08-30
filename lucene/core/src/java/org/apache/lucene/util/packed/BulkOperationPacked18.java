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
final class BulkOperationPacked18 extends BulkOperationPacked {

    public BulkOperationPacked18() {
      super(18);
      assert blockCount() == 9;
      assert valueCount() == 32;
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (block0 >>> 46);
        values[valuesOffset++] = (int) ((block0 >>> 28) & 262143L);
        values[valuesOffset++] = (int) ((block0 >>> 10) & 262143L);
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block0 & 1023L) << 8) | (block1 >>> 56));
        values[valuesOffset++] = (int) ((block1 >>> 38) & 262143L);
        values[valuesOffset++] = (int) ((block1 >>> 20) & 262143L);
        values[valuesOffset++] = (int) ((block1 >>> 2) & 262143L);
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block1 & 3L) << 16) | (block2 >>> 48));
        values[valuesOffset++] = (int) ((block2 >>> 30) & 262143L);
        values[valuesOffset++] = (int) ((block2 >>> 12) & 262143L);
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block2 & 4095L) << 6) | (block3 >>> 58));
        values[valuesOffset++] = (int) ((block3 >>> 40) & 262143L);
        values[valuesOffset++] = (int) ((block3 >>> 22) & 262143L);
        values[valuesOffset++] = (int) ((block3 >>> 4) & 262143L);
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block3 & 15L) << 14) | (block4 >>> 50));
        values[valuesOffset++] = (int) ((block4 >>> 32) & 262143L);
        values[valuesOffset++] = (int) ((block4 >>> 14) & 262143L);
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block4 & 16383L) << 4) | (block5 >>> 60));
        values[valuesOffset++] = (int) ((block5 >>> 42) & 262143L);
        values[valuesOffset++] = (int) ((block5 >>> 24) & 262143L);
        values[valuesOffset++] = (int) ((block5 >>> 6) & 262143L);
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block5 & 63L) << 12) | (block6 >>> 52));
        values[valuesOffset++] = (int) ((block6 >>> 34) & 262143L);
        values[valuesOffset++] = (int) ((block6 >>> 16) & 262143L);
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block6 & 65535L) << 2) | (block7 >>> 62));
        values[valuesOffset++] = (int) ((block7 >>> 44) & 262143L);
        values[valuesOffset++] = (int) ((block7 >>> 26) & 262143L);
        values[valuesOffset++] = (int) ((block7 >>> 8) & 262143L);
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = (int) (((block7 & 255L) << 10) | (block8 >>> 54));
        values[valuesOffset++] = (int) ((block8 >>> 36) & 262143L);
        values[valuesOffset++] = (int) ((block8 >>> 18) & 262143L);
        values[valuesOffset++] = (int) (block8 & 262143L);
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
        values[valuesOffset++] = (byte0 << 10) | (byte1 << 2) | (byte2 >>> 6);
        final int byte3 = blocks[blocksOffset++] & 0xFF;
        final int byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 63) << 12) | (byte3 << 4) | (byte4 >>> 4);
        final int byte5 = blocks[blocksOffset++] & 0xFF;
        final int byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 15) << 14) | (byte5 << 6) | (byte6 >>> 2);
        final int byte7 = blocks[blocksOffset++] & 0xFF;
        final int byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 3) << 16) | (byte7 << 8) | byte8;
        final int byte9 = blocks[blocksOffset++] & 0xFF;
        final int byte10 = blocks[blocksOffset++] & 0xFF;
        final int byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte9 << 10) | (byte10 << 2) | (byte11 >>> 6);
        final int byte12 = blocks[blocksOffset++] & 0xFF;
        final int byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 63) << 12) | (byte12 << 4) | (byte13 >>> 4);
        final int byte14 = blocks[blocksOffset++] & 0xFF;
        final int byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 15) << 14) | (byte14 << 6) | (byte15 >>> 2);
        final int byte16 = blocks[blocksOffset++] & 0xFF;
        final int byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 16) | (byte16 << 8) | byte17;
        final int byte18 = blocks[blocksOffset++] & 0xFF;
        final int byte19 = blocks[blocksOffset++] & 0xFF;
        final int byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte18 << 10) | (byte19 << 2) | (byte20 >>> 6);
        final int byte21 = blocks[blocksOffset++] & 0xFF;
        final int byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte20 & 63) << 12) | (byte21 << 4) | (byte22 >>> 4);
        final int byte23 = blocks[blocksOffset++] & 0xFF;
        final int byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 14) | (byte23 << 6) | (byte24 >>> 2);
        final int byte25 = blocks[blocksOffset++] & 0xFF;
        final int byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 3) << 16) | (byte25 << 8) | byte26;
        final int byte27 = blocks[blocksOffset++] & 0xFF;
        final int byte28 = blocks[blocksOffset++] & 0xFF;
        final int byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte27 << 10) | (byte28 << 2) | (byte29 >>> 6);
        final int byte30 = blocks[blocksOffset++] & 0xFF;
        final int byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 63) << 12) | (byte30 << 4) | (byte31 >>> 4);
        final int byte32 = blocks[blocksOffset++] & 0xFF;
        final int byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 14) | (byte32 << 6) | (byte33 >>> 2);
        final int byte34 = blocks[blocksOffset++] & 0xFF;
        final int byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 16) | (byte34 << 8) | byte35;
        final int byte36 = blocks[blocksOffset++] & 0xFF;
        final int byte37 = blocks[blocksOffset++] & 0xFF;
        final int byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte36 << 10) | (byte37 << 2) | (byte38 >>> 6);
        final int byte39 = blocks[blocksOffset++] & 0xFF;
        final int byte40 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 63) << 12) | (byte39 << 4) | (byte40 >>> 4);
        final int byte41 = blocks[blocksOffset++] & 0xFF;
        final int byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte40 & 15) << 14) | (byte41 << 6) | (byte42 >>> 2);
        final int byte43 = blocks[blocksOffset++] & 0xFF;
        final int byte44 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 3) << 16) | (byte43 << 8) | byte44;
        final int byte45 = blocks[blocksOffset++] & 0xFF;
        final int byte46 = blocks[blocksOffset++] & 0xFF;
        final int byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte45 << 10) | (byte46 << 2) | (byte47 >>> 6);
        final int byte48 = blocks[blocksOffset++] & 0xFF;
        final int byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 63) << 12) | (byte48 << 4) | (byte49 >>> 4);
        final int byte50 = blocks[blocksOffset++] & 0xFF;
        final int byte51 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte49 & 15) << 14) | (byte50 << 6) | (byte51 >>> 2);
        final int byte52 = blocks[blocksOffset++] & 0xFF;
        final int byte53 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte51 & 3) << 16) | (byte52 << 8) | byte53;
        final int byte54 = blocks[blocksOffset++] & 0xFF;
        final int byte55 = blocks[blocksOffset++] & 0xFF;
        final int byte56 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte54 << 10) | (byte55 << 2) | (byte56 >>> 6);
        final int byte57 = blocks[blocksOffset++] & 0xFF;
        final int byte58 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte56 & 63) << 12) | (byte57 << 4) | (byte58 >>> 4);
        final int byte59 = blocks[blocksOffset++] & 0xFF;
        final int byte60 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte58 & 15) << 14) | (byte59 << 6) | (byte60 >>> 2);
        final int byte61 = blocks[blocksOffset++] & 0xFF;
        final int byte62 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte60 & 3) << 16) | (byte61 << 8) | byte62;
        final int byte63 = blocks[blocksOffset++] & 0xFF;
        final int byte64 = blocks[blocksOffset++] & 0xFF;
        final int byte65 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte63 << 10) | (byte64 << 2) | (byte65 >>> 6);
        final int byte66 = blocks[blocksOffset++] & 0xFF;
        final int byte67 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte65 & 63) << 12) | (byte66 << 4) | (byte67 >>> 4);
        final int byte68 = blocks[blocksOffset++] & 0xFF;
        final int byte69 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte67 & 15) << 14) | (byte68 << 6) | (byte69 >>> 2);
        final int byte70 = blocks[blocksOffset++] & 0xFF;
        final int byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte69 & 3) << 16) | (byte70 << 8) | byte71;
      }
    }

    @Override
    public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
      assert blocksOffset + iterations * blockCount() <= blocks.length;
      assert valuesOffset + iterations * valueCount() <= values.length;
      for (int i = 0; i < iterations; ++i) {
        final long block0 = blocks[blocksOffset++];
        values[valuesOffset++] = block0 >>> 46;
        values[valuesOffset++] = (block0 >>> 28) & 262143L;
        values[valuesOffset++] = (block0 >>> 10) & 262143L;
        final long block1 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block0 & 1023L) << 8) | (block1 >>> 56);
        values[valuesOffset++] = (block1 >>> 38) & 262143L;
        values[valuesOffset++] = (block1 >>> 20) & 262143L;
        values[valuesOffset++] = (block1 >>> 2) & 262143L;
        final long block2 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block1 & 3L) << 16) | (block2 >>> 48);
        values[valuesOffset++] = (block2 >>> 30) & 262143L;
        values[valuesOffset++] = (block2 >>> 12) & 262143L;
        final long block3 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block2 & 4095L) << 6) | (block3 >>> 58);
        values[valuesOffset++] = (block3 >>> 40) & 262143L;
        values[valuesOffset++] = (block3 >>> 22) & 262143L;
        values[valuesOffset++] = (block3 >>> 4) & 262143L;
        final long block4 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block3 & 15L) << 14) | (block4 >>> 50);
        values[valuesOffset++] = (block4 >>> 32) & 262143L;
        values[valuesOffset++] = (block4 >>> 14) & 262143L;
        final long block5 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block4 & 16383L) << 4) | (block5 >>> 60);
        values[valuesOffset++] = (block5 >>> 42) & 262143L;
        values[valuesOffset++] = (block5 >>> 24) & 262143L;
        values[valuesOffset++] = (block5 >>> 6) & 262143L;
        final long block6 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block5 & 63L) << 12) | (block6 >>> 52);
        values[valuesOffset++] = (block6 >>> 34) & 262143L;
        values[valuesOffset++] = (block6 >>> 16) & 262143L;
        final long block7 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block6 & 65535L) << 2) | (block7 >>> 62);
        values[valuesOffset++] = (block7 >>> 44) & 262143L;
        values[valuesOffset++] = (block7 >>> 26) & 262143L;
        values[valuesOffset++] = (block7 >>> 8) & 262143L;
        final long block8 = blocks[blocksOffset++];
        values[valuesOffset++] = ((block7 & 255L) << 10) | (block8 >>> 54);
        values[valuesOffset++] = (block8 >>> 36) & 262143L;
        values[valuesOffset++] = (block8 >>> 18) & 262143L;
        values[valuesOffset++] = block8 & 262143L;
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
        values[valuesOffset++] = (byte0 << 10) | (byte1 << 2) | (byte2 >>> 6);
        final long byte3 = blocks[blocksOffset++] & 0xFF;
        final long byte4 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte2 & 63) << 12) | (byte3 << 4) | (byte4 >>> 4);
        final long byte5 = blocks[blocksOffset++] & 0xFF;
        final long byte6 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte4 & 15) << 14) | (byte5 << 6) | (byte6 >>> 2);
        final long byte7 = blocks[blocksOffset++] & 0xFF;
        final long byte8 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte6 & 3) << 16) | (byte7 << 8) | byte8;
        final long byte9 = blocks[blocksOffset++] & 0xFF;
        final long byte10 = blocks[blocksOffset++] & 0xFF;
        final long byte11 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte9 << 10) | (byte10 << 2) | (byte11 >>> 6);
        final long byte12 = blocks[blocksOffset++] & 0xFF;
        final long byte13 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte11 & 63) << 12) | (byte12 << 4) | (byte13 >>> 4);
        final long byte14 = blocks[blocksOffset++] & 0xFF;
        final long byte15 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte13 & 15) << 14) | (byte14 << 6) | (byte15 >>> 2);
        final long byte16 = blocks[blocksOffset++] & 0xFF;
        final long byte17 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte15 & 3) << 16) | (byte16 << 8) | byte17;
        final long byte18 = blocks[blocksOffset++] & 0xFF;
        final long byte19 = blocks[blocksOffset++] & 0xFF;
        final long byte20 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte18 << 10) | (byte19 << 2) | (byte20 >>> 6);
        final long byte21 = blocks[blocksOffset++] & 0xFF;
        final long byte22 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte20 & 63) << 12) | (byte21 << 4) | (byte22 >>> 4);
        final long byte23 = blocks[blocksOffset++] & 0xFF;
        final long byte24 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte22 & 15) << 14) | (byte23 << 6) | (byte24 >>> 2);
        final long byte25 = blocks[blocksOffset++] & 0xFF;
        final long byte26 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte24 & 3) << 16) | (byte25 << 8) | byte26;
        final long byte27 = blocks[blocksOffset++] & 0xFF;
        final long byte28 = blocks[blocksOffset++] & 0xFF;
        final long byte29 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte27 << 10) | (byte28 << 2) | (byte29 >>> 6);
        final long byte30 = blocks[blocksOffset++] & 0xFF;
        final long byte31 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte29 & 63) << 12) | (byte30 << 4) | (byte31 >>> 4);
        final long byte32 = blocks[blocksOffset++] & 0xFF;
        final long byte33 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte31 & 15) << 14) | (byte32 << 6) | (byte33 >>> 2);
        final long byte34 = blocks[blocksOffset++] & 0xFF;
        final long byte35 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte33 & 3) << 16) | (byte34 << 8) | byte35;
        final long byte36 = blocks[blocksOffset++] & 0xFF;
        final long byte37 = blocks[blocksOffset++] & 0xFF;
        final long byte38 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte36 << 10) | (byte37 << 2) | (byte38 >>> 6);
        final long byte39 = blocks[blocksOffset++] & 0xFF;
        final long byte40 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte38 & 63) << 12) | (byte39 << 4) | (byte40 >>> 4);
        final long byte41 = blocks[blocksOffset++] & 0xFF;
        final long byte42 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte40 & 15) << 14) | (byte41 << 6) | (byte42 >>> 2);
        final long byte43 = blocks[blocksOffset++] & 0xFF;
        final long byte44 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte42 & 3) << 16) | (byte43 << 8) | byte44;
        final long byte45 = blocks[blocksOffset++] & 0xFF;
        final long byte46 = blocks[blocksOffset++] & 0xFF;
        final long byte47 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte45 << 10) | (byte46 << 2) | (byte47 >>> 6);
        final long byte48 = blocks[blocksOffset++] & 0xFF;
        final long byte49 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte47 & 63) << 12) | (byte48 << 4) | (byte49 >>> 4);
        final long byte50 = blocks[blocksOffset++] & 0xFF;
        final long byte51 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte49 & 15) << 14) | (byte50 << 6) | (byte51 >>> 2);
        final long byte52 = blocks[blocksOffset++] & 0xFF;
        final long byte53 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte51 & 3) << 16) | (byte52 << 8) | byte53;
        final long byte54 = blocks[blocksOffset++] & 0xFF;
        final long byte55 = blocks[blocksOffset++] & 0xFF;
        final long byte56 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte54 << 10) | (byte55 << 2) | (byte56 >>> 6);
        final long byte57 = blocks[blocksOffset++] & 0xFF;
        final long byte58 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte56 & 63) << 12) | (byte57 << 4) | (byte58 >>> 4);
        final long byte59 = blocks[blocksOffset++] & 0xFF;
        final long byte60 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte58 & 15) << 14) | (byte59 << 6) | (byte60 >>> 2);
        final long byte61 = blocks[blocksOffset++] & 0xFF;
        final long byte62 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte60 & 3) << 16) | (byte61 << 8) | byte62;
        final long byte63 = blocks[blocksOffset++] & 0xFF;
        final long byte64 = blocks[blocksOffset++] & 0xFF;
        final long byte65 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = (byte63 << 10) | (byte64 << 2) | (byte65 >>> 6);
        final long byte66 = blocks[blocksOffset++] & 0xFF;
        final long byte67 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte65 & 63) << 12) | (byte66 << 4) | (byte67 >>> 4);
        final long byte68 = blocks[blocksOffset++] & 0xFF;
        final long byte69 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte67 & 15) << 14) | (byte68 << 6) | (byte69 >>> 2);
        final long byte70 = blocks[blocksOffset++] & 0xFF;
        final long byte71 = blocks[blocksOffset++] & 0xFF;
        values[valuesOffset++] = ((byte69 & 3) << 16) | (byte70 << 8) | byte71;
      }
    }

}
