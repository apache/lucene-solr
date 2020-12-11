// This file has been automatically generated, DO NOT EDIT

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
package org.apache.lucene.util.packed;

/**
 * Efficient sequential read/write of packed integers.
 */
final class BulkOperationPacked13 extends BulkOperationPacked {

  public BulkOperationPacked13() {
    super(13);
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (block0 & 8191L);
      values[valuesOffset++] = (int) ((block0 >>> 13) & 8191L);
      values[valuesOffset++] = (int) ((block0 >>> 26) & 8191L);
      values[valuesOffset++] = (int) ((block0 >>> 39) & 8191L);
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block1 & 1L) << 12) | (block0 >>> 52));
      values[valuesOffset++] = (int) ((block1 >>> 1) & 8191L);
      values[valuesOffset++] = (int) ((block1 >>> 14) & 8191L);
      values[valuesOffset++] = (int) ((block1 >>> 27) & 8191L);
      values[valuesOffset++] = (int) ((block1 >>> 40) & 8191L);
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block2 & 3L) << 11) | (block1 >>> 53));
      values[valuesOffset++] = (int) ((block2 >>> 2) & 8191L);
      values[valuesOffset++] = (int) ((block2 >>> 15) & 8191L);
      values[valuesOffset++] = (int) ((block2 >>> 28) & 8191L);
      values[valuesOffset++] = (int) ((block2 >>> 41) & 8191L);
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block3 & 7L) << 10) | (block2 >>> 54));
      values[valuesOffset++] = (int) ((block3 >>> 3) & 8191L);
      values[valuesOffset++] = (int) ((block3 >>> 16) & 8191L);
      values[valuesOffset++] = (int) ((block3 >>> 29) & 8191L);
      values[valuesOffset++] = (int) ((block3 >>> 42) & 8191L);
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block4 & 15L) << 9) | (block3 >>> 55));
      values[valuesOffset++] = (int) ((block4 >>> 4) & 8191L);
      values[valuesOffset++] = (int) ((block4 >>> 17) & 8191L);
      values[valuesOffset++] = (int) ((block4 >>> 30) & 8191L);
      values[valuesOffset++] = (int) ((block4 >>> 43) & 8191L);
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block5 & 31L) << 8) | (block4 >>> 56));
      values[valuesOffset++] = (int) ((block5 >>> 5) & 8191L);
      values[valuesOffset++] = (int) ((block5 >>> 18) & 8191L);
      values[valuesOffset++] = (int) ((block5 >>> 31) & 8191L);
      values[valuesOffset++] = (int) ((block5 >>> 44) & 8191L);
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block6 & 63L) << 7) | (block5 >>> 57));
      values[valuesOffset++] = (int) ((block6 >>> 6) & 8191L);
      values[valuesOffset++] = (int) ((block6 >>> 19) & 8191L);
      values[valuesOffset++] = (int) ((block6 >>> 32) & 8191L);
      values[valuesOffset++] = (int) ((block6 >>> 45) & 8191L);
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block7 & 127L) << 6) | (block6 >>> 58));
      values[valuesOffset++] = (int) ((block7 >>> 7) & 8191L);
      values[valuesOffset++] = (int) ((block7 >>> 20) & 8191L);
      values[valuesOffset++] = (int) ((block7 >>> 33) & 8191L);
      values[valuesOffset++] = (int) ((block7 >>> 46) & 8191L);
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block8 & 255L) << 5) | (block7 >>> 59));
      values[valuesOffset++] = (int) ((block8 >>> 8) & 8191L);
      values[valuesOffset++] = (int) ((block8 >>> 21) & 8191L);
      values[valuesOffset++] = (int) ((block8 >>> 34) & 8191L);
      values[valuesOffset++] = (int) ((block8 >>> 47) & 8191L);
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block9 & 511L) << 4) | (block8 >>> 60));
      values[valuesOffset++] = (int) ((block9 >>> 9) & 8191L);
      values[valuesOffset++] = (int) ((block9 >>> 22) & 8191L);
      values[valuesOffset++] = (int) ((block9 >>> 35) & 8191L);
      values[valuesOffset++] = (int) ((block9 >>> 48) & 8191L);
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block10 & 1023L) << 3) | (block9 >>> 61));
      values[valuesOffset++] = (int) ((block10 >>> 10) & 8191L);
      values[valuesOffset++] = (int) ((block10 >>> 23) & 8191L);
      values[valuesOffset++] = (int) ((block10 >>> 36) & 8191L);
      values[valuesOffset++] = (int) ((block10 >>> 49) & 8191L);
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block11 & 2047L) << 2) | (block10 >>> 62));
      values[valuesOffset++] = (int) ((block11 >>> 11) & 8191L);
      values[valuesOffset++] = (int) ((block11 >>> 24) & 8191L);
      values[valuesOffset++] = (int) ((block11 >>> 37) & 8191L);
      values[valuesOffset++] = (int) ((block11 >>> 50) & 8191L);
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = (int) (((block12 & 4095L) << 1) | (block11 >>> 63));
      values[valuesOffset++] = (int) ((block12 >>> 12) & 8191L);
      values[valuesOffset++] = (int) ((block12 >>> 25) & 8191L);
      values[valuesOffset++] = (int) ((block12 >>> 38) & 8191L);
      values[valuesOffset++] = (int) (block12 >>> 51);
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, int[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final int byte0 = blocks[blocksOffset++] & 0xFF;
      final int byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 31) << 8) | byte0;
      final int byte2 = blocks[blocksOffset++] & 0xFF;
      final int byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 3) << 11) | (byte2 << 3) | (byte1 >>> 5);
      final int byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 127) << 6) | (byte3 >>> 2);
      final int byte5 = blocks[blocksOffset++] & 0xFF;
      final int byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 15) << 9) | (byte5 << 1) | (byte4 >>> 7);
      final int byte7 = blocks[blocksOffset++] & 0xFF;
      final int byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 1) << 12) | (byte7 << 4) | (byte6 >>> 4);
      final int byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 63) << 7) | (byte8 >>> 1);
      final int byte10 = blocks[blocksOffset++] & 0xFF;
      final int byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 7) << 10) | (byte10 << 2) | (byte9 >>> 6);
      final int byte12 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte12 << 5) | (byte11 >>> 3);
    }
  }

  @Override
  public void decode(long[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long block0 = blocks[blocksOffset++];
      values[valuesOffset++] = block0 & 8191L;
      values[valuesOffset++] = (block0 >>> 13) & 8191L;
      values[valuesOffset++] = (block0 >>> 26) & 8191L;
      values[valuesOffset++] = (block0 >>> 39) & 8191L;
      final long block1 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block1 & 1L) << 12) | (block0 >>> 52);
      values[valuesOffset++] = (block1 >>> 1) & 8191L;
      values[valuesOffset++] = (block1 >>> 14) & 8191L;
      values[valuesOffset++] = (block1 >>> 27) & 8191L;
      values[valuesOffset++] = (block1 >>> 40) & 8191L;
      final long block2 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block2 & 3L) << 11) | (block1 >>> 53);
      values[valuesOffset++] = (block2 >>> 2) & 8191L;
      values[valuesOffset++] = (block2 >>> 15) & 8191L;
      values[valuesOffset++] = (block2 >>> 28) & 8191L;
      values[valuesOffset++] = (block2 >>> 41) & 8191L;
      final long block3 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block3 & 7L) << 10) | (block2 >>> 54);
      values[valuesOffset++] = (block3 >>> 3) & 8191L;
      values[valuesOffset++] = (block3 >>> 16) & 8191L;
      values[valuesOffset++] = (block3 >>> 29) & 8191L;
      values[valuesOffset++] = (block3 >>> 42) & 8191L;
      final long block4 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block4 & 15L) << 9) | (block3 >>> 55);
      values[valuesOffset++] = (block4 >>> 4) & 8191L;
      values[valuesOffset++] = (block4 >>> 17) & 8191L;
      values[valuesOffset++] = (block4 >>> 30) & 8191L;
      values[valuesOffset++] = (block4 >>> 43) & 8191L;
      final long block5 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block5 & 31L) << 8) | (block4 >>> 56);
      values[valuesOffset++] = (block5 >>> 5) & 8191L;
      values[valuesOffset++] = (block5 >>> 18) & 8191L;
      values[valuesOffset++] = (block5 >>> 31) & 8191L;
      values[valuesOffset++] = (block5 >>> 44) & 8191L;
      final long block6 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block6 & 63L) << 7) | (block5 >>> 57);
      values[valuesOffset++] = (block6 >>> 6) & 8191L;
      values[valuesOffset++] = (block6 >>> 19) & 8191L;
      values[valuesOffset++] = (block6 >>> 32) & 8191L;
      values[valuesOffset++] = (block6 >>> 45) & 8191L;
      final long block7 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block7 & 127L) << 6) | (block6 >>> 58);
      values[valuesOffset++] = (block7 >>> 7) & 8191L;
      values[valuesOffset++] = (block7 >>> 20) & 8191L;
      values[valuesOffset++] = (block7 >>> 33) & 8191L;
      values[valuesOffset++] = (block7 >>> 46) & 8191L;
      final long block8 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block8 & 255L) << 5) | (block7 >>> 59);
      values[valuesOffset++] = (block8 >>> 8) & 8191L;
      values[valuesOffset++] = (block8 >>> 21) & 8191L;
      values[valuesOffset++] = (block8 >>> 34) & 8191L;
      values[valuesOffset++] = (block8 >>> 47) & 8191L;
      final long block9 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block9 & 511L) << 4) | (block8 >>> 60);
      values[valuesOffset++] = (block9 >>> 9) & 8191L;
      values[valuesOffset++] = (block9 >>> 22) & 8191L;
      values[valuesOffset++] = (block9 >>> 35) & 8191L;
      values[valuesOffset++] = (block9 >>> 48) & 8191L;
      final long block10 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block10 & 1023L) << 3) | (block9 >>> 61);
      values[valuesOffset++] = (block10 >>> 10) & 8191L;
      values[valuesOffset++] = (block10 >>> 23) & 8191L;
      values[valuesOffset++] = (block10 >>> 36) & 8191L;
      values[valuesOffset++] = (block10 >>> 49) & 8191L;
      final long block11 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block11 & 2047L) << 2) | (block10 >>> 62);
      values[valuesOffset++] = (block11 >>> 11) & 8191L;
      values[valuesOffset++] = (block11 >>> 24) & 8191L;
      values[valuesOffset++] = (block11 >>> 37) & 8191L;
      values[valuesOffset++] = (block11 >>> 50) & 8191L;
      final long block12 = blocks[blocksOffset++];
      values[valuesOffset++] = ((block12 & 4095L) << 1) | (block11 >>> 63);
      values[valuesOffset++] = (block12 >>> 12) & 8191L;
      values[valuesOffset++] = (block12 >>> 25) & 8191L;
      values[valuesOffset++] = (block12 >>> 38) & 8191L;
      values[valuesOffset++] = block12 >>> 51;
    }
  }

  @Override
  public void decode(byte[] blocks, int blocksOffset, long[] values, int valuesOffset, int iterations) {
    for (int i = 0; i < iterations; ++i) {
      final long byte0 = blocks[blocksOffset++] & 0xFF;
      final long byte1 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte1 & 31) << 8) | byte0;
      final long byte2 = blocks[blocksOffset++] & 0xFF;
      final long byte3 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte3 & 3) << 11) | (byte2 << 3) | (byte1 >>> 5);
      final long byte4 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte4 & 127) << 6) | (byte3 >>> 2);
      final long byte5 = blocks[blocksOffset++] & 0xFF;
      final long byte6 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte6 & 15) << 9) | (byte5 << 1) | (byte4 >>> 7);
      final long byte7 = blocks[blocksOffset++] & 0xFF;
      final long byte8 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte8 & 1) << 12) | (byte7 << 4) | (byte6 >>> 4);
      final long byte9 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte9 & 63) << 7) | (byte8 >>> 1);
      final long byte10 = blocks[blocksOffset++] & 0xFF;
      final long byte11 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = ((byte11 & 7) << 10) | (byte10 << 2) | (byte9 >>> 6);
      final long byte12 = blocks[blocksOffset++] & 0xFF;
      values[valuesOffset++] = (byte12 << 5) | (byte11 >>> 3);
    }
  }

}
