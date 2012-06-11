package org.apache.lucene.util.packed;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.RamUsageEstimator;

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

/** 24 bitsPerValue backed by byte[] */
final class Packed8ThreeBlocks extends PackedInts.MutableImpl {

  public static final int MAX_SIZE = Integer.MAX_VALUE / 3;

  private final byte[] blocks;

  Packed8ThreeBlocks(int valueCount) {
    super(valueCount, 24);
    if (valueCount > MAX_SIZE) {
      throw new ArrayIndexOutOfBoundsException("MAX_SIZE exceeded");
    }
    this.blocks = new byte[3 * valueCount];
  }

  Packed8ThreeBlocks(DataInput in, int valueCount) throws IOException {
    this(valueCount);
    for (int i = 0; i < blocks.length; i++) {
      blocks[i] = in.readByte();
    }
    final int mod = blocks.length % 8;
    if (mod != 0) {
      final int pad = 8 - mod;
      // round out long
      for (int i = 0; i < pad; i++) {
        in.readByte();
      }
    }
  }

  @Override
  public long get(int index) {
    final int o = index * 3;
    return (blocks[o] & 0xffL) << 16 | (blocks[o+1] & 0xffL) << 8 | (blocks[o+2] & 0xffL);
  }

  @Override
  public void set(int index, long value) {
    final int o = index * 3;
    blocks[o+2] = (byte) value;
    blocks[o+1] = (byte) (value >> 8);
    blocks[o] = (byte) (value >> 16);
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    byte block1 = (byte) (val >> 16);
    byte block2 = (byte) (val >> 8);
    byte block3 = (byte) val;
    for (int i = fromIndex * 3, end = toIndex * 3; i < end; ) {
      blocks[i++] = block1;
      blocks[i++] = block2;
      blocks[i++] = block3;
    }
  }

  @Override
  public void clear() {
    Arrays.fill(blocks, (byte) 0);
  }

  public long ramBytesUsed() {
    return RamUsageEstimator.sizeOf(blocks);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(bitsPerValue=" + bitsPerValue
        + ", size=" + size() + ", elements.length=" + blocks.length + ")";
  }

}
