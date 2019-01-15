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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;

/**
 * Packs integers into 3 shorts (48 bits per value).
 * @lucene.internal
 */
final class Packed16ThreeBlocks extends PackedInts.MutableImpl {
  final short[] blocks;

  public static final int MAX_SIZE = Integer.MAX_VALUE / 3;

  Packed16ThreeBlocks(int valueCount) {
    super(valueCount, 48);
    if (valueCount > MAX_SIZE) {
      throw new ArrayIndexOutOfBoundsException("MAX_SIZE exceeded");
    }
    blocks = new short[valueCount * 3];
  }

  Packed16ThreeBlocks(int packedIntsVersion, DataInput in, int valueCount) throws IOException {
    this(valueCount);
    for (int i = 0; i < 3 * valueCount; ++i) {
      blocks[i] = in.readShort();
    }
  }

  @Override
  public long get(int index) {
    final int o = index * 3;
    return (blocks[o] & 0xFFFFL) << 32 | (blocks[o+1] & 0xFFFFL) << 16 | (blocks[o+2] & 0xFFFFL);
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int gets = Math.min(valueCount - index, len);
    for (int i = index * 3, end = (index + gets) * 3; i < end; i+=3) {
      arr[off++] = (blocks[i] & 0xFFFFL) << 32 | (blocks[i+1] & 0xFFFFL) << 16 | (blocks[i+2] & 0xFFFFL);
    }
    return gets;
  }

  @Override
  public void set(int index, long value) {
    final int o = index * 3;
    blocks[o] = (short) (value >>> 32);
    blocks[o+1] = (short) (value >>> 16);
    blocks[o+2] = (short) value;
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int sets = Math.min(valueCount - index, len);
    for (int i = off, o = index * 3, end = off + sets; i < end; ++i) {
      final long value = arr[i];
      blocks[o++] = (short) (value >>> 32);
      blocks[o++] = (short) (value >>> 16);
      blocks[o++] = (short) value;
    }
    return sets;
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    final short block1 = (short) (val >>> 32);
    final short block2 = (short) (val >>> 16);
    final short block3 = (short) val;
    for (int i = fromIndex * 3, end = toIndex * 3; i < end; i += 3) {
      blocks[i] = block1;
      blocks[i+1] = block2;
      blocks[i+2] = block3;
    }
  }

  @Override
  public void clear() {
    Arrays.fill(blocks, (short) 0);
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 2 * Integer.BYTES                       // valueCount,bitsPerValue
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // blocks ref
        + RamUsageEstimator.sizeOf(blocks);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(bitsPerValue=" + bitsPerValue
        + ",size=" + size() + ",blocks=" + blocks.length + ")";
  }
}
