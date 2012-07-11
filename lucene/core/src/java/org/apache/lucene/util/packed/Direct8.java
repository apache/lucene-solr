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

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Arrays;

/**
 * Direct wrapping of 8-bits values to a backing array.
 * @lucene.internal
 */
final class Direct8 extends PackedInts.MutableImpl {
  final byte[] values;

  Direct8(int valueCount) {
    super(valueCount, 8);
    values = new byte[valueCount];
  }

  Direct8(DataInput in, int valueCount) throws IOException {
    this(valueCount);
    for (int i = 0; i < valueCount; ++i) {
      values[i] = in.readByte();
    }
    final int mod = valueCount % 8;
    if (mod != 0) {
      for (int i = mod; i < 8; ++i) {
        in.readByte();
      }
    }
  }

  @Override
  public long get(final int index) {
    return values[index] & 0xFFL;
  }

  public void set(final int index, final long value) {
    values[index] = (byte) (value);
  }

  public long ramBytesUsed() {
    return RamUsageEstimator.sizeOf(values);
  }

  public void clear() {
    Arrays.fill(values, (byte) 0L);
  }

  @Override
  public Object getArray() {
    return values;
  }

  @Override
  public boolean hasArray() {
    return true;
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int gets = Math.min(valueCount - index, len);
    for (int i = index, o = off, end = index + gets; i < end; ++i, ++o) {
      arr[o] = values[i] & 0xFFL;
    }
    return gets;
  }

  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    assert off + len <= arr.length;

    final int sets = Math.min(valueCount - index, len);
    for (int i = index, o = off, end = index + sets; i < end; ++i, ++o) {
      values[i] = (byte) arr[o];
    }
    return sets;
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    assert val == (val & 0xFFL);
    Arrays.fill(values, fromIndex, toIndex, (byte) val);
  }
}
