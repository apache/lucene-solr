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

import java.io.IOException;

import org.apache.lucene.store.DataOutput;

/**     
 * Implements {@link PackedInts.Mutable}, but grows the
 * bit count of the underlying packed ints on-demand.
 *
 * <p>@lucene.internal</p>
 */

public class GrowableWriter implements PackedInts.Mutable {

  private long currentMaxValue;
  private PackedInts.Mutable current;
  private final float acceptableOverheadRatio;

  public GrowableWriter(int startBitsPerValue, int valueCount, float acceptableOverheadRatio) {
    this.acceptableOverheadRatio = acceptableOverheadRatio;
    current = PackedInts.getMutable(valueCount, startBitsPerValue, this.acceptableOverheadRatio);
    currentMaxValue = PackedInts.maxValue(current.getBitsPerValue());
  }

  @Override
  public long get(int index) {
    return current.get(index);
  }

  @Override
  public int size() {
    return current.size();
  }

  @Override
  public int getBitsPerValue() {
    return current.getBitsPerValue();
  }

  public PackedInts.Mutable getMutable() {
    return current;
  }

  @Override
  public Object getArray() {
    return current.getArray();
  }

  @Override
  public boolean hasArray() {
    return current.hasArray();
  }

  private void ensureCapacity(long value) {
    assert value >= 0;
    if (value <= currentMaxValue) {
      return;
    }
    final int bitsRequired = PackedInts.bitsRequired(value);
    final int valueCount = size();
    PackedInts.Mutable next = PackedInts.getMutable(valueCount, bitsRequired, acceptableOverheadRatio);
    PackedInts.copy(current, 0, next, 0, valueCount, PackedInts.DEFAULT_BUFFER_SIZE);
    current = next;
    currentMaxValue = PackedInts.maxValue(current.getBitsPerValue());
  }

  @Override
  public void set(int index, long value) {
    ensureCapacity(value);
    current.set(index, value);
  }

  @Override
  public void clear() {
    current.clear();
  }

  public GrowableWriter resize(int newSize) {
    GrowableWriter next = new GrowableWriter(getBitsPerValue(), newSize, acceptableOverheadRatio);
    final int limit = Math.min(size(), newSize);
    PackedInts.copy(current, 0, next, 0, limit, PackedInts.DEFAULT_BUFFER_SIZE);
    return next;
  }

  @Override
  public int get(int index, long[] arr, int off, int len) {
    return current.get(index, arr, off, len);
  }

  @Override
  public int set(int index, long[] arr, int off, int len) {
    long max = 0;
    for (int i = off, end = off + len; i < end; ++i) {
      max |= arr[i];
    }
    ensureCapacity(max);
    return current.set(index, arr, off, len);
  }

  @Override
  public void fill(int fromIndex, int toIndex, long val) {
    ensureCapacity(val);
    current.fill(fromIndex, toIndex, val);
  }

  @Override
  public long ramBytesUsed() {
    return current.ramBytesUsed();
  }

  @Override
  public void save(DataOutput out) throws IOException {
    current.save(out);
  }

}
