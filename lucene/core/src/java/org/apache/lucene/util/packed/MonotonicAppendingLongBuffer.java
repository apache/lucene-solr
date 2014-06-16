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

import static org.apache.lucene.util.packed.MonotonicBlockPackedReader.expected;

import java.util.Arrays;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Utility class to buffer signed longs in memory, which is optimized for the
 * case where the sequence is monotonic, although it can encode any sequence of
 * arbitrary longs. It only supports appending.
 *
 * @lucene.internal
 */
public final class MonotonicAppendingLongBuffer extends AbstractAppendingLongBuffer {

  float[] averages;
  long[] minValues;

  /**
   * @param initialPageCount        the initial number of pages
   * @param pageSize                the size of a single page
   * @param acceptableOverheadRatio an acceptable overhead ratio per value
   */
  public MonotonicAppendingLongBuffer(int initialPageCount, int pageSize, float acceptableOverheadRatio) {
    super(initialPageCount, pageSize, acceptableOverheadRatio);
    averages = new float[values.length];
    minValues = new long[values.length];
  }

  /**
   * Create an {@link MonotonicAppendingLongBuffer} with initialPageCount=16,
   * pageSize=1024 and acceptableOverheadRatio={@link PackedInts#DEFAULT}
   */
  public MonotonicAppendingLongBuffer() {
    this(16, 1024, PackedInts.DEFAULT);
  }

  /**
   * Create an {@link AppendingDeltaPackedLongBuffer} with initialPageCount=16,
   * pageSize=1024
   */
  public MonotonicAppendingLongBuffer(float acceptableOverheadRatio) {
    this(16, 1024, acceptableOverheadRatio);
  }

  @Override
  long get(int block, int element) {
    if (block == valuesOff) {
      return pending[element];
    } else {
      return expected(minValues[block], averages[block], element) + values[block].get(element);
    }
  }

  @Override
  int get(int block, int element, long[] arr, int off, int len) {
    if (block == valuesOff) {
      int sysCopyToRead = Math.min(len, pendingOff - element);
      System.arraycopy(pending, element, arr, off, sysCopyToRead);
      return sysCopyToRead;
    } else {
      int read = values[block].get(element, arr, off, len);
      for (int r = 0; r < read; r++, off++, element++) {
        arr[off] += expected(minValues[block], averages[block], element);
      }
      return read;
    }
  }

  @Override
  void grow(int newBlockCount) {
    super.grow(newBlockCount);
    this.averages = Arrays.copyOf(averages, newBlockCount);
    this.minValues = Arrays.copyOf(minValues, newBlockCount);
  }

  @Override
  void packPendingValues() {
    assert pendingOff > 0;
    final float average = pendingOff == 1 ? 0 : (float) (pending[pendingOff - 1] - pending[0]) / (pendingOff - 1);
    long minValue = pending[0];
    // adjust minValue so that all deltas will be positive
    for (int i = 1; i < pendingOff; ++i) {
      final long actual = pending[i];
      final long expected = expected(minValue, average, i);
      if (expected > actual) {
        minValue -= (expected - actual);
      }
    }

    minValues[valuesOff] = minValue;
    averages[valuesOff] = average;

    for (int i = 0; i < pendingOff; ++i) {
      pending[i] = pending[i] - expected(minValue, average, i);
    }
    long maxDelta = 0;
    for (int i = 0; i < pendingOff; ++i) {
      if (pending[i] < 0) {
        maxDelta = -1;
        break;
      } else {
        maxDelta = Math.max(maxDelta, pending[i]);
      }
    }
    if (maxDelta == 0) {
      values[valuesOff] = new PackedInts.NullReader(pendingOff);
    } else {
      final int bitsRequired = PackedInts.unsignedBitsRequired(maxDelta);
      final PackedInts.Mutable mutable = PackedInts.getMutable(pendingOff, bitsRequired, acceptableOverheadRatio);
      for (int i = 0; i < pendingOff; ) {
        i += mutable.set(i, pending, i, pendingOff - i);
      }
      values[valuesOff] = mutable;
    }
  }

  @Override
  long baseRamBytesUsed() {
    return super.baseRamBytesUsed()
        + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF; // 2 additional arrays
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed()
        + RamUsageEstimator.sizeOf(averages) + RamUsageEstimator.sizeOf(minValues);
  }

}
