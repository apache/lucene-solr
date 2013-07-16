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

import java.util.Arrays;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Utility class to buffer signed longs in memory, which is optimized for the
 * case where the sequence is monotonic, although it can encode any sequence of
 * arbitrary longs. It only supports appending.
 * @lucene.internal
 */
public final class MonotonicAppendingLongBuffer extends AbstractAppendingLongBuffer {

  static long zigZagDecode(long n) {
    return ((n >>> 1) ^ -(n & 1));
  }
  
  static long zigZagEncode(long n) {
    return (n >> 63) ^ (n << 1);
  }

  float[] averages;

  /** @param initialPageCount the initial number of pages
   *  @param pageSize         the size of a single page */
  public MonotonicAppendingLongBuffer(int initialPageCount, int pageSize) {
    super(initialPageCount, pageSize);
    averages = new float[pageSize];
  }

  /** Create an {@link MonotonicAppendingLongBuffer} with initialPageCount=16
   *  and pageSize=1024. */
  public MonotonicAppendingLongBuffer() {
    this(16, 1024);
  }

  @Override
  long get(int block, int element) {
    if (block == valuesOff) {
      return pending[element];
    } else {
      final long base = minValues[block] + (long) (averages[block] * (long) element);
      if (deltas[block] == null) {
        return base;
      } else {
        return base + zigZagDecode(deltas[block].get(element));
      }
    }
  }

  @Override
  void grow(int newBlockCount) {
    super.grow(newBlockCount);
    this.averages = Arrays.copyOf(averages, newBlockCount);
  }

  @Override
  void packPendingValues() {
    assert pendingOff > 0;
    minValues[valuesOff] = pending[0];
    averages[valuesOff] = pendingOff == 1 ? 0 : (float) (pending[pendingOff - 1] - pending[0]) / (pendingOff - 1);

    for (int i = 0; i < pendingOff; ++i) {
      pending[i] = zigZagEncode(pending[i] - minValues[valuesOff] - (long) (averages[valuesOff] * (long) i));
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
      deltas[valuesOff] = new  PackedInts.NullReader(pendingOff);
    } else {
      final int bitsRequired = maxDelta < 0 ? 64 : PackedInts.bitsRequired(maxDelta);
      final PackedInts.Mutable mutable = PackedInts.getMutable(pendingOff, bitsRequired, PackedInts.COMPACT);
      for (int i = 0; i < pendingOff; ) {
        i += mutable.set(i, pending, i, pendingOff - i);
      }
      deltas[valuesOff] = mutable;
    }
  }

  /** Return an iterator over the values of this buffer. */
  @Override
  public Iterator iterator() {
    return new Iterator();
  }

  /** A long iterator. */
  public final class Iterator extends AbstractAppendingLongBuffer.Iterator {

    Iterator() {
      super();
    }

    @Override
    void fillValues() {
      if (vOff == valuesOff) {
        currentValues = pending;
        currentCount = pendingOff;
      } else {
        currentCount = deltas[vOff].size();
        for (int k = 0; k < currentCount; ) {
          k += deltas[vOff].get(k, currentValues, k, currentCount - k);
        }
        for (int k = 0; k < currentCount; ++k) {
          currentValues[k] = minValues[vOff] + (long) (averages[vOff] * (long) k) + zigZagDecode(currentValues[k]);
        }
      }
    }

  }

  @Override
  long baseRamBytesUsed() {
    return super.baseRamBytesUsed()
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF; // the additional array
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed()
        + RamUsageEstimator.sizeOf(averages);
  }

}
