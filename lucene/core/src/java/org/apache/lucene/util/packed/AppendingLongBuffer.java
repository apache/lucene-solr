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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Utility class to buffer a list of signed longs in memory. This class only
 * supports appending.
 * @lucene.internal
 */
public class AppendingLongBuffer {

  private static final int MAX_PENDING_COUNT = 1024;

  private long[] minValues;
  private PackedInts.Reader[] values;
  private int valuesOff;
  private long[] pending;
  private int pendingOff;

  /** Sole constructor. */
  public AppendingLongBuffer() {
    minValues = new long[16];
    values = new PackedInts.Reader[16];
    pending = new long[MAX_PENDING_COUNT];
    valuesOff = 0;
    pendingOff = 0;
  }

  /** Append a value to this buffer. */
  public void add(long l) {
    if (pendingOff == MAX_PENDING_COUNT) {
      packPendingValues();
    }
    pending[pendingOff++] = l;
  }

  private void packPendingValues() {
    assert pendingOff == MAX_PENDING_COUNT;

    // check size
    if (values.length == valuesOff) {
      final int newLength = ArrayUtil.oversize(valuesOff + 1, 8);
      minValues = Arrays.copyOf(minValues, newLength);
      values = Arrays.copyOf(values, newLength);
    }

    // compute max delta
    long minValue = pending[0];
    long maxValue = pending[0];
    for (int i = 1; i < pendingOff; ++i) {
      minValue = Math.min(minValue, pending[i]);
      maxValue = Math.max(maxValue, pending[i]);
    }
    final long delta = maxValue - minValue;

    minValues[valuesOff] = minValue;
    if (delta != 0) {
      // build a new packed reader
      final int bitsRequired = delta < 0 ? 64 : PackedInts.bitsRequired(delta);
      for (int i = 0; i < pendingOff; ++i) {
        pending[i] -= minValue;
      }
      final PackedInts.Mutable mutable = PackedInts.getMutable(pendingOff, bitsRequired, PackedInts.COMPACT);
      for (int i = 0; i < pendingOff; ) {
        i += mutable.set(i, pending, i, pendingOff - i);
      }
      values[valuesOff] = mutable;
    }
    ++valuesOff;

    // reset pending buffer
    pendingOff = 0;
  }

  /** Get the number of values that have been added to the buffer. */
  public int size() {
    return valuesOff * MAX_PENDING_COUNT + pendingOff;
  }

  /** Return an iterator over the values of this buffer. */
  public Iterator iterator() {
    return new Iterator();
  }

  /** A long iterator. */
  public class Iterator {

    long[] currentValues;
    int vOff, pOff;

    private Iterator() {
      vOff = pOff = 0;
      if (valuesOff == 0) {
        currentValues = pending;
      } else {
        currentValues = new long[MAX_PENDING_COUNT];
        fillValues();
      }
    }

    private void fillValues() {
      if (vOff == valuesOff) {
        currentValues = pending;
      } else if (values[vOff] == null) {
        Arrays.fill(currentValues, minValues[vOff]);
      } else {
        for (int k = 0; k < MAX_PENDING_COUNT; ++k) {
          k += values[vOff].get(k, currentValues, k, MAX_PENDING_COUNT - k);
        }
        for (int k = 0; k < MAX_PENDING_COUNT; ++k) {
          currentValues[k] += minValues[vOff];
        }
      }
    }

    /** Whether or not there are remaining values. */
    public boolean hasNext() {
      return vOff < valuesOff || pOff < pendingOff;
    }

    /** Return the next long in the buffer. */
    public long next() {
      assert hasNext();
      long result = currentValues[pOff++];
      if (pOff == MAX_PENDING_COUNT) {
        vOff += 1;
        pOff = 0;
        if (vOff <= valuesOff) {
          fillValues();
        }
      }
      return result;
    }

  }

  /**
   * Return the number of bytes used by this instance.
   */
  public long ramBytesUsed() {
    long bytesUsed = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // the 3 arrays
        + 2 * RamUsageEstimator.NUM_BYTES_INT) // the 2 offsets
        + RamUsageEstimator.sizeOf(pending)
        + RamUsageEstimator.sizeOf(minValues)
        + RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * values.length); // values

    for (int i = 0; i < valuesOff; ++i) {
      if (values[i] != null) {
        bytesUsed += values[i].ramBytesUsed();
      }
    }
    return bytesUsed;
  }

}
