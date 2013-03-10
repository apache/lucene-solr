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

/** Common functionality shared by {@link AppendingLongBuffer} and {@link MonotonicAppendingLongBuffer}. */
abstract class AbstractAppendingLongBuffer {

  static final int BLOCK_BITS = 10;
  static final int MAX_PENDING_COUNT = 1 << BLOCK_BITS;
  static final int BLOCK_MASK = MAX_PENDING_COUNT - 1;

  long[] minValues;
  PackedInts.Reader[] deltas;
  private long deltasBytes;
  int valuesOff;
  long[] pending;
  int pendingOff;

  AbstractAppendingLongBuffer(int initialBlockCount) {
    minValues = new long[16];
    deltas = new PackedInts.Reader[16];
    pending = new long[MAX_PENDING_COUNT];
    valuesOff = 0;
    pendingOff = 0;
  }

  /** Get the number of values that have been added to the buffer. */
  public final long size() {
    return valuesOff * (long) MAX_PENDING_COUNT + pendingOff;
  }

  /** Append a value to this buffer. */
  public final void add(long l) {
    if (pendingOff == MAX_PENDING_COUNT) {
      // check size
      if (deltas.length == valuesOff) {
        final int newLength = ArrayUtil.oversize(valuesOff + 1, 8);
        grow(newLength);
      }
      packPendingValues();
      if (deltas[valuesOff] != null) {
        deltasBytes += deltas[valuesOff].ramBytesUsed();
      }
      ++valuesOff;
      // reset pending buffer
      pendingOff = 0;
    }
    pending[pendingOff++] = l;
  }

  void grow(int newBlockCount) {
    minValues = Arrays.copyOf(minValues, newBlockCount);
    deltas = Arrays.copyOf(deltas, newBlockCount);
  }

  abstract void packPendingValues();

  /** Get a value from this buffer. */
  public final long get(long index) {
    if (index < 0 || index >= size()) {
      throw new IndexOutOfBoundsException("" + index);
    }
    int block = (int) (index >> BLOCK_BITS);
    int element = (int) (index & BLOCK_MASK);
    return get(block, element);
  }

  abstract long get(int block, int element);

  abstract Iterator iterator();

  abstract class Iterator {

    long[] currentValues;
    int vOff, pOff;

    Iterator() {
      vOff = pOff = 0;
      if (valuesOff == 0) {
        currentValues = pending;
      } else {
        currentValues = new long[MAX_PENDING_COUNT];
        fillValues();
      }
    }

    abstract void fillValues();

    /** Whether or not there are remaining values. */
    public final boolean hasNext() {
      return vOff < valuesOff || (vOff == valuesOff && pOff < pendingOff);
    }

    /** Return the next long in the buffer. */
    public final long next() {
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

  long baseRamBytesUsed() {
    return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // the 3 arrays
        + 2 * RamUsageEstimator.NUM_BYTES_INT; // the 2 offsets
  }

  /**
   * Return the number of bytes used by this instance.
   */
  public long ramBytesUsed() {
    // TODO: this is called per-doc-per-norms/dv-field, can we optimize this?
    long bytesUsed = RamUsageEstimator.alignObjectSize(baseRamBytesUsed())
        + RamUsageEstimator.NUM_BYTES_LONG // valuesBytes
        + RamUsageEstimator.sizeOf(pending)
        + RamUsageEstimator.sizeOf(minValues)
        + RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * deltas.length); // values

    return bytesUsed + deltasBytes;
  }

}
