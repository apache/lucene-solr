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

import static org.apache.lucene.util.packed.PackedInts.checkBlockSize;

import java.util.Arrays;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/** Common functionality shared by {@link AppendingLongBuffer} and {@link MonotonicAppendingLongBuffer}. */
abstract class AbstractAppendingLongBuffer {

  static final int MIN_PAGE_SIZE = 64;
  // More than 1M doesn't really makes sense with these appending buffers
  // since their goal is to try to have small numbers of bits per value
  static final int MAX_PAGE_SIZE = 1 << 20;

  final int pageShift, pageMask;
  long[] minValues;
  PackedInts.Reader[] deltas;
  private long deltasBytes;
  int valuesOff;
  long[] pending;
  int pendingOff;

  AbstractAppendingLongBuffer(int initialBlockCount, int pageSize) {
    minValues = new long[initialBlockCount];
    deltas = new PackedInts.Reader[initialBlockCount];
    pending = new long[pageSize];
    pageShift = checkBlockSize(pageSize, MIN_PAGE_SIZE, MAX_PAGE_SIZE);
    pageMask = pageSize - 1;
    valuesOff = 0;
    pendingOff = 0;
  }

  final int pageSize() {
    return pageMask + 1;
  }

  /** Get the number of values that have been added to the buffer. */
  public final long size() {
    long size = pendingOff;
    if (valuesOff > 0) {
      size += deltas[valuesOff - 1].size();
    }
    if (valuesOff > 1) {
      size += (long) (valuesOff - 1) * pageSize();
    }
    return size;
  }

  /** Append a value to this buffer. */
  public final void add(long l) {
    if (pending == null) {
      throw new IllegalStateException("This buffer is frozen");
    }
    if (pendingOff == pending.length) {
      // check size
      if (deltas.length == valuesOff) {
        final int newLength = ArrayUtil.oversize(valuesOff + 1, 8);
        grow(newLength);
      }
      packPendingValues();
      deltasBytes += deltas[valuesOff].ramBytesUsed();
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
    final int block = (int) (index >> pageShift);
    final int element = (int) (index & pageMask);
    return get(block, element);
  }

  abstract long get(int block, int element);

  abstract Iterator iterator();

  abstract class Iterator {

    long[] currentValues;
    int vOff, pOff;
    int currentCount; // number of entries of the current page

    Iterator() {
      vOff = pOff = 0;
      if (valuesOff == 0) {
        currentValues = pending;
        currentCount = pendingOff;
      } else {
        currentValues = new long[deltas[0].size()];
        fillValues();
      }
    }

    abstract void fillValues();

    /** Whether or not there are remaining values. */
    public final boolean hasNext() {
      return pOff < currentCount;
    }

    /** Return the next long in the buffer. */
    public final long next() {
      assert hasNext();
      long result = currentValues[pOff++];
      if (pOff == currentCount) {
        vOff += 1;
        pOff = 0;
        if (vOff <= valuesOff) {
          fillValues();
        } else {
          currentCount = 0;
        }
      }
      return result;
    }

  }

  long baseRamBytesUsed() {
    return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // the 3 arrays
        + 2 * RamUsageEstimator.NUM_BYTES_INT // the 2 offsets
        + 2 * RamUsageEstimator.NUM_BYTES_INT // pageShift, pageMask
        + RamUsageEstimator.NUM_BYTES_LONG; // deltasBytes
  }

  /**
   * Return the number of bytes used by this instance.
   */
  public long ramBytesUsed() {
    // TODO: this is called per-doc-per-norms/dv-field, can we optimize this?
    long bytesUsed = RamUsageEstimator.alignObjectSize(baseRamBytesUsed())
        + (pending != null ? RamUsageEstimator.sizeOf(pending) : 0L)
        + RamUsageEstimator.sizeOf(minValues)
        + RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * deltas.length); // values

    return bytesUsed + deltasBytes;
  }

  /** Pack all pending values in this buffer. Subsequent calls to {@link #add(long)} will fail. */
  public void freeze() {
    if (pendingOff > 0) {
      if (deltas.length == valuesOff) {
        grow(valuesOff + 1); // don't oversize!
      }
      packPendingValues();
      deltasBytes += deltas[valuesOff].ramBytesUsed();
      ++valuesOff;
      pendingOff = 0;
    }
    pending = null;
  }

}
