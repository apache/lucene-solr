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
package org.apache.lucene.util;

import java.util.Arrays;
import java.util.Comparator;

/**
 * A simple append only random-access {@link BytesRef} array that stores full
 * copies of the appended bytes in a {@link ByteBlockPool}.
 * 
 * 
 * <b>Note: This class is not Thread-Safe!</b>
 * 
 * @lucene.internal
 * @lucene.experimental
 */
public final class BytesRefArray implements SortableBytesRefArray {
  private final ByteBlockPool pool;
  private int[] offsets = new int[1];
  private int lastElement = 0;
  private int currentOffset = 0;
  private final Counter bytesUsed;
  
  /**
   * Creates a new {@link BytesRefArray} with a counter to track allocated bytes
   */
  public BytesRefArray(Counter bytesUsed) {
    this.pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(
        bytesUsed));
    pool.nextBuffer();
    bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER * Integer.BYTES);
    this.bytesUsed = bytesUsed;
  }
 
  /**
   * Clears this {@link BytesRefArray}
   */
  @Override
  public void clear() {
    lastElement = 0;
    currentOffset = 0;
    // TODO: it's trappy that this does not return storage held by int[] offsets array!
    Arrays.fill(offsets, 0);
    pool.reset(false, true); // no need to 0 fill the buffers we control the allocator
  }
  
  /**
   * Appends a copy of the given {@link BytesRef} to this {@link BytesRefArray}.
   * @param bytes the bytes to append
   * @return the index of the appended bytes
   */
  @Override
  public int append(BytesRef bytes) {
    if (lastElement >= offsets.length) {
      int oldLen = offsets.length;
      offsets = ArrayUtil.grow(offsets, offsets.length + 1);
      bytesUsed.addAndGet((offsets.length - oldLen) * Integer.BYTES);
    }
    pool.append(bytes);
    offsets[lastElement++] = currentOffset;
    currentOffset += bytes.length;
    return lastElement-1;
  }
  
  /**
   * Returns the current size of this {@link BytesRefArray}
   * @return the current size of this {@link BytesRefArray}
   */
  @Override
  public int size() {
    return lastElement;
  }
  
  /**
   * Returns the <i>n'th</i> element of this {@link BytesRefArray}
   * @param spare a spare {@link BytesRef} instance
   * @param index the elements index to retrieve 
   * @return the <i>n'th</i> element of this {@link BytesRefArray}
   */
  public BytesRef get(BytesRefBuilder spare, int index) {
    if (lastElement > index) {
      int offset = offsets[index];
      int length = index == lastElement - 1 ? currentOffset - offset
          : offsets[index + 1] - offset;
      spare.grow(length);
      spare.setLength(length);
      pool.readBytes(offset, spare.bytes(), 0, spare.length());
      return spare.get();
    }
    throw new IndexOutOfBoundsException("index " + index
        + " must be less than the size: " + lastElement);
  }

  /** Used only by sort below, to set a {@link BytesRef} with the specified slice, avoiding copying bytes in the common case when the slice
   *  is contained in a single block in the byte block pool. */
  private void setBytesRef(BytesRefBuilder spare, BytesRef result, int index) {
    if (index < lastElement) {
      int offset = offsets[index];
      int length;
      if (index == lastElement - 1) {
        length = currentOffset - offset;
      } else {
        length = offsets[index + 1] - offset;
      }
      pool.setBytesRef(spare, result, offset, length);
    } else {
      throw new IndexOutOfBoundsException("index " + index + " must be less than the size: " + lastElement);
    }
  }
  
  private int[] sort(final Comparator<BytesRef> comp) {
    final int[] orderedEntries = new int[size()];
    for (int i = 0; i < orderedEntries.length; i++) {
      orderedEntries[i] = i;
    }
    new IntroSorter() {
      @Override
      protected void swap(int i, int j) {
        final int o = orderedEntries[i];
        orderedEntries[i] = orderedEntries[j];
        orderedEntries[j] = o;
      }
      
      @Override
      protected int compare(int i, int j) {
        final int idx1 = orderedEntries[i], idx2 = orderedEntries[j];
        setBytesRef(scratch1, scratchBytes1, idx1);
        setBytesRef(scratch2, scratchBytes2, idx2);
        return comp.compare(scratchBytes1, scratchBytes2);
      }
      
      @Override
      protected void setPivot(int i) {
        final int index = orderedEntries[i];
        setBytesRef(pivotBuilder, pivot, index);
      }
      
      @Override
      protected int comparePivot(int j) {
        final int index = orderedEntries[j];
        setBytesRef(scratch2, scratchBytes2, index);
        return comp.compare(pivot, scratchBytes2);
      }

      private final BytesRef pivot = new BytesRef();
      private final BytesRef scratchBytes1 = new BytesRef();
      private final BytesRef scratchBytes2 = new BytesRef();
      private final BytesRefBuilder pivotBuilder = new BytesRefBuilder();
      private final BytesRefBuilder scratch1 = new BytesRefBuilder();
      private final BytesRefBuilder scratch2 = new BytesRefBuilder();
    }.sort(0, size());
    return orderedEntries;
  }
  
  /**
   * sugar for {@link #iterator(Comparator)} with a <code>null</code> comparator
   */
  public BytesRefIterator iterator() {
    return iterator(null);
  }
  
  /**
   * <p>
   * Returns a {@link BytesRefIterator} with point in time semantics. The
   * iterator provides access to all so far appended {@link BytesRef} instances.
   * </p>
   * <p>
   * If a non <code>null</code> {@link Comparator} is provided the iterator will
   * iterate the byte values in the order specified by the comparator. Otherwise
   * the order is the same as the values were appended.
   * </p>
   * <p>
   * This is a non-destructive operation.
   * </p>
   */
  @Override
  public BytesRefIterator iterator(final Comparator<BytesRef> comp) {
    final BytesRefBuilder spare = new BytesRefBuilder();
    final BytesRef result = new BytesRef();
    final int size = size();
    final int[] indices = comp == null ? null : sort(comp);
    return new BytesRefIterator() {
      int pos = 0;
      
      @Override
      public BytesRef next() {
        if (pos < size) {
          setBytesRef(spare, result, indices == null ? pos++ : indices[pos++]);
          return result;
        }
        return null;
      }
    };
  }
}
