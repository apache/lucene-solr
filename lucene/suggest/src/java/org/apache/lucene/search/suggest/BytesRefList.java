package org.apache.lucene.search.suggest;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.SorterTemplate;

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
public final class BytesRefList {
  // TODO rename to BytesRefArray
  private final ByteBlockPool pool;
  private int[] offsets = new int[1];
  private int lastElement = 0;
  private int currentOffset = 0;
  private final Counter bytesUsed = Counter.newCounter(false);
  
  /**
   * Creates a new {@link BytesRefList}
   */
  public BytesRefList() {
    this.pool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(
        bytesUsed));
    pool.nextBuffer();
    bytesUsed.addAndGet(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
        + RamUsageEstimator.NUM_BYTES_INT);
  }
 
  /**
   * Clears this {@link BytesRefList}
   */
  public void clear() {
    lastElement = 0;
    currentOffset = 0;
    Arrays.fill(offsets, 0);
    pool.reset();
  }
  
  /**
   * Appends a copy of the given {@link BytesRef} to this {@link BytesRefList}.
   * @param bytes the bytes to append
   * @return the ordinal of the appended bytes
   */
  public int append(BytesRef bytes) {
    if (lastElement >= offsets.length) {
      int oldLen = offsets.length;
      offsets = ArrayUtil.grow(offsets, offsets.length + 1);
      bytesUsed.addAndGet((offsets.length - oldLen)
          * RamUsageEstimator.NUM_BYTES_INT);
    }
    pool.copy(bytes);
    offsets[lastElement++] = currentOffset;
    currentOffset += bytes.length;
    return lastElement;
  }
  
  /**
   * Returns the current size of this {@link BytesRefList}
   * @return the current size of this {@link BytesRefList}
   */
  public int size() {
    return lastElement;
  }
  
  /**
   * Returns the <i>n'th</i> element of this {@link BytesRefList}
   * @param spare a spare {@link BytesRef} instance
   * @param ord the elements ordinal to retrieve 
   * @return the <i>n'th</i> element of this {@link BytesRefList}
   */
  public BytesRef get(BytesRef spare, int ord) {
    if (lastElement > ord) {
      spare.offset = offsets[ord];
      spare.length = ord == lastElement - 1 ? currentOffset - spare.offset
          : offsets[ord + 1] - spare.offset;
      pool.copyFrom(spare);
      return spare;
    }
    throw new IndexOutOfBoundsException("index " + ord
        + " must be less than the size: " + lastElement);
    
  }
  
  /**
   * Returns the number internally used bytes to hold the appended bytes in
   * memory
   * 
   * @return the number internally used bytes to hold the appended bytes in
   *         memory
   */
  public long bytesUsed() {
    return bytesUsed.get();
  }
  
  private int[] sort(final Comparator<BytesRef> comp) {
    final int[] orderedEntries = new int[size()];
    for (int i = 0; i < orderedEntries.length; i++) {
      orderedEntries[i] = i;
    }
    new SorterTemplate() {
      @Override
      protected void swap(int i, int j) {
        final int o = orderedEntries[i];
        orderedEntries[i] = orderedEntries[j];
        orderedEntries[j] = o;
      }
      
      @Override
      protected int compare(int i, int j) {
        final int ord1 = orderedEntries[i], ord2 = orderedEntries[j];
        return comp.compare(get(scratch1, ord1), get(scratch2, ord2));
      }
      
      @Override
      protected void setPivot(int i) {
        final int ord = orderedEntries[i];
        get(pivot, ord);
      }
      
      @Override
      protected int comparePivot(int j) {
        final int ord = orderedEntries[j];
        return comp.compare(pivot, get(scratch2, ord));
      }
      
      private final BytesRef pivot = new BytesRef(), scratch1 = new BytesRef(),
          scratch2 = new BytesRef();
    }.quickSort(0, size() - 1);
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
  public BytesRefIterator iterator(final Comparator<BytesRef> comp) {
    final BytesRef spare = new BytesRef();
    final int size = size();
    final int[] ords = comp == null ? null : sort(comp);
    return new BytesRefIterator() {
      int pos = 0;
      
      @Override
      public BytesRef next() throws IOException {
        if (pos < size) {
          return get(spare, ords == null ? pos++ : ords[pos++]);
        }
        return null;
      }
      
      @Override
      public Comparator<BytesRef> getComparator() {
        return comp;
      }
    };
  }
}
