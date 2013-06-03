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
import static org.apache.lucene.util.packed.PackedInts.numBlocks;

/**
 * A {@link PagedGrowableWriter}. This class slices data into fixed-size blocks
 * which have independent numbers of bits per value and grow on-demand.
 * <p>You should use this class instead of {@link AppendingLongBuffer} only when
 * you need random write-access. Otherwise this class will likely be slower and
 * less memory-efficient.
 * @lucene.internal
 */
public final class PagedGrowableWriter {

  static final int MIN_BLOCK_SIZE = 1 << 6;
  static final int MAX_BLOCK_SIZE = 1 << 30;

  final long size;
  final int pageShift;
  final int pageMask;
  final GrowableWriter[] subWriters;
  final int startBitsPerValue;
  final float acceptableOverheadRatio;

  /**
   * Create a new {@link PagedGrowableWriter} instance.
   *
   * @param size the number of values to store.
   * @param pageSize the number of values per page
   * @param startBitsPerValue the initial number of bits per value
   * @param acceptableOverheadRatio an acceptable overhead ratio
   */
  public PagedGrowableWriter(long size, int pageSize,
      int startBitsPerValue, float acceptableOverheadRatio) {
    this(size, pageSize, startBitsPerValue, acceptableOverheadRatio, true);
  }

  PagedGrowableWriter(long size, int pageSize,int startBitsPerValue, float acceptableOverheadRatio, boolean fillPages) {
    this.size = size;
    this.startBitsPerValue = startBitsPerValue;
    this.acceptableOverheadRatio = acceptableOverheadRatio;
    pageShift = checkBlockSize(pageSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
    pageMask = pageSize - 1;
    final int numPages = numBlocks(size, pageSize);
    subWriters = new GrowableWriter[numPages];
    if (fillPages) {
      for (int i = 0; i < numPages; ++i) {
        // do not allocate for more entries than necessary on the last page
        final int valueCount = i == numPages - 1 ? lastPageSize(size) : pageSize;
        subWriters[i] = new GrowableWriter(startBitsPerValue, valueCount, acceptableOverheadRatio);
      }
    }
  }

  private int lastPageSize(long size) {
    final int sz = indexInPage(size);
    return sz == 0 ? pageSize() : sz;
  }

  private int pageSize() {
    return pageMask + 1;
  }

  /** The number of values. */
  public long size() {
    return size;
  }

  int pageIndex(long index) {
    return (int) (index >>> pageShift);
  }

  int indexInPage(long index) {
    return (int) index & pageMask;
  }

  /** Get value at <code>index</code>. */
  public long get(long index) {
    assert index >= 0 && index < size;
    final int pageIndex = pageIndex(index);
    final int indexInPage = indexInPage(index);
    return subWriters[pageIndex].get(indexInPage);
  }

  /** Set value at <code>index</code>. */
  public void set(long index, long value) {
    assert index >= 0 && index < size;
    final int pageIndex = pageIndex(index);
    final int indexInPage = indexInPage(index);
    subWriters[pageIndex].set(indexInPage, value);
  }

  /** Create a new {@link PagedGrowableWriter} of size <code>newSize</code>
   *  based on the content of this buffer. This method is much more efficient
   *  than creating a new {@link PagedGrowableWriter} and copying values one by
   *  one. */
  public PagedGrowableWriter resize(long newSize) {
    final PagedGrowableWriter newWriter = new PagedGrowableWriter(newSize, pageSize(), startBitsPerValue, acceptableOverheadRatio, false);
    final int numCommonPages = Math.min(newWriter.subWriters.length, subWriters.length);
    final long[] copyBuffer = new long[1024];
    for (int i = 0; i < newWriter.subWriters.length; ++i) {
      final int valueCount = i == newWriter.subWriters.length - 1 ? lastPageSize(newSize) : pageSize();
      final int bpv = i < numCommonPages ? subWriters[i].getBitsPerValue() : startBitsPerValue;
      newWriter.subWriters[i] = new GrowableWriter(bpv, valueCount, acceptableOverheadRatio);
      if (i < numCommonPages) {
        final int copyLength = Math.min(valueCount, subWriters[i].size());
        PackedInts.copy(subWriters[i], 0, newWriter.subWriters[i].getMutable(), 0, copyLength, copyBuffer);
      }
    }
    return newWriter;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(size=" + size() + ",pageSize=" + pageSize() + ")";
  }

}
