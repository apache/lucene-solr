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

/** Radix sorter for variable-length strings. This class sorts based on the most
 *  significant byte first and falls back to {@link IntroSorter} when the size
 *  of the buckets to sort becomes small. It is <b>NOT</b> stable.
 *  Worst-case memory usage is about {@code 2.3 KB}.
 *  @lucene.internal */
public abstract class MSBRadixSorter extends Sorter {

  // after that many levels of recursion we fall back to introsort anyway
  // this is used as a protection against the fact that radix sort performs
  // worse when there are long common prefixes (probably because of cache
  // locality)
  private static final int LEVEL_THRESHOLD = 8;
  // size of histograms: 256 + 1 to indicate that the string is finished
  private static final int HISTOGRAM_SIZE = 257;
  // buckets below this size will be sorted with introsort
  private static final int LENGTH_THRESHOLD = 100;

  // we store one histogram per recursion level
  private final int[][] histograms = new int[LEVEL_THRESHOLD][];
  private final int[] endOffsets = new int[HISTOGRAM_SIZE];

  private final int maxLength;

  /**
   * Sole constructor.
   * @param maxLength the maximum length of keys, pass {@link Integer#MAX_VALUE} if unknown.
   */
  protected MSBRadixSorter(int maxLength) {
    this.maxLength = maxLength;
  }

  /** Return the k-th byte of the entry at index {@code i}, or {@code -1} if
   * its length is less than or equal to {@code k}. This may only be called
   * with a value of {@code i} between {@code 0} included and
   * {@code maxLength} excluded. */
  protected abstract int byteAt(int i, int k);

  /** Get a fall-back sorter which may assume that the first k bytes of all compared strings are equal. */
  protected Sorter getFallbackSorter(int k) {
    return new IntroSorter() {
      @Override
      protected void swap(int i, int j) {
        MSBRadixSorter.this.swap(i, j);
      }

      @Override
      protected int compare(int i, int j) {
        for (int o = k; o < maxLength; ++o) {
          final int b1 = byteAt(i, o);
          final int b2 = byteAt(j, o);
          if (b1 != b2) {
            return b1 - b2;
          } else if (b1 == -1) {
            break;
          }
        }
        return 0;
      }

      @Override
      protected void setPivot(int i) {
        pivot.setLength(0);
        for (int o = k; o < maxLength; ++o) {
          final int b = byteAt(i, o);
          if (b == -1) {
            break;
          }
          pivot.append((byte) b);
        }
      }

      @Override
      protected int comparePivot(int j) {
        for (int o = 0; o < pivot.length(); ++o) {
          final int b1 = pivot.byteAt(o) & 0xff;
          final int b2 = byteAt(j, k + o);
          if (b1 != b2) {
            return b1 - b2;
          }
        }
        if (k + pivot.length() == maxLength) {
          return 0;
        }
        return -1 - byteAt(j, k + pivot.length());
      }

      private final BytesRefBuilder pivot = new BytesRefBuilder();
    };
  }

  @Override
  protected final int compare(int i, int j) {
    throw new UnsupportedOperationException("unused: not a comparison-based sort");
  }

  @Override
  public void sort(int from, int to) {
    checkRange(from, to);
    sort(from, to, 0);
  }

  private void sort(int from, int to, int k) {
    if (to - from <= LENGTH_THRESHOLD || k >= LEVEL_THRESHOLD) {
      introSort(from, to, k);
    } else {
      radixSort(from, to, k);
    }
  }

  private void introSort(int from, int to, int k) {
    getFallbackSorter(k).sort(from, to);
  }

  private void radixSort(int from, int to, int k) {
    int[] histogram = histograms[k];
    if (histogram == null) {
      histogram = histograms[k] = new int[HISTOGRAM_SIZE];
    } else {
      Arrays.fill(histogram, 0);
    }

    buildHistogram(from, to, k, histogram);

    // short-circuit: if all keys have the same byte at offset k, then recurse directly
    for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
      if (histogram[i] == to - from) {
        // everything is in the same bucket, recurse
        if (i > 0) {
          sort(from, to, k + 1);
        }
        return;
      } else if (histogram[i] != 0) {
        break;
      }
    }

    int[] startOffsets = histogram;
    int[] endOffsets = this.endOffsets;
    sumHistogram(histogram, endOffsets);
    reorder(from, to, startOffsets, endOffsets, k);
    endOffsets = startOffsets;

    if (k + 1 < maxLength) {
      // recurse on all but the first bucket since all keys are equals in this
      // bucket (we already compared all bytes)
      for (int prev = endOffsets[0], i = 1; i < HISTOGRAM_SIZE; ++i) {
        int h = endOffsets[i];
        final int bucketLen = h - prev;
        if (bucketLen > 1) {
          sort(from + prev, from + h, k + 1);
        }
        prev = h;
      }
    }
  }

  /** Return a number for the k-th character between 0 and {@link #HISTOGRAM_SIZE}. */
  private int getBucket(int i, int k) {
    return byteAt(i, k) + 1;
  }

  /** Build a histogram of the number of values per {@link #getBucket(int, int) bucket}. */
  private int[] buildHistogram(int from, int to, int k, int[] histogram) {
    for (int i = from; i < to; ++i) {
      histogram[getBucket(i, k)]++;
    }
    return histogram;
  }

  /** Accumulate values of the histogram so that it does not store counts but
   *  start offsets. {@code endOffsets} will store the end offsets. */
  private static void sumHistogram(int[] histogram, int[] endOffsets) {
    int accum = 0;
    for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
      final int count = histogram[i];
      histogram[i] = accum;
      accum += count;
      endOffsets[i] = accum;
    }
  }

  /**
   * Reorder based on start/end offsets for each bucket. When this method
   * returns, startOffsets and endOffsets are equal.
   * @param startOffsets start offsets per bucket
   * @param endOffsets end offsets per bucket
   */
  private void reorder(int from, int to, int[] startOffsets, int[] endOffsets, int k) {
    // reorder in place, like the dutch flag problem
    for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
      final int limit = endOffsets[i];
      for (int h1 = startOffsets[i]; h1 < limit; h1 = startOffsets[i]) {
        final int b = getBucket(from + h1, k);
        final int h2 = startOffsets[b]++;
        swap(from + h1, from + h2);
      }
    }
  }
}
