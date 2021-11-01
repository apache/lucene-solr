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

/**
 * {@link Sorter} implementation based on a variant of the quicksort algorithm called <a
 * href="http://en.wikipedia.org/wiki/Introsort">introsort</a>: when the recursion level exceeds the
 * log of the length of the array to sort, it falls back to heapsort. This prevents quicksort from
 * running into its worst-case quadratic runtime. Small ranges are sorted with insertion sort.
 *
 * <p>This sort algorithm is fast on most data shapes, especially with low cardinality. If the data
 * to sort is known to be strictly ascending or descending, prefer {@link TimSorter}.
 *
 * @lucene.internal
 */
public abstract class IntroSorter extends Sorter {

  /** Below this size threshold, the partition selection is simplified to a single median. */
  private static final int SINGLE_MEDIAN_THRESHOLD = 40;

  /** Create a new {@link IntroSorter}. */
  public IntroSorter() {}

  @Override
  public final void sort(int from, int to) {
    checkRange(from, to);
    sort(from, to, 2 * MathUtil.log(to - from, 2));
  }

  /**
   * Sorts between from (inclusive) and to (exclusive) with intro sort.
   *
   * <p>Sorts small ranges with insertion sort. Fallbacks to heap sort to avoid quadratic worst
   * case. Selects the pivot with medians and partitions with the Bentley-McIlroy fast 3-ways
   * algorithm (Engineering a Sort Function, Bentley-McIlroy).
   */
  void sort(int from, int to, int maxDepth) {
    int size;

    // Sort small ranges with insertion sort.
    while ((size = to - from) > INSERTION_SORT_THRESHOLD) {

      if (--maxDepth < 0) {
        // Max recursion depth reached: fallback to heap sort.
        heapSort(from, to);
        return;
      }

      // Pivot selection based on medians.
      int last = to - 1;
      int mid = (from + last) >>> 1;
      int pivot;
      if (size <= SINGLE_MEDIAN_THRESHOLD) {
        // Select the pivot with a single median around the middle element.
        // Do not take the median between [from, mid, last] because it hurts performance
        // if the order is descending.
        int range = size >> 2;
        pivot = median(mid - range, mid, mid + range);
      } else {
        // Select the pivot with the median of medians.
        int range = size >> 3;
        int doubleRange = range << 1;
        int medianFirst = median(from, from + range, from + doubleRange);
        int medianMiddle = median(mid - range, mid, mid + range);
        int medianLast = median(last - doubleRange, last - range, last);
        pivot = median(medianFirst, medianMiddle, medianLast);
      }

      // Bentley-McIlroy 3-way partitioning.
      setPivot(pivot);
      swap(from, pivot);
      int i = from;
      int j = to;
      int p = from + 1;
      int q = last;
      while (true) {
        int leftCmp, rightCmp;
        while ((leftCmp = comparePivot(++i)) > 0) {}
        while ((rightCmp = comparePivot(--j)) < 0) {}
        if (i >= j) {
          if (i == j && rightCmp == 0) {
            swap(i, p);
          }
          break;
        }
        swap(i, j);
        if (rightCmp == 0) {
          swap(i, p++);
        }
        if (leftCmp == 0) {
          swap(j, q--);
        }
      }
      i = j + 1;
      for (int k = from; k < p; ) {
        swap(k++, j--);
      }
      for (int k = last; k > q; ) {
        swap(k--, i++);
      }

      // Recursion on the smallest partition. Replace the tail recursion by a loop.
      if (j - from < last - i) {
        sort(from, j + 1, maxDepth);
        from = i;
      } else {
        sort(i, to, maxDepth);
        to = j + 1;
      }
    }

    insertionSort(from, to);
  }

  /** Returns the index of the median element among three elements at provided indices. */
  private int median(int i, int j, int k) {
    if (compare(i, j) < 0) {
      if (compare(j, k) <= 0) {
        return j;
      }
      return compare(i, k) < 0 ? k : i;
    }
    if (compare(j, k) >= 0) {
      return j;
    }
    return compare(i, k) < 0 ? i : k;
  }

  // Don't rely on the slow default impl of setPivot/comparePivot since
  // quicksort relies on these methods to be fast for good performance

  @Override
  protected abstract void setPivot(int i);

  @Override
  protected abstract int comparePivot(int j);

  @Override
  protected int compare(int i, int j) {
    setPivot(i);
    return comparePivot(j);
  }
}
