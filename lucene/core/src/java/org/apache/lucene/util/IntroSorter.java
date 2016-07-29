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
 * {@link Sorter} implementation based on a variant of the quicksort algorithm
 * called <a href="http://en.wikipedia.org/wiki/Introsort">introsort</a>: when
 * the recursion level exceeds the log of the length of the array to sort, it
 * falls back to heapsort. This prevents quicksort from running into its
 * worst-case quadratic runtime. Small arrays are sorted with
 * insertion sort.
 * @lucene.internal
 */
public abstract class IntroSorter extends Sorter {

  /** Create a new {@link IntroSorter}. */
  public IntroSorter() {}

  @Override
  public final void sort(int from, int to) {
    checkRange(from, to);
    quicksort(from, to, 2 * MathUtil.log(to - from, 2));
  }

  void quicksort(int from, int to, int maxDepth) {
    if (to - from < BINARY_SORT_THRESHOLD) {
      binarySort(from, to);
      return;
    } else if (--maxDepth < 0) {
      heapSort(from, to);
      return;
    }

    final int mid = (from + to) >>> 1;

    if (compare(from, mid) > 0) {
      swap(from, mid);
    }

    if (compare(mid, to - 1) > 0) {
      swap(mid, to - 1);
      if (compare(from, mid) > 0) {
        swap(from, mid);
      }
    }

    int left = from + 1;
    int right = to - 2;

    setPivot(mid);
    for (;;) {
      while (comparePivot(right) < 0) {
        --right;
      }

      while (left < right && comparePivot(left) >= 0) {
        ++left;
      }

      if (left < right) {
        swap(left, right);
        --right;
      } else {
        break;
      }
    }

    quicksort(from, left + 1, maxDepth);
    quicksort(left + 1, to, maxDepth);
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
