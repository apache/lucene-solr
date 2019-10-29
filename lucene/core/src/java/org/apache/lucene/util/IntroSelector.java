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

import java.util.Comparator;

/** Implementation of the quick select algorithm.
 *  <p>It uses the median of the first, middle and last values as a pivot and
 *  falls back to a median of medians when the number of recursion levels exceeds
 *  {@code 2 lg(n)}, as a consequence it runs in linear time on average.</p>
 *  @lucene.internal */
public abstract class IntroSelector extends Selector {

  @Override
  public final void select(int from, int to, int k) {
    checkArgs(from, to, k);
    final int maxDepth = 2 * MathUtil.log(to - from, 2);
    quickSelect(from, to, k, maxDepth);
  }

  int slowSelect(int from, int to, int k) {
    return medianOfMediansSelect(from, to-1, k);
  }

  int medianOfMediansSelect(int left, int right, int k) {
    do {
      // Defensive check, this is also checked in the calling
      // method. Including here so this method can be used
      // as a self contained quickSelect implementation.
      if (left == right) {
        return left;
      }
      int pivotIndex = pivot(left, right);
      pivotIndex = partition(left, right, k, pivotIndex);
      if (k == pivotIndex) {
        return k;
      } else if (k < pivotIndex) {
        right = pivotIndex-1;
      } else {
        left = pivotIndex+1;
      }
    } while (left != right);
    return left;
  }

  private int partition(int left, int right, int k, int pivotIndex) {
    setPivot(pivotIndex);
    swap(pivotIndex, right);
    int storeIndex = left;
    for (int i = left; i < right; i++) {
      if (comparePivot(i) > 0) {
        swap(storeIndex, i);
        storeIndex++;
      }
    }
    int storeIndexEq = storeIndex;
    for (int i = storeIndex; i < right; i++) {
      if (comparePivot(i) == 0) {
        swap(storeIndexEq, i);
        storeIndexEq++;
      }
    }
    swap(right, storeIndexEq);
    if (k < storeIndex) {
      return storeIndex;
    } else if (k <= storeIndexEq) {
      return k;
    }
    return storeIndexEq;
  }

  private int pivot(int left, int right) {
    if (right - left < 5) {
      int pivotIndex = partition5(left, right);
      return pivotIndex;
    }

    for (int i = left; i <= right; i=i+5) {
      int subRight = i + 4;
      if (subRight > right) {
        subRight = right;
      }
      int median5 = partition5(i, subRight);
      swap(median5, left + ((i-left)/5));
    }
    int mid = ((right - left) / 10) + left + 1;
    int to = left + ((right - left)/5);
    return medianOfMediansSelect(left, to, mid);
  }

  // selects the median of a group of at most five elements,
  // implemented using insertion sort. Efficient due to
  // bounded nature of data set.
  private int partition5(int left, int right) {
    int i = left + 1;
    while( i <= right) {
      int j = i;
      while (j > left && compare(j-1,j)>0) {
        swap(j-1, j);
        j--;
      }
      i++;
    }
    return (left + right) >>> 1;
  }

  private void quickSelect(int from, int to, int k, int maxDepth) {
    assert from <= k;
    assert k < to;
    if (to - from == 1) {
      return;
    }
    if (--maxDepth < 0) {
      slowSelect(from, to, k);
      return;
    }

    final int mid = (from + to) >>> 1;
    // heuristic: we use the median of the values at from, to-1 and mid as a pivot
    if (compare(from, to - 1) > 0) {
      swap(from, to - 1);
    }
    if (compare(to - 1, mid) > 0) {
      swap(to - 1, mid);
      if (compare(from, to - 1) > 0) {
        swap(from, to - 1);
      }
    }

    setPivot(to - 1);

    int left = from + 1;
    int right = to - 2;

    for (;;) {
      while (comparePivot(left) > 0) {
        ++left;
      }

      while (left < right && comparePivot(right) <= 0) {
        --right;
      }

      if (left < right) {
        swap(left, right);
        --right;
      } else {
        break;
      }
    }
    swap(left, to - 1);

    if (left == k) {
      return;
    } else if (left < k) {
      quickSelect(left + 1, to, k, maxDepth);
    } else {
      quickSelect(from, left, k, maxDepth);
    }
  }

  /** Compare entries found in slots <code>i</code> and <code>j</code>.
   *  The contract for the returned value is the same as
   *  {@link Comparator#compare(Object, Object)}. */
  protected int compare(int i, int j) {
    setPivot(i);
    return comparePivot(j);
  }

  /** Save the value at slot <code>i</code> so that it can later be used as a
   * pivot, see {@link #comparePivot(int)}. */
  protected abstract void setPivot(int i);

  /** Compare the pivot with the slot at <code>j</code>, similarly to
   *  {@link #compare(int, int) compare(i, j)}. */
  protected abstract int comparePivot(int j);
}
