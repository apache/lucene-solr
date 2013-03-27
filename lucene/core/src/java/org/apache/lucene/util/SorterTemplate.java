package org.apache.lucene.util;


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

/**
 * This class was inspired by CGLIB, but provides a better
 * QuickSort algorithm without additional InsertionSort
 * at the end.
 * To use, subclass and override the four abstract methods
 * which compare and modify your data.
 * Allows custom swap so that two arrays can be sorted
 * at the same time.
 * @lucene.internal
 */
public abstract class SorterTemplate {

  private static final int TIMSORT_MINRUN = 32;
  private static final int TIMSORT_THRESHOLD = 64;
  private static final int TIMSORT_STACKSIZE = 40; // change if you change TIMSORT_MINRUN
  private static final int MERGESORT_THRESHOLD = 12;
  private static final int QUICKSORT_THRESHOLD = 7;

  static {
    // check whether TIMSORT_STACKSIZE is large enough
    // for a run length of TIMSORT_MINRUN and an array
    // of 2B values when TimSort invariants are verified
    final long[] lengths = new long[TIMSORT_STACKSIZE];
    lengths[0] = TIMSORT_MINRUN;
    lengths[1] = lengths[0] + 1;
    for (int i = 2; i < TIMSORT_STACKSIZE; ++i) {
      lengths[i] = lengths[i-2] + lengths[i-1] + 1;
    }
    if (lengths[TIMSORT_STACKSIZE - 1] < Integer.MAX_VALUE) {
      throw new Error("TIMSORT_STACKSIZE is too small");
    }
  }

  /** Implement this method, that swaps slots {@code i} and {@code j} in your data */
  protected abstract void swap(int i, int j);
  
  /** Compares slots {@code i} and {@code j} of you data.
   * Should be implemented like <code><em>valueOf(i)</em>.compareTo(<em>valueOf(j)</em>)</code> */
  protected abstract int compare(int i, int j);

  /** Implement this method, that stores the value of slot {@code i} as pivot value */
  protected abstract void setPivot(int i);
  
  /** Implements the compare function for the previously stored pivot value.
   * Should be implemented like <code>pivot.compareTo(<em>valueOf(j)</em>)</code> */
  protected abstract int comparePivot(int j);
  
  /** Sorts via stable in-place InsertionSort algorithm (O(n<sup>2</sup>))
   *(ideal for small collections which are mostly presorted). */
  public final void insertionSort(int lo, int hi) {
    for (int i = lo + 1 ; i <= hi; i++) {
      for (int j = i; j > lo; j--) {
        if (compare(j - 1, j) > 0) {
          swap(j - 1, j);
        } else {
          break;
        }
      }
    }
  }

  /** Sorts via stable in-place BinarySort algorithm (O(n<sup>2</sup>))
   * (ideal for small collections which are in random order). */
  public final void binarySort(int lo, int hi) {
    for (int i = lo + 1; i <= hi; ++i) {
      int l = lo;
      int h = i - 1;
      setPivot(i);
      while (l <= h) {
        final int mid = (l + h) >>> 1;
        final int cmp = comparePivot(mid);
        if (cmp < 0) {
          h = mid - 1;
        } else {
          l = mid + 1;
        }
      }
      for (int j = i; j > l; --j) {
        swap(j - 1, j);
      }
    }
  }

  /** Sorts via in-place, but unstable, QuickSort algorithm.
   * For small collections falls back to {@link #insertionSort(int,int)}. */
  public final void quickSort(final int lo, final int hi) {
    if (hi <= lo) return;
    // from Integer's Javadocs: ceil(log2(x)) = 32 - numberOfLeadingZeros(x - 1)
    quickSort(lo, hi, (Integer.SIZE - Integer.numberOfLeadingZeros(hi - lo)) << 1);
  }
  
  private void quickSort(int lo, int hi, int maxDepth) {
    // fall back to insertion when array has short length
    final int diff = hi - lo;
    if (diff <= QUICKSORT_THRESHOLD) {
      insertionSort(lo, hi);
      return;
    }
    
    // fall back to merge sort when recursion depth gets too big
    if (--maxDepth == 0) {
      mergeSort(lo, hi);
      return;
    }
    
    final int mid = lo + (diff >>> 1);
    
    if (compare(lo, mid) > 0) {
      swap(lo, mid);
    }

    if (compare(mid, hi) > 0) {
      swap(mid, hi);
      if (compare(lo, mid) > 0) {
        swap(lo, mid);
      }
    }
    
    int left = lo + 1;
    int right = hi - 1;

    setPivot(mid);
    for (;;) {
      while (comparePivot(right) < 0)
        --right;

      while (left < right && comparePivot(left) >= 0)
        ++left;

      if (left < right) {
        swap(left, right);
        --right;
      } else {
        break;
      }
    }

    quickSort(lo, left, maxDepth);
    quickSort(left + 1, hi, maxDepth);
  }

  /** TimSort implementation. The only difference with the spec is that this
   *  impl reuses {@link SorterTemplate#merge(int, int, int, int, int)} to
   *  merge runs (in place) instead of the original merging routine from
   *  TimSort (which requires extra memory but might be slightly faster). */
  private class TimSort {

    final int hi;
    final int minRun;
    final int[] runEnds;
    int stackSize;

    TimSort(int lo, int hi) {
      assert hi > lo;
      // +1 because the first slot is reserved and always lo
      runEnds = new int[TIMSORT_STACKSIZE + 1];
      runEnds[0] = lo;
      stackSize = 0;
      this.hi = hi;
      minRun = minRun(hi - lo + 1);
    }

    /** Minimum run length for an array of length <code>length</code>. */
    int minRun(int length) {
      assert length >= TIMSORT_MINRUN;
      int n = length;
      int r = 0;
      while (n >= 64) {
        r |= n & 1;
        n >>>= 1;
      }
      final int minRun = n + r;
      assert minRun >= TIMSORT_MINRUN && minRun <= 64;
      return minRun;
    }

    int runLen(int i) {
      final int off = stackSize - i;
      return runEnds[off] - runEnds[off - 1];
    }

    int runBase(int i) {
      return runEnds[stackSize - i - 1];
    }

    int runEnd(int i) {
      return runEnds[stackSize - i];
    }

    void setRunEnd(int i, int runEnd) {
      runEnds[stackSize - i] = runEnd;
    }

    void pushRunLen(int len) {
      runEnds[stackSize + 1] = runEnds[stackSize] + len;
      ++stackSize;
    }

    /** Merge run i with run i+1 */
    void mergeAt(int i) {
      assert stackSize > i + 1;
      final int l = runBase(i+1);
      final int pivot = runBase(i);
      final int h = runEnd(i);
      runMerge(l, pivot, h, pivot - l, h - pivot);
      for (int j = i + 1; j > 0; --j) {
        setRunEnd(j, runEnd(j-1));
      }
      --stackSize;
    }

    /** Compute the length of the next run, make the run sorted and return its
     *  length. */
    int nextRun() {
      final int runBase = runEnd(0);
      if (runBase == hi) {
        return 1;
      }
      int l = 1; // length of the run
      if (compare(runBase, runBase+1) > 0) {
        // run must be strictly descending
        while (runBase + l <= hi && compare(runBase + l - 1, runBase + l) > 0) {
          ++l;
        }
        if (l < minRun && runBase + l <= hi) {
          l = Math.min(hi - runBase + 1, minRun);
          binarySort(runBase, runBase + l - 1);
        } else {
          // revert
          for (int i = 0, halfL = l >>> 1; i < halfL; ++i) {
            swap(runBase + i, runBase + l - i - 1);
          }
        }
      } else {
        // run must be non-descending
        while (runBase + l <= hi && compare(runBase + l - 1, runBase + l) <= 0) {
          ++l;
        }
        if (l < minRun && runBase + l <= hi) {
          l = Math.min(hi - runBase + 1, minRun);
          binarySort(runBase, runBase + l - 1);
        } // else nothing to do, the run is already sorted
      }
      return l;
    }

    void ensureInvariants() {
      while (stackSize > 1) {
        final int runLen0 = runLen(0);
        final int runLen1 = runLen(1);

        if (stackSize > 2) {
          final int runLen2 = runLen(2);

          if (runLen2 <= runLen1 + runLen0) {
            // merge the smaller of 0 and 2 with 1
            if (runLen2 < runLen0) {
              mergeAt(1);
            } else {
              mergeAt(0);
            }
            continue;
          }
        }

        if (runLen1 <= runLen0) {
          mergeAt(0);
          continue;
        }

        break;
      }
    }

    void exhaustStack() {
      while (stackSize > 1) {
        mergeAt(0);
      }
    }

    void sort() {
      do {
        ensureInvariants();

        // Push a new run onto the stack
        pushRunLen(nextRun());

      } while (runEnd(0) <= hi);

      exhaustStack();
      assert runEnd(0) == hi + 1;
    }

  }

  /** Sorts using <a href="http://svn.python.org/projects/python/trunk/Objects/listsort.txt">TimSort</a>, see 
   *  also <a href="http://svn.python.org/projects/python/trunk/Objects/listobject.c">source code</a>.
   *  TimSort is a stable sorting algorithm based on MergeSort but known to
   *  perform extremely well on partially-sorted inputs.
   *  For small collections, falls back to {@link #binarySort(int, int)}. */
  public final void timSort(int lo, int hi) {
    if (hi - lo <= TIMSORT_THRESHOLD) {
      binarySort(lo, hi);
      return;
    }

    new TimSort(lo, hi).sort();
  }

  /** Sorts via stable in-place MergeSort algorithm
   * For small collections falls back to {@link #insertionSort(int,int)}. */
  public final void mergeSort(int lo, int hi) {
    final int diff = hi - lo;
    if (diff <= MERGESORT_THRESHOLD) {
      insertionSort(lo, hi);
      return;
    }
    
    final int mid = lo + (diff >>> 1);
    
    mergeSort(lo, mid);
    mergeSort(mid, hi);
    runMerge(lo, mid, hi, mid - lo, hi - mid);
  }

  /** Sort out trivial cases and reduce the scope of the merge as much as
   *  possible before calling {@link #merge}/ */
  private void runMerge(int lo, int pivot, int hi, int len1, int len2) {
    if (len1 == 0 || len2 == 0) {
      return;
    }
    setPivot(pivot - 1);
    if (comparePivot(pivot) <= 0) {
      // all values from the first run are below all values from the 2nd run
      // this shortcut makes mergeSort run in linear time on sorted arrays
      return;
    }
    while (comparePivot(hi - 1) <= 0) {
      --hi;
      --len2;
    }
    setPivot(pivot);
    while (comparePivot(lo) >= 0) {
      ++lo;
      --len1;
    }
    if (len1 + len2 == 2) {
      assert len1 == len2;
      assert compare(lo, pivot) > 0;
      swap(pivot, lo);
      return;
    }
    merge(lo, pivot, hi, len1, len2);
  }

  /** Merge the slices [lo-pivot[ (of length len1) and [pivot-hi[ (of length
   *  len2) which are already sorted. This method merges in-place but can be
   *  extended to provide a faster implementation using extra memory. */
  protected void merge(int lo, int pivot, int hi, int len1, int len2) {
    int first_cut, second_cut;
    int len11, len22;
    if (len1 > len2) {
      len11 = len1 >>> 1;
      first_cut = lo + len11;
      second_cut = lower(pivot, hi, first_cut);
      len22 = second_cut - pivot;
    } else {
      len22 = len2 >>> 1;
      second_cut = pivot + len22;
      first_cut = upper(lo, pivot, second_cut);
      len11 = first_cut - lo;
    }
    rotate(first_cut, pivot, second_cut);
    final int new_mid = first_cut + len22;
    runMerge(lo, first_cut, new_mid, len11, len22);
    runMerge(new_mid, second_cut, hi, len1 - len11, len2 - len22);
  }

  private void rotate(int lo, int mid, int hi) {
    int lot = lo;
    int hit = mid - 1;
    while (lot < hit) {
      swap(lot++, hit--);
    }
    lot = mid; hit = hi - 1;
    while (lot < hit) {
      swap(lot++, hit--);
    }
    lot = lo; hit = hi - 1;
    while (lot < hit) {
      swap(lot++, hit--);
    }
  }

  private int lower(int lo, int hi, int val) {
    int len = hi - lo;
    while (len > 0) {
      final int half = len >>> 1,
        mid = lo + half;
      if (compare(mid, val) < 0) {
        lo = mid + 1;
        len = len - half -1;
      } else {
        len = half;
      }
    }
    return lo;
  }

  private int upper(int lo, int hi, int val) {
    int len = hi - lo;
    while (len > 0) {
      final int half = len >>> 1,
        mid = lo + half;
      if (compare(val, mid) < 0) {
        len = half;
      } else {
        lo = mid + 1;
        len = len - half -1;
      }
    }
    return lo;
  }

}
