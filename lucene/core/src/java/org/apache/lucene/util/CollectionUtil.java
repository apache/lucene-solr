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

import static org.apache.lucene.util.ArrayUtil.MERGE_EXTRA_MEMORY_THRESHOLD;
import static org.apache.lucene.util.ArrayUtil.MERGE_OVERHEAD_RATIO;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.RandomAccess;

/**
 * Methods for manipulating (sorting) collections.
 * Sort methods work directly on the supplied lists and don't copy to/from arrays
 * before/after. For medium size collections as used in the Lucene indexer that is
 * much more efficient.
 *
 * @lucene.internal
 */

public final class CollectionUtil {

  private CollectionUtil() {} // no instance

  private static abstract class ListSorterTemplate<T> extends SorterTemplate {

    protected final List<T> list;

    ListSorterTemplate(List<T> list) {
      this.list = list;
    }

    protected abstract int compare(T a, T b);

    @Override
    protected void swap(int i, int j) {
      Collections.swap(list, i, j);
    }

    @Override
    protected int compare(int i, int j) {
      return compare(list.get(i), list.get(j));
    }

    @Override
    protected void setPivot(int i) {
      pivot = list.get(i);
    }

    @Override
    protected int comparePivot(int j) {
      return compare(pivot, list.get(j));
    }

    private T pivot;

  }

  // a template for merge-based sorts which uses extra memory to speed up merging
  private static abstract class ListMergeSorterTemplate<T> extends ListSorterTemplate<T> {

    private final int threshold; // maximum length of a merge that can be made using extra memory
    private final T[] tmp;

    ListMergeSorterTemplate(List<T> list, float overheadRatio) {
      super(list);
      this.threshold = (int) (list.size() * overheadRatio);
      @SuppressWarnings("unchecked")
      final T[] tmpBuf = (T[]) new Object[threshold];
      this.tmp = tmpBuf;
    }

    private void mergeWithExtraMemory(int lo, int pivot, int hi, int len1, int len2) {
      for (int i = 0; i < len1; ++i) {
        tmp[i] = list.get(lo + i);
      }
      int i = 0, j = pivot, dest = lo;
      while (i < len1 && j < hi) {
        if (compare(tmp[i], list.get(j)) <= 0) {
          list.set(dest++, tmp[i++]);
        } else {
          list.set(dest++, list.get(j++));
        }
      }
      while (i < len1) {
        list.set(dest++, tmp[i++]);
      }
      assert j == dest;
    }

    @Override
    protected void merge(int lo, int pivot, int hi, int len1, int len2) {
      if (len1 <= threshold) {
        mergeWithExtraMemory(lo, pivot, hi, len1, len2);
      } else {
        // since this method recurses to run merge on smaller arrays, it will
        // end up using mergeWithExtraMemory
        super.merge(lo, pivot, hi, len1, len2);
      }
    }

  }

  /** SorterTemplate with custom {@link Comparator} */
  private static <T> SorterTemplate getSorter(final List<T> list, final Comparator<? super T> comp) {
    if (!(list instanceof RandomAccess))
      throw new IllegalArgumentException("CollectionUtil can only sort random access lists in-place.");
    return new ListSorterTemplate<T>(list) {

      @Override
      protected int compare(T a, T b) {
        return comp.compare(a, b);
      }

    };
  }
  
  /** Natural SorterTemplate */
  private static <T extends Comparable<? super T>> SorterTemplate getSorter(final List<T> list) {
    if (!(list instanceof RandomAccess))
      throw new IllegalArgumentException("CollectionUtil can only sort random access lists in-place.");
    return new ListSorterTemplate<T>(list) {

      @Override
      protected int compare(T a, T b) {
        return a.compareTo(b);
      }

    };
  }

  /** SorterTemplate with custom {@link Comparator} for merge-based sorts. */
  private static <T> SorterTemplate getMergeSorter(final List<T> list, final Comparator<? super T> comp) {
    if (!(list instanceof RandomAccess))
      throw new IllegalArgumentException("CollectionUtil can only sort random access lists in-place.");
    if (list.size() < MERGE_EXTRA_MEMORY_THRESHOLD) {
      return getSorter(list, comp);
    } else {
      return new ListMergeSorterTemplate<T>(list, MERGE_OVERHEAD_RATIO) {

        @Override
        protected int compare(T a, T b) {
          return comp.compare(a, b);
        }

      };
    }
  }
  
  /** Natural SorterTemplate for merge-based sorts. */
  private static <T extends Comparable<? super T>> SorterTemplate getMergeSorter(final List<T> list) {
    if (!(list instanceof RandomAccess))
      throw new IllegalArgumentException("CollectionUtil can only sort random access lists in-place.");
    if (list.size() < MERGE_EXTRA_MEMORY_THRESHOLD) {
      return getSorter(list);
    } else {
      return new ListMergeSorterTemplate<T>(list, MERGE_OVERHEAD_RATIO) {

        @Override
        protected int compare(T a, T b) {
          return a.compareTo(b);
        }

      };
    }
  }

  /**
   * Sorts the given random access {@link List} using the {@link Comparator}.
   * The list must implement {@link RandomAccess}. This method uses the quick sort
   * algorithm, but falls back to insertion sort for small lists.
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T> void quickSort(List<T> list, Comparator<? super T> comp) {
    final int size = list.size();
    if (size <= 1) return;
    getSorter(list, comp).quickSort(0, size-1);
  }
  
  /**
   * Sorts the given random access {@link List} in natural order.
   * The list must implement {@link RandomAccess}. This method uses the quick sort
   * algorithm, but falls back to insertion sort for small lists.
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T extends Comparable<? super T>> void quickSort(List<T> list) {
    final int size = list.size();
    if (size <= 1) return;
    getSorter(list).quickSort(0, size-1);
  }

  // mergeSorts:
  
  /**
   * Sorts the given random access {@link List} using the {@link Comparator}.
   * The list must implement {@link RandomAccess}. This method uses the merge sort
   * algorithm, but falls back to insertion sort for small lists.
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T> void mergeSort(List<T> list, Comparator<? super T> comp) {
    final int size = list.size();
    if (size <= 1) return;
    getMergeSorter(list, comp).mergeSort(0, size-1);
  }
  
  /**
   * Sorts the given random access {@link List} in natural order.
   * The list must implement {@link RandomAccess}. This method uses the merge sort
   * algorithm, but falls back to insertion sort for small lists.
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T extends Comparable<? super T>> void mergeSort(List<T> list) {
    final int size = list.size();
    if (size <= 1) return;
    getMergeSorter(list).mergeSort(0, size-1);
  }

  // timSorts:
  
  /**
   * Sorts the given random access {@link List} using the {@link Comparator}.
   * The list must implement {@link RandomAccess}. This method uses the TimSort
   * algorithm, but falls back to binary sort for small lists.
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T> void timSort(List<T> list, Comparator<? super T> comp) {
    final int size = list.size();
    if (size <= 1) return;
    getMergeSorter(list, comp).timSort(0, size-1);
  }
  
  /**
   * Sorts the given random access {@link List} in natural order.
   * The list must implement {@link RandomAccess}. This method uses the TimSort
   * algorithm, but falls back to binary sort for small lists.
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T extends Comparable<? super T>> void timSort(List<T> list) {
    final int size = list.size();
    if (size <= 1) return;
    getMergeSorter(list).timSort(0, size-1);
  }

  // insertionSorts:
  
  /**
   * Sorts the given random access {@link List} using the {@link Comparator}.
   * The list must implement {@link RandomAccess}. This method uses the insertion sort
   * algorithm. It is only recommended to use this algorithm for partially sorted small lists!
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T> void insertionSort(List<T> list, Comparator<? super T> comp) {
    final int size = list.size();
    if (size <= 1) return;
    getSorter(list, comp).insertionSort(0, size-1);
  }
  
  /**
   * Sorts the given random access {@link List} in natural order.
   * The list must implement {@link RandomAccess}. This method uses the insertion sort
   * algorithm. It is only recommended to use this algorithm for partially sorted small lists!
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T extends Comparable<? super T>> void insertionSort(List<T> list) {
    final int size = list.size();
    if (size <= 1) return;
    getSorter(list).insertionSort(0, size-1);
  }

  // binarySorts:
  
  /**
   * Sorts the given random access {@link List} using the {@link Comparator}.
   * The list must implement {@link RandomAccess}. This method uses the binary sort
   * algorithm. It is only recommended to use this algorithm for small lists!
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T> void binarySort(List<T> list, Comparator<? super T> comp) {
    final int size = list.size();
    if (size <= 1) return;
    getSorter(list, comp).binarySort(0, size-1);
  }
  
  /**
   * Sorts the given random access {@link List} in natural order.
   * The list must implement {@link RandomAccess}. This method uses the insertion sort
   * algorithm. It is only recommended to use this algorithm for small lists!
   * @throws IllegalArgumentException if list is e.g. a linked list without random access.
   */
  public static <T extends Comparable<? super T>> void binarySort(List<T> list) {
    final int size = list.size();
    if (size <= 1) return;
    getSorter(list).binarySort(0, size-1);
  }
}