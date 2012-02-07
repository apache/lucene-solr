package org.apache.lucene.util;

/**
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

import java.util.Comparator;
import java.util.Collections;
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
  
  /** SorterTemplate with custom {@link Comparator} */
  private static <T> SorterTemplate getSorter(final List<T> list, final Comparator<? super T> comp) {
    if (!(list instanceof RandomAccess))
      throw new IllegalArgumentException("CollectionUtil can only sort random access lists in-place.");
    return new SorterTemplate() {
      @Override
      protected void swap(int i, int j) {
        Collections.swap(list, i, j);
      }
      
      @Override
      protected int compare(int i, int j) {
        return comp.compare(list.get(i), list.get(j));
      }

      @Override
      protected void setPivot(int i) {
        pivot = list.get(i);
      }
  
      @Override
      protected int comparePivot(int j) {
        return comp.compare(pivot, list.get(j));
      }
      
      private T pivot;
    };
  }
  
  /** Natural SorterTemplate */
  private static <T extends Comparable<? super T>> SorterTemplate getSorter(final List<T> list) {
    if (!(list instanceof RandomAccess))
      throw new IllegalArgumentException("CollectionUtil can only sort random access lists in-place.");
    return new SorterTemplate() {
      @Override
      protected void swap(int i, int j) {
        Collections.swap(list, i, j);
      }
      
      @Override
      protected int compare(int i, int j) {
        return list.get(i).compareTo(list.get(j));
      }

      @Override
      protected void setPivot(int i) {
        pivot = list.get(i);
      }
  
      @Override
      protected int comparePivot(int j) {
        return pivot.compareTo(list.get(j));
      }
      
      private T pivot;
    };
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
    getSorter(list, comp).mergeSort(0, size-1);
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
    getSorter(list).mergeSort(0, size-1);
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
  
}