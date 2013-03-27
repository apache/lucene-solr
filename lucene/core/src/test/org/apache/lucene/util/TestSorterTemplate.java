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

import java.util.Arrays;

public class TestSorterTemplate extends LuceneTestCase {

  private static final int SLOW_SORT_THRESHOLD = 1000;

  // A sorter template that compares only the last 32 bits
  static class Last32BitsSorterTemplate extends SorterTemplate {

    final long[] arr;
    long pivot;

    Last32BitsSorterTemplate(long[] arr) {
      this.arr = arr;
    }

    @Override
    protected void swap(int i, int j) {
      final long tmp = arr[i];
      arr[i] = arr[j];
      arr[j] = tmp;
    }

    private int compareValues(long i, long j) {
      // only compare the last 32 bits
      final long a = i & 0xFFFFFFFFL;
      final long b = j & 0xFFFFFFFFL;
      return a < b ? -1 : a == b ? 0 : 1;
    }

    @Override
    protected int compare(int i, int j) {
      return compareValues(arr[i], arr[j]);
    }

    @Override
    protected void setPivot(int i) {
      pivot = arr[i];
    }

    @Override
    protected int comparePivot(int j) {
      return compareValues(pivot, arr[j]);
    }

    @Override
    protected void merge(int lo, int pivot, int hi, int len1, int len2) {
      // timSort and mergeSort should call runMerge to sort out trivial cases
      assertTrue(len1 >= 1);
      assertTrue(len2 >= 1);
      assertTrue(len1 + len2 >= 3);
      assertTrue(compare(lo, pivot) > 0);
      assertTrue(compare(pivot - 1, hi - 1) > 0);
      assertFalse(compare(pivot - 1, pivot) <= 0);
      super.merge(lo, pivot, hi, len1, len2);
    }

  }

  void testSort(int[] intArr) {
    // we modify the array as a long[] and store the original ord in the first 32 bits
    // to be able to check stability
    final long[] arr = toLongsAndOrds(intArr);

    // use MergeSort as a reference
    // assertArrayEquals checks for sorting + stability
    // assertArrayEquals(toInts) checks for sorting only
    final long[] mergeSorted = Arrays.copyOf(arr, arr.length);
    new Last32BitsSorterTemplate(mergeSorted).mergeSort(0, arr.length - 1);

    if (arr.length < SLOW_SORT_THRESHOLD) {
      final long[] insertionSorted = Arrays.copyOf(arr, arr.length);
      new Last32BitsSorterTemplate(insertionSorted).insertionSort(0, arr.length - 1);
      assertArrayEquals(mergeSorted, insertionSorted);
      
      final long[] binarySorted = Arrays.copyOf(arr, arr.length);
      new Last32BitsSorterTemplate(binarySorted).binarySort(0, arr.length - 1);
      assertArrayEquals(mergeSorted, binarySorted);
    }

    final long[] quickSorted = Arrays.copyOf(arr, arr.length);
    new Last32BitsSorterTemplate(quickSorted).quickSort(0, arr.length - 1);
    assertArrayEquals(toInts(mergeSorted), toInts(quickSorted));

    final long[] timSorted = Arrays.copyOf(arr, arr.length);
    new Last32BitsSorterTemplate(timSorted).timSort(0, arr.length - 1);
    assertArrayEquals(mergeSorted, timSorted);
  }

  private int[] toInts(long[] longArr) {
    int[] arr = new int[longArr.length];
    for (int i = 0; i < longArr.length; ++i) {
      arr[i] = (int) longArr[i];
    }
    return arr;
  }

  private long[] toLongsAndOrds(int[] intArr) {
    final long[] arr = new long[intArr.length];
    for (int i = 0; i < intArr.length; ++i) {
      arr[i] = (((long) i) << 32) | (intArr[i] & 0xFFFFFFFFL);
    }
    return arr;
  }

  int randomLength() {
    return _TestUtil.nextInt(random(), 1, random().nextBoolean() ? SLOW_SORT_THRESHOLD : 100000);
  }

  public void testEmpty() {
    testSort(new int[0]);
  }

  public void testAscending() {
    final int length = randomLength();
    final int[] arr = new int[length];
    arr[0] = random().nextInt(10);
    for (int i = 1; i < arr.length; ++i) {
      arr[i] = arr[i-1] + _TestUtil.nextInt(random(), 0, 10);
    }
    testSort(arr);
  }

  public void testDescending() {
    final int length = randomLength();
    final int[] arr = new int[length];
    arr[0] = random().nextInt(10);
    for (int i = 1; i < arr.length; ++i) {
      arr[i] = arr[i-1] - _TestUtil.nextInt(random(), 0, 10);
    }
    testSort(arr);
  }

  public void testStrictlyDescending() {
    final int length = randomLength();
    final int[] arr = new int[length];
    arr[0] = random().nextInt(10);
    for (int i = 1; i < arr.length; ++i) {
      arr[i] = arr[i-1] - _TestUtil.nextInt(random(), 1, 10);
    }
    testSort(arr);
  }

  public void testRandom1() {
    final int length = randomLength();
    final int[] arr = new int[length];
    for (int i = 1; i < arr.length; ++i) {
      arr[i] = random().nextInt();
    }
    testSort(arr);
  }

  public void testRandom2() {
    final int length = randomLength();
    final int[] arr = new int[length];
    for (int i = 1; i < arr.length; ++i) {
      arr[i] = random().nextInt(10);
    }
    testSort(arr);
  }

}
