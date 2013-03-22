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
import java.util.Collections;
import java.util.Random;

public class TestArrayUtil extends LuceneTestCase {

  // Ensure ArrayUtil.getNextSize gives linear amortized cost of realloc/copy
  public void testGrowth() {
    int currentSize = 0;
    long copyCost = 0;

    // Make sure ArrayUtil hits Integer.MAX_VALUE, if we insist:
    while(currentSize != Integer.MAX_VALUE) {
      int nextSize = ArrayUtil.oversize(1+currentSize, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      assertTrue(nextSize > currentSize);
      if (currentSize > 0) {
        copyCost += currentSize;
        double copyCostPerElement = ((double) copyCost)/currentSize;
        assertTrue("cost " + copyCostPerElement, copyCostPerElement < 10.0);
      }
      currentSize = nextSize;
    }
  }

  public void testMaxSize() {
    // intentionally pass invalid elemSizes:
    for(int elemSize=0;elemSize<10;elemSize++) {
      assertEquals(Integer.MAX_VALUE, ArrayUtil.oversize(Integer.MAX_VALUE, elemSize));
      assertEquals(Integer.MAX_VALUE, ArrayUtil.oversize(Integer.MAX_VALUE-1, elemSize));
    }
  }

  public void testInvalidElementSizes() {
    final Random rnd = random();
    final int num = atLeast(10000);
    for (int iter = 0; iter < num; iter++) {
      final int minTargetSize = rnd.nextInt(Integer.MAX_VALUE);
      final int elemSize = rnd.nextInt(11);
      final int v = ArrayUtil.oversize(minTargetSize, elemSize);
      assertTrue(v >= minTargetSize);
    }
  }

  public void testParseInt() throws Exception {
    int test;
    try {
      test = ArrayUtil.parseInt("".toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }
    try {
      test = ArrayUtil.parseInt("foo".toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }
    try {
      test = ArrayUtil.parseInt(String.valueOf(Long.MAX_VALUE).toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }
    try {
      test = ArrayUtil.parseInt("0.34".toCharArray());
      assertTrue(false);
    } catch (NumberFormatException e) {
      //expected
    }

    try {
      test = ArrayUtil.parseInt("1".toCharArray());
      assertTrue(test + " does not equal: " + 1, test == 1);
      test = ArrayUtil.parseInt("-10000".toCharArray());
      assertTrue(test + " does not equal: " + -10000, test == -10000);
      test = ArrayUtil.parseInt("1923".toCharArray());
      assertTrue(test + " does not equal: " + 1923, test == 1923);
      test = ArrayUtil.parseInt("-1".toCharArray());
      assertTrue(test + " does not equal: " + -1, test == -1);
      test = ArrayUtil.parseInt("foo 1923 bar".toCharArray(), 4, 4);
      assertTrue(test + " does not equal: " + 1923, test == 1923);
    } catch (NumberFormatException e) {
      e.printStackTrace();
      assertTrue(false);
    }

  }

  public void testSliceEquals() {
    String left = "this is equal";
    String right = left;
    char[] leftChars = left.toCharArray();
    char[] rightChars = right.toCharArray();
    assertTrue(left + " does not equal: " + right, ArrayUtil.equals(leftChars, 0, rightChars, 0, left.length()));
    
    assertFalse(left + " does not equal: " + right, ArrayUtil.equals(leftChars, 1, rightChars, 0, left.length()));
    assertFalse(left + " does not equal: " + right, ArrayUtil.equals(leftChars, 1, rightChars, 2, left.length()));

    assertFalse(left + " does not equal: " + right, ArrayUtil.equals(leftChars, 25, rightChars, 0, left.length()));
    assertFalse(left + " does not equal: " + right, ArrayUtil.equals(leftChars, 12, rightChars, 0, left.length()));
  }
  
  private Integer[] createRandomArray(int maxSize) {
    final Random rnd = random();
    final Integer[] a = new Integer[rnd.nextInt(maxSize) + 1];
    for (int i = 0; i < a.length; i++) {
      a[i] = Integer.valueOf(rnd.nextInt(a.length));
    }
    return a;
  }
  
  public void testQuickSort() {
    int num = atLeast(50);
    for (int i = 0; i < num; i++) {
      Integer[] a1 = createRandomArray(2000), a2 = a1.clone();
      ArrayUtil.quickSort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
      
      a1 = createRandomArray(2000);
      a2 = a1.clone();
      ArrayUtil.quickSort(a1, Collections.reverseOrder());
      Arrays.sort(a2, Collections.reverseOrder());
      assertArrayEquals(a2, a1);
      // reverse back, so we can test that completely backwards sorted array (worst case) is working:
      ArrayUtil.quickSort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
    }
  }
  
  private Integer[] createSparseRandomArray(int maxSize) {
    final Random rnd = random();
    final Integer[] a = new Integer[rnd.nextInt(maxSize) + 1];
    for (int i = 0; i < a.length; i++) {
      a[i] = Integer.valueOf(rnd.nextInt(2));
    }
    return a;
  }
  
  // This is a test for LUCENE-3054 (which fails without the merge sort fall back with stack overflow in most cases)
  public void testQuickToMergeSortFallback() {
    int num = atLeast(50);
    for (int i = 0; i < num; i++) {
      Integer[] a1 = createSparseRandomArray(40000), a2 = a1.clone();
      ArrayUtil.quickSort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
    }
  }
  
  public void testMergeSort() {
    int num = atLeast(50);
    for (int i = 0; i < num; i++) {
      Integer[] a1 = createRandomArray(2000), a2 = a1.clone();
      ArrayUtil.mergeSort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
      
      a1 = createRandomArray(2000);
      a2 = a1.clone();
      ArrayUtil.mergeSort(a1, Collections.reverseOrder());
      Arrays.sort(a2, Collections.reverseOrder());
      assertArrayEquals(a2, a1);
      // reverse back, so we can test that completely backwards sorted array (worst case) is working:
      ArrayUtil.mergeSort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
    }
  }

  public void testTimSort() {
    int num = atLeast(65);
    for (int i = 0; i < num; i++) {
      Integer[] a1 = createRandomArray(2000), a2 = a1.clone();
      ArrayUtil.timSort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
      
      a1 = createRandomArray(2000);
      a2 = a1.clone();
      ArrayUtil.timSort(a1, Collections.reverseOrder());
      Arrays.sort(a2, Collections.reverseOrder());
      assertArrayEquals(a2, a1);
      // reverse back, so we can test that completely backwards sorted array (worst case) is working:
      ArrayUtil.timSort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
    }
  }

  public void testInsertionSort() {
    for (int i = 0, c = atLeast(500); i < c; i++) {
      Integer[] a1 = createRandomArray(30), a2 = a1.clone();
      ArrayUtil.insertionSort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
      
      a1 = createRandomArray(30);
      a2 = a1.clone();
      ArrayUtil.insertionSort(a1, Collections.reverseOrder());
      Arrays.sort(a2, Collections.reverseOrder());
      assertArrayEquals(a2, a1);
      // reverse back, so we can test that completely backwards sorted array (worst case) is working:
      ArrayUtil.insertionSort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
    }
  }
  
  public void testBinarySort() {
    for (int i = 0, c = atLeast(500); i < c; i++) {
      Integer[] a1 = createRandomArray(30), a2 = a1.clone();
      ArrayUtil.binarySort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
      
      a1 = createRandomArray(30);
      a2 = a1.clone();
      ArrayUtil.binarySort(a1, Collections.reverseOrder());
      Arrays.sort(a2, Collections.reverseOrder());
      assertArrayEquals(a2, a1);
      // reverse back, so we can test that completely backwards sorted array (worst case) is working:
      ArrayUtil.binarySort(a1);
      Arrays.sort(a2);
      assertArrayEquals(a2, a1);
    }
  }
  
  static class Item implements Comparable<Item> {
    final int val, order;
    
    Item(int val, int order) {
      this.val = val;
      this.order = order;
    }
    
    @Override
    public int compareTo(Item other) {
      return this.order - other.order;
    }
    
    @Override
    public String toString() {
      return Integer.toString(val);
    }
  }
  
  public void testMergeSortStability() {
    final Random rnd = random();
    Item[] items = new Item[100];
    for (int i = 0; i < items.length; i++) {
      // half of the items have value but same order. The value of this items is sorted,
      // so they should always be in order after sorting.
      // The other half has defined order, but no (-1) value (they should appear after
      // all above, when sorted).
      final boolean equal = rnd.nextBoolean();
      items[i] = new Item(equal ? (i+1) : -1, equal ? 0 : (rnd.nextInt(1000)+1));
    }
    
    if (VERBOSE) System.out.println("Before: " + Arrays.toString(items));
    // if you replace this with ArrayUtil.quickSort(), test should fail:
    ArrayUtil.mergeSort(items);
    if (VERBOSE) System.out.println("Sorted: " + Arrays.toString(items));
    
    Item last = items[0];
    for (int i = 1; i < items.length; i++) {
      final Item act = items[i];
      if (act.order == 0) {
        // order of "equal" items should be not mixed up
        assertTrue(act.val > last.val);
      }
      assertTrue(act.order >= last.order);
      last = act;
    }
  }

  public void testTimSortStability() {
    final Random rnd = random();
    Item[] items = new Item[100];
    for (int i = 0; i < items.length; i++) {
      // half of the items have value but same order. The value of this items is sorted,
      // so they should always be in order after sorting.
      // The other half has defined order, but no (-1) value (they should appear after
      // all above, when sorted).
      final boolean equal = rnd.nextBoolean();
      items[i] = new Item(equal ? (i+1) : -1, equal ? 0 : (rnd.nextInt(1000)+1));
    }
    
    if (VERBOSE) System.out.println("Before: " + Arrays.toString(items));
    // if you replace this with ArrayUtil.quickSort(), test should fail:
    ArrayUtil.timSort(items);
    if (VERBOSE) System.out.println("Sorted: " + Arrays.toString(items));
    
    Item last = items[0];
    for (int i = 1; i < items.length; i++) {
      final Item act = items[i];
      if (act.order == 0) {
        // order of "equal" items should be not mixed up
        assertTrue(act.val > last.val);
      }
      assertTrue(act.order >= last.order);
      last = act;
    }
  }

  // should produce no exceptions
  public void testEmptyArraySort() {
    Integer[] a = new Integer[0];
    ArrayUtil.quickSort(a);
    ArrayUtil.mergeSort(a);
    ArrayUtil.insertionSort(a);
    ArrayUtil.binarySort(a);
    ArrayUtil.timSort(a);
    ArrayUtil.quickSort(a, Collections.reverseOrder());
    ArrayUtil.mergeSort(a, Collections.reverseOrder());
    ArrayUtil.timSort(a, Collections.reverseOrder());
    ArrayUtil.insertionSort(a, Collections.reverseOrder());
    ArrayUtil.binarySort(a, Collections.reverseOrder());
  }
  
}
