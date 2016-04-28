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

import org.apache.lucene.util.packed.PackedInts;

public class TestLSBRadixSorter extends LuceneTestCase {

  public void test(LSBRadixSorter sorter, int maxLen) {
    for (int iter = 0; iter < 10; ++iter) {
      final int len = TestUtil.nextInt(random(), 0, maxLen);
      int[] arr = new int[len + random().nextInt(10)];
      final int numBits = random().nextInt(31);
      final int maxValue = (1 << numBits) - 1;
      for (int i = 0; i < arr.length; ++i) {
        arr[i] = TestUtil.nextInt(random(), 0, maxValue);
      }
      test(sorter, arr, len);
    }
  }

  public void test(LSBRadixSorter sorter, int[] arr, int len) {
    final int[] expected = Arrays.copyOf(arr, len);
    Arrays.sort(expected);

    int numBits = 0;
    for (int i = 0; i < len; ++i) {
      numBits = Math.max(numBits, PackedInts.bitsRequired(arr[i]));
    }

    if (random().nextBoolean()) {
      numBits = TestUtil.nextInt(random(), numBits, 32);
    }

    sorter.sort(numBits, arr, len);
    final int[] actual = Arrays.copyOf(arr, len);
    assertArrayEquals(expected, actual);
  }

  public void testEmpty() {
    test(new LSBRadixSorter(), 0);
  }

  public void testOne() {
    test(new LSBRadixSorter(), 1);
  }

  public void testTwo() {
    test(new LSBRadixSorter(), 2);
  }

  public void testSimple() {
    test(new LSBRadixSorter(), 100);
  }

  public void testRandom() {
    test(new LSBRadixSorter(), 10000);
  }

  public void testSorted() {
    LSBRadixSorter sorter = new LSBRadixSorter();
    for (int iter = 0; iter < 10; ++iter) {
      int[] arr = new int[10000];
      int a = 0;
      for (int i = 0; i < arr.length; ++i) {
        a += random().nextInt(10);
        arr[i] = a;
      }
      final int len = TestUtil.nextInt(random(), 0, arr.length);
      test(sorter, arr, len);
    }
  }
}
