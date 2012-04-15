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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class TestCollectionUtil extends LuceneTestCase {

  private List<Integer> createRandomList(int maxSize) {
    final Integer[] a = new Integer[random().nextInt(maxSize) + 1];
    for (int i = 0; i < a.length; i++) {
      a[i] = Integer.valueOf(random().nextInt(a.length));
    }
    return Arrays.asList(a);
  }
  
  public void testQuickSort() {
    for (int i = 0, c = atLeast(500); i < c; i++) {
      List<Integer> list1 = createRandomList(1000), list2 = new ArrayList<Integer>(list1);
      CollectionUtil.quickSort(list1);
      Collections.sort(list2);
      assertEquals(list2, list1);
      
      list1 = createRandomList(1000);
      list2 = new ArrayList<Integer>(list1);
      CollectionUtil.quickSort(list1, Collections.reverseOrder());
      Collections.sort(list2, Collections.reverseOrder());
      assertEquals(list2, list1);
      // reverse back, so we can test that completely backwards sorted array (worst case) is working:
      CollectionUtil.quickSort(list1);
      Collections.sort(list2);
      assertEquals(list2, list1);
    }
  }
  
  public void testMergeSort() {
    for (int i = 0, c = atLeast(500); i < c; i++) {
      List<Integer> list1 = createRandomList(1000), list2 = new ArrayList<Integer>(list1);
      CollectionUtil.mergeSort(list1);
      Collections.sort(list2);
      assertEquals(list2, list1);
      
      list1 = createRandomList(1000);
      list2 = new ArrayList<Integer>(list1);
      CollectionUtil.mergeSort(list1, Collections.reverseOrder());
      Collections.sort(list2, Collections.reverseOrder());
      assertEquals(list2, list1);
      // reverse back, so we can test that completely backwards sorted array (worst case) is working:
      CollectionUtil.mergeSort(list1);
      Collections.sort(list2);
      assertEquals(list2, list1);
    }
  }
  
  public void testInsertionSort() {
    for (int i = 0, c = atLeast(500); i < c; i++) {
      List<Integer> list1 = createRandomList(30), list2 = new ArrayList<Integer>(list1);
      CollectionUtil.insertionSort(list1);
      Collections.sort(list2);
      assertEquals(list2, list1);
      
      list1 = createRandomList(30);
      list2 = new ArrayList<Integer>(list1);
      CollectionUtil.insertionSort(list1, Collections.reverseOrder());
      Collections.sort(list2, Collections.reverseOrder());
      assertEquals(list2, list1);
      // reverse back, so we can test that completely backwards sorted array (worst case) is working:
      CollectionUtil.insertionSort(list1);
      Collections.sort(list2);
      assertEquals(list2, list1);
    }
  }
  
  public void testEmptyListSort() {
    // should produce no exceptions
    List<Integer> list = Arrays.asList(new Integer[0]); // LUCENE-2989
    CollectionUtil.quickSort(list);
    CollectionUtil.mergeSort(list);
    CollectionUtil.insertionSort(list);
    CollectionUtil.quickSort(list, Collections.reverseOrder());
    CollectionUtil.mergeSort(list, Collections.reverseOrder());
    CollectionUtil.insertionSort(list, Collections.reverseOrder());
    
    // check that empty non-random access lists pass sorting without ex (as sorting is not needed)
    list = new LinkedList<Integer>();
    CollectionUtil.quickSort(list);
    CollectionUtil.mergeSort(list);
    CollectionUtil.insertionSort(list);
    CollectionUtil.quickSort(list, Collections.reverseOrder());
    CollectionUtil.mergeSort(list, Collections.reverseOrder());
    CollectionUtil.insertionSort(list, Collections.reverseOrder());
  }
  
  public void testOneElementListSort() {
    // check that one-element non-random access lists pass sorting without ex (as sorting is not needed)
    List<Integer> list = new LinkedList<Integer>();
    list.add(1);
    CollectionUtil.quickSort(list);
    CollectionUtil.mergeSort(list);
    CollectionUtil.insertionSort(list);
    CollectionUtil.quickSort(list, Collections.reverseOrder());
    CollectionUtil.mergeSort(list, Collections.reverseOrder());
    CollectionUtil.insertionSort(list, Collections.reverseOrder());
  }
  
}
