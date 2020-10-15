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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class TestCollectionUtil extends LuceneTestCase {

  private List<Integer> createRandomList(int maxSize) {
    final Random rnd = random();
    final Integer[] a = new Integer[rnd.nextInt(maxSize) + 1];
    for (int i = 0; i < a.length; i++) {
      a[i] = Integer.valueOf(rnd.nextInt(a.length));
    }
    return Arrays.asList(a);
  }
  
  public void testIntroSort() {
    for (int i = 0, c = atLeast(100); i < c; i++) {
      List<Integer> list1 = createRandomList(2000), list2 = new ArrayList<>(list1);
      CollectionUtil.introSort(list1);
      Collections.sort(list2);
      assertEquals(list2, list1);
      
      list1 = createRandomList(2000);
      list2 = new ArrayList<>(list1);
      CollectionUtil.introSort(list1, Collections.reverseOrder());
      Collections.sort(list2, Collections.reverseOrder());
      assertEquals(list2, list1);
      // reverse back, so we can test that completely backwards sorted array (worst case) is working:
      CollectionUtil.introSort(list1);
      Collections.sort(list2);
      assertEquals(list2, list1);
    }
  }

  public void testTimSort() {
    for (int i = 0, c = atLeast(100); i < c; i++) {
      List<Integer> list1 = createRandomList(2000), list2 = new ArrayList<>(list1);
      CollectionUtil.timSort(list1);
      Collections.sort(list2);
      assertEquals(list2, list1);
      
      list1 = createRandomList(2000);
      list2 = new ArrayList<>(list1);
      CollectionUtil.timSort(list1, Collections.reverseOrder());
      Collections.sort(list2, Collections.reverseOrder());
      assertEquals(list2, list1);
      // reverse back, so we can test that completely backwards sorted array (worst case) is working:
      CollectionUtil.timSort(list1);
      Collections.sort(list2);
      assertEquals(list2, list1);
    }
  }

  public void testEmptyListSort() {
    // should produce no exceptions
    List<Integer> list = Arrays.asList(new Integer[0]); // LUCENE-2989
    CollectionUtil.introSort(list);
    CollectionUtil.timSort(list);
    CollectionUtil.introSort(list, Collections.reverseOrder());
    CollectionUtil.timSort(list, Collections.reverseOrder());
    
    // check that empty non-random access lists pass sorting without ex (as sorting is not needed)
    list = new LinkedList<>();
    CollectionUtil.introSort(list);
    CollectionUtil.timSort(list);
    CollectionUtil.introSort(list, Collections.reverseOrder());
    CollectionUtil.timSort(list, Collections.reverseOrder());
  }
  
  public void testOneElementListSort() {
    // check that one-element non-random access lists pass sorting without ex (as sorting is not needed)
    List<Integer> list = new LinkedList<>();
    list.add(1);
    CollectionUtil.introSort(list);
    CollectionUtil.timSort(list);
    CollectionUtil.introSort(list, Collections.reverseOrder());
    CollectionUtil.timSort(list, Collections.reverseOrder());
  }
  
}
