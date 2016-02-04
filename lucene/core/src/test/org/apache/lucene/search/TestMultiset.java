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
package org.apache.lucene.search;


import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;

public class TestMultiset extends LuceneTestCase {

  public void testDuplicatesMatter() {
    Multiset<Integer> s1 = new Multiset<>();
    Multiset<Integer> s2 = new Multiset<>();
    assertEquals(s1.size(), s2.size());
    assertEquals(s1, s2);

    assertTrue(s1.add(42));
    assertTrue(s2.add(42));
    assertEquals(s1, s2);

    s2.add(42);
    assertFalse(s1.equals(s2));

    s1.add(43);
    s1.add(43);
    s2.add(43);
    assertEquals(s1.size(), s2.size());
    assertFalse(s1.equals(s2));
  }

  private static <T> Map<T, Integer> toCountMap(Multiset<T> set) {
    Map<T, Integer> map = new HashMap<>();
    int recomputedSize = 0;
    for (T element : set) {
      add(map, element);
      recomputedSize += 1;
    }
    assertEquals(set.toString(), recomputedSize, set.size());
    return map;
  }

  private static <T> void add(Map<T, Integer> map, T element) {
    Integer currentFreq = map.get(element);
    if (currentFreq == null) {
      currentFreq = 0;
    }
    map.put(element, currentFreq + 1);
  }

  private static <T> void remove(Map<T, Integer> map, T element) {
    Integer count = map.get(element);
    if (count == null) {
      return;
    } else if (count.intValue() == 1) {
      map.remove(element);
    } else {
      map.put(element, count - 1);
    }
  }

  public void testRandom() {
    Map<Integer, Integer> reference = new HashMap<>();
    Multiset<Integer> multiset = new Multiset<>();
    final int iters = atLeast(100);
    for (int i = 0; i < iters; ++i) {
      final int value = random().nextInt(10);
      switch (random().nextInt(10)) {
        case 0:
        case 1:
        case 2:
          remove(reference, value);
          multiset.remove(value);
          break;
        case 3:
          reference.clear();
          multiset.clear();
          break;
        default:
          add(reference, value);
          multiset.add(value);
          break;
      }
      assertEquals(reference, toCountMap(multiset));
    }
  }

}
