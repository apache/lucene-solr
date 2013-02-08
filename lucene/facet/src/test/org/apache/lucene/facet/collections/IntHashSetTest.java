package org.apache.lucene.facet.collections;

import java.util.HashSet;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.collections.IntHashSet;
import org.junit.Test;

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

public class IntHashSetTest extends FacetTestCase {

  @Test
  public void test0() {
    IntHashSet set0 = new IntHashSet();

    assertEquals(0, set0.size());
    assertTrue(set0.isEmpty());
    set0.add(0);
    assertEquals(1, set0.size());
    assertFalse(set0.isEmpty());
    set0.remove(0);
    assertEquals(0, set0.size());
    assertTrue(set0.isEmpty());
  }

  @Test
  public void test1() {
    IntHashSet set0 = new IntHashSet();

    assertEquals(0, set0.size());
    assertTrue(set0.isEmpty());
    for (int i = 0; i < 1000; ++i) {
      set0.add(i);
    }
    assertEquals(1000, set0.size());
    assertFalse(set0.isEmpty());
    for (int i = 0; i < 1000; ++i) {
      assertTrue(set0.contains(i));
    }

    set0.clear();
    assertEquals(0, set0.size());
    assertTrue(set0.isEmpty());

  }

  @Test
  public void test2() {
    IntHashSet set0 = new IntHashSet();

    assertEquals(0, set0.size());
    assertTrue(set0.isEmpty());
    for (int i = 0; i < 1000; ++i) {
      set0.add(1);
      set0.add(-382);
    }
    assertEquals(2, set0.size());
    assertFalse(set0.isEmpty());
    set0.remove(-382);
    set0.remove(1);
    assertEquals(0, set0.size());
    assertTrue(set0.isEmpty());

  }

  @Test
  public void test3() {
    IntHashSet set0 = new IntHashSet();

    assertEquals(0, set0.size());
    assertTrue(set0.isEmpty());
    for (int i = 0; i < 1000; ++i) {
      set0.add(i);
    }

    for (int i = 0; i < 1000; i += 2) {
      set0.remove(i);
    }

    assertEquals(500, set0.size());
    for (int i = 0; i < 1000; ++i) {
      if (i % 2 == 0) {
        assertFalse(set0.contains(i));
      } else {
        assertTrue(set0.contains(i));
      }
    }

  }

  @Test
  public void test4() {
    IntHashSet set1 = new IntHashSet();
    HashSet<Integer> set2 = new HashSet<Integer>();
    for (int i = 0; i < ArrayHashMapTest.RANDOM_TEST_NUM_ITERATIONS; ++i) {
      int value = random().nextInt() % 500;
      boolean shouldAdd = random().nextBoolean();
      if (shouldAdd) {
        set1.add(value);
        set2.add(value);
      } else {
        set1.remove(value);
        set2.remove(value);
      }
    }
    assertEquals(set2.size(), set1.size());
    for (int value : set2) {
      assertTrue(set1.contains(value));
    }
  }

  @Test
  public void testRegularJavaSet() {
    HashSet<Integer> set = new HashSet<Integer>();
    for (int j = 0; j < 100; ++j) {
      for (int i = 0; i < ArrayHashMapTest.RANDOM_TEST_NUM_ITERATIONS; ++i) {
        int value = random().nextInt() % 5000;
        boolean shouldAdd = random().nextBoolean();
        if (shouldAdd) {
          set.add(value);
        } else {
          set.remove(value);
        }
      }
      set.clear();
    }
  }

  @Test
  public void testMySet() {
    IntHashSet set = new IntHashSet();
    for (int j = 0; j < 100; ++j) {
      for (int i = 0; i < ArrayHashMapTest.RANDOM_TEST_NUM_ITERATIONS; ++i) {
        int value = random().nextInt() % 5000;
        boolean shouldAdd = random().nextBoolean();
        if (shouldAdd) {
          set.add(value);
        } else {
          set.remove(value);
        }
      }
      set.clear();
    }
  }

  @Test
  public void testToArray() {
    IntHashSet set = new IntHashSet();
    for (int j = 0; j < 100; ++j) {
      for (int i = 0; i < ArrayHashMapTest.RANDOM_TEST_NUM_ITERATIONS; ++i) {
        int value = random().nextInt() % 5000;
        boolean shouldAdd = random().nextBoolean();
        if (shouldAdd) {
          set.add(value);
        } else {
          set.remove(value);
        }
      }
      int[] vals = set.toArray();
      assertEquals(set.size(), vals.length);

      int[] realValues = new int[set.size()];
      int[] unrealValues = set.toArray(realValues);
      assertEquals(realValues, unrealValues);
      for (int value : vals) {
        assertTrue(set.remove(value));
      }
      for (int i = 0; i < vals.length; ++i) {
        assertEquals(vals[i], realValues[i]);
      }
    }
  }

  @Test
  public void testZZRegularJavaSet() {
    HashSet<Integer> set = new HashSet<Integer>();
    for (int j = 0; j < 100; ++j) {
      for (int i = 0; i < ArrayHashMapTest.RANDOM_TEST_NUM_ITERATIONS; ++i) {
        int value = random().nextInt() % 5000;
        boolean shouldAdd = random().nextBoolean();
        if (shouldAdd) {
          set.add(value);
        } else {
          set.remove(value);
        }
      }
      set.clear();
    }
  }

  @Test
  public void testZZMySet() {
    IntHashSet set = new IntHashSet();
    for (int j = 0; j < 100; ++j) {
      for (int i = 0; i < ArrayHashMapTest.RANDOM_TEST_NUM_ITERATIONS; ++i) {
        int value = random().nextInt() % 5000;
        boolean shouldAdd = random().nextBoolean();
        if (shouldAdd) {
          set.add(value);
        } else {
          set.remove(value);
        }
      }
      set.clear();
    }
  }
}
