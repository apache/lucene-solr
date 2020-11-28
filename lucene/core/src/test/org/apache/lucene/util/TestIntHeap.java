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


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.apache.lucene.util.IntHeap.Order.MAX;
import static org.apache.lucene.util.IntHeap.Order.MIN;

public class TestIntHeap extends LuceneTestCase {

  private static class AssertingIntHeap extends IntHeap {
    public AssertingIntHeap(int count) {
      super(count);
    }

    @Override
    protected boolean lessThan(int a, int b) {
      return (a < b);
    }

    protected final void checkValidity() {
      int[] heapArray = getHeapArray();
      for (int i = 1; i <= size(); i++) {
        int parent = i >>> 1;
        if (parent > 1) {
          if (lessThan(heapArray[parent], heapArray[i]) == false) {
            assertEquals(heapArray[parent], heapArray[i]);
          }
        }
      }
    }
  }

  public void testPQ() {
    testPQ(atLeast(10000), random());
  }

  public static void testPQ(int count, Random gen) {
    IntHeap pq = IntHeap.create(MIN, count);
    int sum = 0, sum2 = 0;

    for (int i = 0; i < count; i++) {
      int next = gen.nextInt();
      sum += next;
      pq.push(next);
    }

    int last = Integer.MIN_VALUE;
    for (int i = 0; i < count; i++) {
      Integer next = pq.pop();
      assertTrue(next.intValue() >= last);
      last = next.intValue();
      sum2 += last;
    }

    assertEquals(sum, sum2);
  }

  public void testClear() {
    IntHeap pq = IntHeap.create(MIN, 3);
    pq.push(2);
    pq.push(3);
    pq.push(1);
    assertEquals(3, pq.size());
    pq.clear();
    assertEquals(0, pq.size());
  }

  public void testExceedBounds() {
    IntHeap pq = IntHeap.create(MIN, 1);
    pq.push(2);
    expectThrows(ArrayIndexOutOfBoundsException.class, () -> pq.push(0));
    assertEquals(2, pq.size()); // the heap is unusable at this point
  }

  public void testFixedSize() {
    IntHeap pq = IntHeap.create(MIN, 3);
    pq.insertWithOverflow(2);
    pq.insertWithOverflow(3);
    pq.insertWithOverflow(1);
    pq.insertWithOverflow(5);
    pq.insertWithOverflow(7);
    pq.insertWithOverflow(1);
    assertEquals(3, pq.size());
    assertEquals(3, pq.top());
  }

  public void testFixedSizeMax() {
    IntHeap pq = IntHeap.create(MAX, 3);
    pq.insertWithOverflow(2);
    pq.insertWithOverflow(3);
    pq.insertWithOverflow(1);
    pq.insertWithOverflow(5);
    pq.insertWithOverflow(7);
    pq.insertWithOverflow(1);
    assertEquals(3, pq.size());
    assertEquals(2, pq.top());
  }

  public void testDuplicateValues() {
    IntHeap pq = IntHeap.create(MIN, 3);
    pq.push(2);
    pq.push(3);
    pq.push(1);
    assertEquals(1, pq.top());
    pq.updateTop(3);
    assertEquals(3, pq.size());
    assertArrayEquals(new int[]{0, 2, 3, 3}, pq.getHeapArray());
  }

  public void testInsertions() {
    Random random = random();
    int numDocsInPQ = TestUtil.nextInt(random, 1, 100);
    AssertingIntHeap pq = new AssertingIntHeap(numDocsInPQ);
    Integer lastLeast = null;

    // Basic insertion of new content
    ArrayList<Integer> sds = new ArrayList<Integer>(numDocsInPQ);
    for (int i = 0; i < numDocsInPQ * 10; i++) {
      Integer newEntry = Math.abs(random.nextInt());
      sds.add(newEntry);
      pq.insertWithOverflow(newEntry);
      pq.checkValidity();
      int newLeast = pq.top();
      if ((lastLeast != null) && (newLeast != newEntry)
          && (newLeast != lastLeast)) {
        // If there has been a change of least entry and it wasn't our new
        // addition we expect the scores to increase
        assertTrue(newLeast <= newEntry);
        assertTrue(newLeast >= lastLeast);
      }
      lastLeast = newLeast;
    }
  }

  public void testIteratorEmpty() {
    IntHeap queue = IntHeap.create(MIN, 3);
    IntHeap.IntIterator it = queue.iterator();
    assertFalse(it.hasNext());
    expectThrows(IllegalStateException.class, () -> {
      it.next();
    });
  }

  public void testIteratorOne() {
    IntHeap queue = IntHeap.create(MIN, 3);

    queue.push(1);
    IntHeap.IntIterator it = queue.iterator();
    assertTrue(it.hasNext());
    assertEquals(1, it.next());
    assertFalse(it.hasNext());
    expectThrows(IllegalStateException.class, () -> {
      it.next();
    });
  }

  public void testIteratorTwo() {
    IntHeap queue = IntHeap.create(MIN, 3);

    queue.push(1);
    queue.push(2);
    IntHeap.IntIterator it = queue.iterator();
    assertTrue(it.hasNext());
    assertEquals(1, it.next());
    assertTrue(it.hasNext());
    assertEquals(2, it.next());
    assertFalse(it.hasNext());
    expectThrows(IllegalStateException.class, () -> {
      it.next();
    });
  }

  public void testIteratorRandom() {
    IntHeap.Order order;
    if (random().nextBoolean()) {
      order = MIN;
    } else {
      order = MAX;
    }
    final int maxSize = TestUtil.nextInt(random(), 1, 20);
    IntHeap queue = IntHeap.create(order, maxSize);
    final int iters = atLeast(100);
    final List<Integer> expected = new ArrayList<>();
    for (int iter = 0; iter < iters; ++iter) {
      if (queue.size() == 0 || (queue.size() < maxSize && random().nextBoolean())) {
        final Integer value = random().nextInt(10);
        queue.push(value);
        expected.add(value);
      } else {
        expected.remove(Integer.valueOf(queue.pop()));
      }
      List<Integer> actual = new ArrayList<>();
      IntHeap.IntIterator it = queue.iterator();
      while (it.hasNext()) {
        actual.add(it.next());
      }
      CollectionUtil.introSort(expected);
      CollectionUtil.introSort(actual);
      assertEquals(expected, actual);
    }
  }

  public void testMaxIntSize() {
    expectThrows(IllegalArgumentException.class, () -> IntHeap.create(MIN, Integer.MAX_VALUE));
    expectThrows(IllegalArgumentException.class, () -> IntHeap.create(MIN, -2));
  }

  public void testUnbounded() {
    IntHeap pq = IntHeap.create(MAX, IntHeap.UNBOUNDED);
    int num = random().nextInt(100) + 1;
    int maxValue = Integer.MIN_VALUE;
    for (int i = 0; i < num; i++) {
      int value = random().nextInt();
      if (random().nextBoolean()) {
        pq.push(value);
      } else {
        pq.insertWithOverflow(value);
      }
      maxValue = Math.max(maxValue, value);
    }
    assertEquals(num, pq.size());
    assertEquals(maxValue, pq.top());
    int last = maxValue;
    int count = 0;
    while (pq.size() > 0) {
      int next = pq.pop();
      ++ count;
      assertTrue(next <= last);
      last = next;
    }
    assertEquals(num, count);
  }

}
