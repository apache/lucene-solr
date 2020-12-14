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
import java.util.List;
import java.util.Random;

import static org.apache.lucene.util.LongHeap.Order.MAX;
import static org.apache.lucene.util.LongHeap.Order.MIN;

public class TestLongHeap extends LuceneTestCase {

  private static class AssertingLongHeap extends LongHeap {
    public AssertingLongHeap(int count) {
      super(count);
    }

    @Override
    public boolean lessThan(long a, long b) {
      return (a < b);
    }

    protected final void checkValidity() {
      long[] heapArray = getHeapArray();
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
    LongHeap pq = LongHeap.create(MIN, count);
    long sum = 0, sum2 = 0;

    for (int i = 0; i < count; i++) {
      long next = gen.nextLong();
      sum += next;
      pq.push(next);
    }

    long last = Long.MIN_VALUE;
    for (long i = 0; i < count; i++) {
      long next = pq.pop();
      assertTrue(next >= last);
      last = next;
      sum2 += last;
    }

    assertEquals(sum, sum2);
  }

  public void testClear() {
    LongHeap pq = LongHeap.create(MIN, 3);
    pq.push(2);
    pq.push(3);
    pq.push(1);
    assertEquals(3, pq.size());
    pq.clear();
    assertEquals(0, pq.size());
  }

  public void testExceedBounds() {
    LongHeap pq = LongHeap.create(MIN, 1);
    pq.push(2);
    expectThrows(ArrayIndexOutOfBoundsException.class, () -> pq.push(0));
    assertEquals(2, pq.size()); // the heap is unusable at this point
  }

  public void testFixedSize() {
    LongHeap pq = LongHeap.create(MIN, 3);
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
    LongHeap pq = LongHeap.create(MAX, 3);
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
    LongHeap pq = LongHeap.create(MIN, 3);
    pq.push(2);
    pq.push(3);
    pq.push(1);
    assertEquals(1, pq.top());
    pq.updateTop(3);
    assertEquals(3, pq.size());
    assertArrayEquals(new long[]{0, 2, 3, 3}, pq.getHeapArray());
  }

  public void testInsertions() {
    Random random = random();
    int numDocsInPQ = TestUtil.nextInt(random, 1, 100);
    AssertingLongHeap pq = new AssertingLongHeap(numDocsInPQ);
    Long lastLeast = null;

    // Basic insertion of new content
    ArrayList<Long> sds = new ArrayList<Long>(numDocsInPQ);
    for (int i = 0; i < numDocsInPQ * 10; i++) {
      long newEntry = Math.abs(random.nextLong());
      sds.add(newEntry);
      pq.insertWithOverflow(newEntry);
      pq.checkValidity();
      long newLeast = pq.top();
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
    LongHeap queue = LongHeap.create(MIN, 3);
    LongHeap.LongIterator it = queue.iterator();
    assertFalse(it.hasNext());
    expectThrows(IllegalStateException.class, () -> {
      it.next();
    });
  }

  public void testIteratorOne() {
    LongHeap queue = LongHeap.create(MIN, 3);

    queue.push(1);
    LongHeap.LongIterator it = queue.iterator();
    assertTrue(it.hasNext());
    assertEquals(1, it.next());
    assertFalse(it.hasNext());
    expectThrows(IllegalStateException.class, () -> {
      it.next();
    });
  }

  public void testIteratorTwo() {
    LongHeap queue = LongHeap.create(MIN, 3);

    queue.push(1);
    queue.push(2);
    LongHeap.LongIterator it = queue.iterator();
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
    LongHeap.Order order;
    if (random().nextBoolean()) {
      order = MIN;
    } else {
      order = MAX;
    }
    final int maxSize = TestUtil.nextInt(random(), 1, 20);
    LongHeap queue = LongHeap.create(order, maxSize);
    final int iters = atLeast(100);
    final List<Long> expected = new ArrayList<>();
    for (int iter = 0; iter < iters; ++iter) {
      if (queue.size() == 0 || (queue.size() < maxSize && random().nextBoolean())) {
        final long value = random().nextInt(10);
        queue.push(value);
        expected.add(value);
      } else {
        expected.remove(Long.valueOf(queue.pop()));
      }
      List<Long> actual = new ArrayList<>();
      LongHeap.LongIterator it = queue.iterator();
      while (it.hasNext()) {
        actual.add(it.next());
      }
      CollectionUtil.introSort(expected);
      CollectionUtil.introSort(actual);
      assertEquals(expected, actual);
    }
  }

  public void testUnbounded() {
    LongHeap pq = LongHeap.create(MAX, -1);
    int num = random().nextInt(100) + 1;
    long maxValue = Long.MIN_VALUE;
    for (int i = 0; i < num; i++) {
      long value = random().nextLong();
      if (random().nextBoolean()) {
        pq.push(value);
      } else {
        pq.insertWithOverflow(value);
      }
      maxValue = Math.max(maxValue, value);
    }
    assertEquals(num, pq.size());
    assertEquals(maxValue, pq.top());
    long last = maxValue;
    int count = 0;
    while (pq.size() > 0) {
      long next = pq.pop();
      ++ count;
      assertTrue(next <= last);
      last = next;
    }
    assertEquals(num, count);
  }

}
