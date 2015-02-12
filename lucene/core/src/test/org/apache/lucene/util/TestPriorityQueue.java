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

import java.util.ArrayList;
import java.util.Random;

public class TestPriorityQueue extends LuceneTestCase {

  private static class IntegerQueue extends PriorityQueue<Integer> {
    public IntegerQueue(int count) {
      super(count);
    }

    @Override
    protected boolean lessThan(Integer a, Integer b) {
      if (a.equals(b)) {
        assert (a != b);
        int hashA = System.identityHashCode(a);
        int hashB = System.identityHashCode(b);
        assert (hashA != hashB);
        return hashA < hashB;
      }
      return (a < b);
    }

    protected final void checkValidity() {
      Object[] heapArray = getHeapArray();
      for (int i = 1; i <= size(); i++) {
        int parent = i >>> 1;
        if (parent > 1) {
          assertTrue(lessThan((Integer) heapArray[parent],
              (Integer) heapArray[i]));
        }
      }
    }

  }

    public void testPQ() throws Exception {
        testPQ(atLeast(10000), random());
    }

    public static void testPQ(int count, Random gen) {
        PriorityQueue<Integer> pq = new IntegerQueue(count);
        int sum = 0, sum2 = 0;

        for (int i = 0; i < count; i++)
        {
            int next = gen.nextInt();
            sum += next;
            pq.add(next);
        }

        //      Date end = new Date();

        //      System.out.print(((float)(end.getTime()-start.getTime()) / count) * 1000);
        //      System.out.println(" microseconds/put");

        //      start = new Date();

        int last = Integer.MIN_VALUE;
        for (int i = 0; i < count; i++)
        {
            Integer next = pq.pop();
            assertTrue(next.intValue() >= last);
            last = next.intValue();
            sum2 += last;
        }

        assertEquals(sum, sum2);
        //      end = new Date();

        //      System.out.print(((float)(end.getTime()-start.getTime()) / count) * 1000);
        //      System.out.println(" microseconds/pop");
    }

    public void testClear() {
        PriorityQueue<Integer> pq = new IntegerQueue(3);
        pq.add(2);
        pq.add(3);
        pq.add(1);
        assertEquals(3, pq.size());
        pq.clear();
        assertEquals(0, pq.size());
    }
    
    public void testFixedSize() {
        PriorityQueue<Integer> pq = new IntegerQueue(3);
        pq.insertWithOverflow(2);
        pq.insertWithOverflow(3);
        pq.insertWithOverflow(1);
        pq.insertWithOverflow(5);
        pq.insertWithOverflow(7);
        pq.insertWithOverflow(1);
        assertEquals(3, pq.size());
        assertEquals((Integer) 3, pq.top());
    }
    
    public void testInsertWithOverflow() {
      int size = 4;
      PriorityQueue<Integer> pq = new IntegerQueue(size);
      Integer i1 = 2;
      Integer i2 = 3;
      Integer i3 = 1;
      Integer i4 = 5;
      Integer i5 = 7;
      Integer i6 = 1;
      
      assertNull(pq.insertWithOverflow(i1));
      assertNull(pq.insertWithOverflow(i2));
      assertNull(pq.insertWithOverflow(i3));
      assertNull(pq.insertWithOverflow(i4));
      assertTrue(pq.insertWithOverflow(i5) == i3); // i3 should have been dropped
      assertTrue(pq.insertWithOverflow(i6) == i6); // i6 should not have been inserted
      assertEquals(size, pq.size());
      assertEquals((Integer) 2, pq.top());
    }

  public void testRemovalsAndInsertions() {
    Random random = random();
    int numDocsInPQ = TestUtil.nextInt(random, 1, 100);
    IntegerQueue pq = new IntegerQueue(numDocsInPQ);
    Integer lastLeast = null;

    // Basic insertion of new content
    ArrayList<Integer> sds = new ArrayList<Integer>(numDocsInPQ);
    for (int i = 0; i < numDocsInPQ * 10; i++) {
      Integer newEntry = new Integer(Math.abs(random.nextInt()));
      sds.add(newEntry);
      Integer evicted = pq.insertWithOverflow(newEntry);
      pq.checkValidity();
      if (evicted != null) {
        assertTrue(sds.remove(evicted));
        if (evicted != newEntry) {
          assertTrue(evicted == lastLeast);
        }
      }
      Integer newLeast = pq.top();
      if ((lastLeast != null) && (newLeast != newEntry)
          && (newLeast != lastLeast)) {
        // If there has been a change of least entry and it wasn't our new
        // addition we expect the scores to increase
        assertTrue(newLeast <= newEntry);
        assertTrue(newLeast >= lastLeast);
      }
      lastLeast = newLeast;

    }

    // Try many random additions to existing entries - we should always see
    // increasing scores in the lowest entry in the PQ
    for (int p = 0; p < 500000; p++) {
      int element = (int) (random.nextFloat() * (sds.size() - 1));
      Integer objectToRemove = sds.get(element);
      assertTrue(sds.remove(element) == objectToRemove);
      assertTrue(pq.remove(objectToRemove));
      pq.checkValidity();
      Integer newEntry = new Integer(Math.abs(random.nextInt()));
      sds.add(newEntry);
      assertNull(pq.insertWithOverflow(newEntry));
      pq.checkValidity();
      Integer newLeast = pq.top();
      if ((objectToRemove != lastLeast) && (lastLeast != null)
          && (newLeast != newEntry)) {
        // If there has been a change of least entry and it wasn't our new
        // addition or the loss of our randomly removed entry we expect the
        // scores to increase
        assertTrue(newLeast <= newEntry);
        assertTrue(newLeast >= lastLeast);
      }
      lastLeast = newLeast;
    }
  }

}
