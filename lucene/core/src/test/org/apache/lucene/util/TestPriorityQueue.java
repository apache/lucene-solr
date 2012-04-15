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

import java.util.Random;

public class TestPriorityQueue extends LuceneTestCase {

    private static class IntegerQueue extends PriorityQueue<Integer> {
        public IntegerQueue(int count) {
            super(count);
        }

        @Override
        protected boolean lessThan(Integer a, Integer b) {
            return (a < b);
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
  
}
