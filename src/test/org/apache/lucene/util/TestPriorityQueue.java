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

public class TestPriorityQueue
    extends LuceneTestCase
{
    public TestPriorityQueue(String name)
    {
	super(name);
    }

    private static class IntegerQueue
	extends PriorityQueue
    {
	public IntegerQueue(int count)
	{
	    super();
	    initialize(count);
	}

	protected boolean lessThan(Object a, Object b)
	{
	    return ((Integer) a).intValue() < ((Integer) b).intValue();
	}
    }

    public void testPQ()
	throws Exception
    {
	testPQ(10000);
    }

    public static void testPQ(int count)
    {
	PriorityQueue pq = new IntegerQueue(count);
	Random gen = new Random();
	int sum = 0, sum2 = 0;

	for (int i = 0; i < count; i++)
	{
	    int next = gen.nextInt();
	    sum += next;
	    pq.put(new Integer(next));
	}

	//      Date end = new Date();

	//      System.out.print(((float)(end.getTime()-start.getTime()) / count) * 1000);
	//      System.out.println(" microseconds/put");

	//      start = new Date();

	int last = Integer.MIN_VALUE;
	for (int i = 0; i < count; i++)
	{
	    Integer next = (Integer)pq.pop();
	    assertTrue(next.intValue() >= last);
	    last = next.intValue();
	    sum2 += last;
	}

	assertEquals(sum, sum2);
	//      end = new Date();

	//      System.out.print(((float)(end.getTime()-start.getTime()) / count) * 1000);
	//      System.out.println(" microseconds/pop");
    }

    public void testClear()
    {
	PriorityQueue pq = new IntegerQueue(3);
	pq.put(new Integer(2));
	pq.put(new Integer(3));
	pq.put(new Integer(1));
	assertEquals(3, pq.size());
	pq.clear();
	assertEquals(0, pq.size());
    }
    
    public void testFixedSize(){
        PriorityQueue pq = new IntegerQueue(3);
        pq.insert(new Integer(2));
        pq.insert(new Integer(3));
        pq.insert(new Integer(1));
        pq.insert(new Integer(5));
        pq.insert(new Integer(7));
        pq.insert(new Integer(1));
        assertEquals(3, pq.size());
        assertEquals(3, ((Integer) pq.top()).intValue());
    }
    
  public void testInsertWithOverflow() {
    int size = 4;
    PriorityQueue pq = new IntegerQueue(size);
    Integer i1 = new Integer(2);
    Integer i2 = new Integer(3);
    Integer i3 = new Integer(1);
    Integer i4 = new Integer(5);
    Integer i5 = new Integer(7);
    Integer i6 = new Integer(1);
    
    assertNull(pq.insertWithOverflow(i1));
    assertNull(pq.insertWithOverflow(i2));
    assertNull(pq.insertWithOverflow(i3));
    assertNull(pq.insertWithOverflow(i4));
    assertTrue(pq.insertWithOverflow(i5) == i3); // i3 should have been dropped
    assertTrue(pq.insertWithOverflow(i6) == i6); // i6 should not have been inserted
    assertEquals(size, pq.size());
    assertEquals(2, ((Integer) pq.top()).intValue());
  }
  
}
