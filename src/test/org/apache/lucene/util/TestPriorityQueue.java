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
import junit.framework.TestCase;

public class TestPriorityQueue
    extends TestCase
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
}
