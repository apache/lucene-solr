package org.apache.lucene.util;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.util.Date;
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

	Date start = new Date();

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
}
