package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.util.PriorityQueue;


/**
 * Describe class <code>MultipleTermPositions</code> here.
 *
 * @author Anders Nielsen
 * @version 1.0
 */
public class MultipleTermPositions
    implements TermPositions
{
    private static final class TermPositionsQueue
	extends PriorityQueue
    {
	TermPositionsQueue(List termPositions)
	    throws IOException
	{
	    initialize(termPositions.size());

	    Iterator i = termPositions.iterator();
	    while (i.hasNext())
	    {
		TermPositions tp = (TermPositions)i.next();
		if (tp.next())
		    put(tp);
	    }
	}

	final TermPositions peek()
	{
	    return (TermPositions)top();
	}

	public final boolean lessThan(Object a, Object b)
	{
	    return ((TermPositions)a).doc() < ((TermPositions)b).doc();
	}
    }

    private static final class IntQueue
    {
	private int _arraySize = 16;

	private int _index = 0;
	private int _lastIndex = 0;

	private int[] _array = new int[_arraySize];

	final void add(int i)
	{
	    if (_lastIndex == _arraySize)
		growArray();

	    _array[_lastIndex++] = i;
	}

	final int next()
	{
	    return _array[_index++];
	}

	final void sort()
	{
	    Arrays.sort(_array, _index, _lastIndex);
	}

	final void clear()
	{
	    _index = 0;
	    _lastIndex = 0;
	}

	final int size()
	{
	    return (_lastIndex-_index);
	}

	private void growArray()
	{
	    int[] newArray = new int[_arraySize*2];
	    System.arraycopy(_array, 0, newArray, 0, _arraySize);
	    _array = newArray;
	    _arraySize *= 2;
	}
    }

    private int _doc;
    private int _freq;

    private TermPositionsQueue _termPositionsQueue;
    private IntQueue _posList;

    /**
     * Creates a new <code>MultipleTermPositions</code> instance.
     *
     * @param indexReader an <code>IndexReader</code> value
     * @param terms a <code>Term[]</code> value
     * @exception IOException if an error occurs
     */
    public MultipleTermPositions(IndexReader indexReader, Term[] terms)
	throws IOException
    {
	List termPositions = new LinkedList();

	for (int i=0; i<terms.length; i++)
	    termPositions.add(indexReader.termPositions(terms[i]));

	_termPositionsQueue = new TermPositionsQueue(termPositions);
	_posList = new IntQueue();
    }

    /**
     * Describe <code>next</code> method here.
     *
     * @return a <code>boolean</code> value
     * @exception IOException if an error occurs
     * @see TermDocs#next()
     */
    public final boolean next()
	throws IOException
    {
	if (_termPositionsQueue.size() == 0)
	    return false;

	_posList.clear();
	_doc = _termPositionsQueue.peek().doc();

	TermPositions tp;
	do
	{
	    tp = _termPositionsQueue.peek();

	    for (int i=0; i<tp.freq(); i++)
		_posList.add(tp.nextPosition());

	    if (tp.next())
		_termPositionsQueue.adjustTop();
	    else
	    {
		_termPositionsQueue.pop();
		tp.close();
	    }
	}
	while (_termPositionsQueue.size() > 0 && _termPositionsQueue.peek().doc() == _doc);

	_posList.sort();
	_freq = _posList.size();

	return true;
    }

    /**
     * Describe <code>nextPosition</code> method here.
     *
     * @return an <code>int</code> value
     * @exception IOException if an error occurs
     * @see TermPositions#nextPosition()
     */
    public final int nextPosition()
	throws IOException
    {
	return _posList.next();
    }

    /**
     * Describe <code>skipTo</code> method here.
     *
     * @param target an <code>int</code> value
     * @return a <code>boolean</code> value
     * @exception IOException if an error occurs
     * @see TermDocs#skipTo(int)
     */
    public final boolean skipTo(int target)
	throws IOException
    {
	while (target > _termPositionsQueue.peek().doc())
	{
	    TermPositions tp = (TermPositions)_termPositionsQueue.pop();

	    if (tp.skipTo(target))
		_termPositionsQueue.put(tp);
	    else
		tp.close();
	}

	return next();
    }

    /**
     * Describe <code>doc</code> method here.
     *
     * @return an <code>int</code> value
     * @see TermDocs#doc()
     */
    public final int doc()
    {
	return _doc;
    }

    /**
     * Describe <code>freq</code> method here.
     *
     * @return an <code>int</code> value
     * @see TermDocs#freq()
     */
    public final int freq()
    {
	return _freq;
    }

    /**
     * Describe <code>close</code> method here.
     *
     * @exception IOException if an error occurs
     * @see TermDocs#close()
     */
    public final void close()
	throws IOException
    {
	while (_termPositionsQueue.size() > 0)
	    ((TermPositions)_termPositionsQueue.pop()).close();
    }

    /**
     * Describe <code>seek</code> method here.
     *
     * @param arg0 a <code>Term</code> value
     * @exception IOException if an error occurs
     * @see TermDocs#seek(Term)
     */
    public void seek(Term arg0)
	throws IOException
    {
	throw new UnsupportedOperationException();
    }

    /**
     * Describe <code>read</code> method here.
     *
     * @param arg0 an <code>int[]</code> value
     * @param arg1 an <code>int[]</code> value
     * @return an <code>int</code> value
     * @exception IOException if an error occurs
     * @see TermDocs#read(int[], int[])
     */
    public int read(int[] arg0, int[] arg1)
	throws IOException
    {
	throw new UnsupportedOperationException();
    }
}
