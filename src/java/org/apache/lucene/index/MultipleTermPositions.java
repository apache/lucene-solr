package org.apache.lucene.index;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    public void seek(TermEnum termEnum) throws IOException {
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
