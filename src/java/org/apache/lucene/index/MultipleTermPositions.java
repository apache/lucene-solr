package org.apache.lucene.index;

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

import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Allows you to iterate over the {@link TermPositions} for multiple {@link Term}s as
 * a single {@link TermPositions}.
 *
 */
public class MultipleTermPositions implements TermPositions {

  private static final class TermPositionsQueue extends PriorityQueue {
    TermPositionsQueue(List termPositions) throws IOException {
      initialize(termPositions.size());

      Iterator i = termPositions.iterator();
      while (i.hasNext()) {
        TermPositions tp = (TermPositions) i.next();
        if (tp.next())
          put(tp);
      }
    }

    final TermPositions peek() {
      return (TermPositions) top();
    }

    public final boolean lessThan(Object a, Object b) {
      return ((TermPositions) a).doc() < ((TermPositions) b).doc();
    }
  }

  private static final class IntQueue {
    private int _arraySize = 16;
    private int _index = 0;
    private int _lastIndex = 0;
    private int[] _array = new int[_arraySize];

    final void add(int i) {
      if (_lastIndex == _arraySize)
        growArray();

      _array[_lastIndex++] = i;
    }

    final int next() {
      return _array[_index++];
    }

    final void sort() {
      Arrays.sort(_array, _index, _lastIndex);
    }

    final void clear() {
      _index = 0;
      _lastIndex = 0;
    }

    final int size() {
      return (_lastIndex - _index);
    }

    private void growArray() {
      int[] newArray = new int[_arraySize * 2];
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
   * @exception IOException
   */
  public MultipleTermPositions(IndexReader indexReader, Term[] terms) throws IOException {
    List termPositions = new LinkedList();

    for (int i = 0; i < terms.length; i++)
      termPositions.add(indexReader.termPositions(terms[i]));

    _termPositionsQueue = new TermPositionsQueue(termPositions);
    _posList = new IntQueue();
  }

  public final boolean next() throws IOException {
    if (_termPositionsQueue.size() == 0)
      return false;

    _posList.clear();
    _doc = _termPositionsQueue.peek().doc();

    TermPositions tp;
    do {
      tp = _termPositionsQueue.peek();

      for (int i = 0; i < tp.freq(); i++)
        _posList.add(tp.nextPosition());

      if (tp.next())
        _termPositionsQueue.adjustTop();
      else {
        _termPositionsQueue.pop();
        tp.close();
      }
    } while (_termPositionsQueue.size() > 0 && _termPositionsQueue.peek().doc() == _doc);

    _posList.sort();
    _freq = _posList.size();

    return true;
  }

  public final int nextPosition() {
    return _posList.next();
  }

  public final boolean skipTo(int target) throws IOException {
    while (_termPositionsQueue.peek() != null && target > _termPositionsQueue.peek().doc()) {
      TermPositions tp = (TermPositions) _termPositionsQueue.pop();
      if (tp.skipTo(target))
        _termPositionsQueue.put(tp);
      else
        tp.close();
    }
    return next();
  }

  public final int doc() {
    return _doc;
  }

  public final int freq() {
    return _freq;
  }

  public final void close() throws IOException {
    while (_termPositionsQueue.size() > 0)
      ((TermPositions) _termPositionsQueue.pop()).close();
  }

  /**
   * Not implemented.
   * @throws UnsupportedOperationException
   */
  public void seek(Term arg0) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Not implemented.
   * @throws UnsupportedOperationException
   */
  public void seek(TermEnum termEnum) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Not implemented.
   * @throws UnsupportedOperationException
   */
  public int read(int[] arg0, int[] arg1) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  
  /**
   * Not implemented.
   * @throws UnsupportedOperationException
   */
  public int getPayloadLength() {
    throw new UnsupportedOperationException();
  }
   
  /**
   * Not implemented.
   * @throws UnsupportedOperationException
   */
  public byte[] getPayload(byte[] data, int offset) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   *
   * @return false
   */
  // TODO: Remove warning after API has been finalized
  public boolean isPayloadAvailable() {
    return false;
  }
}
