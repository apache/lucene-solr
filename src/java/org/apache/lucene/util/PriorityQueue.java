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

/** A PriorityQueue maintains a partial ordering of its elements such that the
  least element can always be found in constant time.  Put()'s and pop()'s
  require log(size) time. */
public abstract class PriorityQueue {
  private Object[] heap;
  private int size;

  /** Determines the ordering of objects in this priority queue.  Subclasses
    must define this one method. */
  abstract protected boolean lessThan(Object a, Object b);

  /** Subclass constructors must call this. */
  protected final void initialize(int maxSize) {
    size = 0;
    int heapSize = (maxSize * 2) + 1;
    heap = new Object[heapSize];
  }

  /** Adds an Object to a PriorityQueue in log(size) time. */ 
  public final void put(Object element) {
    size++;	
    heap[size] = element;
    upHeap();
  }

  /** Returns the least element of the PriorityQueue in constant time. */
  public final Object top() {
    if (size > 0)
      return heap[1];
    else
      return null;
  }

  /** Removes and returns the least element of the PriorityQueue in log(size)
    time. */ 
  public final Object pop() {
    if (size > 0) {
      Object result = heap[1];			  // save first value
      heap[1] = heap[size];			  // move last to first
      heap[size] = null;			  // permit GC of objects
      size--;
      downHeap();				  // adjust heap
      return result;
    } else
      return null;
  }

  /** Should be called when the Object at top changes values.  Still log(n)
   * worst case, but it's at least twice as fast to <pre>
   *  { pq.top().change(); pq.adjustTop(); }
   * </pre> instead of <pre>
   *  { o = pq.pop(); o.change(); pq.push(o); }
   * </pre>
   */
  public final void adjustTop() {
    downHeap();
  }
    

  /** Returns the number of elements currently stored in the PriorityQueue. */
  public final int size() {
    return size;
  }
  
  /** Removes all entries from the PriorityQueue. */
  public final void clear() {
    for (int i = 0; i < size; i++)
      heap[i] = null;
    size = 0;
  }

  private final void upHeap() {
    int i = size;
    Object node = heap[i];			  // save bottom node
    int j = i >>> 1;
    while (j > 0 && lessThan(node, heap[j])) {
      heap[i] = heap[j];			  // shift parents down
      i = j;
      j = j >>> 1;
    }
    heap[i] = node;				  // install saved node
  }
  
  private final void downHeap() {
    int i = 1;
    Object node = heap[i];			  // save top node
    int j = i << 1;				  // find smaller child
    int k = j + 1;
    if (k <= size && lessThan(heap[k], heap[j])) {
      j = k;
    }
    while (j <= size && lessThan(heap[j], node)) {
      heap[i] = heap[j];			  // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && lessThan(heap[k], heap[j])) {
	j = k;
      }
    }
    heap[i] = node;				  // install saved node
  }
}
