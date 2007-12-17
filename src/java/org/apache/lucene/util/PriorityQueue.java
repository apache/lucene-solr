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

/** A PriorityQueue maintains a partial ordering of its elements such that the
  least element can always be found in constant time.  Put()'s and pop()'s
  require log(size) time. */
public abstract class PriorityQueue {
  private int size;
  private int maxSize;
  protected Object[] heap;

  /** Determines the ordering of objects in this priority queue.  Subclasses
    must define this one method. */
  protected abstract boolean lessThan(Object a, Object b);

  /** Subclass constructors must call this. */
  protected final void initialize(int maxSize) {
    size = 0;
    int heapSize;
    if (0 == maxSize)
      // We allocate 1 extra to avoid if statement in top()
      heapSize = 2;
    else
      heapSize = maxSize + 1;
    heap = new Object[heapSize];
    this.maxSize = maxSize;
  }

  /**
   * Adds an Object to a PriorityQueue in log(size) time.
   * If one tries to add more objects than maxSize from initialize
   * a RuntimeException (ArrayIndexOutOfBound) is thrown.
   */
  public final void put(Object element) {
    size++;
    heap[size] = element;
    upHeap();
  }

  /**
   * Adds element to the PriorityQueue in log(size) time if either
   * the PriorityQueue is not full, or not lessThan(element, top()).
   * @param element
   * @return true if element is added, false otherwise.
   */
  public boolean insert(Object element) {
    return insertWithOverflow(element) != element;
  }

  /**
   * insertWithOverflow() is the same as insert() except its
   * return value: it returns the object (if any) that was
   * dropped off the heap because it was full. This can be
   * the given parameter (in case it is smaller than the
   * full heap's minimum, and couldn't be added), or another
   * object that was previously the smallest value in the
   * heap and now has been replaced by a larger one, or null
   * if the queue wasn't yet full with maxSize elements.
   */
  public Object insertWithOverflow(Object element) {
    if (size < maxSize) {
      put(element);
      return null;
    } else if (size > 0 && !lessThan(element, heap[1])) {
      Object ret = heap[1];
      heap[1] = element;
      adjustTop();
      return ret;
    } else {
      return element;
    }
  }

  /** Returns the least element of the PriorityQueue in constant time. */
  public final Object top() {
    // We don't need to check size here: if maxSize is 0,
    // then heap is length 2 array with both entries null.
    // If size is 0 then heap[1] is already null.
    return heap[1];
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
    for (int i = 0; i <= size; i++)
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
