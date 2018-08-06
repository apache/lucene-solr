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
package org.apache.lucene.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;


/**
 * A PriorityQueue maintains a partial ordering of its elements such that the
 * least element can always be found in constant time. Put()'s and pop()'s
 * require log(size) time but the remove() cost implemented here is linear.
 *
 * <p>
 * <b>NOTE</b>: This class pre-allocates an array of length {@code maxSize+1}
 * and pre-fills it with elements if instantiated via the
 * {@link #PriorityQueue(int,Supplier)} constructor.
 *
 * <b>NOTE</b>: Iteration order is not specified.
 *
 * @lucene.internal
 */
public abstract class PriorityQueue<T> implements Iterable<T> {
  private int size = 0;
  private final int maxSize;
  private final T[] heap;

  /**
   * Create an empty priority queue of the configured size.
   */
  public PriorityQueue(int maxSize) {
    this(maxSize, () -> null);
  }

  /**
   * Create a priority queue that is pre-filled with sentinel objects, so that
   * the code which uses that queue can always assume it's full and only change
   * the top without attempting to insert any new object.<br>
   *
   * Those sentinel values should always compare worse than any non-sentinel
   * value (i.e., {@link #lessThan} should always favor the
   * non-sentinel values).<br>
   *
   * By default, the supplier returns null, which means the queue will not be
   * filled with sentinel values. Otherwise, the value returned will be used to
   * pre-populate the queue.<br>
   *
   * If this method is extended to return a non-null value, then the following
   * usage pattern is recommended:
   *
   * <pre class="prettyprint">
   * PriorityQueue&lt;MyObject&gt; pq = new MyQueue&lt;MyObject&gt;(numHits);
   * // save the 'top' element, which is guaranteed to not be null.
   * MyObject pqTop = pq.top();
   * &lt;...&gt;
   * // now in order to add a new element, which is 'better' than top (after
   * // you've verified it is better), it is as simple as:
   * pqTop.change().
   * pqTop = pq.updateTop();
   * </pre>
   *
   * <b>NOTE:</b> the given supplier will be called {@code maxSize} times,
   * relying on a new object to be returned and will not check if it's null again.
   * Therefore you should ensure any call to this method creates a new instance and
   * behaves consistently, e.g., it cannot return null if it previously returned
   * non-null and all returned instances must {@link #lessThan compare equal}.
   */
  public PriorityQueue(int maxSize, Supplier<T> sentinelObjectSupplier) {
    final int heapSize;
    if (0 == maxSize) {
      // We allocate 1 extra to avoid if statement in top()
      heapSize = 2;
    } else {

      if ((maxSize < 0) || (maxSize >= ArrayUtil.MAX_ARRAY_LENGTH)) {
        // Throw exception to prevent confusing OOME:
        throw new IllegalArgumentException("maxSize must be >= 0 and < " + (ArrayUtil.MAX_ARRAY_LENGTH) + "; got: " + maxSize);
      }

      // NOTE: we add +1 because all access to heap is
      // 1-based not 0-based.  heap[0] is unused.
      heapSize = maxSize + 1;
    }
    // T is unbounded type, so this unchecked cast works always:
    @SuppressWarnings("unchecked") final T[] h = (T[]) new Object[heapSize];
    this.heap = h;
    this.maxSize = maxSize;

    // If sentinel objects are supported, populate the queue with them
    T sentinel = sentinelObjectSupplier.get();
    if (sentinel != null) {
      heap[1] = sentinel;
      for (int i = 2; i < heap.length; i++) {
        heap[i] = sentinelObjectSupplier.get();
      }
      size = maxSize;
    }
  }

  /** Determines the ordering of objects in this priority queue.  Subclasses
   *  must define this one method.
   *  @return <code>true</code> iff parameter <tt>a</tt> is less than parameter <tt>b</tt>.
   */
  protected abstract boolean lessThan(T a, T b);

  /**
   * Adds an Object to a PriorityQueue in log(size) time. If one tries to add
   * more objects than maxSize from initialize an
   * {@link ArrayIndexOutOfBoundsException} is thrown.
   *
   * @return the new 'top' element in the queue.
   */
  public final T add(T element) {
    size++;
    heap[size] = element;
    upHeap(size);
    return heap[1];
  }

  /**
   * Adds an Object to a PriorityQueue in log(size) time.
   * It returns the object (if any) that was
   * dropped off the heap because it was full. This can be
   * the given parameter (in case it is smaller than the
   * full heap's minimum, and couldn't be added), or another
   * object that was previously the smallest value in the
   * heap and now has been replaced by a larger one, or null
   * if the queue wasn't yet full with maxSize elements.
   */
  public T insertWithOverflow(T element) {
    if (size < maxSize) {
      add(element);
      return null;
    } else if (size > 0 && !lessThan(element, heap[1])) {
      T ret = heap[1];
      heap[1] = element;
      updateTop();
      return ret;
    } else {
      return element;
    }
  }

  /** Returns the least element of the PriorityQueue in constant time. */
  public final T top() {
    // We don't need to check size here: if maxSize is 0,
    // then heap is length 2 array with both entries null.
    // If size is 0 then heap[1] is already null.
    return heap[1];
  }

  /** Removes and returns the least element of the PriorityQueue in log(size)
    time. */
  public final T pop() {
    if (size > 0) {
      T result = heap[1];       // save first value
      heap[1] = heap[size];     // move last to first
      heap[size] = null;        // permit GC of objects
      size--;
      downHeap(1);              // adjust heap
      return result;
    } else {
      return null;
    }
  }

  /**
   * Should be called when the Object at top changes values. Still log(n) worst
   * case, but it's at least twice as fast to
   *
   * <pre class="prettyprint">
   * pq.top().change();
   * pq.updateTop();
   * </pre>
   *
   * instead of
   *
   * <pre class="prettyprint">
   * o = pq.pop();
   * o.change();
   * pq.push(o);
   * </pre>
   *
   * @return the new 'top' element.
   */
  public final T updateTop() {
    downHeap(1);
    return heap[1];
  }

  /**
   * Replace the top of the pq with {@code newTop} and run {@link #updateTop()}.
   */
  public final T updateTop(T newTop) {
    heap[1] = newTop;
    return updateTop();
  }

  /** Returns the number of elements currently stored in the PriorityQueue. */
  public final int size() {
    return size;
  }

  /** Removes all entries from the PriorityQueue. */
  public final void clear() {
    for (int i = 0; i <= size; i++) {
      heap[i] = null;
    }
    size = 0;
  }

  /**
   * Removes an existing element currently stored in the PriorityQueue. Cost is
   * linear with the size of the queue. (A specialization of PriorityQueue which
   * tracks element positions would provide a constant remove time but the
   * trade-off would be extra cost to all additions/insertions)
   */
  public final boolean remove(T element) {
    for (int i = 1; i <= size; i++) {
      if (heap[i] == element) {
        heap[i] = heap[size];
        heap[size] = null; // permit GC of objects
        size--;
        if (i <= size) {
          if (!upHeap(i)) {
            downHeap(i);
          }
        }
        return true;
      }
    }
    return false;
  }

  private final boolean upHeap(int origPos) {
    int i = origPos;
    T node = heap[i];          // save bottom node
    int j = i >>> 1;
    while (j > 0 && lessThan(node, heap[j])) {
      heap[i] = heap[j];       // shift parents down
      i = j;
      j = j >>> 1;
    }
    heap[i] = node;            // install saved node
    return i != origPos;
  }

  private final void downHeap(int i) {
    T node = heap[i];          // save top node
    int j = i << 1;            // find smaller child
    int k = j + 1;
    if (k <= size && lessThan(heap[k], heap[j])) {
      j = k;
    }
    while (j <= size && lessThan(heap[j], node)) {
      heap[i] = heap[j];       // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && lessThan(heap[k], heap[j])) {
        j = k;
      }
    }
    heap[i] = node;            // install saved node
  }

  /** This method returns the internal heap array as Object[].
   * @lucene.internal
   */
  protected final Object[] getHeapArray() {
    return (Object[]) heap;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {

      int i = 1;

      @Override
      public boolean hasNext() {
        return i <= size;
      }

      @Override
      public T next() {
        if (hasNext() == false) {
          throw new NoSuchElementException();
        }
        return heap[i++];
      }

    };
  }
}
