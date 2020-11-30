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

/**
 * A heap that stores ints; a primitive priority queue that like all priority queues maintains a
 * partial ordering of its elements such that the least element can always be found in constant
 * time. Put()'s and pop()'s require log(size). This heap may be bounded by constructing with a
 * finite maxSize, or enabled to grow dynamically by passing the constant UNBOUNDED for the maxSize.
 * The heap may be either a min heap, in which case the least element is the smallest integer, or a
 * max heap, when it is the largest, depending on the Order parameter.
 *
 * <b>NOTE</b>: Iteration order is not specified.
 *
 * @lucene.internal
 */
public abstract class IntHeap {

  /**
   * Used to specify the ordering of the heap. A min-heap provides access to the minimum element in
   * constant time, and when bounded, retains the maximum <code>maxSize</code> elements. A max-heap
   * conversely provides access to the maximum element in constant time, and when bounded retains
   * the minimum <code>maxSize</code> elements.
   */
  public enum Order {
    MIN, MAX
  };

  public final static int UNBOUNDED = -1;

  private final int maxSize;

  private int[] heap;
  private int size = 0;

  /**
   * Create an empty priority queue of the configured size.
   */
  IntHeap(int maxSize) {
    final int heapSize;
    if (UNBOUNDED == maxSize) {
      // arbitrary initial size; this may grow
      heapSize = 32;
    } else {
      if ((maxSize < 1) || (maxSize >= ArrayUtil.MAX_ARRAY_LENGTH)) {
        // Throw exception to prevent confusing OOME:
        throw new IllegalArgumentException("maxSize must be UNBOUNDED(-1) or > 0 and < " + (ArrayUtil.MAX_ARRAY_LENGTH) + "; got: " + maxSize);
      }
      // NOTE: we add +1 because all access to heap is 1-based not 0-based.  heap[0] is unused.
      heapSize = maxSize + 1;
    }
    this.maxSize = maxSize;
    this.heap = new int[heapSize];
  }

  public static IntHeap create(Order order, int maxSize) {
    if (order == Order.MIN) {
      return new IntHeap(maxSize) {
        @Override
        protected boolean lessThan(int a, int b) {
          return a < b;
        }
      };
    } else {
      return new IntHeap(maxSize) {
        @Override
        protected boolean lessThan(int a, int b) {
          return a > b;
        }
      };
    }
  }

  /** Determines the ordering of objects in this priority queue. Subclasses must define this one
   *  method.
   *  @return <code>true</code> iff parameter <code>a</code> is less than parameter <code>b</code>.
   */
  protected abstract boolean lessThan(int a, int b);

  /**
   * Adds a value in log(size) time. If one tries to add more values than maxSize from initialize an
   * {@link ArrayIndexOutOfBoundsException} is thrown, unless maxSize is {@link #UNBOUNDED}.
   *
   * @return the new 'top' element in the queue.
   */
  public final int push(int element) {
    size++;
    if (maxSize == UNBOUNDED) {
      heap = ArrayUtil.grow(heap, size * 3 / 2);
    }
    heap[size] = element;
    upHeap(size);
    return heap[1];
  }

  /**
   * Adds a value to an IntHeap in log(size) time, if the number of values would exceed the heap's
   * maxSize, the least value is discarded.
   */
  public void insertWithOverflow(int value) {
    if (size < maxSize || maxSize == UNBOUNDED) {
      push(value);
    } else if (size > 0 && !lessThan(value, heap[1])) {
      updateTop(value);
    }
  }

  /** Returns the least element of the IntHeap in constant time. It is up to the caller to verify
   * that the heap is not empty; no checking is done, and if no elements have been added, 0 is
   * returned.
   */
  public final int top() {
    return heap[1];
  }

  /** Removes and returns the least element of the PriorityQueue in log(size) time.
   * @throws IllegalStateException if the IntHeap is empty.
  */
  public final int pop() {
    if (size > 0) {
      int result = heap[1];     // save first value
      heap[1] = heap[size];     // move last to first
      size--;
      downHeap(1);              // adjust heap
      return result;
    } else {
      throw new IllegalStateException("The heap is empty");
    }
  }

  /**
   * Replace the top of the pq with {@code newTop}. Should be called when the top value
   * changes. Still log(n) worst case, but it's at least twice as fast to
   *
   * <pre class="prettyprint">
   * pq.updateTop(value);
   * </pre>
   *
   * instead of
   *
   * <pre class="prettyprint">
   * pq.pop();
   * pq.push(value);
   * </pre>
   *
   * Calling this method on an empty IntHeap has no visible effect.
   *
   * @param value the new element that is less than the current top.
   * @return the new 'top' element after shuffling the heap.
   */
  public final int updateTop(int value) {
    heap[1] = value;
    downHeap(1);
    return heap[1];
  }

  /** Returns the number of elements currently stored in the PriorityQueue. */
  public final int size() {
    return size;
  }

  /** Removes all entries from the PriorityQueue. */
  public final void clear() {
    size = 0;
  }

  private final void upHeap(int origPos) {
    int i = origPos;
    int node = heap[i];         // save bottom node
    int j = i >>> 1;
    while (j > 0 && lessThan(node, heap[j])) {
      heap[i] = heap[j];       // shift parents down
      i = j;
      j = j >>> 1;
    }
    heap[i] = node;            // install saved node
  }

  private final void downHeap(int i) {
    int node = heap[i];          // save top node
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

  public IntIterator iterator() {
    return new IntIterator();
  }

  /**
   * Iterator over the contents of the heap, returning successive ints.
   */
  public class IntIterator {
    int i = 1;

    public boolean hasNext() {
      return i <= size;
    }

    public int next() {
      if (hasNext() == false) {
        throw new IllegalStateException("attempt to iterate beyond maximum element, size=" + size);
      }
      return heap[i++];
    }
  }

  /** This method returns the internal heap array.
   * @lucene.internal
   */
  protected final int[] getHeapArray() {
    return heap;
  }

}
