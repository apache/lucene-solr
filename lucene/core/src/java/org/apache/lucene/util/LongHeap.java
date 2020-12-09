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
 * A heap that stores longs; a primitive priority queue that like all priority queues maintains a
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
public abstract class LongHeap {

  /**
   * Used to specify the ordering of the heap. A min-heap provides access to the minimum element in
   * constant time, and when bounded, retains the maximum <code>maxSize</code> elements. A max-heap
   * conversely provides access to the maximum element in constant time, and when bounded retains
   * the minimum <code>maxSize</code> elements.
   */
  public enum Order {
    MIN, MAX
  };

  private static final int UNBOUNDED = -1;
  private final int maxSize;

  private long[] heap;
  private int size = 0;

  /**
   * Create an empty priority queue of the configured size.
   * @param maxSize the maximum size of the heap, or if negative, the initial size of an unbounded heap
   */
  LongHeap(int maxSize) {
    final int heapSize;
    if (maxSize < 0) {
      // initial size; this may grow
      heapSize = -maxSize;
      this.maxSize = UNBOUNDED;
    } else {
      if ((maxSize < 1) || (maxSize >= ArrayUtil.MAX_ARRAY_LENGTH)) {
        // Throw exception to prevent confusing OOME:
        throw new IllegalArgumentException("maxSize must be UNBOUNDED(-1) or > 0 and < " + (ArrayUtil.MAX_ARRAY_LENGTH) + "; got: " + maxSize);
      }
      // NOTE: we add +1 because all access to heap is 1-based not 0-based.  heap[0] is unused.
      heapSize = maxSize + 1;
      this.maxSize = maxSize;
    }
    this.heap = new long[heapSize];
  }

  public static LongHeap create(Order order, int maxSize) {
    // TODO: override push() for unbounded queue
    if (order == Order.MIN) {
      return new LongHeap(maxSize) {
        @Override
        public boolean lessThan(long a, long b) {
          return a < b;
        }
      };
    } else {
      return new LongHeap(maxSize) {
        @Override
        public boolean lessThan(long a, long b) {
          return a > b;
        }
      };
    }
  }

  /** Determines the ordering of objects in this priority queue. Subclasses must define this one
   *  method.
   *  @return <code>true</code> iff parameter <code>a</code> is less than parameter <code>b</code>.
   */
  public abstract boolean lessThan(long a, long b);

  /**
   * Adds a value in log(size) time. If one tries to add more values than maxSize from initialize an
   * {@link ArrayIndexOutOfBoundsException} is thrown, unless maxSize is {@link #UNBOUNDED}.
   *
   * @return the new 'top' element in the queue.
   */
  public final long push(long element) {
    size++;
    if (maxSize == UNBOUNDED && size == heap.length) {
      heap = ArrayUtil.grow(heap, (size * 3 + 1) / 2);
    }
    heap[size] = element;
    upHeap(size);
    return heap[1];
  }

  /**
   * Adds a value to an IntHeap in log(size) time. if the number of values would exceed the heap's
   * maxSize, the least value is discarded.
   * @return whether the value was added (unless the heap is full, or the new value is less than the top value)
   */
  public boolean insertWithOverflow(long value) {
    if (size < maxSize || maxSize == UNBOUNDED) {
      push(value);
      return true;
    } else if (size > 0 && !lessThan(value, heap[1])) {
      updateTop(value);
      return true;
    }
    return false;
  }

  /** Returns the least element of the IntHeap in constant time. It is up to the caller to verify
   * that the heap is not empty; no checking is done, and if no elements have been added, 0 is
   * returned.
   */
  public final long top() {
    return heap[1];
  }

  /** Removes and returns the least element of the PriorityQueue in log(size) time.
   * @throws IllegalStateException if the IntHeap is empty.
  */
  public final long pop() {
    if (size > 0) {
      long result = heap[1];     // save first value
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
   * Calling this method on an empty LongHeap has no visible effect.
   *
   * @param value the new element that is less than the current top.
   * @return the new 'top' element after shuffling the heap.
   */
  public final long updateTop(long value) {
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
    long value = heap[i];         // save bottom value
    int j = i >>> 1;
    while (j > 0 && lessThan(value, heap[j])) {
      heap[i] = heap[j];       // shift parents down
      i = j;
      j = j >>> 1;
    }
    heap[i] = value;            // install saved value
  }

  private final void downHeap(int i) {
    long value = heap[i];          // save top value
    int j = i << 1;            // find smaller child
    int k = j + 1;
    if (k <= size && lessThan(heap[k], heap[j])) {
      j = k;
    }
    while (j <= size && lessThan(heap[j], value)) {
      heap[i] = heap[j];       // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && lessThan(heap[k], heap[j])) {
        j = k;
      }
    }
    heap[i] = value;            // install saved value
  }

  public LongIterator iterator() {
    return new LongIterator();
  }

  /**
   * Iterator over the contents of the heap, returning successive ints.
   */
  public class LongIterator {
    int i = 1;

    public boolean hasNext() {
      return i <= size;
    }

    public long next() {
      if (hasNext() == false) {
        throw new IllegalStateException("attempt to iterate beyond maximum element, size=" + size);
      }
      return heap[i++];
    }
  }

  /** This method returns the internal heap array.
   * @lucene.internal
   */
  protected final long[] getHeapArray() {
    return heap;
  }

}
