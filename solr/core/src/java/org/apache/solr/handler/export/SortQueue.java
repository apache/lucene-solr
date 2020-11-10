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

package org.apache.solr.handler.export;

import org.apache.lucene.util.ArrayUtil;

/**
 * Specialized class that reuses most of the code from {@link org.apache.lucene.util.PriorityQueue}
 * but contains optimizations for the /export handler use.
 */
final class SortQueue {

  protected int size = 0;
  protected final int maxSize;
  private final SortDoc[] heap;
  private final SortDoc proto;
  private SortDoc[] cache;

  public SortQueue(int maxSize, SortDoc proto) {
    this.proto = proto;
    final int heapSize;
    if (0 == maxSize) {
      // We allocate 1 extra to avoid if statement in top()
      heapSize = 2;
    } else {
      if (maxSize > ArrayUtil.MAX_ARRAY_LENGTH) {
        // Don't wrap heapSize to -1, in this case, which
        // causes a confusing NegativeArraySizeException.
        // Note that very likely this will simply then hit
        // an OOME, but at least that's more indicative to
        // caller that this values is too big.  We don't +1
        // in this case, but it's very unlikely in practice
        // one will actually insert this many objects into
        // the PQ:
        // Throw exception to prevent confusing OOME:
        throw new IllegalArgumentException("maxSize must be <= " + ArrayUtil.MAX_ARRAY_LENGTH + "; got: " + maxSize);
      } else {
        // NOTE: we add +1 because all access to heap is
        // 1-based not 0-based.  heap[0] is unused.
        heapSize = maxSize + 1;
      }
    }
    this.heap = new SortDoc[heapSize];
    this.maxSize = maxSize;
  }

  private static final boolean lessThan(SortDoc t1, SortDoc t2) {
    return t1.lessThan(t2);
  }

  protected void populate() {
    cache = new SortDoc[heap.length];
    for (int i = 1; i < heap.length; i++) {
      cache[i] = heap[i] = proto.copy();
    }
    size = maxSize;
  }

  protected void reset() {
    if (cache != null) {
      System.arraycopy(cache, 1, heap, 1, heap.length-1);
      size = maxSize;
    } else {
      populate();
    }
  }

  // ==================
  /**
   * Adds an Object to a PriorityQueue in log(size) time. If one tries to add
   * more objects than maxSize from initialize an
   *
   * @return the new 'top' element in the queue.
   */
  public final SortDoc add(SortDoc element) {
    size++;
    heap[size] = element;
    upHeap();
    return heap[1];
  }

  /** Returns the least element of the PriorityQueue in constant time. */
  public final SortDoc top() {
    // We don't need to check size here: if maxSize is 0,
    // then heap is length 2 array with both entries null.
    // If size is 0 then heap[1] is already null.
    return heap[1];
  }

  /** Removes and returns the least element of the PriorityQueue in log(size)
   time. */
  public final SortDoc pop() {
    if (size > 0) {
      SortDoc result = heap[1];       // save first value
      heap[1] = heap[size];     // move last to first
      heap[size] = null;        // permit GC of objects
      size--;
      downHeap();               // adjust heap
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
  public final SortDoc updateTop() {
    downHeap();
    return heap[1];
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

  private final void upHeap() {
    int i = size;
    SortDoc node = heap[i];          // save bottom node
    int j = i >>> 1;
    while (j > 0 && lessThan(node, heap[j])) {
      heap[i] = heap[j];       // shift parents down
      i = j;
      j = j >>> 1;
    }
    heap[i] = node;            // install saved node
  }

  private final void downHeap() {
    int i = 1;
    SortDoc node = heap[i];          // save top node
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

}