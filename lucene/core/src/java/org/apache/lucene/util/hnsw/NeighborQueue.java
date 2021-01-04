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

package org.apache.lucene.util.hnsw;

import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.NumericUtils;

/**
 * NeighborQueue uses a {@link LongHeap} to store lists of arcs in an HNSW graph, represented as a
 * neighbor node id with an associated score packed together as a sortable long, which is sorted
 * primarily by score. The queue provides both fixed-size and unbounded operations via {@link
 * #insertWithOverflow(int, float)} and {@link #add(int, float)}, and provides MIN and MAX heap
 * subclasses.
 */
public class NeighborQueue {

  private final LongHeap heap;

  // Used to track the number of neighbors visited during a single graph traversal
  private int visitedCount;

  NeighborQueue(int initialSize, boolean reversed) {
    if (reversed) {
      heap = LongHeap.create(LongHeap.Order.MAX, initialSize);
    } else {
      heap = LongHeap.create(LongHeap.Order.MIN, initialSize);
    }
  }

  NeighborQueue copy(boolean reversed) {
    int size = size();
    NeighborQueue copy = new NeighborQueue(size, reversed);
    copy.heap.pushAll(heap);
    return copy;
  }

  /** @return the number of elements in the heap */
  public int size() {
    return heap.size();
  }

  /**
   * Adds a new graph arc, extending the storage as needed.
   *
   * @param newNode the neighbor node id
   * @param newScore the score of the neighbor, relative to some other node
   */
  public void add(int newNode, float newScore) {
    heap.push(encode(newNode, newScore));
  }

  /**
   * If the heap is not full (size is less than the initialSize provided to the constructor), adds a
   * new node-and-score element. If the heap is full, compares the score against the current top
   * score, and replaces the top element if newScore is better than (greater than unless the heap is
   * reversed), the current top score.
   *
   * @param newNode the neighbor node id
   * @param newScore the score of the neighbor, relative to some other node
   */
  public boolean insertWithOverflow(int newNode, float newScore) {
    return heap.insertWithOverflow(encode(newNode, newScore));
  }

  private long encode(int node, float score) {
    return (((long) NumericUtils.floatToSortableInt(score)) << 32) | node;
  }

  /** Removes the top element and returns its node id. */
  public int pop() {
    return (int) heap.pop();
  }

  int[] nodes() {
    int size = size();
    int[] nodes = new int[size];
    for (int i = 0; i < size; i++) {
      nodes[i] = (int) heap.get(i + 1);
    }
    return nodes;
  }

  /** Returns the top element's node id. */
  public int topNode() {
    return (int) heap.top();
  }

  /** Returns the top element's node score. */
  public float topScore() {
    return NumericUtils.sortableIntToFloat((int) (heap.top() >> 32));
  }

  public int visitedCount() {
    return visitedCount;
  }

  void setVisitedCount(int visitedCount) {
    this.visitedCount = visitedCount;
  }

  @Override
  public String toString() {
    return "Neighbors[" + heap.size() + "]";
  }
}
