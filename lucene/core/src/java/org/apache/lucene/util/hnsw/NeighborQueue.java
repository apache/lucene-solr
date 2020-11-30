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

/** Neighbors encodes the neighbors of a node in the HNSW graph.
 */
public class NeighborQueue {

  private final LongHeap heap;

  // Used to track the number of neighbors visited during a single graph traversal
  private int visitedCount;

  NeighborQueue(int maxSize, boolean reversed) {
    if (reversed) {
      heap = LongHeap.create(LongHeap.Order.MAX, maxSize);
    } else {
      heap = LongHeap.create(LongHeap.Order.MIN, maxSize);
    }
  }

  public int size() {
    return heap.size();
  }

  public void add(int newNode, float newScore) {
    heap.push(encode(newNode, newScore));
  }

  public boolean insertWithOverflow(int newNode, float newScore) {
    return heap.insertWithOverflow(encode(newNode, newScore));
  }

  private long encode(int node, float score) {
    return (((long) NumericUtils.floatToSortableInt(score)) << 32) | node;
  }

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

  public int topNode() {
    return (int) heap.top();
  }

  public float topScore() {
    return NumericUtils.sortableIntToFloat((int) (heap.top() >> 32));
  }

  public int visitedCount() {
    return visitedCount;
  }

  public NeighborQueue copy(boolean reversed) {
    int size = size();
    NeighborQueue copy = new NeighborQueue(size, reversed);
    copy.heap.pushAll(heap);
    return copy;
  }

  void setVisitedCount(int visitedCount) {
    this.visitedCount = visitedCount;
  }

  @Override
  public String toString() {
    return "Neighbors[" + heap.size() + "]";
  }
}
