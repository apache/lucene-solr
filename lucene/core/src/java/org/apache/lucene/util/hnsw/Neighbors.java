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

import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Neighbors encodes the neighbors of a node in the HNSW graph. */
public class Neighbors {

  private static final int INITIAL_SIZE = 128;

  public static Neighbors create(int maxSize, VectorValues.SearchStrategy searchStrategy) {
    return new Neighbors(maxSize, searchStrategy, searchStrategy.reversed);
  }

  public static Neighbors createReversed(int maxSize, VectorValues.SearchStrategy searchStrategy) {
    return new Neighbors(maxSize, searchStrategy, !searchStrategy.reversed);
  }

  private final LongHeap heap;
  private final VectorValues.SearchStrategy searchStrategy;

  // Used to track the number of neighbors visited during a single graph traversal
  private int visitedCount;

  private Neighbors(int maxSize, VectorValues.SearchStrategy searchStrategy, boolean reversed) {
    this.searchStrategy = searchStrategy;
    if (reversed) {
      heap = LongHeap.create(LongHeap.Order.MAX, maxSize);
    } else {
      heap = LongHeap.create(LongHeap.Order.MIN, maxSize);
    }
  }

  public int size() {
    return heap.size();
  }

  public boolean reversed() {
    return searchStrategy.reversed;
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

  public int topNode() {
    return (int) heap.top();
  }

  public float topScore() {
    return NumericUtils.sortableIntToFloat((int) (heap.top() >> 32));
  }

  void setVisitedCount(int visitedCount) {
    this.visitedCount = visitedCount;
  }

  public int visitedCount() {
    return visitedCount;
  }

  public NeighborIterator iterator() {
    return new NeighborIterator();
  }

  class NeighborIterator {
    private long value;
    private final LongHeap.LongIterator heapIterator = heap.iterator();

    /** Return the next node */
    public int next() {
      if (heapIterator.hasNext()) {
        value = heapIterator.next();
        return (int) value;
      }
      return NO_MORE_DOCS;
    }

    /** Return the score corresponding to the last node returned by next() */
    public float score() {
      return NumericUtils.sortableIntToFloat((int) (value >> 32));
    }
  }

  @Override
  public String toString() {
    return "Neighbors[" + heap.size() + "]";
  }

}
