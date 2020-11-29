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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntHeap;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/** Neighbors encodes the neighbors of a node in the HNSW graph. */
public class Neighbors {

  private static final int INITIAL_SIZE = 128;

  public static Neighbors create(int maxSize, VectorValues.SearchStrategy searchStrategy) {
    return new Neighbors(maxSize, searchStrategy, false);
  }

  public static Neighbors createReversed(int maxSize, VectorValues.SearchStrategy searchStrategy) {
    return new Neighbors(maxSize, searchStrategy, true);
  }

  private final int maxSize;
  private final IntHeap heap;
  private final VectorValues.SearchStrategy searchStrategy;

  private int size;
  private int[] node;
  private float[] score;

  // Used to track the number of neighbors visited during a single graph traversal
  private int visitedCount;

  private Neighbors(int maxSize, VectorValues.SearchStrategy searchStrategy, boolean reversed) {
    this.searchStrategy = searchStrategy;
    if (reversed) {
      heap = IntHeap.create(IntHeap.Order.MAX, maxSize);
    } else {
      heap = IntHeap.create(IntHeap.Order.MIN, maxSize);
    }
    this.maxSize = maxSize;
    int heapSize;
    if (maxSize == IntHeap.UNBOUNDED) {
      heapSize = INITIAL_SIZE;
    } else {
      heapSize = maxSize;
    }
    node = new int[heapSize];
    score = new float[heapSize];
  }

  public int size() {
    return heap.size();
  }

  public boolean reversed() {
    return searchStrategy.reversed;
  }

  public void add(int newNode, float newScore) {
    short normScore = normScore(newScore);
    if (size == node.length) {
      node = ArrayUtil.grow(node, size * 3 / 2);
      score = ArrayUtil.growExact(score, node.length);
    }
    node[size] = newNode;
    score[size] = newScore;
    int ordAndScore = normScore << 16 | size;
    heap.push(ordAndScore);
    ++ size;
  }

  public boolean insertWithOverflow(int newNode, float newScore) {
    short normScore = normScore(newScore);
    if (size < maxSize) {
      node[size] = newNode;
      score[size] = newScore;
      int ordAndScore = normScore << 16 | size;
      heap.push(ordAndScore);
      ++ size;
      return true;
    } else {
      int top = heap.top();
      if (top >> 16 < normScore) { // extend the sign
        int nbrOrd = (short) top;
        node[nbrOrd] = newNode;
        score[nbrOrd] = newScore;
        int ordAndScore = normScore << 16 | nbrOrd;
        heap.updateTop(ordAndScore);
        return true;
      }
    }
    return false;
  }

  public void pop() {
    // This is destructive! Once you pop, you can never add() again.
    heap.pop();
  }

  public int topNode() {
    return node[(short) heap.top()];
  }

  public float topScore() {
    return score[(short) heap.top()];
  }

  int[] nodes() {
    return ArrayUtil.copyOfSubArray(node, 0, size);
  }

  // Convert scores into fp16 so we can pack them in with node ords
  private short normScore(float score) {
    short sortable = HnswGraphBuilder.f16converter.floatToSortableShort(score);
    if (searchStrategy.reversed) {
      return (short) -sortable;
    } else {
      return sortable;
    }
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
    private short nbrOrd = -1;
    private IntHeap.IntIterator heapIterator = heap.iterator();

    public int next() {
      if (heapIterator.hasNext()) {
        nbrOrd = (short) heapIterator.next();
        return node[nbrOrd];
      }
      return NO_MORE_DOCS;
    }

    public float score() {
      return score[nbrOrd];
    }
  }

  @Override
  public String toString() {
    return "Neighbors[" + heap.size() + "]";
  }

}
