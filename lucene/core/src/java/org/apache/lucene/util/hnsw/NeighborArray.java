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

import org.apache.lucene.util.ArrayUtil;

/** NeighborArray encodes the neighbors of a node in the HNSW graph as a pair of fixed-capacity arrays and is designed for the peculiar update
 * strategy used by the node-diversity heuristic used when constructing an HNSW graph.
 * <p>Initially, while the array is not full, nodes and their scores are added in an arbitrary order (by calling {@link #add(int, float)},
 * and are not sorted. Once the arrays are full, they are sorted in order by score. If the search strategy is inverted, scores are
 * inverted when stored, so that the sort order is always worst-first.</p>
 * <p>Once the arrays are full (and sorted), new neighbors are inserted by scanning for the neighbor to replace,
 * using {link #find(float, int)}, to find an insert point (by binary search over an appropriate range of indices),
 * and calling {link #replace(int, float, int, int)}, which shifts the affected sub-range of the two arrays so as to overwrite
 * the node at replacePoint and insert the new node and score at insertPoint, while maintaining the sorted property.</p>
 * @lucene.internal
 */
public class NeighborArray {

  float[] score;
  int[] node;

  private int upto;
  private int size;

  NeighborArray(int maxSize) {
    node = new int[maxSize];
    score = new float[maxSize];
  }

  public void add(int newNode, float newScore) {
    if (size == node.length - 1) {
      node = ArrayUtil.grow(node, (size + 1) * 3 / 2);
      score = ArrayUtil.growExact(score, node.length);
    }
    node[size] = newNode;
    score[size] = newScore;
    ++size;
  }

  public int size() {
    return size;
  }

  /** Direct access to the internal list of node ids; provided for efficient writing of the graph
   * @lucene.internal
   */
  public int[] node() {
    return node;
  }

  public void removeLast() {
    size--;
  }

  public void clear() {
    size = 0;
  }

  @Override
  public String toString() {
    return "NeighborArray[" + size + "]";
  }

}

