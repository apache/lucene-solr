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

/**
 * NeighborArray encodes the neighbors of a node and their mutual scores in the HNSW graph as a pair
 * of growable arrays.
 *
 * @lucene.internal
 */
public class NeighborArray {

  private int size;
  private int upto;

  float[] score;
  int[] node;

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

  /**
   * Direct access to the internal list of node ids; provided for efficient writing of the graph
   *
   * @lucene.internal
   */
  public int[] node() {
    return node;
  }

  public void clear() {
    size = 0;
  }

  void removeLast() {
    size--;
  }

  @Override
  public String toString() {
    return "NeighborArray[" + size + "]";
  }
}
