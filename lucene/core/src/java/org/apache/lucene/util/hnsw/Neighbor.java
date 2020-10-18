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

/** A neighbor node in the HNSW graph; holds the node ordinal and its distance score. */
public class Neighbor implements Comparable<Neighbor> {

  final public int node;

  final public float score;

  public Neighbor(int node, float score) {
    this.node = node;
    this.score = score;
  }

  public int node() {
    return node;
  }

  @Override
  public int compareTo(Neighbor o) {
    if (score == o.score) {
      return o.node - node;
    } else {
      assert node != o.node : "attempt to add the same node " + node + " twice with different scores: " + score + " != " + o.score;
      return Float.compare(score, o.score);
    }
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof Neighbor
      && ((Neighbor) other).node == node;
  }

  @Override
  public int hashCode() {
    return 39 + 61 * node;
  }

  @Override
  public String toString() {
    return "(" + node + ", " + score + ")";
  }
}
