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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/** A knn graph that consists of connected friends (i.e. neighbors) lists. */
final class Layer implements Accountable {

  // Sentinel that indicates a document has no friends *ie does not participate in the graph*
  // This is distinct from a document in a single-node graph, which has no friends *yet* (sad).
  public static final Collection<Neighbor> NO_FRIENDS = Collections.unmodifiableSet(new HashSet<>());

  private final int level;
  private final Map<Integer, TreeSet<Neighbor>> friendsMap;

  /**
   * Constructs a layer with level.
   * @param level level of this layer
   */
  Layer(int level) {
    this.level = level;
    this.friendsMap = new HashMap<>();
  }

  List<Integer> getNodes() {
    return List.copyOf(friendsMap.keySet());
  }

  void addNodeIfAbsent(int node) {
    friendsMap.putIfAbsent(node, new TreeSet<>());
  }

  void connectNode(int src, int dest, float distance) {
    addNodeIfAbsent(src);
    Neighbor neighbor1 = new ImmutableNeighbor(dest, distance);
    friendsMap.get(src).add(neighbor1);
  }

  void connectNodes(int node1, int node2) {
    addNodeIfAbsent(node1);
    addNodeIfAbsent(node2);
    Neighbor neighbor1 = new DeferredNeighbor(node2);
    friendsMap.get(node1).add(neighbor1);
    Neighbor neighbor2 = new DeferredNeighbor(node1);
    friendsMap.get(node2).add(neighbor2);
  }

  TreeSet<Neighbor> doShrink(final TreeSet<Neighbor> results, int mMax,
                             HNSWGraph graph, VectorValues vectorValues) throws IOException {
    final TreeSet<Neighbor> friends = new TreeSet<>();
    while (!results.isEmpty()) {
      final Neighbor v1 = results.pollFirst();
      float v1qDist = v1.distance();

      boolean good = true;
      for (final Neighbor v2 : friends) {
        float v1v2Dist = graph.distance(v1.docId(), v2.docId(), vectorValues);

        if (v1v2Dist < v1qDist) {
          good = false;
          break;
        }
      }

      if (good) {
        friends.add(v1);
        if (friends.size() >= mMax) {
          break;
        }
      }
    }

    return friends;
  }

  void addLink(int srcId, int destId, int mMax, float distance, HNSWGraph graph, VectorValues vectorValues) throws IOException {
    /// connect from dest to src
    connectNode(srcId, destId, distance);
    final TreeSet<Neighbor> friends = friendsMap.get(srcId);
    if (friends.size() <= mMax) {
      return;
    }

    final TreeSet<Neighbor> newNeighbors = doShrink(friends, mMax, graph, vectorValues);
    friendsMap.put(srcId, newNeighbors);
  }

  Collection<Neighbor> getFriends(int node) {
    Collection<Neighbor> friends = friendsMap.get(node);
    return Objects.requireNonNullElse(friends, NO_FRIENDS);
  }

  int size() {
    return friendsMap.size();
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.shallowSizeOfInstance(getClass()) + RamUsageEstimator.sizeOfMap(friendsMap);
  }
}
