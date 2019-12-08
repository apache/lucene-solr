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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/** A knn graph that consists of connected friends (i.e. neighbors) lists. */
final class Layer implements Accountable {

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

  void connectNodes(int node1, int node2, float distance) {
    addNodeIfAbsent(node1);
    addNodeIfAbsent(node2);
    Neighbor neighbor1 = new ImmutableNeighbor(node2, distance);
    friendsMap.get(node1).add(neighbor1);
    Neighbor neighbor2 = new ImmutableNeighbor(node1, distance);
    friendsMap.get(node2).add(neighbor2);
  }

  void connectNodes(int node1, int node2) {
    addNodeIfAbsent(node1);
    addNodeIfAbsent(node2);
    Neighbor neighbor1 = new DeferredNeighbor(node2);
    friendsMap.get(node1).add(neighbor1);
    Neighbor neighbor2 = new DeferredNeighbor(node1);
    friendsMap.get(node2).add(neighbor2);
  }

  void shrink(int node, int maxFriends) {
    TreeSet<Neighbor> friends = friendsMap.get(node);
    if (friends == null) {
      throw new IllegalArgumentException("node " + node + " does not exist at this layer: " + level);
    }
    if (friends.size() > maxFriends) {
      TreeSet<Neighbor> newFriends = new TreeSet<>();
      Iterator<Neighbor> itr = friends.iterator();
      while(itr.hasNext()) {
        Neighbor n = itr.next();
        if (newFriends.size() < maxFriends || friendsMap.get(n.docId()).size() == 1) {
          newFriends.add(n);
        } else {
          friendsMap.get(n.docId()).removeIf(n2 -> n2.docId() == node);
        }
      }
      friendsMap.put(node, newFriends);
    }
  }

  List<Neighbor> getFriends(int node) {
    TreeSet<Neighbor> friends = friendsMap.get(node);
    if (friends == null) {
      return Collections.emptyList();
    }
    return List.copyOf(friends);
  }

  int size() {
    return friendsMap.size();
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.shallowSizeOfInstance(getClass()) + RamUsageEstimator.sizeOfMap(friendsMap);
  }
}
