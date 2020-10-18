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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.VectorValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Navigable Small-world graph. Provides efficient approximate nearest neighbor search for high
 * dimensional vectors.  This isn't thread-safe.  See <a
 * href="https://arxiv.org/abs/1603.09320">this paper</a> for details.
 */
public final class HnswGraph {

  // each entry lists the neighbors of a node, in node order
  private final List<List<Neighbor>> graph;

  public HnswGraph() {
    graph = new ArrayList<>();
    graph.add(new ArrayList<>());
  }

  /**
   * Searches for the nearest neighbors of a query vector.
   * @param query search query vector
   * @param topK the number of nodes to be returned
   * @param fanout the number of random entry points to sample
   * @param vectors vector values
   * @param graphValues the graph values. May represent the entire graph, or a level in a hierarchical graph.
   * @param random a source of randomness, used for generating entry points to the graph
   * @return a priority queue holding the neighbors found
   */
  public static Neighbors search(float[] query, int topK, int fanout, VectorValues.RandomAccess vectors, KnnGraphValues graphValues,
                                 Random random) throws IOException {
    VectorValues.ScoreFunction scoreFunction = vectors.scoreFunction();
    boolean scoreReversed = scoreFunction.reversed;
    TreeSet<Neighbor> candidates;
    if (scoreReversed) {
      candidates = new TreeSet<>(Comparator.reverseOrder());
    } else {
      candidates = new TreeSet<>();
    }
    int size = vectors.size();
    for (int i = 0; i < fanout && i < size; i++) {
      int entryPoint = random.nextInt(size);
      candidates.add(new Neighbor(entryPoint, VectorValues.compare(query, vectors.vectorValue(entryPoint), scoreFunction)));
    }
    // set of ordinals that have been visited by search on this layer, used to avoid backtracking
    //IntHashSet visited = new IntHashSet();
    Set<Integer> visited = new HashSet<>();
    Neighbors results = Neighbors.create(topK, scoreReversed);
    BoundsChecker bound = BoundsChecker.create(scoreReversed, topK);
    for (Neighbor c :candidates) {
      visited.add(c.node);
      results.insertWithOverflow(c);
      bound.update(c.score);
    }
    // Remember the least, ie "worst" current result
    bound.setWorst(results.top().score);
    while (candidates.size() > 0) {
      // get the best candidate (closest or best scoring)
      Neighbor c = candidates.pollLast();
      if (results.size() >= topK) {
        if (bound.check(c.score)) {
          break;
        }
      }
      graphValues.seek(c.node);
      int friendOrd;
      while ((friendOrd = graphValues.nextArc()) != NO_MORE_DOCS) {
        if (visited.contains(friendOrd)) {
          continue;
        }
        visited.add(friendOrd);
        float score = VectorValues.compare(query, vectors.vectorValue(friendOrd), scoreFunction);
        if (results.size() < topK || bound.check(score) == false) {
          //if (results.size() < topK || score > lowerBound) {
          Neighbor n = new Neighbor(friendOrd, score);
          candidates.add(n);
          results.insertWithOverflow(n);
          bound.bound = results.top().score;
        }
      }
    }
    return results;
  }

  public boolean isEmpty() {
    return graph.isEmpty();
  }

  public int[] getFriends(int node) {
    return graph.get(node).stream().mapToInt(Neighbor::node).toArray();
  }

  /** Connects two nodes
   * node1 must be less than node2 and must already have been inserted to the graph */
  void connectNodes(int node1, int node2, float score, int maxConnections) {
    assert node1 >= 0 && node2 >= 0;
    assert node1 < node2;
    List<Neighbor> arcs1 = graph.get(node1);
    assert arcs1 != null;
    assert arcs1.isEmpty() || arcs1.get(arcs1.size() - 1).node < node2;
    arcs1.add(new Neighbor(node2, score));
    List<Neighbor> arcs2;
    if (node2 < graph.size()) {
      arcs2 = graph.get(node2);
      assert arcs2.get(arcs2.size() - 1).node < node1;
    } else {
      assert node2 == graph.size();
      arcs2 = new ArrayList<>();
      graph.add(arcs2);
    }
    arcs2.add(new Neighbor(node1, score));

    // ensure #arcs <= maxConnections
    /*
    if (maxConnections > 0) {
      shrink(node2, maxConnections);
    }
     */
  }

  KnnGraphValues getGraphValues() {
    return new HnswGraphValues();
  }

  /**
   * Present this graph as KnnGraphValues, used for searching while inserting new nodes.
   */
  private class HnswGraphValues extends KnnGraphValues {

    private int arcUpTo;
    private int[] arcs;

    @Override
    public void seek(int targetNode) {
      arcUpTo = 0;
      arcs = HnswGraph.this.getFriends(targetNode);
    }

    @Override
    public int nextArc() {
      if (arcUpTo >= arcs.length) {
        return NO_MORE_DOCS;
      }
      return arcs[arcUpTo++];
    }

  }

}
