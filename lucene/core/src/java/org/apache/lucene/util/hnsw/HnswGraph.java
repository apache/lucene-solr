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
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.VectorValues;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Navigable Small-world graph. Provides efficient approximate nearest neighbor
 * search for high dimensional vectors.  See <a href="https://doi.org/10.1016/j.is.2013.10.006">Approximate nearest
 * neighbor algorithm based on navigable small world graphs [2014]</a> and <a
 * href="https://arxiv.org/abs/1603.09320">this paper [2018]</a> for details.
 *
 * The nomenclature is a bit different here from what's used in those papers:
 *
 * <h2>Hyperparameters</h2>
 * <ul>
 *   <li><code>numSeed</code> is the equivalent of <code>m</code> in the 2012 paper; it controls the number of random entry points to sample.</li>
 *   <li><code>beamWidth</code> in {@link HnswGraphBuilder} has the same meaning as <code>efConst</code> in the 2016 paper. It is the number of
 *   nearest neighbor candidates to track while searching the graph for each newly inserted node.</li>
 *   <li><code>maxConn</code> has the same meaning as <code>M</code> in the later paper; it controls how many of the <code>efConst</code> neighbors are
 *   connected to the new node</li>
 *   <li><code>fanout</code> the fanout parameter of {@link VectorValues#search(float[], int, int)}
 *   is used to control the values of <code>numSeed</code> and <code>topK</code> that are passed to this API.
 *   Thus <code>fanout</code> is like a combination of <code>ef</code> (search beam width) from the 2016 paper and <code>m</code> from the 2014 paper.
 *   </li>
 * </ul>
 *
 * <p>Note: The graph may be searched by multiple threads concurrently, but updates are not thread-safe. Also note: there is no notion of
 * deletions. Document searching built on top of this must do its own deletion-filtering.</p>
 */
public final class HnswGraph {

  private final int maxConn;
  private final VectorValues.SearchStrategy searchStrategy;

  // Each entry lists the top maxConn neighbors of a node. The nodes correspond to vectors added to HnswBuilder, and the
  // node values are the ordinals of those vectors.
  private final List<Neighbors> graph;

  HnswGraph(int maxConn, VectorValues.SearchStrategy searchStrategy) {
    graph = new ArrayList<>();
    graph.add(Neighbors.create(maxConn, searchStrategy.reversed));
    this.maxConn = maxConn;
    this.searchStrategy = searchStrategy;
  }

  /**
   * Searches for the nearest neighbors of a query vector.
   * @param query search query vector
   * @param topK the number of nodes to be returned
   * @param numSeed the number of random entry points to sample
   * @param vectors vector values
   * @param graphValues the graph values. May represent the entire graph, or a level in a hierarchical graph.
   * @param random a source of randomness, used for generating entry points to the graph
   * @return a priority queue holding the neighbors found
   */
  public static Neighbors search(float[] query, int topK, int numSeed, RandomAccessVectorValues vectors, KnnGraphValues graphValues,
                                 Random random) throws IOException {
    VectorValues.SearchStrategy searchStrategy = vectors.searchStrategy();
    // TODO: use unbounded priority queue
    TreeSet<Neighbor> candidates;
    if (searchStrategy.reversed) {
      candidates = new TreeSet<>(Comparator.reverseOrder());
    } else {
      candidates = new TreeSet<>();
    }
    int size = vectors.size();
    for (int i = 0; i < numSeed && i < size; i++) {
      int entryPoint = random.nextInt(size);
      candidates.add(new Neighbor(entryPoint, searchStrategy.compare(query, vectors.vectorValue(entryPoint))));
    }
    // set of ordinals that have been visited by search on this layer, used to avoid backtracking
    Set<Integer> visited = new HashSet<>();
    // TODO: use PriorityQueue's sentinel optimization?
    Neighbors results = Neighbors.create(topK, searchStrategy.reversed);
    for (Neighbor c : candidates) {
      visited.add(c.node());
      results.insertWithOverflow(c);
    }
    // Set the bound to the worst current result and below reject any newly-generated candidates failing
    // to exceed this bound
    BoundsChecker bound = BoundsChecker.create(searchStrategy.reversed);
    bound.bound = results.top().score();
    while (candidates.size() > 0) {
      // get the best candidate (closest or best scoring)
      Neighbor c = candidates.pollLast();
      if (results.size() >= topK) {
        if (bound.check(c.score())) {
          break;
        }
      }
      graphValues.seek(c.node());
      int friendOrd;
      while ((friendOrd = graphValues.nextNeighbor()) != NO_MORE_DOCS) {
        if (visited.contains(friendOrd)) {
          continue;
        }
        visited.add(friendOrd);
        float score = searchStrategy.compare(query, vectors.vectorValue(friendOrd));
        if (results.size() < topK || bound.check(score) == false) {
          Neighbor n = new Neighbor(friendOrd, score);
          candidates.add(n);
          results.insertWithOverflow(n);
          bound.bound = results.top().score();
        }
      }
    }
    results.setVisitedCount(visited.size());
    return results;
  }

  /**
   * Returns the nodes connected to the given node by its outgoing neighborNodes in an unpredictable order. Each node inserted
   * by HnswGraphBuilder corresponds to a vector, and the node is the vector's ordinal.
   * @param node the node whose friends are returned
   */
  public int[] getNeighbors(int node) {
    Neighbors neighbors = graph.get(node);
    int[] result = new int[neighbors.size()];
    int i = 0;
    for (Neighbor n : neighbors) {
      result[i++] = n.node();
    }
    return result;
  }

  /** Connects two nodes symmetrically, limiting the maximum number of connections from either node.
   * node1 must be less than node2 and must already have been inserted to the graph */
  void connectNodes(int node1, int node2, float score) {
    connect(node1, node2, score);
    if (node2 == graph.size()) {
      addNode();
    }
    connect(node2, node1, score);
  }

  KnnGraphValues getGraphValues() {
    return new HnswGraphValues();
  }

  /**
   * Makes a connection from the node to a neighbor, dropping the worst connection when maxConn is exceeded
   * @param node1 node to connect *from*
   * @param node2 node to connect *to*
   * @param score searchStrategy.score() of the vectors associated with the two nodes
   */
  boolean connect(int node1, int node2, float score) {
    //System.out.println("    HnswGraph.connect " + node1 + " -> " + node2);
    assert node1 >= 0 && node2 >= 0;
    Neighbors nn = graph.get(node1);
    assert nn != null;
    if (nn.size() == maxConn) {
      Neighbor top = nn.top();
      if (score < top.score() == nn.reversed()) {
        top.update(node2, score);
        nn.updateTop();
        return true;
      }
    } else {
      nn.add(new Neighbor(node2, score));
      return true;
    }
    return false;
  }

  int addNode() {
    graph.add(Neighbors.create(maxConn, searchStrategy.reversed));
    return graph.size() - 1;
  }

  /**
   * Present this graph as KnnGraphValues, used for searching while inserting new nodes.
   */
  private class HnswGraphValues extends KnnGraphValues {

    private int arcUpTo;
    private int[] neighborNodes;

    @Override
    public void seek(int targetNode) {
      arcUpTo = 0;
      neighborNodes = HnswGraph.this.getNeighbors(targetNode);
    }

    @Override
    public int nextNeighbor() {
      if (arcUpTo >= neighborNodes.length) {
        return NO_MORE_DOCS;
      }
      return neighborNodes[arcUpTo++];
    }

  }

}
