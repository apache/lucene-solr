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
import static org.apache.lucene.util.VectorUtil.dotProduct;
import static org.apache.lucene.util.VectorUtil.squareDistance;

/**
 * Navigable Small-world graph. Provides efficient approximate nearest neighbor search for high
 * dimensional vectors.  See <a href="https://doi.org/10.1016/j.is.2013.10.006">Approximate nearest
 * neighbor algorithm based on navigable small world graphs [2014]</a> and <a
 * href="https://arxiv.org/abs/1603.09320">this paper [2018]</a> for details.
 *
 * This implementation is actually more like the one in the same authors' earlier 2014 paper in that
 * there is no hierarchy (just one layer), and no fanout restriction on the graph: nodes are allowed to accumulate
 * an unbounded number of outbound links, but it does incorporate some of the innovations of the later paper, like
 * using a priority queue to perform a beam search while traversing the graph. The nomenclature is a bit different
 * here from what's used in those papers:
 *
 * <h3>Hyperparameters</h3>
 * <ul>
 *   <li><code>numSeed</code> is the equivalent of <code>m</code> in the 2012 paper; it controls the number of random entry points to sample.</li>
 *   <li><code>beamWidth</code> in {@link HnswGraphBuilder} has the same meaning as <code>efConst</code> in the 2016 paper. It is the number of
 *   nearest neighbor candidates to track while searching the graph for each newly inserted node.</li>
 *   <li><code>maxConn</code> has the same meaning as <code>M</code> in the later paper; it controls how many of the <code>efConst</code> neighbors are
 *   connected to the new node</li>
 *   <li><code>fanout</code> the fanout parameter of {@link VectorValues.RandomAccess#search(float[], int, int)}
 *   is used to control the values of <code>numSeed</code> and <code>topK</code> that are passed to this API.
 *   Thus <code>fanout</code> is like a combination of <code>ef</code> (search beam width) from the 2016 paper and <code>m</code> from the 2014 paper.
 *   </li>
 * </ul>
 *
 * <h3>Annealing</h3>
 * <p>Another difference of this algorithm from the papers cited is the addition of a simulated annealing strategy. The idea is to
 * avoid getting stuck in local minima while traversing the graph by accepting candidates that lie some small delta beyond the maximum distance
 * of the current top K, reducing delta as iteration progresses. See {@link BoundsChecker}.</p>
 *
 * <p>Note: The graph may be searched by multiple threads concurrently, but updates are not thread-safe. Also note: there is no notion of
 * deletions. Document searching built on top of this must do its own deletion-filtering.</p>
 */
public final class HnswGraph {

  // each entry lists the neighbors of a node, in node order
  private final List<List<Neighbor>> graph;

  HnswGraph() {
    graph = new ArrayList<>();
    graph.add(new ArrayList<>());
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
  public static Neighbors search(float[] query, int topK, int numSeed, VectorValues.RandomAccess vectors, KnnGraphValues graphValues,
                                 Random random) throws IOException {
    VectorValues.SearchStrategy searchStrategy = vectors.searchStrategy();
    boolean scoreReversed = isReversed(searchStrategy);
    TreeSet<Neighbor> candidates;
    if (scoreReversed) {
      candidates = new TreeSet<>(Comparator.reverseOrder());
    } else {
      candidates = new TreeSet<>();
    }
    int size = vectors.size();
    for (int i = 0; i < numSeed && i < size; i++) {
      int entryPoint = random.nextInt(size);
      candidates.add(new Neighbor(entryPoint, compare(query, vectors.vectorValue(entryPoint), searchStrategy)));
    }
    // set of ordinals that have been visited by search on this layer, used to avoid backtracking
    //IntHashSet visited = new IntHashSet();
    Set<Integer> visited = new HashSet<>();
    Neighbors results = Neighbors.create(topK, scoreReversed);
    BoundsChecker bound = BoundsChecker.create(scoreReversed);
    for (Neighbor c :candidates) {
      visited.add(c.node);
      results.insertWithOverflow(c);
      // gather the best candidate score in the bounds checker
      bound.update(c.score);
    }
    // Set the bound to the least, ie "worst" current result, also updating the bound delta according to the
    // difference between the best and worst candidate.
    bound.set(results.top().score, bound.bound);
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
        float score = compare(query, vectors.vectorValue(friendOrd), searchStrategy);
        if (results.size() < topK || bound.check(score) == false) {
          Neighbor n = new Neighbor(friendOrd, score);
          candidates.add(n);
          results.insertWithOverflow(n);
          bound.bound = results.top().score;
        }
      }
    }
    return results;
  }

  /**
   * Returns the nodes connected to the given node by its outgoing arcs.
   * @param node the node whose friends are returned
   */
  public int[] getNeighbors(int node) {
    return graph.get(node).stream().mapToInt(Neighbor::node).toArray();
  }

  /** Connects two nodes symmetrically.
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

  /**
   * Calculates a similarity score between the two vectors with a specified function.
   * @param v1 a vector
   * @param v2 another vector, of the same dimension
   * @return the value of the strategy's score function applied to the two vectors
   */
  public static float compare(float[] v1, float[] v2, VectorValues.SearchStrategy searchStrategy) {
    switch (searchStrategy) {
      case EUCLIDEAN_HNSW:
        return squareDistance(v1, v2);
      case DOT_PRODUCT_HNSW:
        return dotProduct(v1, v2);
      default:
        throw new IllegalStateException("invalid search strategy: " + searchStrategy);
    }
  }

  /**
   * Return whether the given strategy returns lower values for nearer vectors
   * @param searchStrategy the strategy
   */
  public static boolean isReversed(VectorValues.SearchStrategy searchStrategy) {
    switch (searchStrategy) {
      case EUCLIDEAN_HNSW:
        return true;
      case DOT_PRODUCT_HNSW:
        return false;
      default:
        throw new IllegalStateException("invalid search strategy: " + searchStrategy);
    }
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
      arcs = HnswGraph.this.getNeighbors(targetNode);
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
