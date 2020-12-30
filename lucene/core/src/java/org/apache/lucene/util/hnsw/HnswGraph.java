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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Navigable Small-world graph. Provides efficient approximate nearest neighbor search for high
 * dimensional vectors. See <a href="https://doi.org/10.1016/j.is.2013.10.006">Approximate nearest
 * neighbor algorithm based on navigable small world graphs [2014]</a> and <a
 * href="https://arxiv.org/abs/1603.09320">this paper [2018]</a> for details.
 *
 * <p>The nomenclature is a bit different here from what's used in those papers:
 *
 * <h2>Hyperparameters</h2>
 *
 * <ul>
 *   <li><code>numSeed</code> is the equivalent of <code>m</code> in the 2012 paper; it controls the
 *       number of random entry points to sample.
 *   <li><code>beamWidth</code> in {@link HnswGraphBuilder} has the same meaning as <code>efConst
 *       </code> in the 2016 paper. It is the number of nearest neighbor candidates to track while
 *       searching the graph for each newly inserted node.
 *   <li><code>maxConn</code> has the same meaning as <code>M</code> in the later paper; it controls
 *       how many of the <code>efConst</code> neighbors are connected to the new node
 *   <li><code>fanout</code> the fanout parameter of {@link VectorValues#search(float[], int, int)}
 *       is used to control the values of <code>numSeed</code> and <code>topK</code> that are passed
 *       to this API. Thus <code>fanout</code> is like a combination of <code>ef</code> (search beam
 *       width) from the 2016 paper and <code>m</code> from the 2014 paper.
 * </ul>
 *
 * <p>Note: The graph may be searched by multiple threads concurrently, but updates are not
 * thread-safe. Also note: there is no notion of deletions. Document searching built on top of this
 * must do its own deletion-filtering.
 */
public final class HnswGraph extends KnnGraphValues {

  private final int maxConn;
  private final VectorValues.SearchStrategy searchStrategy;

  // Each entry lists the top maxConn neighbors of a node. The nodes correspond to vectors added to
  // HnswBuilder, and the
  // node values are the ordinals of those vectors.
  private final List<NeighborArray> graph;

  // KnnGraphValues iterator members
  private int upto;
  private NeighborArray cur;

  HnswGraph(int maxConn, VectorValues.SearchStrategy searchStrategy) {
    graph = new ArrayList<>();
    // Typically with diversity criteria we see nodes not fully occupied; average fanout seems to be
    // about 1/2 maxConn. There is some indexing time penalty for under-allocating, but saves RAM
    graph.add(new NeighborArray(Math.max(32, maxConn / 4)));
    this.maxConn = maxConn;
    this.searchStrategy = searchStrategy;
  }

  /**
   * Searches for the nearest neighbors of a query vector.
   *
   * @param query search query vector
   * @param topK the number of nodes to be returned
   * @param numSeed the size of the queue maintained while searching, and controls the number of
   *     random entry points to sample
   * @param vectors vector values
   * @param graphValues the graph values. May represent the entire graph, or a level in a
   *     hierarchical graph.
   * @param random a source of randomness, used for generating entry points to the graph
   * @return a priority queue holding the closest neighbors found
   */
  public static NeighborQueue search(
      float[] query,
      int topK,
      int numSeed,
      RandomAccessVectorValues vectors,
      KnnGraphValues graphValues,
      Random random)
      throws IOException {
    VectorValues.SearchStrategy searchStrategy = vectors.searchStrategy();
    int size = graphValues.size();

    // MIN heap, holding the top results
    NeighborQueue results = new NeighborQueue(numSeed, searchStrategy.reversed);

    // set of ordinals that have been visited by search on this layer, used to avoid backtracking
    SparseFixedBitSet visited = new SparseFixedBitSet(size);
    // get initial candidates at random
    int boundedNumSeed = Math.min(numSeed, 2 * size);
    for (int i = 0; i < boundedNumSeed; i++) {
      int entryPoint = random.nextInt(size);
      if (visited.get(entryPoint) == false) {
        visited.set(entryPoint);
        // explore the topK starting points of some random numSeed probes
        results.add(entryPoint, searchStrategy.compare(query, vectors.vectorValue(entryPoint)));
      }
    }

    // MAX heap, from which to pull the candidate nodes
    NeighborQueue candidates = results.copy(!searchStrategy.reversed);

    // Set the bound to the worst current result and below reject any newly-generated candidates
    // failing
    // to exceed this bound
    BoundsChecker bound = BoundsChecker.create(searchStrategy.reversed);
    bound.set(results.topScore());
    while (candidates.size() > 0) {
      // get the best candidate (closest or best scoring)
      float topCandidateScore = candidates.topScore();
      if (results.size() >= topK) {
        if (bound.check(topCandidateScore)) {
          break;
        }
      }
      int topCandidateNode = candidates.pop();
      graphValues.seek(topCandidateNode);
      int friendOrd;
      while ((friendOrd = graphValues.nextNeighbor()) != NO_MORE_DOCS) {
        if (visited.get(friendOrd)) {
          continue;
        }
        visited.set(friendOrd);
        float score = searchStrategy.compare(query, vectors.vectorValue(friendOrd));
        if (results.insertWithOverflow(friendOrd, score)) {
          candidates.add(friendOrd, score);
          bound.set(results.topScore());
        }
      }
    }
    while (results.size() > topK) {
      results.pop();
    }
    results.setVisitedCount(visited.approximateCardinality());
    return results;
  }

  /**
   * Returns the {@link NeighborQueue} connected to the given node.
   *
   * @param node the node whose neighbors are returned
   */
  public NeighborArray getNeighbors(int node) {
    return graph.get(node);
  }

  @Override
  public int size() {
    return graph.size();
  }

  int addNode() {
    graph.add(new NeighborArray(maxConn + 1));
    return graph.size() - 1;
  }

  @Override
  public void seek(int targetNode) {
    cur = getNeighbors(targetNode);
    upto = -1;
  }

  @Override
  public int nextNeighbor() {
    if (++upto < cur.size()) {
      return cur.node[upto];
    }
    return NO_MORE_DOCS;
  }
}
