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
import java.util.Locale;
import java.util.Random;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.InfoStream;

/**
 * Builder for HNSW graph. See {@link HnswGraph} for a gloss on the algorithm and the meaning of the
 * hyperparameters.
 */
public final class HnswGraphBuilder {

  // default random seed for level generation
  private static final long DEFAULT_RAND_SEED = System.currentTimeMillis();
  public static final String HNSW_COMPONENT = "HNSW";

  // expose for testing.
  public static long randSeed = DEFAULT_RAND_SEED;

  // These "default" hyper-parameter settings are exposed (and non-final) to enable performance
  // testing
  // since the indexing API doesn't provide any control over them.

  // default max connections per node
  public static int DEFAULT_MAX_CONN = 16;

  // default candidate list size
  public static int DEFAULT_BEAM_WIDTH = 16;

  private final int maxConn;
  private final int beamWidth;
  private final NeighborArray scratch;

  private final VectorValues.SearchStrategy searchStrategy;
  private final RandomAccessVectorValues vectorValues;
  private final Random random;
  private final BoundsChecker bound;
  final HnswGraph hnsw;

  private InfoStream infoStream = InfoStream.getDefault();

  // we need two sources of vectors in order to perform diversity check comparisons without
  // colliding
  private RandomAccessVectorValues buildVectors;

  /** Construct the builder with default configurations */
  public HnswGraphBuilder(RandomAccessVectorValuesProducer vectors) {
    this(vectors, DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, randSeed);
  }

  /**
   * Reads all the vectors from a VectorValues, builds a graph connecting them by their dense
   * ordinals, using the given hyperparameter settings, and returns the resulting graph.
   *
   * @param vectors the vectors whose relations are represented by the graph - must provide a
   *     different view over those vectors than the one used to add via addGraphNode.
   * @param maxConn the number of connections to make when adding a new graph node; roughly speaking
   *     the graph fanout.
   * @param beamWidth the size of the beam search to use when finding nearest neighbors.
   * @param seed the seed for a random number generator used during graph construction. Provide this
   *     to ensure repeatable construction.
   */
  public HnswGraphBuilder(
      RandomAccessVectorValuesProducer vectors, int maxConn, int beamWidth, long seed) {
    vectorValues = vectors.randomAccess();
    buildVectors = vectors.randomAccess();
    searchStrategy = vectorValues.searchStrategy();
    if (searchStrategy == VectorValues.SearchStrategy.NONE) {
      throw new IllegalStateException("No distance function");
    }
    if (maxConn <= 0) {
      throw new IllegalArgumentException("maxConn must be positive");
    }
    if (beamWidth <= 0) {
      throw new IllegalArgumentException("beamWidth must be positive");
    }
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.hnsw = new HnswGraph(maxConn, searchStrategy);
    bound = BoundsChecker.create(searchStrategy.reversed);
    random = new Random(seed);
    scratch = new NeighborArray(Math.max(beamWidth, maxConn + 1));
  }

  /**
   * Reads all the vectors from two copies of a random access VectorValues. Providing two copies
   * enables efficient retrieval without extra data copying, while avoiding collision of the
   * returned values.
   *
   * @param vectors the vectors for which to build a nearest neighbors graph. Must be an independet
   *     accessor for the vectors
   */
  public HnswGraph build(RandomAccessVectorValues vectors) throws IOException {
    if (vectors == vectorValues) {
      throw new IllegalArgumentException(
          "Vectors to build must be independent of the source of vectors provided to HnswGraphBuilder()");
    }
    long start = System.nanoTime(), t = start;
    // start at node 1! node 0 is added implicitly, in the constructor
    for (int node = 1; node < vectors.size(); node++) {
      addGraphNode(vectors.vectorValue(node));
      if (node % 10000 == 0) {
        if (infoStream.isEnabled(HNSW_COMPONENT)) {
          long now = System.nanoTime();
          infoStream.message(
              HNSW_COMPONENT,
              String.format(
                  Locale.ROOT,
                  "built %d in %d/%d ms",
                  node,
                  ((now - t) / 1_000_000),
                  ((now - start) / 1_000_000)));
          t = now;
        }
      }
    }
    return hnsw;
  }

  public void setInfoStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  /** Inserts a doc with vector value to the graph */
  void addGraphNode(float[] value) throws IOException {
    NeighborQueue candidates =
        HnswGraph.search(value, beamWidth, beamWidth, vectorValues, hnsw, random);

    int node = hnsw.addNode();

    // connect neighbors to the new node, using a diversity heuristic that chooses successive
    // nearest neighbors that are closer to the new node than they are to the previously-selected
    // neighbors
    addDiverseNeighbors(node, candidates, buildVectors);
  }

  private void addDiverseNeighbors(
      int node, NeighborQueue candidates, RandomAccessVectorValues vectors) throws IOException {
    // For each of the beamWidth nearest candidates (going from best to worst), select it only if it
    // is closer to target
    // than it is to any of the already-selected neighbors (ie selected in this method, since the
    // node is new and has no
    // prior neighbors).
    NeighborArray neighbors = hnsw.getNeighbors(node);
    assert neighbors.size() == 0; // new node
    popToScratch(candidates);
    selectDiverse(neighbors, scratch, vectors);

    // Link the selected nodes to the new node, and the new node to the selected nodes (again
    // applying diversity heuristic)
    int size = neighbors.size();
    for (int i = 0; i < size; i++) {
      int nbr = neighbors.node[i];
      NeighborArray nbrNbr = hnsw.getNeighbors(nbr);
      nbrNbr.add(node, neighbors.score[i]);
      if (nbrNbr.size() > maxConn) {
        diversityUpdate(nbrNbr, buildVectors);
      }
    }
  }

  private void selectDiverse(
      NeighborArray neighbors, NeighborArray candidates, RandomAccessVectorValues vectors)
      throws IOException {
    // Select the best maxConn neighbors of the new node, applying the diversity heuristic
    for (int i = candidates.size() - 1; neighbors.size() < maxConn && i >= 0; i--) {
      // compare each neighbor (in distance order) against the closer neighbors selected so far,
      // only adding it if it is closer to the target than to any of the other selected neighbors
      int cNode = candidates.node[i];
      float cScore = candidates.score[i];
      if (diversityCheck(vectors.vectorValue(cNode), cScore, neighbors, buildVectors)) {
        neighbors.add(cNode, cScore);
      }
    }
  }

  private void popToScratch(NeighborQueue candidates) {
    scratch.clear();
    int candidateCount = candidates.size();
    // extract all the Neighbors from the queue into an array; these will now be
    // sorted from worst to best
    for (int i = 0; i < candidateCount; i++) {
      float score = candidates.topScore();
      scratch.add(candidates.pop(), score);
    }
  }

  /**
   * @param candidate the vector of a new candidate neighbor of a node n
   * @param score the score of the new candidate and node n, to be compared with scores of the
   *     candidate and n's neighbors
   * @param neighbors the neighbors selected so far
   * @param vectorValues source of values used for making comparisons between candidate and existing
   *     neighbors
   * @return whether the candidate is diverse given the existing neighbors
   */
  private boolean diversityCheck(
      float[] candidate,
      float score,
      NeighborArray neighbors,
      RandomAccessVectorValues vectorValues)
      throws IOException {
    bound.set(score);
    for (int i = 0; i < neighbors.size(); i++) {
      float diversityCheck =
          searchStrategy.compare(candidate, vectorValues.vectorValue(neighbors.node[i]));
      if (bound.check(diversityCheck) == false) {
        return false;
      }
    }
    return true;
  }

  private void diversityUpdate(NeighborArray neighbors, RandomAccessVectorValues vectorValues)
      throws IOException {
    assert neighbors.size() == maxConn + 1;
    int replacePoint = findNonDiverse(neighbors, vectorValues);
    if (replacePoint == -1) {
      // none found; check score against worst existing neighbor
      bound.set(neighbors.score[0]);
      if (bound.check(neighbors.score[maxConn])) {
        // drop the new neighbor; it is not competitive and there were no diversity failures
        neighbors.removeLast();
        return;
      } else {
        replacePoint = 0;
      }
    }
    neighbors.node[replacePoint] = neighbors.node[maxConn];
    neighbors.score[replacePoint] = neighbors.score[maxConn];
    neighbors.removeLast();
  }

  // scan neighbors looking for diversity violations
  private int findNonDiverse(NeighborArray neighbors, RandomAccessVectorValues vectorValues)
      throws IOException {
    for (int i = neighbors.size() - 1; i >= 0; i--) {
      // check each neighbor against its better-scoring neighbors. If it fails diversity check with
      // them, drop it
      int nbrNode = neighbors.node[i];
      bound.set(neighbors.score[i]);
      float[] nbrVector = vectorValues.vectorValue(nbrNode);
      for (int j = maxConn; j > i; j--) {
        float diversityCheck =
            searchStrategy.compare(nbrVector, vectorValues.vectorValue(neighbors.node[j]));
        if (bound.check(diversityCheck) == false) {
          // node j is too similar to node i given its score relative to the base node
          // replace it with the new node, which is at [maxConn]
          return i;
        }
      }
    }
    return -1;
  }
}
