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

import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.RandomAccessVectorValues;
import org.apache.lucene.index.RandomAccessVectorValuesProducer;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Builder for HNSW graph. See {@link HnswGraph} for a gloss on the algorithm and the meaning of the hyperparameters.
 */
public final class HnswGraphBuilder {

  // default random seed for level generation
  private static final long DEFAULT_RAND_SEED = System.currentTimeMillis();
  public static final String HNSW_COMPONENT = "HNSW";

  // expose for testing.
  public static long randSeed = DEFAULT_RAND_SEED;

  // These "default" hyperparameter settings are exposed (and nonfinal) to enable performance testing
  // since the indexing API doesn't provide any control over them.

  // default max connections per node
  public static int DEFAULT_MAX_CONN = 16;

  // default candidate list size
  static int DEFAULT_BEAM_WIDTH = 16;

  private final int maxConn;
  private final int beamWidth;

  // TODO: how to pass this in?
  InfoStream infoStream = InfoStream.getDefault();
  // InfoStream infoStream = new PrintStreamInfoStream(System.out);

  private final BoundedVectorValues boundedVectors;
  private final VectorValues.SearchStrategy searchStrategy;
  private final HnswGraph hnsw;
  private final Random random;

  /**
   * Reads all the vectors from a VectorValues, builds a graph connecting them by their dense ordinals, using default
   * hyperparameter settings, and returns the resulting graph.
   * @param vectorValues the vectors whose relations are represented by the graph
   */
  public static HnswGraph build(RandomAccessVectorValuesProducer vectorValues) throws IOException {
    HnswGraphBuilder builder = new HnswGraphBuilder(vectorValues);
    return builder.build(vectorValues.randomAccess());
  }

  /**
   * Reads all the vectors from a VectorValues, builds a graph connecting them by their dense ordinals, using the given
   * hyperparameter settings, and returns the resulting graph.
   * @param vectorValues the vectors whose relations are represented by the graph
   * @param maxConn the number of connections to make when adding a new graph node; roughly speaking the graph fanout.
   * @param beamWidth the size of the beam search to use when finding nearest neighbors.
   * @param seed the seed for a random number generator used during graph construction. Provide this to ensure repeatable construction.
   */
  public static HnswGraph build(RandomAccessVectorValuesProducer vectorValues, int maxConn, int beamWidth, long seed) throws IOException {
    HnswGraphBuilder builder = new HnswGraphBuilder(vectorValues, maxConn, beamWidth, seed);
    return builder.build(vectorValues.randomAccess());
  }

  /**
   * Reads all the vectors from two copies of a random access VectorValues. Providing two copies enables efficient retrieval
   * without extra data copying, while avoiding collision of the returned values.
   * @param vectors the vectors for which to build a nearest neighbors graph. Must be an independet accessor for the vectors
   */
  HnswGraph build(RandomAccessVectorValues vectors) throws IOException {
    if (vectors == boundedVectors.raDelegate) {
      throw new IllegalArgumentException("Vectors to build must be independent of the source of vectors provided to HnswGraphBuilder()");
    }
    long start = System.nanoTime(), t = start;
    for (int node = 1; node < vectors.size(); node++) {
      insert(vectors.vectorValue(node));
      if (node % 10000 == 0) {
        if (infoStream.isEnabled(HNSW_COMPONENT)) {
          long now = System.nanoTime();
          infoStream.message(HNSW_COMPONENT,
              String.format(Locale.ROOT, "HNSW built %d in %d/%d ms", node, ((now - t) / 1_000_000), ((now - start) / 1_000_000)));
          t = now;
        }
      }
    }
    return hnsw;
  }

  /** Construct the builder with default configurations */
  private HnswGraphBuilder(RandomAccessVectorValuesProducer vectors) {
    this(vectors, DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, randSeed);
  }

  /** Full constructor */
  HnswGraphBuilder(RandomAccessVectorValuesProducer vectors, int maxConn, int beamWidth, long seed) {
    RandomAccessVectorValues vectorValues = vectors.randomAccess();
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
    boundedVectors = new BoundedVectorValues(vectorValues);
    this.hnsw = new HnswGraph(maxConn, searchStrategy);
    random = new Random(seed);
  }

  /** Inserts a doc with vector value to the graph */
  private void insert(float[] value) throws IOException {
    addGraphNode(value);

    // add the vector value
    boundedVectors.inc();
  }

  private void addGraphNode(float[] value) throws IOException {
    KnnGraphValues graphValues = hnsw.getGraphValues();
    Neighbors candidates = HnswGraph.search(value, beamWidth, 2 * beamWidth, boundedVectors, graphValues, random);

    int node = hnsw.addNode();

    // connect the nearest neighbors to the just inserted node
    addNearestNeighbors(node, candidates);
  }

  private void addNearestNeighbors(int newNode, Neighbors neighbors) {
    // connect the nearest neighbors, relying on the graph's Neighbors' priority queues to drop off distant neighbors
    Neighbors.NeighborIterator it = neighbors.iterator();
    for (int node = it.next(); node != NO_MORE_DOCS; node = it.next()) {
      float score = it.score();
      if (hnsw.connect(newNode, node, score)) {
        hnsw.connect(node, newNode, score);
      }
    }
  }

  /**
   * Provides a random access VectorValues view over a delegate VectorValues, bounding the maximum ord.
   * TODO: get rid of this, all it does is track a counter
   */
  private static class BoundedVectorValues implements RandomAccessVectorValues {

    final RandomAccessVectorValues raDelegate;

    int size;

    BoundedVectorValues(RandomAccessVectorValues delegate) {
      raDelegate = delegate;
      if (delegate.size() > 0) {
        // we implicitly add the first node
        size = 1;
      }
    }

    void inc() {
        ++size;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int dimension() { return raDelegate.dimension(); }

    @Override
    public VectorValues.SearchStrategy searchStrategy() {
      return raDelegate.searchStrategy();
    }

    @Override
    public float[] vectorValue(int target) throws IOException {
      return raDelegate.vectorValue(target);
    }

    @Override
    public BytesRef binaryValue(int targetOrd) throws IOException {
      throw new UnsupportedOperationException();
    }
  }


}
