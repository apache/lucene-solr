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
import java.util.List;
import java.util.Random;

import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

/**
 * Builder for HNSW graph.
 */
public final class HnswGraphBuilder {

  // default random seed for level generation
  private static final long DEFAULT_RAND_SEED = System.currentTimeMillis();

  // expose for testing. TODO: make a better way to initialize this
  public static long randSeed = DEFAULT_RAND_SEED;

  // default max connections per node
  public static final int DEFAULT_MAX_CONNECTIONS = 16;

  // default candidate list size
  public static final int DEFAULT_EF_CONST = 50;

  private final int maxConn;
  private final int efConst;
  private final BoundedVectorValues boundedVectors;
  private final VectorValues.ScoreFunction scoreFunction;
  private final HnswGraph hnsw;
  private final Random random;

  /** Construct the builder with default configurations */
  private HnswGraphBuilder(VectorValues.RandomAccess vectors) {
    this(DEFAULT_MAX_CONNECTIONS, DEFAULT_EF_CONST, randSeed, vectors);
  }

  /** Full constructor */
  private HnswGraphBuilder(int maxConn, int efConst, long seed, VectorValues.RandomAccess vectors) {
    scoreFunction = vectors.scoreFunction();
    if (scoreFunction == VectorValues.ScoreFunction.NONE) {
      throw new IllegalStateException("No distance function");
    }
    this.maxConn = maxConn;
    this.efConst = efConst;
    boundedVectors = new BoundedVectorValues(vectors);
    this.hnsw = new HnswGraph();
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
    Neighbors results = HnswGraph.search(value, efConst, 2 * efConst, boundedVectors, graphValues, random);

    // Get the best maxConn nodes
    Neighbors nn = Neighbors.create(maxConn, scoreFunction.reversed);
    for (Neighbor n : results) {
      nn.insertWithOverflow(n);
    }
    // Sort by node id, ascending
    List<Neighbor> sortedByNodeId = new ArrayList<>();
    for (Neighbor n : nn) {
      sortedByNodeId.add(n);
    }
    sortedByNodeId.sort(Comparator.comparingInt(Neighbor::node));
    // add arcs
    for (Neighbor n : sortedByNodeId) {
      // TODO: shrink to maxConn
      hnsw.connectNodes(n.node, boundedVectors.size, n.score, 0);
    }
  }

  /**
   * Provides a random access VectorValues view over a delegate VectorValues, bounding the maximum ord.
   */
  private static class BoundedVectorValues implements VectorValues.RandomAccess {

    final VectorValues.RandomAccess raDelegate;

    int size;

    BoundedVectorValues(VectorValues.RandomAccess delegate) {
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
    public VectorValues.ScoreFunction scoreFunction() {
      return raDelegate.scoreFunction();
    }

    @Override
    public float[] vectorValue(int target) throws IOException {
      return raDelegate.vectorValue(target);
    }

    @Override
    public BytesRef binaryValue(int targetOrd) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TopDocs search(float[] target, int k, int fanout) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Reads all the vectors from a VectorValues, writes a graph connecting them by their dense ordinals and
   * returns the resulting graph.
   */
  public static HnswGraph build(VectorValues vectorValues) throws IOException {
    return build(vectorValues.randomAccess(), vectorValues.randomAccess());
  }

  /**
   * Reads all the vectors from two copies of a random access VectorValues. Providing two copies enables efficient retrieval
   * without extra memcpy, while avoiding collision of the returned values.
   * @param vectors the vectors for which to build a nearest neighbors graph
   * @param vcopy a copy of the same vectors
   */
  public static HnswGraph build(VectorValues.RandomAccess vectors, VectorValues.RandomAccess vcopy) throws IOException {
    HnswGraphBuilder builder = new HnswGraphBuilder(vectors);
    for (int node = 1; node < vcopy.size(); node++) {
      builder.insert(vcopy.vectorValue(node));
    }
    return builder.hnsw;
  }

}
