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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;

public final class HNSWGraphWriter implements Accountable {

  private static final int MAX_LEVEL_LIMIT = 5;
  // default random seed for level generation
  private static final long DEFAULT_RAND_SEED = System.currentTimeMillis();
  // expose for testing. TODO: make a better way to initialize this
  public static long RAND_SEED = DEFAULT_RAND_SEED;
  // default max connections per node
  public static final int DEFAULT_MAX_CONNECTIONS = 6;
  // default max connections per node at layer zero
  public static final int DEFAULT_MAX_CONNECTIONS_L0 = DEFAULT_MAX_CONNECTIONS * 2;
  // default candidate list size
  public static final int DEFAULT_EF_CONST = 50;

  private final int maxConn;
  private final int maxConn0;
  private final int efConst;
  private final IntsRefBuilder docsRef;
  private final List<float[]> rawVectors;
  private final int numDimensions;
  private final VectorValues.DistanceFunction distFunc;
  private final HNSWGraph hnsw;
  private final RandomLevelGenerator levelGenerator;

  private int addedDocs = 0;
  private int lastDocID = -1;

  /** Construct the builder with default configurations */
  public HNSWGraphWriter(int numDimensions, VectorValues.DistanceFunction distFunc) {
    this(DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS_L0, DEFAULT_EF_CONST, RAND_SEED, numDimensions, distFunc);
  }

  /** Full constructor */
  public HNSWGraphWriter(int maxConn, int maxConn0, int efConst, long seed, int numDimensions, VectorValues.DistanceFunction distFunc) {
    this.maxConn = maxConn;
    this.maxConn0 = maxConn0;
    this.efConst = efConst;
    this.numDimensions = numDimensions;
    this.distFunc = distFunc;
    this.docsRef = new IntsRefBuilder();
    this.rawVectors = new ArrayList<>();
    this.hnsw = new HNSWGraph(distFunc);
    float ml = (float) (1.0 / Math.log(maxConn));
    this.levelGenerator = new RandomLevelGenerator(seed, ml, MAX_LEVEL_LIMIT);
  }

  /** Inserts a doc with vector value to the graph */
  public void insert(int docId, BytesRef binaryValue) throws IOException {
    // add the vector value
    float[] value = VectorValues.decode(binaryValue, numDimensions);
    rawVectors.add(value);
    docsRef.grow(docId + 1);
    docsRef.setIntAt(docId, addedDocs++);
    docsRef.setLength(docId + 1);
    if (docId > lastDocID + 1) {
      Arrays.fill(docsRef.ints(), lastDocID + 1, docId, -1);
    }
    lastDocID = docId;

    if (distFunc == VectorValues.DistanceFunction.NONE) {
      return;
    }

    // add the graph node
    if (hnsw.isEmpty()) {
      // initialize the graph
      hnsw.ensureLevel(0);
      hnsw.addNode(0, docId);
      return;
    }

    VectorValues vectorValues = getVectorValues();

    int enterPoint = hnsw.getFirstEnterPoint();
    if (!vectorValues.seek(enterPoint)) {
      throw new IllegalStateException("enter point (=" + enterPoint + ") has no vector value");
    }
    float distFromEp = VectorValues.distance(value, vectorValues.vectorValue(), distFunc);
    Neighbor ep = new ImmutableNeighbor(enterPoint, distFromEp);

    int level = levelGenerator.randomLevel();
    FurthestNeighbors results = new FurthestNeighbors(efConst, ep);
    // down to the level from the hnsw's top level
    for (int l = hnsw.topLevel(); l > level; l--) {
      hnsw.searchLayer(value, results, efConst, l, vectorValues);
    }

    // down to level 0 with placing the doc to each layer
    hnsw.ensureLevel(level);
    for (int l = level; l >= 0; l--) {
      if (!hnsw.hasNodes(l)) {
        hnsw.addNode(l, docId);
        continue;
      }

      hnsw.searchLayer(value, results, efConst, l, vectorValues);
      int maxConnections = l == 0 ? maxConn0 : maxConn;
      NearestNeighbors neighbors = new NearestNeighbors(maxConnections, results.top());
      for (Neighbor n : results) {
        neighbors.insertWithOverflow(n);
      }
      for (Neighbor n : neighbors) {
        // TODO: limit *total* num connections by pruning (shrinking)
        hnsw.connectNodes(l, docId, n.docId(), n.distance(), maxConnections);
      }
    }
  }

  public void finish() {
    hnsw.finish();
  }

  public float[][] rawVectorsArray() {
    return rawVectors.toArray(new float[0][]);
  }

  /** Returns the built HNSW graph*/
  public HNSWGraph hnswGraph() {
    return hnsw;
  }

  private VectorValues getVectorValues() {
    return new RandomAccessVectorValues(docsRef.toIntsRef(), rawVectors);
  }

  private static class RandomAccessVectorValues extends VectorValues {

    final IntsRef docsRef;
    final List<float[]> buffer;

    int docID = -1;
    float[] value = new float[0];

    RandomAccessVectorValues(IntsRef docsRef, List<float[]> buffer) {
      this.docsRef = docsRef;
      this.buffer = buffer;
    }

    @Override
    public float[] vectorValue() throws IOException {
      return value;
    }

    @Override
    public boolean seek(int target) throws IOException {
      if (target < 0) {
        throw new IllegalArgumentException("target must be a positive integer: " + target);
      }
      if (target >= docsRef.length) {
        docID = NO_MORE_DOCS;
        return false;
      }
      docID = target;

      int position = docsRef.ints[target];
      if (position < 0) {
        throw new IllegalArgumentException("no vector value for doc: " + target);
      }
      value = buffer.get(position);
      return true;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return docsRef.length;
    }
  }

  @Override
  public long ramBytesUsed() {
    // calculating the exact ram usage is time consuming so we make rough estimation here
    long bytesUsed = RamUsageEstimator.sizeOf(docsRef.ints()) +
        Float.BYTES * numDimensions * rawVectors.size() +
        RamUsageEstimator.shallowSizeOfInstance(HNSWGraph.class) +
        RamUsageEstimator.shallowSizeOfInstance(Layer.class) +
        Integer.BYTES * 2 * DEFAULT_MAX_CONNECTIONS_L0 * addedDocs;
    if (hnsw.topLevel() > 0) {
      for (int l = hnsw.topLevel(); l > 0; l--) {
        bytesUsed += Integer.BYTES * 2 * DEFAULT_MAX_CONNECTIONS * addedDocs;
      }
    }
    return bytesUsed;
  }
}
