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

import org.apache.lucene.index.VectorValues;

/** Builder of {@link HNSWGraph} */
public final class HNSWGraphBuilder {

  private static final int MAX_LEVEL_LIMIT = 5;
  // default random seed for level generation
  private static final long DEFAULT_RAND_SEED = System.currentTimeMillis();
  // default max connections per node
  public static final int DEFAULT_MAX_CONNECTIONS = 6;
  // default max connections per node at layer zero
  public static final int DEFAULT_MAX_CONNECTIONS_L0 = DEFAULT_MAX_CONNECTIONS * 2;
  // default candidate list size
  public static final int DEFAULT_EF_CONST = 50;

  private final int maxConn;
  private final int maxConn0;
  private final int efConst;
  private final VectorValues.DistanceFunction distFunc;
  private final HNSWGraph hnsw;
  private final RandomLevelGenerator levelGenerator;

  /** Construct the builder with default configurations */
  public HNSWGraphBuilder(VectorValues.DistanceFunction distFunc) {
    this(DEFAULT_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS_L0, DEFAULT_EF_CONST, DEFAULT_RAND_SEED, distFunc);
  }

  /** Full constructor */
  public HNSWGraphBuilder(int maxConn, int maxConn0, int efConst, long seed, VectorValues.DistanceFunction distFunc) {
    this.maxConn = maxConn;
    this.maxConn0 = maxConn0;
    this.efConst = efConst;
    this.distFunc = distFunc;
    this.hnsw = new HNSWGraph(distFunc);
    float ml = (float) (1.0 / Math.log(maxConn));
    this.levelGenerator = new RandomLevelGenerator(seed, ml, MAX_LEVEL_LIMIT);
  }

  /** Inserts a doc with vector value to the graph */
  public void insert(int docId, float[] value, VectorValues vectorValues) throws IOException {
    if (hnsw.isEmpty()) {
      // initialize the graph
      hnsw.ensureLevel(0);
      hnsw.addNode(0, docId);
      return;
    }

    int enterPoint = hnsw.getEnterPoint();
    if (!vectorValues.seek(enterPoint)) {
      throw new IllegalStateException("enter point (=" + enterPoint + ") has no vector value");
    }
    float distFromEp = VectorValues.distance(value, vectorValues.vectorValue(), distFunc);
    Neighbor ep = new ImmutableNeighbor(enterPoint, distFromEp);

    int level = levelGenerator.randomLevel();

    // down to the level from the hnsw's top level
    for (int l = hnsw.topLevel(); l > level; l--) {
      NearestNeighbors neighbors = hnsw.searchLayer(value, ep, 1, l, vectorValues);
      ep = neighbors.top();
    }

    // down to level 0 with placing the doc to each layer
    hnsw.ensureLevel(level);
    for (int l = level; l >= 0; l--) {
      if (!hnsw.hasNodes(l)) {
        hnsw.addNode(l, docId);
        continue;
      }

      NearestNeighbors neighbors = hnsw.searchLayer(value, ep, efConst, l, vectorValues);
      ep = neighbors.top();
      int maxConnections = l == 0 ? maxConn0 : maxConn;
      for (int i = 0; i < maxConnections; i++) {
        if (neighbors.size() == 0) {
          break;
        }
        Neighbor n = neighbors.pop();
        hnsw.connectNodes(l, docId, n.docId(), n.distance(), maxConnections);
      }
    }
  }

  /** Returns the built HNSW graph*/
  public HNSWGraph build() {
    hnsw.finish();
    return hnsw;
  }

}
