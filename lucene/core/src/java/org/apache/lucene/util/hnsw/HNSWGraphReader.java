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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.KnnGraphValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.DocIdSetIterator;

/** Executes approximate nearest neighbor search on per-reader {@link HNSWGraph}.
 * This also caches built {@link HNSWGraph}s for repeated use. */
public final class HNSWGraphReader {

  private static final Map<GraphKey, HNSWGraph> cache = new ConcurrentHashMap<>();

  private final String field;
  private final LeafReaderContext context;
  private final VectorValues.DistanceFunction distFunc;

  public HNSWGraphReader(String field, LeafReaderContext context) {
    this.field = field;
    this.context = context;
    this.distFunc = context.reader().getFieldInfos().fieldInfo(field).getVectorDistFunc();
  }

  public Neighbors searchNeighbors(float[] query, int ef, VectorValues vectorValues) throws IOException {
    HNSWGraph hnsw = get(field, context, false);
    int enterPoint = hnsw.getEnterPoint();
    if (!vectorValues.seek(enterPoint)) {
      throw new IllegalStateException("enterPoint=" + enterPoint + " has no vector value");
    }

    Neighbors neighbors;
    Neighbor ep = new ImmutableNeighbor(enterPoint, VectorValues.distance(query, vectorValues.vectorValue(), distFunc));
    for (int l = hnsw.topLevel(); l > 0; l--) {
      neighbors = hnsw.searchLayer(query, ep, 1, l, vectorValues);
      ep = neighbors.top();
    }
    return hnsw.searchLayer(query, ep, ef, 0, vectorValues);
  }

  public static long reloadGraph(String field, IndexReader reader) throws IOException {
    long bytesUsed = 0L;
    for (LeafReaderContext ctx : reader.leaves()) {
      HNSWGraph hnsw = get(field, ctx, true);
      bytesUsed += hnsw.ramBytesUsed();
    }
    return bytesUsed;
  }

  private static HNSWGraph get(String field, LeafReaderContext context, boolean reload) throws IOException {
    GraphKey key = new GraphKey(field, context.id());
    IOException[] exc = new IOException[]{null};
    if (reload) {
      cache.put(key, load(field, context));
    } else {
      cache.computeIfAbsent(key, (k -> {
        try {
          return load(k.field, context);
        } catch (IOException e) {
          exc[0] = e;
          return null;
        }
      }));
      if (exc[0] != null) {
        throw exc[0];
      }
    }
    return cache.get(key);
  }

  private static HNSWGraph load(String field, LeafReaderContext context) throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    int numDimensions = fi.getVectorNumDimensions();
    if (numDimensions == 0) {
      // the field has no vector values
      return null;
    }
    VectorValues.DistanceFunction distFunc = fi.getVectorDistFunc();

    KnnGraphValues graphValues = context.reader().getKnnGraphValues(field);
    HNSWGraph hnsw = new HNSWGraph(distFunc);
    for (int doc = graphValues.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = graphValues.nextDoc()) {
      int maxLevel = graphValues.getMaxLevel();
      hnsw.ensureLevel(maxLevel);
      for (int l = 0; l <= maxLevel; l++) {
        for (int friend : graphValues.getFriends(l).ints) {
          hnsw.connectNodes(l, doc, friend);
        }
      }
    }
    hnsw.finish();

    return hnsw;
  }

  private static class GraphKey {
    final String field;
    final Object readerId;

    GraphKey(String field, Object readerId) {
      this.field = field;
      this.readerId = readerId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GraphKey graphKey = (GraphKey) o;
      return Objects.equals(field, graphKey.field) &&
          Objects.equals(readerId, graphKey.readerId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, readerId);
    }

    public String toString() {
      return "field=" + field + ", readerId=" + readerId;
    }
  }

}
