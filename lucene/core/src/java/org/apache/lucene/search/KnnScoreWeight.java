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

package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.hnsw.HNSWGraphReader;
import org.apache.lucene.util.hnsw.Neighbor;
import org.apache.lucene.util.hnsw.Neighbors;

class KnnScoreWeight extends ConstantScoreWeight {

  private final String field;
  private final ScoreMode scoreMode;
  private final float[] queryVector;
  private final int ef;

  KnnScoreWeight(Query query, float score, ScoreMode scoreMode, String field, float[] queryVector, int ef) {
    super(query, score);
    this.scoreMode = scoreMode;
    this.field = field;
    this.queryVector = queryVector;
    this.ef = ef;
  }

  @Override
  public Scorer scorer(LeafReaderContext context) throws IOException {
    ScorerSupplier supplier = scorerSupplier(context);
    if (supplier == null) {
      return null;
    }
    return supplier.get(Long.MAX_VALUE);
  }

  @Override
  public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    int numDimensions = fi.getVectorNumDimensions();
    if (numDimensions != queryVector.length) {
      throw new IllegalArgumentException("field=\"" + field + "\" was indexed with dimensions=" + numDimensions + "; this is incompatible with query dimensions=" + queryVector.length);
    }

    final HNSWGraphReader hnswReader = new HNSWGraphReader(field, context);
    final VectorValues vectorValues = context.reader().getVectorValues(field);
    if (vectorValues == null) {
      // No docs in this segment/field indexed any vector values
      return null;
    }

    final Weight weight = this;
    return new ScorerSupplier() {
      @Override
      public Scorer get(long leadCost) throws IOException {
        Neighbors neighbors = hnswReader.searchNeighbors(queryVector, ef, vectorValues);
        return new Scorer(weight) {

          int doc = -1;
          float score = 0.0f;
          int size = neighbors.size();
          int offset = 0;

          @Override
          public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
              @Override
              public int docID() {
                return doc;
              }

              @Override
              public int nextDoc() throws IOException {
                return advance(offset);
              }

              @Override
              public int advance(int target) throws IOException {
                if (target > size || neighbors.size() == 0) {
                  doc = NO_MORE_DOCS;
                  score = 0.0f;
                } else {
                  while (offset < target) {
                    neighbors.pop();
                    offset++;
                  }
                  Neighbor next = neighbors.pop();
                  offset++;
                  if (next == null) {
                    doc = NO_MORE_DOCS;
                    score = 0.0f;
                  } else {
                    doc = next.docId();
                    switch (fi.getVectorDistFunc()) {
                      case MANHATTAN:
                      case EUCLIDEAN:
                        score = 1.0f / (next.distance() / numDimensions + 0.01f);
                        break;
                      case COSINE:
                        score = 1.0f - next.distance();
                        break;
                      default:
                        break;
                    }
                  }
                }
                return doc;
              }

              @Override
              public long cost() {
                return size;
              }
            };
          }

          @Override
          public float getMaxScore(int upTo) throws IOException {
            return Float.POSITIVE_INFINITY;
          }

          @Override
          public float score() throws IOException {
            return score;
          }

          @Override
          public int docID() {
            return doc;
          }
        };
      }

      @Override
      public long cost() {
        return ef;
      }
    };
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }
}
