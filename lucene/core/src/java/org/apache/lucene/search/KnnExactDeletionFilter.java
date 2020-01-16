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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.hnsw.HNSWGraphReader;
import org.apache.lucene.util.hnsw.Neighbor;
import org.apache.lucene.util.hnsw.Neighbors;

/**
 * {@code KnnExactDeletionFilter} applies in-set (i.e. the query vector is exactly in the index)
 * deletion strategy to filter all unmatched results searched by {@link KnnExactDeletionCondition},
 * and deletes at most ef*segmentCnt vectors that are the same to the specified queryVector.
 */
public class KnnExactDeletionFilter extends KnnScoreWeight {
  KnnExactDeletionFilter(Query query, float score, ScoreMode scoreMode, String field, float[] queryVector, int ef) {
    super(query, score, scoreMode, field, queryVector, ef);
  }

  @Override
  public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    int numDimensions = fi.getVectorNumDimensions();
    if (numDimensions != queryVector.length) {
      throw new IllegalArgumentException("field=\"" + field + "\" was indexed with dimensions=" + numDimensions +
          "; this is incompatible with query dimensions=" + queryVector.length);
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
        final Neighbors neighbors = hnswReader.searchNeighbors(queryVector, ef, vectorValues);
        visitedCounter.addAndGet(hnswReader.getVisitedCount());

        if (neighbors.size() > 0) {
          Neighbor top = neighbors.top();
          if (top.distance() > 0) {
            neighbors.clear();
          } else {
            final List<Neighbor> toDeleteNeighbors = new ArrayList<>(neighbors.size());
            for (Neighbor neighbor : neighbors) {
              if (neighbor.distance() == 0) {
                toDeleteNeighbors.add(neighbor);
              } else {
                break;
              }
            }

            neighbors.clear();

            toDeleteNeighbors.forEach(neighbor -> neighbors.add(neighbor));
          }
        }

        return new Scorer(weight) {

          int doc = -1;
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
                } else {
                  while (offset < target) {
                    neighbors.pop();
                    offset++;
                  }
                  Neighbor next = neighbors.pop();
                  offset++;
                  if (next == null) {
                    doc = NO_MORE_DOCS;
                  } else {
                    doc = next.docId();
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
            return 0.0f;
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
}
