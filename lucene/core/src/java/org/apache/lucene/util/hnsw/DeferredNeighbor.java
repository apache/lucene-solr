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
import java.util.Arrays;

import org.apache.lucene.index.VectorValues;

/** Calculates the distance as needed basis */
final class DeferredNeighbor extends Neighbor {

  final int docId;
  float[] query;
  VectorValues vectorValues;
  VectorValues.DistanceFunction distFunc;

  DeferredNeighbor(int docId) {
    this.docId = docId;
  }

  @Override
  public int docId() {
    return docId;
  }

  @Override
  public boolean isDeferred() {
    return true;
  }

  @Override
  public void prepareQuery(float[] query, VectorValues vectorValues, VectorValues.DistanceFunction distFunc) {
    this.query = query;
    this.vectorValues = vectorValues;
    this.distFunc = distFunc;
  }

  @Override
  public float distance() {
    if (query == null || vectorValues == null || distFunc == null) {
      throw new IllegalStateException("cannot calculate the distance: query, vector values, or distFunc are not set.");
    }
    try {
      if (vectorValues.seek(docId)) {
        return VectorValues.distance(query, vectorValues.vectorValue(), distFunc);
      } else {
        throw new IllegalStateException("docId=" + docId + " has no vector value");
      }
    } catch (IOException e) {
      throw new RuntimeException("cannot calculate the distance between docId=" + docId + " and query=" + Arrays.toString(query), e);
    }
  }

  @Override
  public int compareTo(Neighbor o) {
    return this.docId() < o.docId() ? -1 : 1;
  }
}

