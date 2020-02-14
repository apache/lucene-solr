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

package org.apache.lucene.codecs;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

public abstract class VectorValues {

  /**
   * Provides the vector value for each document.
   *
   * The float vectors are encoded as bytes. The methods {@link VectorValues#decode} and {@link VectorValues#l2norm}
   * should be used to access the values.
   */
  public abstract BinaryDocValues getValues() throws IOException;

  /**
   * For the given query vector, finds a set of candidate nearest neighbors.
   *
   * We examine each cluster centroid and find the {@code numCentroids} centroids that are closest to the query vector.
   * The documents contained in those clusters are returned as candidate vectors.
   *
   * @param queryVector the query vector.
   * @param numCentroids the number of closest centroids to consider.
   */
  public abstract DocIdSetIterator getNearestVectors(float[] queryVector, int numCentroids) throws IOException;

  public static float[] decode(BytesRef bytes) {
    int numDims = bytes.length / Float.BYTES;
    float[] value = new float[numDims];
    ByteBuffer buffer = ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length);
    buffer.asFloatBuffer().get(value);
    return value;
  }

  public static double l2norm(float[] first, float[] second) {
    double l2norm = 0;
    for (int v = 0; v < first.length; v++) {
      double diff = first[v] - second[v];
      l2norm += diff * diff;
    }
    return Math.sqrt(l2norm);
  }

  public static double l2norm(float[] first, BytesRef second) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(second.bytes, second.offset, second.length);

    double l2norm = 0;
    for (int v = 0; v < first.length; v++) {
      double diff = first[v] - byteBuffer.getFloat();
      l2norm += diff * diff;
    }
    return Math.sqrt(l2norm);
  }
}
