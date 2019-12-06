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

package org.apache.lucene.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.Arrays;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;

/**
 * Access to per-document vector value.
 */
public abstract class VectorValues extends DocIdSetIterator {

  public static int MAX_DIMENSIONS = 1024;

  /** Sole constructor */
  protected VectorValues() {}

  /**
   * Returns the vector value for the current document ID.
   * It is illegal to call this method after {@link #seek(int)}
   * returned {@code false}.
   * @return vector value
   */
  public abstract float[] vectorValue() throws IOException;

  /**
   * Returns the binary encoded vector value for the current document ID.
   * It is illegal to call this method after {@link #seek(int)}
   * returned {@code false}.
   * @return binary value
   */
  public BytesRef binaryValue() throws IOException {
    return encode(vectorValue());
  }

  /** Move the pointer to exactly {@code target} and return whether
   *  {@code target} has a value.
   *  {@code target} must be a valid doc ID, ie. &ge; 0 and &lt; {@code maxDoc}.
   *  After this method returns, {@link #docID()} retuns {@code target}. */
  public abstract boolean seek(int target) throws IOException;

  /**
   * Calculates the distance between the two vectors with specified distance function.
   */
  public static float distance(float[] v1, float[] v2, DistanceFunction distFunc) {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException("Incompatible number of dimensions: " + v1.length + ", " + v2.length);
    }
    return distFunc.distance(v1, v2);
  }

  /**
   * Encodes float array to byte array.
   */
  public static BytesRef encode(float[] value) {
    byte[] bytes = new byte[Float.BYTES * value.length];
    for (int i = 0; i < value.length; i++) {
      int bits = Float.floatToIntBits(value[i]);
      bytes[i * Float.BYTES] = (byte)(bits >> 24);
      bytes[i * Float.BYTES + 1] = (byte)(bits >> 16);
      bytes[i * Float.BYTES + 2] = (byte)(bits >> 8);
      bytes[i * Float.BYTES + 3] = (byte)(bits);
    }
    return new BytesRef(bytes);
  }

  public static boolean verifyNumDimensions(int numBytes, int numDims) {
    if (numBytes % Float.BYTES != 0) {
      throw new IllegalArgumentException("Cannot decode bytes array to float array. Reason: invalid length: " + numBytes);
    }
    int dims = numBytes / Float.BYTES;
    if (dims != numDims) {
      throw new IllegalArgumentException("Invalid dimensions: " + dims + " (the number of dimensions should be " + numDims + ")");
    }
    return true;
  }

  /**
   * Decodes float array from byte array. TODO: allow caller to supply the array so they control allocation.
   */
  public static float[] decode(BytesRef bytes, int numDims) {
    verifyNumDimensions(bytes.length, numDims);
    float[] value = new float[numDims];
    ByteBuffer buffer = ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length);
    buffer.asFloatBuffer().get(value);
    return value;
  }

  /**
   * Distance function. This is used when both of indexing knn graph and searching.
   */
  public enum DistanceFunction {
    /** No distance function is used.
     * Note: knn graph is not indexed for the field. */
    NONE(0),

    /** Manhattan distance */
    MANHATTAN(1) {
      @Override
      float distance(float[] v1, float[] v2) {
        assert v1.length == v2.length;
        if (Arrays.equals(v1, v2)) {
          return 0.0f;
        }
        float d = 0.0f;
        int dim = v1.length;
        for (int i = 0; i < dim; i++) {
          d += Math.abs(v1[i] - v2[i]);
        }
        return d;
      }
    },

    /** Euclidean distance */
    EUCLIDEAN(2) {
      @Override
      float distance(float[] v1, float[] v2) {
        assert v1.length == v2.length;
        if (Arrays.equals(v1, v2)) {
          return 0.0f;
        }
        float squareSum = 0.0f;
        int dim = v1.length;
        for (int i = 0; i < dim; i++) {
          float diff = v1[i] - v2[i];
          squareSum += diff * diff;
        }
        return (float)Math.sqrt(squareSum);
      }
    },

    /** Cosine distance */
    COSINE(3) {
      @Override
      float distance(float[] v1, float[] v2) {
        // TODO
        return 0.0f;
      }
    };

    /** ID for each enum value; this is supposed to be persisted to the index and cannot be changed after indexing. */
    final int id;

    /** Sole constructor */
    DistanceFunction(int id) {
      this.id = id;
    }

    /**
     * Returns the id of the distance function.
     * @return id
     */
    public int getId() {
      return id;
    }

    /**
     * Calculates the distance between the specified two vectors.
     */
    float distance(float[] v1, float[] v2) {
      throw new UnsupportedOperationException();
    }

    /**
     * Returns the distance function that is specified by the id.
     */
    public static DistanceFunction fromId(int id) {
      for (DistanceFunction d : DistanceFunction.values()) {
        if (d.id == id) {
          return d;
        }
      }
      throw new IllegalArgumentException("no such distance function with id " + id);
    }
  }

  public static VectorValues EMPTY = new VectorValues() {
    @Override
    public float[] vectorValue() throws IOException {
      return new float[0];
    }

    @Override
    public boolean seek(int target) throws IOException {
      return false;
    }

    @Override
    public int docID() {
      return -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return -1;
    }

    @Override
    public int advance(int target) throws IOException {
      return -1;
    }

    @Override
    public long cost() {
      return 0;
    }
  };
}
