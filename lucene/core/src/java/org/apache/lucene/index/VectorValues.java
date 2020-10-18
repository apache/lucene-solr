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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

/**
 * This class provides access to per-document floating point vector values indexed as {@link
 * org.apache.lucene.document.VectorField}.
 *
 * @lucene.experimental
 */
public abstract class VectorValues extends DocIdSetIterator {

  /** The maximum length of a vector */
  public static int MAX_DIMENSIONS = 1024;

  /** Sole constructor */
  protected VectorValues() {}

  /**
   * Return the dimension of the vectors
   */
  public abstract int dimension();

  /**
   * TODO: should we use cost() for this? We rely on its always being exactly the number
   * of documents having a value for this field, which is not guaranteed by the cost() contract,
   * but in all the implementations so far they are the same.
   * @return the number of vectors returned by this iterator
   */
  public abstract int size();

  /**
   * Return the score function used to compare these vectors
   */
  public abstract ScoreFunction scoreFunction();

  /**
   * Return the vector value for the current document ID.
   * It is illegal to call this method when the iterator is not positioned: before advancing, or after failing to advance.
   * The returned array may be shared across calls, re-used, and modified as the iterator advances.
   * @return the vector value
   */
  public abstract float[] vectorValue() throws IOException;

  /**
   * Return the binary encoded vector value for the current document ID. These are the bytes
   * corresponding to the float array return by {@link #vectorValue}.  It is illegal to call this
   * method when the iterator is not positioned: before advancing, or after failing to advance.  The
   * returned storage may be shared across calls, re-used and modified as the iterator advances.
   * @return the binary value
   */
  public BytesRef binaryValue() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Return a random access interface over this iterator's vectors. Calling the RandomAccess methods will
   * have no effect on the progress of the iteration or the values returned by this iterator. Successive calls
   * will retrieve independent copies that do not overwrite each others' returned values.
   */
  public abstract RandomAccess randomAccess();

  /**
   * Provides random access to vectors by dense ordinal.
   *
   * @lucene.experimental
   */
  public interface RandomAccess {

    /**
     * Return the number of vector values
     */
    int size();

    /**
     * Return the dimension of the returned vector values
     */
    int dimension();

    /**
     * Return the score function used to compare these vectors
     */
    ScoreFunction scoreFunction();

    /**
     * Return the vector value indexed at the given ordinal. The provided floating point array may
     * be shared and overwritten by subsequent calls to this method and {@link #binaryValue(int)}.
     * @param targetOrd a valid ordinal, &ge; 0 and &lt; {@link #size()}.
     */
    float[] vectorValue(int targetOrd) throws IOException;

    /**
     * Return the vector indexed at the given ordinal value as an array of bytes in a BytesRef;
     * these are the bytes corresponding to the float array. The provided bytes may be shared and overwritten 
     * by subsequent calls to this method and {@link #vectorValue(int)}.
     * @param targetOrd a valid ordinal, &ge; 0 and &lt; {@link #size()}.
     */
    BytesRef binaryValue(int targetOrd) throws IOException;

    /**
     * Return the k nearest neighbor documents as determined by comparison of their vector values
     * for this field, to the given vector, by the field's score function. If the score function is
     * reversed, lower values indicate nearer vectors, otherwise higher scores indicate nearer
     * vectors. Unlike relevance scores, vector scores may be negative.
     * @param target the vector-valued query
     * @param k      the number of docs to return
     * @param fanout control the accuracy/speed tradeoff - larger values give better recall at higher cost
     * @return the k nearest neighbor documents, along with their (scoreFunction-specific) scores.
     */
    TopDocs search(float[] target, int k, int fanout) throws IOException;
  }

  /**
   * Score function. This is used during indexing and searching of the vectors to determine the nearest neighbors.
   * Score values may be negative. By default high scores indicate nearer documents, unless the function is reversed.
   */
  public enum ScoreFunction {
    /** No distance function is used. Note: {@link VectorValues.RandomAccess#search(float[], int, int)}
     * is not supported for fields specifying this score function. */
    NONE,

    /** Euclidean distance */
    EUCLIDEAN(true) {
      @Override
      public float score(float[] v1, float[] v2) {
        assert v1.length == v2.length;
        float squareSum = 0.0f;
        int dim = v1.length;
        for (int i = 0; i < dim; i++) {
          float diff = v1[i] - v2[i];
          squareSum += diff * diff;
        }
        return squareSum;
      }
    },

    /** dot product - note, may be negative; larger values are better */
    DOT_PRODUCT() {
      @Override
      public float score(float[] a, float[] b) {
        float res = 0f;
        /*
         * If length of vector is larger than 8, we use unrolled dot product to accelerate the
         * calculation.
         */
        int i;
        for (i = 0; i < a.length % 8; i++) {
            res += b[i] * a[i];
        }
        if (a.length < 8) {
            return res;
        }
        float s0 = 0f;
        float s1 = 0f;
        float s2 = 0f;
        float s3 = 0f;
        float s4 = 0f;
        float s5 = 0f;
        float s6 = 0f;
        float s7 = 0f;
        for (; i + 7 < a.length; i += 8) {
            s0 += b[i] * a[i];
            s1 += b[i + 1] * a[i + 1];
            s2 += b[i + 2] * a[i + 2];
            s3 += b[i + 3] * a[i + 3];
            s4 += b[i + 4] * a[i + 4];
            s5 += b[i + 5] * a[i + 5];
            s6 += b[i + 6] * a[i + 6];
            s7 += b[i + 7] * a[i + 7];
        }
        res += s0 + s1 + s2 + s3 + s4 + s5 + s6 + s7;
        return res;
      }
    };

    /** If reversed, smaller values are better */
    final public boolean reversed;

    ScoreFunction(boolean reversed) {
      this.reversed = reversed;
    }

    ScoreFunction() {
      this(false);
    }

    /**
     * Calculates the score between the specified two vectors.
     */
    public float score(float[] v1, float[] v2) {
      throw new UnsupportedOperationException();
    }

  }

   /**
   * Calculates a similarity score between the two vectors with specified function.
   */
  public static float compare(float[] v1, float[] v2, ScoreFunction scoreFunction) {
    assert v1.length == v2.length : "attempt to compare vectors of lengths: " + v1.length + " " + v2.length;
    return scoreFunction.score(v1, v2);
  }

  /**
   * Represents the lack of vector values. It is returned by providers that do not
   * support VectorValues.
   */
  public static final VectorValues EMPTY = new VectorValues() {

    @Override
    public int size() {
      return 0;
    }

    @Override
    public int dimension() {
      return 0;
    }

    @Override
    public ScoreFunction scoreFunction() {
      return ScoreFunction.NONE;
    }

    @Override
    public float[] vectorValue() {
      throw new IllegalStateException("Attempt to get vectors from EMPTY values (which was not advanced)");
    }

    @Override
    public RandomAccess randomAccess() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
      throw new IllegalStateException("VectorValues is EMPTY, and not positioned on a doc");
    }

    @Override
    public int nextDoc() {
      return NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) {
      return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return 0;
    }
  };
}
