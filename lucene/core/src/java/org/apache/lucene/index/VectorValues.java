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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;

/**
 * Access to per-document vector value.
 */
public abstract class VectorValues extends DocIdSetIterator {

  public static int MAX_DIMENSIONS = 1024;

  /** Sole constructor */
  protected VectorValues() {}

  /**
   * @return the dimension of the vectors
   */
  public abstract int dimension();

  /**
   * TODO: should we use cost() for this? We will want to rely on its always being exactly the number
   * of documents having a value for this field.
   * @return the number of vectors returned by this iterator
   */
  public abstract int size();

  /**
   * @return the score function used to compare these vectors
   */
  public abstract ScoreFunction scoreFunction();

  /**
   * Returns the vector value for the current document ID.
   * It is illegal to call this method after the iterator failed to advance.
   * @return vector value
   */
  public abstract float[] vectorValue() throws IOException;

  /**
   * Returns the binary encoded vector value for the current document ID.
   * It is illegal to call this method after the iterator failed to advance.
   * @return binary value
   */
  public BytesRef binaryValue() throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Provide random access to vectors by <i>ordinal</i>
   * This method does not update the state of the iterator. In particular, {@link #vectorValue()} may return a different value.
   * @param targetOrd a valid ordinal, &ge; 0 and &lt; {@link #size()}.
   */
  public abstract float[] vectorValue(int targetOrd) throws IOException;

  /**
   * @return the k nearest neighbors, to the given vector, approximately, along with their (scoreFunction-specific) scores.
   * Note: scores may be negative. In case ScoreFunction.reversed==true (ie these are distances), the scores here will be the negative
   * of those returned by the ScoreFunction.
   * @param target the vector-valued query
   * @param k the number of docs to return
   * @param fanout control the accuracy/speed tradeoff - larger values give better recall at higher cost
   */
  public abstract TopDocs search(float[] target, int k, int fanout) throws IOException;

  /**
   * @return another vector values that iterates independently over the same underlying vectors. The iterator state is not copied;
   * the new iterator will be positioned at the start.
   */
  public abstract VectorValues copy() throws IOException;

  /**
   * Score function. This is used during indexing and searching of the vectors to determine the nearest neighbors.
   */
  public enum ScoreFunction {
    /** No distance function is used. Note: {@link #search(float[], int, int)}
     * is not supported for fields specifying this score function. */
    NONE(0),

    /** Euclidean distance */
    EUCLIDEAN(1) {
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
    DOT_PRODUCT(2, false) {
      @Override
      public float score(float[] a, float[] b) {
        float res = 0f;
        /*
         If length of vector is larger than 8, we use unrolled dot product to accelerate the calculation
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
    
    /** ID for each enum value; this is persisted to the index and cannot be changed after indexing. */
    final public int id;

    /** If reversed, smaller values are better */
    final public boolean reversed;

    ScoreFunction(int id, boolean reversed) {
      this.id = id;
      this.reversed = reversed;
    }

    ScoreFunction(int id) {
      this(id, true);
    }

    /**
     * Calculates the score between the specified two vectors.
     */
    public float score(float[] v1, float[] v2) {
      throw new UnsupportedOperationException();
    }

    /**
     * Returns the distance function that is specified by the id.
     */
    public static ScoreFunction fromId(int id) {
      for (ScoreFunction d : ScoreFunction.values()) {
        if (d.id == id) {
          return d;
        }
      }
      throw new IllegalArgumentException("no such distance function with id " + id);
    }
  }

   /**
   * Calculates a similarity score between the two vectors with specified function.
   */
  public static float compare(float[] v1, float[] v2, ScoreFunction scoreFunction) {
    assert v1.length == v2.length : "attempt to compare vectors of lengths: " + v1.length + " " + v2.length;
    return scoreFunction.score(v1, v2);
  }

  public static VectorValues EMPTY = new VectorValues() {

    private final TopDocs EMPTY_RESULT = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);

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
      throw new IndexOutOfBoundsException("Attempt to get vectors from EMPTY values");
    }

    @Override
    public float[] vectorValue(int ord) {
      throw new IndexOutOfBoundsException("Attempt to get vectors from EMPTY values");
    }

    @Override
    public TopDocs search(float[] target, int k, int fanout) {
      return EMPTY_RESULT;
    }

    @Override
    public int docID() {
      return -1;
    }

    @Override
    public int nextDoc() {
      return -1;
    }

    @Override
    public int advance(int target) {
      return -1;
    }

    @Override
    public long cost() {
      return 0;
    }

    @Override
    public VectorValues copy() {
      // a copy would be indistinguishable
      return this;
    }
  };
}
