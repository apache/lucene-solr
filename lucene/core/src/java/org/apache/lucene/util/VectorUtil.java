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

package org.apache.lucene.util;
 
/**
 * Utilities for computations with numeric arrays
 */
public final class VectorUtil {

  private VectorUtil() {
  }

  /**
   * Returns the vector dot product of the two vectors. IllegalArgumentException is thrown if the vectors'
   * dimensions differ.
   */
  public static float dotProduct(float[] a, float[] b) {
    if (a.length != b.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
    }
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

  /**
   * Returns the sum of squared differences of the two vectors. IllegalArgumentException is thrown if the vectors'
   * dimensions differ.
   */

  public static float squareDistance(float[] v1, float[] v2) {
    if (v1.length != v2.length) {
      throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + v2.length);
    }
    float squareSum = 0.0f;
    int dim = v1.length;
    for (int i = 0; i < dim; i++) {
      float diff = v1[i] - v2[i];
      squareSum += diff * diff;
    }
    return squareSum;
  }

  /**
   * Modifies the argument to be unit length, dividing by its l2-norm.
   * IllegalArgumentException is thrown for zero vectors.
   */
  public static void l2normalize(float[] v) {
    double squareSum = 0.0f;
    int dim = v.length;
    for (float x : v) {
      squareSum += x * x;
    }
    if (squareSum == 0) {
      throw new IllegalArgumentException("Cannot normalize a zero-length vector");
    }
    double length = Math.sqrt(squareSum);
    for (int i = 0; i < dim; i++) {
      v[i] /= length;
    }
  }


}
