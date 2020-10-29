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

  public static double dotProduct(float[] a, float[] b) {
    assert a.length == b.length;
    double result = 0;
    int dim = a.length;
    for (int i = 0; i < dim; i++) {
      result += a[i] * b[i];
    }
    return result;
  }

  public static double squareDistance(float[] v1, float[] v2) {
    assert v1.length == v2.length;
    double squareSum = 0.0;
    int dim = v1.length;
    for (int i = 0; i < dim; i++) {
      double diff = v1[i] - v2[i];
      squareSum += diff * diff;
    }
    return squareSum;
  }

}
