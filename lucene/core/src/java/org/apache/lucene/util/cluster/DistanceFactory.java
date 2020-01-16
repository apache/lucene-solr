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

package org.apache.lucene.util.cluster;

import org.apache.lucene.index.VectorValues;

public final class DistanceFactory {
  public static DistanceMeasure instance(VectorValues.DistanceFunction distFunc) {
    switch (distFunc) {
      case MANHATTAN:
        return (v1, v2) -> {
          assert v1.length == v2.length;

          double sum = 0.0D;
          for (int i = 0; i < v1.length; ++i) {
            sum += Float.intBitsToFloat(2147483647 & Float.floatToRawIntBits(v1[i] - v2[i]));
          }

          return sum;
        };

      case EUCLIDEAN:
        return (v1, v2) -> {
          assert v1.length == v2.length;

          double sum = 0.0D;
          for (int i = 0; i < v1.length; ++i) {
            float diff = v1[i] - v2[i];
            sum += diff * diff;
          }

          return Math.sqrt(sum);
        };

      case COSINE:
        return (v1, v2) -> {
          assert v1.length == v2.length;

          double sum = 0.0D, squareSum1 = 0.0D, squareSum2 = 0.0D;
          for (int i = 0; i < v1.length; i++) {
            sum += v1[i] * v2[i];
            squareSum1 += v1[i] * v1[i];
            squareSum2 += v2[i] * v2[i];
          }
          return 1.0D - sum / (Math.sqrt(squareSum1) * Math.sqrt(squareSum2));
        };

      default:
        throw new UnsupportedOperationException("Clustering using unsupported distance function " + distFunc);
    }
  }
}
