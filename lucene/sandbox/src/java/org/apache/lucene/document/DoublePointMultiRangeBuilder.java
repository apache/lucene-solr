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

package org.apache.lucene.document;

import org.apache.lucene.search.MultiRangeQuery;

import static org.apache.lucene.document.DoublePoint.decodeDimension;
import static org.apache.lucene.document.DoublePoint.pack;

/** Builder for multi range queries for DoublePoints */
public class DoublePointMultiRangeBuilder extends MultiRangeQuery.Builder {
  public DoublePointMultiRangeBuilder(String field, int numDims) {
    super(field, Double.BYTES, numDims);
  }

  @Override
  public MultiRangeQuery build() {
    return new MultiRangeQuery(field, numDims, bytesPerDim, clauses) {
      @Override
      protected String toString(int dimension, byte[] value) {
        return Double.toString(decodeDimension(value, 0));
      }
    };
  }

  public void add(double[] lowerValue, double[] upperValue) {
    if (upperValue.length != numDims || lowerValue.length != numDims) {
      throw new IllegalArgumentException("Passed in range does not conform to specified dimensions");
    }

    for (int i = 0; i < numDims; i++) {
      if (upperValue[i] < lowerValue[i]) {
        throw new IllegalArgumentException("Upper value of range should be greater than lower value of range");
      }
    }
    add(pack(lowerValue).bytes, pack(upperValue).bytes);
  }
}

