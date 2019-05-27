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

import org.apache.lucene.search.Query;

/**
 * DocValues field for DoubleRange. This is a single valued field i.e. can have only one
 * value per document due to being an extension of BinaryDocValuesField.
 */
public class DoubleRangeDocValuesField extends BinaryRangeDocValuesField{
  final String field;
  final double[] min;
  final double[] max;

  /** Sole constructor. */
  public DoubleRangeDocValuesField(String field, final double[] min, final double[] max) {
    super(field, DoubleRange.encode(min, max), min.length, DoubleRange.BYTES);
    checkArgs(min, max);
    this.field = field;
    this.min = min;
    this.max = max;
  }

  /** Get the minimum value for the given dimension. */
  public double getMin(int dimension) {
    if (dimension > 4 || dimension > min.length) {
      throw new IllegalArgumentException("Dimension out of valid range");
    }

    return min[dimension];
  }

  /** Get the maximum value for the given dimension. */
  public double getMax(int dimension) {
    if (dimension > 4 || dimension > min.length) {
      throw new IllegalArgumentException("Dimension out of valid range");
    }

    return max[dimension];
  }

  private static Query newSlowRangeQuery(String field, final double[] min, final double[] max,
                                        RangeFieldQuery.QueryType queryType) {
    checkArgs(min, max);
    return new DoubleRangeSlowRangeQuery(field, min, max, queryType);
  }

  /** Create a new range query that finds all ranges that intersect using doc values.
   *  NOTE: This doesn't leverage indexing and may be slow.
   *  @see DoubleRange#newIntersectsQuery */
  public static Query newSlowIntersectsQuery(String field, final double[] min, final double[] max) {
    return newSlowRangeQuery(field, min, max, RangeFieldQuery.QueryType.INTERSECTS);
  }

  /**
   * validate the arguments
   */
  private static void checkArgs(final double[] min, final double[] max) {
    if (min == null || max == null || min.length == 0 || max.length == 0) {
      throw new IllegalArgumentException("min/max range values cannot be null or empty");
    }
    if (min.length != max.length) {
      throw new IllegalArgumentException("min/max ranges must agree");
    }

    for (int i = 0; i < min.length; i++) {
      if (min[i] > max[i]) {
        throw new IllegalArgumentException("min should be less than max");
      }
    }
  }
}
