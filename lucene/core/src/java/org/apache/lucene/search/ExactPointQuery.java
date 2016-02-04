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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;

/** Searches for single points in fields previously indexed using points
 *  e.g. {@link org.apache.lucene.document.LongPoint}. */

public class ExactPointQuery extends Query {
  final String field;
  final int numDims;
  final byte[][] point;
  final int bytesPerDim;

  public ExactPointQuery(String field, byte[][] point) {
    this.field = field;
    if (point == null) {
      throw new IllegalArgumentException("point must not be null");
    }
    this.point = point;
    this.numDims = point.length;

    int bytesPerDim = -1;
    for(byte[] value : point) {
      if (value == null) {
        throw new IllegalArgumentException("point's dimensional values must not be null");
      }
      if (bytesPerDim == -1) {
        bytesPerDim = value.length;
      } else if (value.length != bytesPerDim) {
        throw new IllegalArgumentException("all dimensions must have same bytes length, but saw " + bytesPerDim + " and " + value.length);
      }
    }
    this.bytesPerDim = bytesPerDim;
  }

  /** Use in the 1D case when you indexed 1D int values using {@link org.apache.lucene.document.IntPoint} */
  public static ExactPointQuery new1DIntExact(String field, int value) {
    return new ExactPointQuery(field, pack(value));
  }

  /** Use in the 1D case when you indexed 1D long values using {@link org.apache.lucene.document.LongPoint} */
  public static ExactPointQuery new1DLongExact(String field, long value) {
    return new ExactPointQuery(field, pack(value));
  }

  /** Use in the 1D case when you indexed 1D float values using {@link org.apache.lucene.document.FloatPoint} */
  public static ExactPointQuery new1DFloatExact(String field, float value) {
    return new ExactPointQuery(field, pack(value));
  }

  /** Use in the 1D case when you indexed 1D double values using {@link org.apache.lucene.document.DoublePoint} */
  public static ExactPointQuery new1DDoubleExact(String field, double value) {
    return new ExactPointQuery(field, pack(value));
  }

  private static byte[][] pack(long value) {
    byte[][] result = new byte[][] {new byte[RamUsageEstimator.NUM_BYTES_LONG]};
    NumericUtils.longToBytes(value, result[0], 0);
    return result;
  }

  private static byte[][] pack(double value) {
    byte[][] result = new byte[][] {new byte[RamUsageEstimator.NUM_BYTES_LONG]};
    NumericUtils.longToBytesDirect(NumericUtils.doubleToSortableLong(value), result[0], 0);
    return result;
  }

  private static byte[][] pack(int value) {
    byte[][] result = new byte[][] {new byte[RamUsageEstimator.NUM_BYTES_INT]};
    NumericUtils.intToBytes(value, result[0], 0);
    return result;
  }

  private static byte[][] pack(float value) {
    byte[][] result = new byte[][] {new byte[RamUsageEstimator.NUM_BYTES_INT]};
    NumericUtils.intToBytesDirect(NumericUtils.floatToSortableInt(value), result[0], 0);
    return result;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    boolean[] inclusive = new boolean[] {true};
    return new PointRangeQuery(field, point, inclusive, point, inclusive);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash += Arrays.hashCode(point)^0x14fa55fb;
    hash += numDims^0x14fa55fb;
    hash += Objects.hashCode(bytesPerDim);
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (super.equals(other)) {
      final ExactPointQuery q = (ExactPointQuery) other;
      return q.numDims == numDims &&
        q.bytesPerDim == bytesPerDim &&
        Arrays.equals(point, q.point);
    }

    return false;
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append("field=");
      sb.append(this.field);
      sb.append(':');
    }

    return sb.append(" point=")
      .append(Arrays.toString(point))
      .toString();
  }
}
