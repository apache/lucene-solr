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

import java.util.Arrays;
import java.util.Comparator;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * An indexed binary field for fast range filters. If you also need to store the value, you should
 * add a separate {@link StoredField} instance.
 *
 * <p>Finding all documents within an N-dimensional shape or range at search time is efficient.
 * Multiple values for the same field in one document is allowed.
 *
 * <p>This field defines static factory methods for creating common queries:
 *
 * <ul>
 *   <li>{@link #newExactQuery(String, byte[])} for matching an exact 1D point.
 *   <li>{@link #newSetQuery(String, byte[][]) newSetQuery(String, byte[]...)} for matching a set of
 *       1D values.
 *   <li>{@link #newRangeQuery(String, byte[], byte[])} for matching a 1D range.
 *   <li>{@link #newRangeQuery(String, byte[][], byte[][])} for matching points/ranges in
 *       n-dimensional space.
 * </ul>
 *
 * @see PointValues
 */
public final class BinaryPoint extends Field {

  private static FieldType getType(byte[][] point) {
    if (point == null) {
      throw new IllegalArgumentException("point must not be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point must not be 0 dimensions");
    }
    int bytesPerDim = -1;
    for (int i = 0; i < point.length; i++) {
      byte[] oneDim = point[i];
      if (oneDim == null) {
        throw new IllegalArgumentException("point must not have null values");
      }
      if (oneDim.length == 0) {
        throw new IllegalArgumentException("point must not have 0-length values");
      }
      if (bytesPerDim == -1) {
        bytesPerDim = oneDim.length;
      } else if (bytesPerDim != oneDim.length) {
        throw new IllegalArgumentException(
            "all dimensions must have same bytes length; got "
                + bytesPerDim
                + " and "
                + oneDim.length);
      }
    }
    return getType(point.length, bytesPerDim);
  }

  private static FieldType getType(int numDims, int bytesPerDim) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, bytesPerDim);
    type.freeze();
    return type;
  }

  private static BytesRef pack(byte[]... point) {
    if (point == null) {
      throw new IllegalArgumentException("point must not be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point must not be 0 dimensions");
    }
    if (point.length == 1) {
      return new BytesRef(point[0]);
    }
    int bytesPerDim = -1;
    for (byte[] dim : point) {
      if (dim == null) {
        throw new IllegalArgumentException("point must not have null values");
      }
      if (bytesPerDim == -1) {
        if (dim.length == 0) {
          throw new IllegalArgumentException("point must not have 0-length values");
        }
        bytesPerDim = dim.length;
      } else if (dim.length != bytesPerDim) {
        throw new IllegalArgumentException(
            "all dimensions must have same bytes length; got "
                + bytesPerDim
                + " and "
                + dim.length);
      }
    }
    byte[] packed = new byte[bytesPerDim * point.length];
    for (int i = 0; i < point.length; i++) {
      System.arraycopy(point[i], 0, packed, i * bytesPerDim, bytesPerDim);
    }
    return new BytesRef(packed);
  }

  /**
   * General purpose API: creates a new BinaryPoint, indexing the provided N-dimensional binary
   * point.
   *
   * @param name field name
   * @param point byte[][] value
   * @throws IllegalArgumentException if the field name or value is null.
   */
  public BinaryPoint(String name, byte[]... point) {
    super(name, pack(point), getType(point));
  }

  /** Expert API */
  public BinaryPoint(String name, byte[] packedPoint, IndexableFieldType type) {
    super(name, packedPoint, type);
    if (packedPoint.length != type.pointDimensionCount() * type.pointNumBytes()) {
      throw new IllegalArgumentException(
          "packedPoint is length="
              + packedPoint.length
              + " but type.pointDimensionCount()="
              + type.pointDimensionCount()
              + " and type.pointNumBytes()="
              + type.pointNumBytes());
    }
  }

  // static methods for generating queries

  /**
   * Create a query for matching an exact binary value.
   *
   * <p>This is for simple one-dimension points, for multidimensional points use {@link
   * #newRangeQuery(String, byte[][], byte[][])} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value binary value
   * @throws IllegalArgumentException if {@code field} is null or {@code value} is null
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, byte[] value) {
    return newRangeQuery(field, value, value);
  }

  /**
   * Create a range query for binary values.
   *
   * <p>This is for simple one-dimension ranges, for multidimensional ranges use {@link
   * #newRangeQuery(String, byte[][], byte[][])} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive). must not be {@code null}
   * @param upperValue upper portion of the range (inclusive). must not be {@code null}
   * @throws IllegalArgumentException if {@code field} is null, if {@code lowerValue} is null, or if
   *     {@code upperValue} is null
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, byte[] lowerValue, byte[] upperValue) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    return newRangeQuery(field, new byte[][] {lowerValue}, new byte[][] {upperValue});
  }

  /**
   * Create a range query for n-dimensional binary values.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive). must not be null.
   * @param upperValue upper portion of the range (inclusive). must not be null.
   * @throws IllegalArgumentException if {@code field} is null, if {@code lowerValue} is null, if
   *     {@code upperValue} is null, or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, byte[][] lowerValue, byte[][] upperValue) {
    return new PointRangeQuery(
        field, pack(lowerValue).bytes, pack(upperValue).bytes, lowerValue.length) {
      @Override
      protected String toString(int dimension, byte[] value) {
        assert value != null;
        StringBuilder sb = new StringBuilder();
        sb.append("binary(");
        for (int i = 0; i < value.length; i++) {
          if (i > 0) {
            sb.append(' ');
          }
          sb.append(Integer.toHexString(value[i] & 0xFF));
        }
        sb.append(')');
        return sb.toString();
      }
    };
  }

  /**
   * Create a query matching any of the specified 1D values. This is the points equivalent of {@code
   * TermsQuery}.
   *
   * @param field field name. must not be {@code null}.
   * @param values all values to match
   */
  public static Query newSetQuery(String field, byte[]... values) {

    // Make sure all byte[] have the same length
    int bytesPerDim = -1;
    for (byte[] value : values) {
      if (bytesPerDim == -1) {
        bytesPerDim = value.length;
      } else if (value.length != bytesPerDim) {
        throw new IllegalArgumentException(
            "all byte[] must be the same length, but saw " + bytesPerDim + " and " + value.length);
      }
    }

    if (bytesPerDim == -1) {
      // There are no points, and we cannot guess the bytesPerDim here, so we return an equivalent
      // query:
      return new MatchNoDocsQuery("empty BinaryPoint.newSetQuery");
    }

    // Don't unexpectedly change the user's incoming values array:
    byte[][] sortedValues = values.clone();
    Arrays.sort(
        sortedValues,
        new Comparator<byte[]>() {
          @Override
          public int compare(byte[] a, byte[] b) {
            return Arrays.compareUnsigned(a, 0, a.length, b, 0, b.length);
          }
        });

    final BytesRef encoded = new BytesRef(new byte[bytesPerDim]);

    return new PointInSetQuery(
        field,
        1,
        bytesPerDim,
        new PointInSetQuery.Stream() {

          int upto;

          @Override
          public BytesRef next() {
            if (upto == sortedValues.length) {
              return null;
            } else {
              encoded.bytes = sortedValues[upto];
              upto++;
              return encoded;
            }
          }
        }) {
      @Override
      protected String toString(byte[] value) {
        return new BytesRef(value).toString();
      }
    };
  }
}
