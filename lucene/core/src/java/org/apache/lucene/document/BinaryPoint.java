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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.StringHelper;

/** 
 * A binary field that is indexed dimensionally such that finding
 * all documents within an N-dimensional shape or range at search time is
 * efficient.  Multiple values for the same field in one documents
 * is allowed.
 * <p>
 * This field defines static factory methods for creating common queries:
 * <ul>
 *   <li>{@link #newExactQuery newExactQuery()} for matching an exact 1D point.
 *   <li>{@link #newRangeQuery newRangeQuery()} for matching a 1D range.
 *   <li>{@link #newMultiRangeQuery newMultiRangeQuery()} for matching points/ranges in n-dimensional space.
 *   <li>{@link #newSetQuery newSetQuery()} for matching a set of 1D values.
 * </ul> 
 */
public final class BinaryPoint extends Field {

  private static FieldType getType(byte[][] point) {
    if (point == null) {
      throw new IllegalArgumentException("point cannot be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point cannot be 0 dimensions");
    }
    int bytesPerDim = -1;
    for(int i=0;i<point.length;i++) {
      byte[] oneDim = point[i];
      if (oneDim == null) {
        throw new IllegalArgumentException("point cannot have null values");
      }
      if (oneDim.length == 0) {
        throw new IllegalArgumentException("point cannot have 0-length values");
      }
      if (bytesPerDim == -1) {
        bytesPerDim = oneDim.length;
      } else if (bytesPerDim != oneDim.length) {
        throw new IllegalArgumentException("all dimensions must have same bytes length; got " + bytesPerDim + " and " + oneDim.length);
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
      throw new IllegalArgumentException("point cannot be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point cannot be 0 dimensions");
    }
    if (point.length == 1) {
      return new BytesRef(point[0]);
    }
    int bytesPerDim = -1;
    for(byte[] dim : point) {
      if (dim == null) {
        throw new IllegalArgumentException("point cannot have null values");
      }
      if (bytesPerDim == -1) {
        if (dim.length == 0) {
          throw new IllegalArgumentException("point cannot have 0-length values");
        }
        bytesPerDim = dim.length;
      } else if (dim.length != bytesPerDim) {
        throw new IllegalArgumentException("all dimensions must have same bytes length; got " + bytesPerDim + " and " + dim.length);
      }
    }
    byte[] packed = new byte[bytesPerDim*point.length];
    for(int i=0;i<point.length;i++) {
      System.arraycopy(point[i], 0, packed, i*bytesPerDim, bytesPerDim);
    }
    return new BytesRef(packed);
  }

  /** General purpose API: creates a new BinaryPoint, indexing the
   *  provided N-dimensional binary point.
   *
   *  @param name field name
   *  @param point byte[][] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public BinaryPoint(String name, byte[]... point) {
    super(name, pack(point), getType(point));
  }

  /** Expert API */
  public BinaryPoint(String name, byte[] packedPoint, FieldType type) {
    super(name, packedPoint, type);
    if (packedPoint.length != type.pointDimensionCount() * type.pointNumBytes()) {
      throw new IllegalArgumentException("packedPoint is length=" + packedPoint.length + " but type.pointDimensionCount()=" + type.pointDimensionCount() + " and type.pointNumBytes()=" + type.pointNumBytes());
    }
  }
  
  // static methods for generating queries

  /** 
   * Create a query for matching an exact binary value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newMultiRangeQuery newMultiRangeQuery()} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value binary value
   * @throws IllegalArgumentException if {@code field} is null or {@code value} is null
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, byte[] value) {
    if (value == null) {
      throw new IllegalArgumentException("value cannot be null");
    }
    return newRangeQuery(field, value, true, value, true);
  }

  /** 
   * Create a range query for binary values.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newMultiRangeQuery newMultiRangeQuery()} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the {@code lowerValue} or {@code upperValue} to {@code null}. 
   * <p>
   * By setting inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} means "open".
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} means "open".
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, byte[] lowerValue, boolean lowerInclusive, byte[] upperValue, boolean upperInclusive) {
    return newMultiRangeQuery(field, new byte[][] {lowerValue}, new boolean[] {lowerInclusive}, new byte[][] {upperValue}, new boolean[] {upperInclusive});
  }
  
  /** 
   * Create a multidimensional range query for binary values.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting a {@code lowerValue} element or {@code upperValue} element to {@code null}. 
   * <p>
   * By setting a dimension's inclusive ({@code lowerInclusive} or {@code upperInclusive}) to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range. {@code null} values mean "open" for that dimension.
   * @param lowerInclusive {@code true} if the lower portion of the range is inclusive, {@code false} if it should be excluded.
   * @param upperValue upper portion of the range. {@code null} values mean "open" for that dimension.
   * @param upperInclusive {@code true} if the upper portion of the range is inclusive, {@code false} if it should be excluded.
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static Query newMultiRangeQuery(String field, byte[][] lowerValue, boolean[] lowerInclusive, byte[][] upperValue, boolean[] upperInclusive) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, lowerValue, lowerInclusive, upperValue, upperInclusive) {
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
   * Create a query matching any of the specified 1D values.  This is the points equivalent of {@code TermsQuery}.
   * 
   * @param field field name. must not be {@code null}.
   * @param valuesIn all values to match
   */
  public static Query newSetQuery(String field, byte[]... valuesIn) throws IOException {

    // Make sure all byte[] have the same length
    int bytesPerDim = -1;
    for(byte[] value : valuesIn) {
      if (bytesPerDim == -1) {
        bytesPerDim = value.length;
      } else if (value.length != bytesPerDim) {
        throw new IllegalArgumentException("all byte[] must be the same length, but saw " + bytesPerDim + " and " + value.length);
      }
    }

    if (bytesPerDim == -1) {
      // There are no points, and we cannot guess the bytesPerDim here, so we return an equivalent query:
      return new MatchNoDocsQuery();
    }

    // Don't unexpectedly change the user's incoming values array:
    byte[][] values = valuesIn.clone();

    Arrays.sort(values,
                new Comparator<byte[]>() {
                  @Override
                  public int compare(byte[] a, byte[] b) {
                    return StringHelper.compare(a.length, a, 0, b, 0);
                  }
                });

    final BytesRef value = new BytesRef(new byte[bytesPerDim]);
    
    return new PointInSetQuery(field, 1, bytesPerDim,
                               new BytesRefIterator() {

                                 int upto;

                                 @Override
                                 public BytesRef next() {
                                   if (upto == values.length) {
                                     return null;
                                   } else {
                                     value.bytes = values[upto];
                                     upto++;
                                     return value;
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
