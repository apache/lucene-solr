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

import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.NumericUtils;

/** 
 * A long field that is indexed dimensionally such that finding
 * all documents within an N-dimensional shape or range at search time is
 * efficient.  Multiple values for the same field in one documents
 * is allowed.
 * <p>
 * This field defines static factory methods for creating common queries:
 * <ul>
 *   <li>{@link #newExactQuery newExactQuery()} for matching an exact 1D point.
 *   <li>{@link #newRangeQuery newRangeQuery()} for matching a 1D range.
 *   <li>{@link #newMultiRangeQuery newMultiRangeQuery()} for matching points/ranges in n-dimensional space.
 * </ul>
 */
public final class LongPoint extends Field {

  private static FieldType getType(int numDims) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, Long.BYTES);
    type.freeze();
    return type;
  }

  @Override
  public void setLongValue(long value) {
    setLongValues(value);
  }

  /** Change the values of this field */
  public void setLongValues(long... point) {
    if (type.pointDimensionCount() != point.length) {
      throw new IllegalArgumentException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot change to (incoming) " + point.length + " dimensions");
    }
    fieldsData = pack(point);
  }

  @Override
  public void setBytesValue(BytesRef bytes) {
    throw new IllegalArgumentException("cannot change value type from long to BytesRef");
  }

  @Override
  public Number numericValue() {
    if (type.pointDimensionCount() != 1) {
      throw new IllegalStateException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot convert to a single numeric value");
    }
    BytesRef bytes = (BytesRef) fieldsData;
    assert bytes.length == Long.BYTES;
    return decodeDimension(bytes.bytes, bytes.offset);
  }

  private static BytesRef pack(long... point) {
    if (point == null) {
      throw new IllegalArgumentException("point cannot be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point cannot be 0 dimensions");
    }
    byte[] packed = new byte[point.length * Long.BYTES];
    
    for (int dim = 0; dim < point.length; dim++) {
      encodeDimension(point[dim], packed, dim * Long.BYTES);
    }

    return new BytesRef(packed);
  }

  /** Creates a new LongPoint, indexing the
   *  provided N-dimensional long point.
   *
   *  @param name field name
   *  @param point long[] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public LongPoint(String name, long... point) {
    super(name, pack(point), getType(point.length));
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(type.toString());
    result.append('<');
    result.append(name);
    result.append(':');

    BytesRef bytes = (BytesRef) fieldsData;
    for (int dim = 0; dim < type.pointDimensionCount(); dim++) {
      if (dim > 0) {
        result.append(',');
      }
      result.append(decodeDimension(bytes.bytes, bytes.offset + dim * Long.BYTES));
    }

    result.append('>');
    return result.toString();
  }
  
  /** Encode n-dimensional long values into binary encoding */
  private static byte[][] encode(Long value[]) {
    byte[][] encoded = new byte[value.length][];
    for (int i = 0; i < value.length; i++) {
      if (value[i] != null) {
        encoded[i] = new byte[Long.BYTES];
        encodeDimension(value[i], encoded[i], 0);
      }
    }
    return encoded;
  }
  
  // public helper methods (e.g. for queries)
  
  /** Encode single long dimension */
  public static void encodeDimension(Long value, byte dest[], int offset) {
    NumericUtils.longToBytes(value, dest, offset);
  }
  
  /** Decode single long dimension */
  public static Long decodeDimension(byte value[], int offset) {
    return NumericUtils.bytesToLong(value, offset);
  }
  
  // static methods for generating queries
  
  /** 
   * Create a query for matching an exact long value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newMultiRangeQuery newMultiRangeQuery()} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, long value) {
    return newRangeQuery(field, value, true, value, true);
  }

  /** 
   * Create a range query for long values.
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
  public static Query newRangeQuery(String field, Long lowerValue, boolean lowerInclusive, Long upperValue, boolean upperInclusive) {
    return newMultiRangeQuery(field, 
                              new Long[] { lowerValue },
                              new boolean[] { lowerInclusive }, 
                              new Long[] { upperValue },
                              new boolean[] { upperInclusive });
  }

  /** 
   * Create a multidimensional range query for long values.
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
  public static Query newMultiRangeQuery(String field, Long[] lowerValue, boolean lowerInclusive[], Long[] upperValue, boolean upperInclusive[]) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, LongPoint.encode(lowerValue), lowerInclusive, LongPoint.encode(upperValue), upperInclusive) {
      @Override
      protected String toString(int dimension, byte[] value) {
        return LongPoint.decodeDimension(value, 0).toString();
      }
    };
  }

  /**
   * Create a query matching any of the specified 1D values.  This is the points equivalent of {@code TermsQuery}.
   * 
   * @param field field name. must not be {@code null}.
   * @param valuesIn all int values to match
   */
  public static Query newSetQuery(String field, long... valuesIn) throws IOException {

    // Don't unexpectedly change the user's incoming values array:
    long[] values = valuesIn.clone();

    Arrays.sort(values);

    final BytesRef value = new BytesRef(new byte[Long.BYTES]);

    return new PointInSetQuery(field, 1, Long.BYTES,
                               new BytesRefIterator() {

                                 int upto;

                                 @Override
                                 public BytesRef next() {
                                   if (upto == values.length) {
                                     return null;
                                   } else {
                                     encodeDimension(values[upto], value.bytes, 0);
                                     upto++;
                                     return value;
                                   }
                                 }
                               }) {
      @Override
      protected String toString(byte[] value) {
        assert value.length == Long.BYTES;
        return Long.toString(decodeDimension(value, 0));
      }
    };
  }
}
