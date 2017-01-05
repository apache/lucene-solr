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
import java.util.Collection;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/** 
 * An indexed {@code int} field for fast range filters.  If you also
 * need to store the value, you should add a separate {@link StoredField} instance.
 * <p>
 * Finding all documents within an N-dimensional shape or range at search time is
 * efficient.  Multiple values for the same field in one document
 * is allowed.
 * <p>
 * This field defines static factory methods for creating common queries:
 * <ul>
 *   <li>{@link #newExactQuery(String, int)} for matching an exact 1D point.
 *   <li>{@link #newSetQuery(String, int...)} for matching a set of 1D values.
 *   <li>{@link #newRangeQuery(String, int, int)} for matching a 1D range.
 *   <li>{@link #newRangeQuery(String, int[], int[])} for matching points/ranges in n-dimensional space.
 * </ul>
 * @see PointValues
 */
public final class IntPoint extends Field {

  private static FieldType getType(int numDims) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, Integer.BYTES);
    type.freeze();
    return type;
  }

  @Override
  public void setIntValue(int value) {
    setIntValues(value);
  }

  /** Change the values of this field */
  public void setIntValues(int... point) {
    if (type.pointDimensionCount() != point.length) {
      throw new IllegalArgumentException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot change to (incoming) " + point.length + " dimensions");
    }
    fieldsData = pack(point);
  }

  @Override
  public void setBytesValue(BytesRef bytes) {
    throw new IllegalArgumentException("cannot change value type from int to BytesRef");
  }

  @Override
  public Number numericValue() {
    if (type.pointDimensionCount() != 1) {
      throw new IllegalStateException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot convert to a single numeric value");
    }
    BytesRef bytes = (BytesRef) fieldsData;
    assert bytes.length == Integer.BYTES;
    return decodeDimension(bytes.bytes, bytes.offset);
  }

  private static BytesRef pack(int... point) {
    if (point == null) {
      throw new IllegalArgumentException("point must not be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point must not be 0 dimensions");
    }
    byte[] packed = new byte[point.length * Integer.BYTES];
    
    for (int dim = 0; dim < point.length; dim++) {
      encodeDimension(point[dim], packed, dim * Integer.BYTES);
    }

    return new BytesRef(packed);
  }

  /** Creates a new IntPoint, indexing the
   *  provided N-dimensional int point.
   *
   *  @param name field name
   *  @param point int[] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public IntPoint(String name, int... point) {
    super(name, pack(point), getType(point.length));
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    BytesRef bytes = (BytesRef) fieldsData;
    for (int dim = 0; dim < type.pointDimensionCount(); dim++) {
      if (dim > 0) {
        result.append(',');
      }
      result.append(decodeDimension(bytes.bytes, bytes.offset + dim * Integer.BYTES));
    }

    result.append('>');
    return result.toString();
  }
  
  // public helper methods (e.g. for queries)
  
  /** Encode single integer dimension */
  public static void encodeDimension(int value, byte dest[], int offset) {
    NumericUtils.intToSortableBytes(value, dest, offset);
  }
  
  /** Decode single integer dimension */
  public static int decodeDimension(byte value[], int offset) {
    return NumericUtils.sortableBytesToInt(value, offset);
  }
  
  // static methods for generating queries
  
  /** 
   * Create a query for matching an exact integer value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newRangeQuery(String, int[], int[])} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, int value) {
    return newRangeQuery(field, value, value);
  }

  /** 
   * Create a range query for integer values.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newRangeQuery(String, int[], int[])} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue = Integer.MIN_VALUE} or {@code upperValue = Integer.MAX_VALUE}.
   * <p>
   * Ranges are inclusive. For exclusive ranges, pass {@code Math.addExact(lowerValue, 1)}
   * or {@code Math.addExact(upperValue, -1)}.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive).
   * @param upperValue upper portion of the range (inclusive).
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, int lowerValue, int upperValue) {
    return newRangeQuery(field, new int[] { lowerValue }, new int[] { upperValue });
  }

  /** 
   * Create a range query for n-dimensional integer values.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue[i] = Integer.MIN_VALUE} or {@code upperValue[i] = Integer.MAX_VALUE}. 
   * <p>
   * Ranges are inclusive. For exclusive ranges, pass {@code Math.addExact(lowerValue[i], 1)}
   * or {@code Math.addExact(upperValue[i], -1)}.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive). must not be {@code null}.
   * @param upperValue upper portion of the range (inclusive). must not be {@code null}.
   * @throws IllegalArgumentException if {@code field} is null, if {@code lowerValue} is null, if {@code upperValue} is null, 
   *                                  or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, int[] lowerValue, int[] upperValue) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, pack(lowerValue).bytes, pack(upperValue).bytes, lowerValue.length) {
      @Override
      protected String toString(int dimension, byte[] value) {
        return Integer.toString(decodeDimension(value, 0));
      }
    };
  }

  /**
   * Create a query matching any of the specified 1D values.  This is the points equivalent of {@code TermsQuery}.
   * 
   * @param field field name. must not be {@code null}.
   * @param values all values to match
   */
  public static Query newSetQuery(String field, int... values) {

    // Don't unexpectedly change the user's incoming values array:
    int[] sortedValues = values.clone();
    Arrays.sort(sortedValues);

    final BytesRef encoded = new BytesRef(new byte[Integer.BYTES]);

    return new PointInSetQuery(field, 1, Integer.BYTES,
                               new PointInSetQuery.Stream() {

                                 int upto;

                                 @Override
                                 public BytesRef next() {
                                   if (upto == sortedValues.length) {
                                     return null;
                                   } else {
                                     encodeDimension(sortedValues[upto], encoded.bytes, 0);
                                     upto++;
                                     return encoded;
                                   }
                                 }
                               }) {
      @Override
      protected String toString(byte[] value) {
        assert value.length == Integer.BYTES;
        return Integer.toString(decodeDimension(value, 0));
      }
    };
  }
  
  /**
   * Create a query matching any of the specified 1D values.  This is the points equivalent of {@code TermsQuery}.
   * 
   * @param field field name. must not be {@code null}.
   * @param values all values to match
   */
  public static Query newSetQuery(String field, Collection<Integer> values) {
    Integer[] boxed = values.toArray(new Integer[0]);
    int[] unboxed = new int[boxed.length];
    for (int i = 0; i < boxed.length; i++) {
      unboxed[i] = boxed[i];
    }
    return newSetQuery(field, unboxed);
  }
}
