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
 * An indexed {@code double} field for fast range filters.  If you also
 * need to store the value, you should add a separate {@link StoredField} instance.
 * <p>
 * Finding all documents within an N-dimensional shape or range at search time is
 * efficient.  Multiple values for the same field in one document
 * is allowed.
 * <p>
 * This field defines static factory methods for creating common queries:
 * <ul>
 *   <li>{@link #newExactQuery(String, double)} for matching an exact 1D point.
 *   <li>{@link #newSetQuery(String, double...)} for matching a set of 1D values.
 *   <li>{@link #newRangeQuery(String, double, double)} for matching a 1D range.
 *   <li>{@link #newRangeQuery(String, double[], double[])} for matching points/ranges in n-dimensional space.
 * </ul> 
 * @see PointValues
 */
public final class DoublePoint extends Field {
  /**
   * Return the least double that compares greater than {@code d} consistently
   * with {@link Double#compare}. The only difference with
   * {@link Math#nextUp(double)} is that this method returns {@code +0d} when
   * the argument is {@code -0d}.
   */
  public static double nextUp(double d) {
    if (Double.doubleToLongBits(d) == 0x8000_0000_0000_0000L) { // -0d
      return +0d;
    }
    return Math.nextUp(d);
  }

  /**
   * Return the greatest double that compares less than {@code d} consistently
   * with {@link Double#compare}. The only difference with
   * {@link Math#nextDown(double)} is that this method returns {@code -0d} when
   * the argument is {@code +0d}.
   */
  public static double nextDown(double d) {
    if (Double.doubleToLongBits(d) == 0L) { // +0d
      return -0f;
    }
    return Math.nextDown(d);
  }

  private static FieldType getType(int numDims) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, Double.BYTES);
    type.freeze();
    return type;
  }

  @Override
  public void setDoubleValue(double value) {
    setDoubleValues(value);
  }

  /** Change the values of this field */
  public void setDoubleValues(double... point) {
    if (type.pointDimensionCount() != point.length) {
      throw new IllegalArgumentException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot change to (incoming) " + point.length + " dimensions");
    }
    fieldsData = pack(point);
  }

  @Override
  public void setBytesValue(BytesRef bytes) {
    throw new IllegalArgumentException("cannot change value type from double to BytesRef");
  }

  @Override
  public Number numericValue() {
    if (type.pointDimensionCount() != 1) {
      throw new IllegalStateException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot convert to a single numeric value");
    }
    BytesRef bytes = (BytesRef) fieldsData;
    assert bytes.length == Double.BYTES;
    return decodeDimension(bytes.bytes, bytes.offset);
  }

  /**
   *  Pack a double point into a BytesRef
   *
   * @param point double[] value
   * @throws IllegalArgumentException is the value is null or of zero length
   */
  public static BytesRef pack(double... point) {
    if (point == null) {
      throw new IllegalArgumentException("point must not be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point must not be 0 dimensions");
    }
    byte[] packed = new byte[point.length * Double.BYTES];
    
    for (int dim = 0; dim < point.length ; dim++) {
      encodeDimension(point[dim], packed, dim * Double.BYTES);
    }

    return new BytesRef(packed);
  }

  /** Creates a new DoublePoint, indexing the
   *  provided N-dimensional double point.
   *
   *  @param name field name
   *  @param point double[] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public DoublePoint(String name, double... point) {
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
      result.append(decodeDimension(bytes.bytes, bytes.offset + dim * Double.BYTES));
    }

    result.append('>');
    return result.toString();
  }
  
  // public helper methods (e.g. for queries)
  
  /** Encode single double dimension */
  public static void encodeDimension(double value, byte dest[], int offset) {
    NumericUtils.longToSortableBytes(NumericUtils.doubleToSortableLong(value), dest, offset);
  }
  
  /** Decode single double dimension */
  public static double decodeDimension(byte value[], int offset) {
    return NumericUtils.sortableLongToDouble(NumericUtils.sortableBytesToLong(value, offset));
  }
  
  // static methods for generating queries

  /** 
   * Create a query for matching an exact double value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newRangeQuery(String, double[], double[])} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value double value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, double value) {
    return newRangeQuery(field, value, value);
  }
  
  /** 
   * Create a range query for double values.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newRangeQuery(String, double[], double[])} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue = Double.NEGATIVE_INFINITY} or {@code upperValue = Double.POSITIVE_INFINITY}.
   * <p> Ranges are inclusive. For exclusive ranges, pass {@link #nextUp(double) nextUp(lowerValue)}
   * or {@link #nextUp(double) nextDown(upperValue)}.
   * <p>
   * Range comparisons are consistent with {@link Double#compareTo(Double)}.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive).
   * @param upperValue upper portion of the range (inclusive).
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, double lowerValue, double upperValue) {
    return newRangeQuery(field, new double[] { lowerValue }, new double[] { upperValue });
  }

  /** 
   * Create a range query for n-dimensional double values.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue[i] = Double.NEGATIVE_INFINITY} or {@code upperValue[i] = Double.POSITIVE_INFINITY}.
   * <p> Ranges are inclusive. For exclusive ranges, pass {@code Math#nextUp(lowerValue[i])}
   * or {@code Math.nextDown(upperValue[i])}.
   * <p>
   * Range comparisons are consistent with {@link Double#compareTo(Double)}.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive). must not be {@code null}.
   * @param upperValue upper portion of the range (inclusive). must not be {@code null}.
   * @throws IllegalArgumentException if {@code field} is null, if {@code lowerValue} is null, if {@code upperValue} is null, 
   *                                  or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, double[] lowerValue, double[] upperValue) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, pack(lowerValue).bytes, pack(upperValue).bytes, lowerValue.length) {
      @Override
      protected String toString(int dimension, byte[] value) {
        return Double.toString(decodeDimension(value, 0));
      }
    };
  }

  /**
   * Create a query matching any of the specified 1D values.  This is the points equivalent of {@code TermsQuery}.
   * 
   * @param field field name. must not be {@code null}.
   * @param values all values to match
   */
  public static Query newSetQuery(String field, double... values) {

    // Don't unexpectedly change the user's incoming values array:
    double[] sortedValues = values.clone();
    Arrays.sort(sortedValues);

    final BytesRef encoded = new BytesRef(new byte[Double.BYTES]);

    return new PointInSetQuery(field, 1, Double.BYTES,
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
        assert value.length == Double.BYTES;
        return Double.toString(decodeDimension(value, 0));
      }
    };
  }
  
  /**
   * Create a query matching any of the specified 1D values.  This is the points equivalent of {@code TermsQuery}.
   * 
   * @param field field name. must not be {@code null}.
   * @param values all values to match
   */
  public static Query newSetQuery(String field, Collection<Double> values) {
    Double[] boxed = values.toArray(new Double[0]);
    double[] unboxed = new double[boxed.length];
    for (int i = 0; i < boxed.length; i++) {
      unboxed[i] = boxed[i];
    }
    return newSetQuery(field, unboxed);
  }
}
