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

import java.math.BigInteger;
import java.util.Arrays;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/** 
 * An indexed 128-bit {@code BigInteger} field.
 * <p>
 * Finding all documents within an N-dimensional shape or range at search time is
 * efficient.  Multiple values for the same field in one document
 * is allowed. 
 * <p>
 * This field defines static factory methods for creating common queries:
 * <ul>
 *   <li>{@link #newExactQuery(String, BigInteger)} for matching an exact 1D point.
 *   <li>{@link #newSetQuery(String, BigInteger...)} for matching a set of 1D values.
 *   <li>{@link #newRangeQuery(String, BigInteger, BigInteger)} for matching a 1D range.
 *   <li>{@link #newRangeQuery(String, BigInteger[], BigInteger[])} for matching points/ranges in n-dimensional space.
 * </ul>
 * @see PointValues
 */
public class BigIntegerPoint extends Field {

  /** The number of bytes per dimension: 128 bits. */
  public static final int BYTES = 16;
  
  /** A constant holding the minimum value a BigIntegerPoint can have, -2<sup>127</sup>. */
  public static final BigInteger MIN_VALUE = BigInteger.ONE.shiftLeft(BYTES * 8 - 1).negate();

  /** A constant holding the maximum value a BigIntegerPoint can have, 2<sup>127</sup>-1. */
  public static final BigInteger MAX_VALUE = BigInteger.ONE.shiftLeft(BYTES * 8 - 1).subtract(BigInteger.ONE);

  private static FieldType getType(int numDims) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, BYTES);
    type.freeze();
    return type;
  }

  /** Change the values of this field */
  public void setBigIntegerValues(BigInteger... point) {
    if (type.pointDimensionCount() != point.length) {
      throw new IllegalArgumentException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot change to (incoming) " + point.length + " dimensions");
    }
    fieldsData = pack(point);
  }

  @Override
  public void setBytesValue(BytesRef bytes) {
    throw new IllegalArgumentException("cannot change value type from BigInteger to BytesRef");
  }

  @Override
  public Number numericValue() {
    if (type.pointDimensionCount() != 1) {
      throw new IllegalStateException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot convert to a single numeric value");
    }
    BytesRef bytes = (BytesRef) fieldsData;
    assert bytes.length == BYTES;
    return decodeDimension(bytes.bytes, bytes.offset);
  }

  private static BytesRef pack(BigInteger... point) {
    if (point == null) {
      throw new IllegalArgumentException("point must not be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point must not be 0 dimensions");
    }
    byte[] packed = new byte[point.length * BYTES];
    
    for (int dim = 0; dim < point.length; dim++) {
      encodeDimension(point[dim], packed, dim * BYTES);
    }

    return new BytesRef(packed);
  }

  /** Creates a new BigIntegerPoint, indexing the
   *  provided N-dimensional big integer point.
   *
   *  @param name field name
   *  @param point BigInteger[] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public BigIntegerPoint(String name, BigInteger... point) {
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
      result.append(decodeDimension(bytes.bytes, bytes.offset + dim * BYTES));
    }

    result.append('>');
    return result.toString();
  }

  // public helper methods (e.g. for queries)
  
  /** Encode single BigInteger dimension */
  public static void encodeDimension(BigInteger value, byte dest[], int offset) {
    NumericUtils.bigIntToSortableBytes(value, BYTES, dest, offset);
  }
  
  /** Decode single BigInteger dimension */
  public static BigInteger decodeDimension(byte value[], int offset) {
    return NumericUtils.sortableBytesToBigInt(value, offset, BYTES);
  }

  // static methods for generating queries

  /** 
   * Create a query for matching an exact big integer value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newRangeQuery(String, BigInteger[], BigInteger[])} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value exact value. must not be {@code null}.
   * @throws IllegalArgumentException if {@code field} is null or {@code value} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, BigInteger value) {
    return newRangeQuery(field, value, value);
  }

  /** 
   * Create a range query for big integer values.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newRangeQuery(String, BigInteger[], BigInteger[])} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue = BigIntegerPoint.MIN_VALUE} 
   * or {@code upperValue = BigIntegerPoint.MAX_VALUE}. 
   * <p>
   * Ranges are inclusive. For exclusive ranges, pass {@code lowerValue.add(BigInteger.ONE)} 
   * or {@code upperValue.subtract(BigInteger.ONE)}
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive). must not be {@code null}.
   * @param upperValue upper portion of the range (inclusive). must not be {@code null}.
   * @throws IllegalArgumentException if {@code field} is null, {@code lowerValue} is null, or {@code upperValue} is null.
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, BigInteger lowerValue, BigInteger upperValue) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    return newRangeQuery(field, new BigInteger[] { lowerValue }, new BigInteger[] { upperValue });
  }

  /** 
   * Create a range query for n-dimensional big integer values.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue[i] = BigIntegerPoint.MIN_VALUE} 
   * or {@code upperValue[i] = BigIntegerPoint.MAX_VALUE}. 
   * <p>
   * Ranges are inclusive. For exclusive ranges, pass {@code lowerValue[i].add(BigInteger.ONE)} 
   * or {@code upperValue[i].subtract(BigInteger.ONE)}
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive). must not be {@code null}.
   * @param upperValue upper portion of the range (inclusive). must not be {@code null}.
   * @throws IllegalArgumentException if {@code field} is null, if {@code lowerValue} is null, if {@code upperValue} is null, 
   *                                  or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, BigInteger[] lowerValue, BigInteger[] upperValue) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, pack(lowerValue).bytes, pack(upperValue).bytes, lowerValue.length) {
      @Override
      protected String toString(int dimension, byte[] value) {
        return BigIntegerPoint.decodeDimension(value, 0).toString();
      }
    };
  }

  /**
   * Create a query matching any of the specified 1D values.  This is the points equivalent of {@code TermsQuery}.
   * 
   * @param field field name. must not be {@code null}.
   * @param values all values to match
   */
  public static Query newSetQuery(String field, BigInteger... values) {

    // Don't unexpectedly change the user's incoming values array:
    BigInteger[] sortedValues = values.clone();
    Arrays.sort(sortedValues);

    final BytesRef encoded = new BytesRef(new byte[BYTES]);

    return new PointInSetQuery(field, 1, BYTES,
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
        assert value.length == BYTES;
        return decodeDimension(value, 0).toString();
      }
    };
  }
}
