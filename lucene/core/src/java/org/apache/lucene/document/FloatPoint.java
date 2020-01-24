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
 * An indexed {@code float} field for fast range filters.  If you also
 * need to store the value, you should add a separate {@link StoredField} instance.
 * <p>
 * Finding all documents within an N-dimensional at search time is
 * efficient.  Multiple values for the same field in one document
 * is allowed.
 * <p>
 * This field defines static factory methods for creating common queries:
 * <ul>
 *   <li>{@link #newExactQuery(String, float)} for matching an exact 1D point.
 *   <li>{@link #newSetQuery(String, float...)} for matching a set of 1D values.
 *   <li>{@link #newRangeQuery(String, float, float)} for matching a 1D range.
 *   <li>{@link #newRangeQuery(String, float[], float[])} for matching points/ranges in n-dimensional space.
 * </ul>
 * @see PointValues
 */
public final class FloatPoint extends Field {
  /**
   * Return the least float that compares greater than {@code f} consistently
   * with {@link Float#compare}. The only difference with
   * {@link Math#nextUp(float)} is that this method returns {@code +0f} when
   * the argument is {@code -0f}.
   */
  public static float nextUp(float f) {
    if (Float.floatToIntBits(f) == 0x8000_0000) { // -0f
      return +0f;
    }
    return Math.nextUp(f);
  }

  /**
   * Return the greatest float that compares less than {@code f} consistently
   * with {@link Float#compare}. The only difference with
   * {@link Math#nextDown(float)} is that this method returns {@code -0f} when
   * the argument is {@code +0f}.
   */
  public static float nextDown(float f) {
    if (Float.floatToIntBits(f) == 0) { // +0f
      return -0f;
    }
    return Math.nextDown(f);
  }

  private static FieldType getType(int numDims) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, Float.BYTES);
    type.freeze();
    return type;
  }

  @Override
  public void setFloatValue(float value) {
    setFloatValues(value);
  }

  /** Change the values of this field */
  public void setFloatValues(float... point) {
    if (type.pointDataDimensionCount() != point.length) {
      throw new IllegalArgumentException("this field (name=" + name + ") uses " + type.pointDataDimensionCount() + " dimensions; cannot change to (incoming) " + point.length + " dimensions");
    }
    fieldsData = pack(point);
  }

  @Override
  public void setBytesValue(BytesRef bytes) {
    throw new IllegalArgumentException("cannot change value type from float to BytesRef");
  }

  @Override
  public Number numericValue() {
    if (type.pointDataDimensionCount() != 1) {
      throw new IllegalStateException("this field (name=" + name + ") uses " + type.pointDataDimensionCount() + " dimensions; cannot convert to a single numeric value");
    }
    BytesRef bytes = (BytesRef) fieldsData;
    assert bytes.length == Float.BYTES;
    return decodeDimension(bytes.bytes, bytes.offset);
  }

  /**
   *  Pack a float point into a BytesRef
   *
   * @param point float[] value
   * @throws IllegalArgumentException is the value is null or of zero length
   */
  public static BytesRef pack(float... point) {
    if (point == null) {
      throw new IllegalArgumentException("point must not be null");
    }
    if (point.length == 0) {
      throw new IllegalArgumentException("point must not be 0 dimensions");
    }
    byte[] packed = new byte[point.length * Float.BYTES];
    
    for (int dim = 0; dim < point.length; dim++) {
      encodeDimension(point[dim], packed, dim * Float.BYTES);
    }

    return new BytesRef(packed);
  }

  /** Creates a new FloatPoint, indexing the
   *  provided N-dimensional float point.
   *
   *  @param name field name
   *  @param point float[] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public FloatPoint(String name, float... point) {
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
    for (int dim = 0; dim < type.pointDataDimensionCount(); dim++) {
      if (dim > 0) {
        result.append(',');
      }
      result.append(decodeDimension(bytes.bytes, bytes.offset + dim * Float.BYTES));
    }

    result.append('>');
    return result.toString();
  }
  
  // public helper methods (e.g. for queries)
  
  /** Encode single float dimension */
  public static void encodeDimension(float value, byte dest[], int offset) {
    NumericUtils.intToSortableBytes(NumericUtils.floatToSortableInt(value), dest, offset);
  }
  
  /** Decode single float dimension */
  public static float decodeDimension(byte value[], int offset) {
    return NumericUtils.sortableIntToFloat(NumericUtils.sortableBytesToInt(value, offset));
  }
  
  // static methods for generating queries

  /** 
   * Create a query for matching an exact float value.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newRangeQuery(String, float[], float[])} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value float value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, float value) {
    return newRangeQuery(field, value, value);
  }
  
  /** 
   * Create a range query for float values.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newRangeQuery(String, float[], float[])} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue = Float.NEGATIVE_INFINITY} or {@code upperValue = Float.POSITIVE_INFINITY}.
   * <p> Ranges are inclusive. For exclusive ranges, pass {@link #nextUp(float) nextUp(lowerValue)}
   * or {@link #nextUp(float) nextDown(upperValue)}.
   * <p>
   * Range comparisons are consistent with {@link Float#compareTo(Float)}.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive).
   * @param upperValue upper portion of the range (inclusive).
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, float lowerValue, float upperValue) {
    return newRangeQuery(field, new float[] { lowerValue }, new float[] { upperValue });
  }

  /** 
   * Create a range query for n-dimensional float values.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue[i] = Float.NEGATIVE_INFINITY} or {@code upperValue[i] = Float.POSITIVE_INFINITY}.
   * <p> Ranges are inclusive. For exclusive ranges, pass {@code Math#nextUp(lowerValue[i])}
   * or {@code Math.nextDown(upperValue[i])}.
   * <p>
   * Range comparisons are consistent with {@link Float#compareTo(Float)}.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerValue lower portion of the range (inclusive). must not be {@code null}.
   * @param upperValue upper portion of the range (inclusive). must not be {@code null}.
   * @throws IllegalArgumentException if {@code field} is null, if {@code lowerValue} is null, if {@code upperValue} is null, 
   *                                  or if {@code lowerValue.length != upperValue.length}
   * @return a query matching documents within this range.
   */
  public static Query newRangeQuery(String field, float[] lowerValue, float[] upperValue) {
    PointRangeQuery.checkArgs(field, lowerValue, upperValue);
    return new PointRangeQuery(field, pack(lowerValue).bytes, pack(upperValue).bytes, lowerValue.length) {
      @Override
      protected String toString(int dimension, byte[] value) {
        return Float.toString(decodeDimension(value, 0));
      }
    };
  }

  /**
   * Create a query matching any of the specified 1D values.  This is the points equivalent of {@code TermsQuery}.
   * 
   * @param field field name. must not be {@code null}.
   * @param values all values to match
   */
  public static Query newSetQuery(String field, float... values) {

    // Don't unexpectedly change the user's incoming values array:
    float[] sortedValues = values.clone();
    Arrays.sort(sortedValues);

    final BytesRef encoded = new BytesRef(new byte[Float.BYTES]);

    return new PointInSetQuery(field, 1, Float.BYTES,
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
        assert value.length == Float.BYTES;
        return Float.toString(decodeDimension(value, 0));
      }
    };
  }

  /**
   * Create a query matching any of the specified 1D values.  This is the points equivalent of {@code TermsQuery}.
   * 
   * @param field field name. must not be {@code null}.
   * @param values all values to match
   */
  public static Query newSetQuery(String field, Collection<Float> values) {
    Float[] boxed = values.toArray(new Float[0]);
    float[] unboxed = new float[boxed.length];
    for (int i = 0; i < boxed.length; i++) {
      unboxed[i] = boxed[i];
    }
    return newSetQuery(field, unboxed);
  }
}
