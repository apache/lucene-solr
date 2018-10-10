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

/**
 * An indexed {@code half-float} field for fast range filters. If you also
 * need to store the value, you should add a separate {@link StoredField} instance.
 * If you need doc values, you can store them in a {@link NumericDocValuesField}
 * and use {@link #halfFloatToSortableShort} and
 * {@link #sortableShortToHalfFloat} for encoding/decoding.
 * <p>
 * The API takes floats, but they will be encoded to half-floats before being
 * indexed. In case the provided floats cannot be represented accurately as a
 * half float, they will be rounded to the closest value that can be
 * represented as a half float. In case of tie, values will be rounded to the
 * value that has a zero as its least significant bit.
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
public final class HalfFloatPoint extends Field {

  /** The number of bytes used to represent a half-float value. */
  public static final int BYTES = 2;

  /**
   * Return the first half float which is immediately greater than {@code v}.
   * If the argument is {@link Float#NaN} then the return value is
   * {@link Float#NaN}. If the argument is {@link Float#POSITIVE_INFINITY}
   * then the return value is {@link Float#POSITIVE_INFINITY}.
   */
  public static float nextUp(float v) {
    if (Float.isNaN(v) || v == Float.POSITIVE_INFINITY) {
      return v;
    }
    short s = halfFloatToSortableShort(v);
    // if the float does not represent a half float accurately then just
    // converting back might give us the value we are looking for
    float r = sortableShortToHalfFloat(s);
    if (r <= v) {
      r = sortableShortToHalfFloat((short) (s + 1));
    }
    return r;
  }

  /**
   * Return the first half float which is immediately smaller than {@code v}.
   * If the argument is {@link Float#NaN} then the return value is
   * {@link Float#NaN}. If the argument is {@link Float#NEGATIVE_INFINITY}
   * then the return value is {@link Float#NEGATIVE_INFINITY}.
   */
  public static float nextDown(float v) {
    if (Float.isNaN(v) || v == Float.NEGATIVE_INFINITY) {
      return v;
    }
    short s = halfFloatToSortableShort(v);
    // if the float does not represent a half float accurately then just
    // converting back might give us the value we are looking for
    float r = sortableShortToHalfFloat(s);
    if (r >= v) {
      r = sortableShortToHalfFloat((short) (s - 1));
    }
    return r;
  }

  /** Convert a half-float to a short value that maintains ordering. */
  public static short halfFloatToSortableShort(float v) {
    return sortableShortBits(halfFloatToShortBits(v));
  }

  /** Convert short bits to a half-float value that maintains ordering. */
  public static float sortableShortToHalfFloat(short bits) {
    return shortBitsToHalfFloat(sortableShortBits(bits));
  }

  private static short sortableShortBits(short s) {
    return (short) (s ^ (s >> 15) & 0x7fff);
  }

  static short halfFloatToShortBits(float v) {
    int floatBits = Float.floatToIntBits(v);
    int sign = floatBits >>> 31;
    int exp = (floatBits >>> 23) & 0xff;
    int mantissa = floatBits & 0x7fffff;

    if (exp == 0xff) {
      // preserve NaN and Infinity
      exp = 0x1f;
      mantissa >>>= (23 - 10);
    } else if (exp == 0x00) {
      // denormal float rounded to zero since even the largest denormal float
      // cannot be represented as a half float
      mantissa = 0;
    } else {
      exp = exp - 127 + 15;
      if (exp >= 0x1f) {
        // too large, make it infinity
        exp = 0x1f;
        mantissa = 0;
      } else if (exp <= 0) {
        // we need to convert to a denormal representation
        int shift = 23 - 10 - exp + 1;
        if (shift >= 32) {
          // need a special case since shifts are mod 32...
          exp = 0;
          mantissa = 0;
        } else {
          // add the implicit bit
          mantissa |= 0x800000;
          mantissa = roundShift(mantissa, shift);
          exp = mantissa >>> 10;
          mantissa &= 0x3ff;
        }
      } else {
        mantissa = roundShift((exp << 23) | mantissa, 23 - 10);
        exp = mantissa >>> 10;
        mantissa &= 0x3ff;
      }
    }
    return (short) ((sign << 15) | (exp << 10) | mantissa);
  }

  // divide by 2^shift and round to the closest int
  // round to even in case of tie
  static int roundShift(int i, int shift) {
    assert shift > 0;
    i += 1 << (shift - 1); // add 2^(shift-1) so that we round rather than truncate
    i -= (i >>> shift) & 1; // and subtract the shift-th bit so that we round to even in case of tie
    return i >>> shift;
  }

  static float shortBitsToHalfFloat(short s) {
    int sign = s >>> 15;
    int exp = (s >>> 10) & 0x1f;
    int mantissa = s & 0x3ff;
    if (exp == 0x1f) {
      // NaN or infinities
      exp = 0xff;
      mantissa <<= (23 - 10);
    } else if (mantissa == 0 && exp == 0) {
      // zero
    } else {
      if (exp == 0) {
        // denormal half float becomes a normal float
        int shift = Integer.numberOfLeadingZeros(mantissa) - (32 - 11);
        mantissa = (mantissa << shift) & 0x3ff; // clear the implicit bit
        exp = exp - shift + 1;
      }
      exp = exp + 127 - 15;
      mantissa <<= (23 - 10);
    }

    return Float.intBitsToFloat((sign << 31) | (exp << 23) | mantissa);
  }

  static void shortToSortableBytes(short value, byte[] result, int offset) {
    // Flip the sign bit, so negative shorts sort before positive shorts correctly:
    value ^= 0x8000;
    result[offset] = (byte) (value >>  8);
    result[offset+1] = (byte) value;
  }

  static short sortableBytesToShort(byte[] encoded, int offset) {
    short x = (short) (((encoded[offset] & 0xFF) <<  8) |  (encoded[offset+1] & 0xFF));
    // Re-flip the sign bit to restore the original value:
    return (short) (x ^ 0x8000);
  }

  private static FieldType getType(int numDims) {
    FieldType type = new FieldType();
    type.setDimensions(numDims, BYTES);
    type.freeze();
    return type;
  }

  @Override
  public void setFloatValue(float value) {
    setFloatValues(value);
  }

  /** Change the values of this field */
  public void setFloatValues(float... point) {
    if (type.pointDimensionCount() != point.length) {
      throw new IllegalArgumentException("this field (name=" + name + ") uses " + type.pointDimensionCount() + " dimensions; cannot change to (incoming) " + point.length + " dimensions");
    }
    fieldsData = pack(point);
  }

  @Override
  public void setBytesValue(BytesRef bytes) {
    throw new IllegalArgumentException("cannot change value type from float to BytesRef");
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

  private static BytesRef pack(float... point) {
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

  /** Creates a new FloatPoint, indexing the
   *  provided N-dimensional float point.
   *
   *  @param name field name
   *  @param point float[] value
   *  @throws IllegalArgumentException if the field name or value is null.
   */
  public HalfFloatPoint(String name, float... point) {
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

  /** Encode single float dimension */
  public static void encodeDimension(float value, byte dest[], int offset) {
    shortToSortableBytes(halfFloatToSortableShort(value), dest, offset);
  }

  /** Decode single float dimension */
  public static float decodeDimension(byte value[], int offset) {
    return sortableShortToHalfFloat(sortableBytesToShort(value, offset));
  }

  // static methods for generating queries

  /**
   * Create a query for matching an exact half-float value. It will be rounded
   * to the closest half-float if {@code value} cannot be represented accurately
   * as a half-float.
   * <p>
   * This is for simple one-dimension points, for multidimensional points use
   * {@link #newRangeQuery(String, float[], float[])} instead.
   *
   * @param field field name. must not be {@code null}.
   * @param value half-float value
   * @throws IllegalArgumentException if {@code field} is null.
   * @return a query matching documents with this exact value
   */
  public static Query newExactQuery(String field, float value) {
    return newRangeQuery(field, value, value);
  }

  /**
   * Create a range query for half-float values. Bounds will be rounded to the
   * closest half-float if they cannot be represented accurately as a
   * half-float.
   * <p>
   * This is for simple one-dimension ranges, for multidimensional ranges use
   * {@link #newRangeQuery(String, float[], float[])} instead.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue = Float.NEGATIVE_INFINITY} or {@code upperValue = Float.POSITIVE_INFINITY}.
   * <p> Ranges are inclusive. For exclusive ranges, pass {@code nextUp(lowerValue)}
   * or {@code nextDown(upperValue)}.
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
   * Create a range query for n-dimensional half-float values. Bounds will be
   * rounded to the closest half-float if they cannot be represented accurately
   * as a half-float.
   * <p>
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting {@code lowerValue[i] = Float.NEGATIVE_INFINITY} or {@code upperValue[i] = Float.POSITIVE_INFINITY}.
   * <p> Ranges are inclusive. For exclusive ranges, pass {@code nextUp(lowerValue[i])}
   * or {@code nextDown(upperValue[i])}.
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
   * Create a query matching any of the specified 1D values.
   * This is the points equivalent of {@code TermsQuery}.
   * Values will be rounded to the closest half-float if they
   * cannot be represented accurately as a half-float.
   *
   * @param field field name. must not be {@code null}.
   * @param values all values to match
   */
  public static Query newSetQuery(String field, float... values) {

    // Don't unexpectedly change the user's incoming values array:
    float[] sortedValues = values.clone();
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
