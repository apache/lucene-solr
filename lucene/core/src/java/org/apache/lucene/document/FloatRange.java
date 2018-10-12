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

import org.apache.lucene.document.RangeFieldQuery.QueryType;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureObjects;
import org.apache.lucene.util.NumericUtils;

/**
 * An indexed Float Range field.
 * <p>
 * This field indexes dimensional ranges defined as min/max pairs. It supports
 * up to a maximum of 4 dimensions (indexed as 8 numeric values). With 1 dimension representing a single float range,
 * 2 dimensions representing a bounding box, 3 dimensions a bounding cube, and 4 dimensions a tesseract.
 * <p>
 * Multiple values for the same field in one document is supported, and open ended ranges can be defined using
 * {@code Float.NEGATIVE_INFINITY} and {@code Float.POSITIVE_INFINITY}.
 *
 * <p>
 * This field defines the following static factory methods for common search operations over float ranges:
 * <ul>
 *   <li>{@link #newIntersectsQuery newIntersectsQuery()} matches ranges that intersect the defined search range.
 *   <li>{@link #newWithinQuery newWithinQuery()} matches ranges that are within the defined search range.
 *   <li>{@link #newContainsQuery newContainsQuery()} matches ranges that contain the defined search range.
 * </ul>
 */
public class FloatRange extends Field {
  /** stores float values so number of bytes is 4 */
  public static final int BYTES = Float.BYTES;

  /**
   * Create a new FloatRange type, from min/max parallel arrays
   *
   * @param name field name. must not be null.
   * @param min range min values; each entry is the min value for the dimension
   * @param max range max values; each entry is the max value for the dimension
   */
  public FloatRange(String name, final float[] min, final float[] max) {
    super(name, getType(min.length));
    setRangeValues(min, max);
  }

  /** set the field type */
  private static FieldType getType(int dimensions) {
    if (dimensions > 4) {
      throw new IllegalArgumentException("FloatRange does not support greater than 4 dimensions");
    }

    FieldType ft = new FieldType();
    // dimensions is set as 2*dimension size (min/max per dimension)
    ft.setDimensions(dimensions*2, BYTES);
    ft.freeze();
    return ft;
  }

  /**
   * Changes the values of the field.
   * @param min array of min values. (accepts {@code Float.NEGATIVE_INFINITY})
   * @param max array of max values. (accepts {@code Float.POSITIVE_INFINITY})
   * @throws IllegalArgumentException if {@code min} or {@code max} is invalid
   */
  public void setRangeValues(float[] min, float[] max) {
    checkArgs(min, max);
    if (min.length*2 != type.pointDataDimensionCount() || max.length*2 != type.pointDataDimensionCount()) {
      throw new IllegalArgumentException("field (name=" + name + ") uses " + type.pointDataDimensionCount()/2
          + " dimensions; cannot change to (incoming) " + min.length + " dimensions");
    }

    final byte[] bytes;
    if (fieldsData == null) {
      bytes = new byte[BYTES*2*min.length];
      fieldsData = new BytesRef(bytes);
    } else {
      bytes = ((BytesRef)fieldsData).bytes;
    }
    verifyAndEncode(min, max, bytes);
  }

  /** validate the arguments */
  private static void checkArgs(final float[] min, final float[] max) {
    if (min == null || max == null || min.length == 0 || max.length == 0) {
      throw new IllegalArgumentException("min/max range values cannot be null or empty");
    }
    if (min.length != max.length) {
      throw new IllegalArgumentException("min/max ranges must agree");
    }
    if (min.length > 4) {
      throw new IllegalArgumentException("FloatRange does not support greater than 4 dimensions");
    }
  }

  /**
   * Encodes the min, max ranges into a byte array
   */
  private static byte[] encode(float[] min, float[] max) {
    checkArgs(min, max);
    byte[] b = new byte[BYTES*2*min.length];
    verifyAndEncode(min, max, b);
    return b;
  }

  /**
   * encode the ranges into a sortable byte array ({@code Float.NaN} not allowed)
   * <p>
   * example for 4 dimensions (8 bytes per dimension value):
   * minD1 ... minD4 | maxD1 ... maxD4
   */
  static void verifyAndEncode(float[] min, float[] max, byte[] bytes) {
    for (int d=0,i=0,j=min.length*BYTES; d<min.length; ++d, i+=BYTES, j+=BYTES) {
      if (Double.isNaN(min[d])) {
        throw new IllegalArgumentException("invalid min value (" + Float.NaN + ")" + " in FloatRange");
      }
      if (Double.isNaN(max[d])) {
        throw new IllegalArgumentException("invalid max value (" + Float.NaN + ")" + " in FloatRange");
      }
      if (min[d] > max[d]) {
        throw new IllegalArgumentException("min value (" + min[d] + ") is greater than max value (" + max[d] + ")");
      }
      encode(min[d], bytes, i);
      encode(max[d], bytes, j);
    }
  }

  /** encode the given value into the byte array at the defined offset */
  private static void encode(float val, byte[] bytes, int offset) {
    NumericUtils.intToSortableBytes(NumericUtils.floatToSortableInt(val), bytes, offset);
  }

  /**
   * Get the min value for the given dimension
   * @param dimension the dimension, always positive
   * @return the decoded min value
   */
  public float getMin(int dimension) {
    FutureObjects.checkIndex(dimension, type.pointDataDimensionCount()/2);
    return decodeMin(((BytesRef)fieldsData).bytes, dimension);
  }

  /**
   * Get the max value for the given dimension
   * @param dimension the dimension, always positive
   * @return the decoded max value
   */
  public float getMax(int dimension) {
    FutureObjects.checkIndex(dimension, type.pointDataDimensionCount()/2);
    return decodeMax(((BytesRef)fieldsData).bytes, dimension);
  }

  /** decodes the min value (for the defined dimension) from the encoded input byte array */
  static float decodeMin(byte[] b, int dimension) {
    int offset = dimension*BYTES;
    return NumericUtils.sortableIntToFloat(NumericUtils.sortableBytesToInt(b, offset));
  }

  /** decodes the max value (for the defined dimension) from the encoded input byte array */
  static float decodeMax(byte[] b, int dimension) {
    int offset = b.length/2 + dimension*BYTES;
    return NumericUtils.sortableIntToFloat(NumericUtils.sortableBytesToInt(b, offset));
  }

  /**
   * Create a query for matching indexed ranges that intersect the defined range.
   * @param field field name. must not be null.
   * @param min array of min values. (accepts {@code Float.NEGATIVE_INFINITY})
   * @param max array of max values. (accepts {@code Float.MAX_VALUE})
   * @return query for matching intersecting ranges (overlap, within, or contains)
   * @throws IllegalArgumentException if {@code field} is null, {@code min} or {@code max} is invalid
   */
  public static Query newIntersectsQuery(String field, final float[] min, final float[] max) {
    return newRelationQuery(field, min, max, QueryType.INTERSECTS);
  }

  /**
   * Create a query for matching indexed float ranges that contain the defined range.
   * @param field field name. must not be null.
   * @param min array of min values. (accepts {@code Float.NEGATIVE_INFINITY})
   * @param max array of max values. (accepts {@code Float.POSITIVE_INFINITY})
   * @return query for matching ranges that contain the defined range
   * @throws IllegalArgumentException if {@code field} is null, {@code min} or {@code max} is invalid
   */
  public static Query newContainsQuery(String field, final float[] min, final float[] max) {
    return newRelationQuery(field, min, max, QueryType.CONTAINS);
  }

  /**
   * Create a query for matching indexed ranges that are within the defined range.
   * @param field field name. must not be null.
   * @param min array of min values. (accepts {@code Float.NEGATIVE_INFINITY})
   * @param max array of max values. (accepts {@code Float.POSITIVE_INFINITY})
   * @return query for matching ranges within the defined range
   * @throws IllegalArgumentException if {@code field} is null, {@code min} or {@code max} is invalid
   */
  public static Query newWithinQuery(String field, final float[] min, final float[] max) {
    return newRelationQuery(field, min, max, QueryType.WITHIN);
  }

  /**
   * Create a query for matching indexed ranges that cross the defined range.
   * A CROSSES is defined as any set of ranges that are not disjoint and not wholly contained by
   * the query. Effectively, its the complement of union(WITHIN, DISJOINT).
   * @param field field name. must not be null.
   * @param min array of min values. (accepts {@code Float.NEGATIVE_INFINITY})
   * @param max array of max values. (accepts {@code Float.POSITIVE_INFINITY})
   * @return query for matching ranges within the defined range
   * @throws IllegalArgumentException if {@code field} is null, {@code min} or {@code max} is invalid
   */
  public static Query newCrossesQuery(String field, final float[] min, final float[] max) {
    return newRelationQuery(field, min, max, QueryType.CROSSES);
  }

  /** helper method for creating the desired relational query */
  private static Query newRelationQuery(String field, final float[] min, final float[] max, QueryType relation) {
    checkArgs(min, max);
    return new RangeFieldQuery(field, encode(min, max), min.length, relation) {
      @Override
      protected String toString(byte[] ranges, int dimension) {
        return FloatRange.toString(ranges, dimension);
      }
    };
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(" <");
    sb.append(name);
    sb.append(':');
    byte[] b = ((BytesRef)fieldsData).bytes;
    toString(b, 0);
    for (int d = 0; d < type.pointDataDimensionCount() / 2; ++d) {
      sb.append(' ');
      sb.append(toString(b, d));
    }
    sb.append('>');

    return sb.toString();
  }

  /**
   * Returns the String representation for the range at the given dimension
   * @param ranges the encoded ranges, never null
   * @param dimension the dimension of interest
   * @return The string representation for the range at the provided dimension
   */
  private static String toString(byte[] ranges, int dimension) {
    return "[" + Float.toString(decodeMin(ranges, dimension)) + " : "
        + Float.toString(decodeMax(ranges, dimension)) + "]";
  }
}
