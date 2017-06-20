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

import java.net.InetAddress;

import org.apache.lucene.document.RangeFieldQuery.QueryType;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

/**
 * An indexed InetAddress Range Field
 * <p>
 * This field indexes an {@code InetAddress} range defined as a min/max pairs. It is single
 * dimension only (indexed as two 16 byte paired values).
 * <p>
 * Multiple values are supported.
 *
 * <p>
 * This field defines the following static factory methods for common search operations over Ip Ranges
 * <ul>
 *   <li>{@link #newIntersectsQuery newIntersectsQuery()} matches ip ranges that intersect the defined search range.
 *   <li>{@link #newWithinQuery newWithinQuery()} matches ip ranges that are within the defined search range.
 *   <li>{@link #newContainsQuery newContainsQuery()} matches ip ranges that contain the defined search range.
 *   <li>{@link #newCrossesQuery newCrossesQuery()} matches ip ranges that cross the defined search range
 * </ul>
 */
public class InetAddressRange extends Field {
  /** The number of bytes per dimension : sync w/ {@code InetAddressPoint} */
  public static final int BYTES = InetAddressPoint.BYTES;

  private static final FieldType TYPE;
  static {
    TYPE = new FieldType();
    TYPE.setDimensions(2, BYTES);
    TYPE.freeze();
  }

  /**
   * Create a new InetAddressRange from min/max value
   * @param name field name. must not be null.
   * @param min range min value; defined as an {@code InetAddress}
   * @param max range max value; defined as an {@code InetAddress}
   */
  public InetAddressRange(String name, final InetAddress min, final InetAddress max) {
    super(name, TYPE);
    setRangeValues(min, max);
  }

  /**
   * Change (or set) the min/max values of the field.
   * @param min range min value; defined as an {@code InetAddress}
   * @param max range max value; defined as an {@code InetAddress}
   */
  public void setRangeValues(InetAddress min, InetAddress max) {
    final byte[] bytes;
    if (fieldsData == null) {
      bytes = new byte[BYTES*2];
      fieldsData = new BytesRef(bytes);
    } else {
      bytes = ((BytesRef)fieldsData).bytes;
    }
    encode(min, max, bytes);
  }

  /** encode the min/max range into the provided byte array */
  private static void encode(final InetAddress min, final InetAddress max, final byte[] bytes) {
    // encode min and max value (consistent w/ InetAddressPoint encoding)
    final byte[] minEncoded = InetAddressPoint.encode(min);
    final byte[] maxEncoded = InetAddressPoint.encode(max);
    // ensure min is lt max
    if (StringHelper.compare(BYTES, minEncoded, 0, maxEncoded, 0) > 0) {
      throw new IllegalArgumentException("min value cannot be greater than max value for InetAddressRange field");
    }
    System.arraycopy(minEncoded, 0, bytes, 0, BYTES);
    System.arraycopy(maxEncoded, 0, bytes, BYTES, BYTES);
  }

  /** encode the min/max range and return the byte array */
  private static byte[] encode(InetAddress min, InetAddress max) {
    byte[] b = new byte[BYTES*2];
    encode(min, max, b);
    return b;
  }

  /**
   * Create a query for matching indexed ip ranges that {@code INTERSECT} the defined range.
   * @param field field name. must not be null.
   * @param min range min value; provided as an {@code InetAddress}
   * @param max range max value; provided as an {@code InetAddress}
   * @return query for matching intersecting ranges (overlap, within, crosses, or contains)
   * @throws IllegalArgumentException if {@code field} is null, {@code min} or {@code max} is invalid
   */
  public static Query newIntersectsQuery(String field, final InetAddress min, final InetAddress max) {
    return newRelationQuery(field, min, max, QueryType.INTERSECTS);
  }

  /**
   * Create a query for matching indexed ip ranges that {@code CONTAINS} the defined range.
   * @param field field name. must not be null.
   * @param min range min value; provided as an {@code InetAddress}
   * @param max range max value; provided as an {@code InetAddress}
   * @return query for matching intersecting ranges (overlap, within, crosses, or contains)
   * @throws IllegalArgumentException if {@code field} is null, {@code min} or {@code max} is invalid
   */
  public static Query newContainsQuery(String field, final InetAddress min, final InetAddress max) {
    return newRelationQuery(field, min, max, QueryType.CONTAINS);
  }

  /**
   * Create a query for matching indexed ip ranges that are {@code WITHIN} the defined range.
   * @param field field name. must not be null.
   * @param min range min value; provided as an {@code InetAddress}
   * @param max range max value; provided as an {@code InetAddress}
   * @return query for matching intersecting ranges (overlap, within, crosses, or contains)
   * @throws IllegalArgumentException if {@code field} is null, {@code min} or {@code max} is invalid
   */
  public static Query newWithinQuery(String field, final InetAddress min, final InetAddress max) {
    return newRelationQuery(field, min, max, QueryType.WITHIN);
  }

  /**
   * Create a query for matching indexed ip ranges that {@code CROSS} the defined range.
   * @param field field name. must not be null.
   * @param min range min value; provided as an {@code InetAddress}
   * @param max range max value; provided as an {@code InetAddress}
   * @return query for matching intersecting ranges (overlap, within, crosses, or contains)
   * @throws IllegalArgumentException if {@code field} is null, {@code min} or {@code max} is invalid
   */
  public static Query newCrossesQuery(String field, final InetAddress min, final InetAddress max) {
    return newRelationQuery(field, min, max, QueryType.CROSSES);
  }

  /** helper method for creating the desired relational query */
  private static Query newRelationQuery(String field, final InetAddress min, final InetAddress max, QueryType relation) {
    return new RangeFieldQuery(field, encode(min, max), 1, relation) {
      @Override
      protected String toString(byte[] ranges, int dimension) {
        return InetAddressRange.toString(ranges, dimension);
      }
    };
  }

  /**
   * Returns the String representation for the range at the given dimension
   * @param ranges the encoded ranges, never null
   * @param dimension the dimension of interest (not used for this field)
   * @return The string representation for the range at the provided dimension
   */
  private static String toString(byte[] ranges, int dimension) {
    byte[] min = new byte[BYTES];
    System.arraycopy(ranges, 0, min, 0, BYTES);
    byte[] max = new byte[BYTES];
    System.arraycopy(ranges, BYTES, max, 0, BYTES);
    return "[" + InetAddressPoint.decode(min) + " : " + InetAddressPoint.decode(max) + "]";
  }
}
