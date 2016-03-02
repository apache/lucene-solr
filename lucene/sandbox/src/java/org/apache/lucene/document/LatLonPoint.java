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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.util.GeoUtils;

/** 
 * An indexed location field.
 * <p>
 * Finding all documents within a range at search time is
 * efficient.  Multiple values for the same field in one document
 * is allowed. 
 * <p>
 * This field defines static factory methods for creating common queries:
 * <ul>
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching points within a bounding box.
 *   <li>{@link #newDistanceQuery newDistanceQuery()} for matching points within a specified distance.
 *   <li>{@link #newPolygonQuery newPolygonQuery()} for matching points within an arbitrary polygon.
 * </ul>
 * <p>
 * <b>WARNING</b>: Values are indexed with some loss of precision, incurring up to 1E-7 error from the
 * original {@code double} values. 
 */
// TODO ^^^ that is very sandy and hurts the API, usage, and tests tremendously, because what the user passes
// to the field is not actually what gets indexed. Float would be 1E-5 error vs 1E-7, but it might be
// a better tradeoff? then it would be completely transparent to the user and lucene would be "lossless".
public class LatLonPoint extends Field {
  /**
   * Type for an indexed LatLonPoint
   * <p>
   * Each point stores two dimensions with 4 bytes per dimension.
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(2, Integer.BYTES);
    TYPE.freeze();
  }
  
  /**
   * Change the values of this field
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @throws IllegalArgumentException if latitude or longitude are out of bounds
   */
  public void setLocationValue(double latitude, double longitude) {
    byte[] bytes = new byte[8];
    NumericUtils.intToBytes(encodeLatitude(latitude), bytes, 0);
    NumericUtils.intToBytes(encodeLongitude(longitude), bytes, Integer.BYTES);
    fieldsData = new BytesRef(bytes);
  }

  /** 
   * Creates a new LatLonPoint with the specified latitude and longitude
   * @param name field name
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of bounds
   */
  public LatLonPoint(String name, double latitude, double longitude) {
    super(name, TYPE);
    setLocationValue(latitude, longitude);
  }

  private static final int BITS = 32;
  private static final double LONGITUDE_SCALE = (0x1L<<BITS)/360.0D;
  private static final double LATITUDE_SCALE = (0x1L<<BITS)/180.0D;
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    BytesRef bytes = (BytesRef) fieldsData;
    result.append(decodeLatitude(BytesRef.deepCopyOf(bytes).bytes, 0));
    result.append(',');
    result.append(decodeLongitude(BytesRef.deepCopyOf(bytes).bytes, Integer.BYTES));

    result.append('>');
    return result.toString();
  }

  // public helper methods (e.g. for queries)

  /** 
   * Quantizes double (64 bit) latitude into 32 bits 
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @return encoded value as a 32-bit {@code int}
   * @throws IllegalArgumentException if latitude is out of bounds
   */
  public static int encodeLatitude(double latitude) {
    if (GeoUtils.isValidLat(latitude) == false) {
      throw new IllegalArgumentException("invalid latitude: " + latitude + ", must be -90 to 90");
    }
    // the maximum possible value cannot be encoded without overflow
    if (latitude == 90.0D) {
      latitude = Math.nextDown(latitude);
    }
    return Math.toIntExact((long) (latitude * LATITUDE_SCALE));
  }

  /** 
   * Quantizes double (64 bit) longitude into 32 bits 
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @return encoded value as a 32-bit {@code int}
   * @throws IllegalArgumentException if longitude is out of bounds
   */
  public static int encodeLongitude(double longitude) {
    if (GeoUtils.isValidLon(longitude) == false) {
      throw new IllegalArgumentException("invalid longitude: " + longitude + ", must be -180 to 180");
    }
    // the maximum possible value cannot be encoded without overflow
    if (longitude == 180.0D) {
      longitude = Math.nextDown(longitude);
    }
    return Math.toIntExact((long) (longitude * LONGITUDE_SCALE));
  }

  /** 
   * Turns quantized value from {@link #encodeLatitude} back into a double. 
   * @param encoded encoded value: 32-bit quantized value.
   * @return decoded latitude value.
   */
  public static double decodeLatitude(int encoded) {
    double result = encoded / LATITUDE_SCALE;
    assert GeoUtils.isValidLat(result);
    return result;
  }
  
  /** 
   * Turns quantized value from byte array back into a double. 
   * @param src byte array containing 4 bytes to decode at {@code offset}
   * @param offset offset into {@code src} to decode from.
   * @return decoded latitude value.
   */
  public static double decodeLatitude(byte[] src, int offset) {
    return decodeLatitude(NumericUtils.bytesToInt(src, offset));
  }

  /** 
   * Turns quantized value from {@link #encodeLongitude} back into a double. 
   * @param encoded encoded value: 32-bit quantized value.
   * @return decoded longitude value.
   */  
  public static double decodeLongitude(int encoded) {
    double result = encoded / LONGITUDE_SCALE;
    assert GeoUtils.isValidLon(result);
    return result;
  }

  /** 
   * Turns quantized value from byte array back into a double. 
   * @param src byte array containing 4 bytes to decode at {@code offset}
   * @param offset offset into {@code src} to decode from.
   * @return decoded longitude value.
   */
  public static double decodeLongitude(byte[] src, int offset) {
    return decodeLongitude(NumericUtils.bytesToInt(src, offset));
  }
  
  /** sugar encodes a single point as a 2D byte array */
  private static byte[][] encode(double latitude, double longitude) {
    byte[][] bytes = new byte[2][];
    bytes[0] = new byte[4];
    NumericUtils.intToBytes(encodeLatitude(latitude), bytes[0], 0);
    bytes[1] = new byte[4];
    NumericUtils.intToBytes(encodeLongitude(longitude), bytes[1], 0);
    return bytes;
  }

  /** helper: checks a fieldinfo and throws exception if its definitely not a LatLonPoint */
  static void checkCompatible(FieldInfo fieldInfo) {
    if (fieldInfo.getPointDimensionCount() != TYPE.pointDimensionCount()) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with numDims=" + fieldInfo.getPointDimensionCount() + 
                                         " but this point type has numDims=" + TYPE.pointDimensionCount() + 
                                         ", is the field really a LatLonPoint?");
    }
    if (fieldInfo.getPointNumBytes() != TYPE.pointNumBytes()) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with bytesPerDim=" + fieldInfo.getPointNumBytes() + 
                                         " but this point type has bytesPerDim=" + TYPE.pointNumBytes() + 
                                         ", is the field really a LatLonPoint?");
    }
  }

  // static methods for generating queries

  /**
   * Create a query for matching a bounding box.
   * <p>
   * The box may cross over the dateline.
   * @param field field name. cannot be null.
   * @param minLatitude latitude lower bound: must be within standard +/-90 coordinate bounds.
   * @param maxLatitude latitude upper bound: must be within standard +/-90 coordinate bounds.
   * @param minLongitude longitude lower bound: must be within standard +/-180 coordinate bounds.
   * @param maxLongitude longitude upper bound: must be within standard +/-180 coordinate bounds.
   * @return query matching points within this box
   * @throws IllegalArgumentException if {@code field} is null, or the box has invalid coordinates.
   */
  public static Query newBoxQuery(String field, double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
    byte[][] lower = encode(minLatitude, minLongitude);
    byte[][] upper = encode(maxLatitude, maxLongitude);
    // Crosses date line: we just rewrite into OR of two bboxes, with longitude as an open range:
    if (maxLongitude < minLongitude) {
      // Disable coord here because a multi-valued doc could match both rects and get unfairly boosted:
      BooleanQuery.Builder q = new BooleanQuery.Builder();
      q.setDisableCoord(true);

      // E.g.: maxLon = -179, minLon = 179
      byte[][] leftOpen = new byte[2][];
      leftOpen[0] = lower[0];
      // leave longitude open
      leftOpen[1] = new byte[Integer.BYTES];
      NumericUtils.intToBytes(Integer.MIN_VALUE, leftOpen[1], 0);
      Query left = newBoxInternal(field, leftOpen, upper);
      q.add(new BooleanClause(left, BooleanClause.Occur.SHOULD));

      byte[][] rightOpen = new byte[2][];
      rightOpen[0] = upper[0];
      // leave longitude open
      rightOpen[1] = new byte[Integer.BYTES];
      NumericUtils.intToBytes(Integer.MAX_VALUE, rightOpen[1], 0);
      Query right = newBoxInternal(field, lower, rightOpen);
      q.add(new BooleanClause(right, BooleanClause.Occur.SHOULD));
      return new ConstantScoreQuery(q.build());
    } else {
      return newBoxInternal(field, lower, upper);
    }
  }
  
  private static Query newBoxInternal(String field, byte[][] min, byte[][] max) {
    return new PointRangeQuery(field, min, max) {
      @Override
      protected String toString(int dimension, byte[] value) {
        if (dimension == 0) {
          return Double.toString(decodeLatitude(value, 0));
        } else if (dimension == 1) {
          return Double.toString(decodeLongitude(value, 0));
        } else {
          throw new AssertionError();
        }
      }
    };
  }
  
  /**
   * Create a query for matching points within the specified distance of the supplied location.
   * @param field field name. cannot be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param radiusMeters maximum distance from the center in meters: must be non-negative and finite.
   * @return query matching points within this distance
   * @throws IllegalArgumentException if {@code field} is null, location has invalid coordinates, or radius is invalid.
   */
  public static Query newDistanceQuery(String field, double latitude, double longitude, double radiusMeters) {
    return new LatLonPointDistanceQuery(field, latitude, longitude, radiusMeters);
  }
  
  /** 
   * Create a query for matching a polygon.
   * <p>
   * The supplied {@code polyLats}/{@code polyLons} must be clockwise or counter-clockwise.
   * @param field field name. cannot be null.
   * @param polyLats latitude values for points of the polygon: must be within standard +/-90 coordinate bounds.
   * @param polyLons longitude values for points of the polygon: must be within standard +/-180 coordinate bounds.
   * @return query matching points within this polygon
   * @throws IllegalArgumentException if {@code field} is null, {@code polyLats} is null or has invalid coordinates, 
   *                                  {@code polyLons} is null or has invalid coordinates, if {@code polyLats} has a different
   *                                  length than {@code polyLons}, if the polygon has less than 4 points, or if polygon is 
   *                                  not closed (first and last points should be the same)
   */
  public static Query newPolygonQuery(String field, double[] polyLats, double[] polyLons) {
    return new LatLonPointInPolygonQuery(field, polyLats, polyLons);
  }
}
