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
package org.apache.lucene.sandbox.document;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.RangeFieldQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * An indexed 2-Dimension Bounding Box field for the Geospatial Lat/Lon Coordinate system
 *
 * <p>This field indexes 2-dimension Latitude, Longitude based Geospatial Bounding Boxes. The
 * bounding boxes are defined as {@code minLat, minLon, maxLat, maxLon} where min/max lat,lon pairs
 * using double floating point precision.
 *
 * <p>Multiple values for the same field in one document is supported.
 *
 * <p>This field defines the following static factory methods for common search operations over
 * double ranges:
 *
 * <ul>
 *   <li>{@link #newIntersectsQuery newIntersectsQuery()} matches bounding boxes that intersect the
 *       defined search bounding box.
 *   <li>{@link #newWithinQuery newWithinQuery()} matches bounding boxes that are within the defined
 *       search bounding box.
 *   <li>{@link #newContainsQuery newContainsQuery()} matches bounding boxes that contain the
 *       defined search bounding box.
 *   <li>{@link #newCrossesQuery newCrosses()} matches bounding boxes that cross the defined search
 *       bounding box.
 * </ul>
 *
 * <p>The following Field limitations and restrictions apply:
 *
 * <ul>
 *   <li>Dateline wrapping is not supported.
 *   <li>Due to an encoding limitation Eastern and Western Hemisphere Bounding Boxes that share the
 *       dateline are not supported.
 * </ul>
 */
public class LatLonBoundingBox extends Field {
  /** uses same encoding as {@link LatLonPoint} so numBytes is the same */
  public static final int BYTES = LatLonPoint.BYTES;

  /**
   * Create a new 2D GeoBoundingBoxField representing a 2 dimensional geospatial bounding box
   *
   * @param name field name. must not be null
   * @param minLat minimum latitude value (in degrees); valid in [-90.0 : 90.0]
   * @param minLon minimum longitude value (in degrees); valid in [-180.0 : 180.0]
   * @param maxLat maximum latitude value (in degrees); valid in [minLat : 90.0]
   * @param maxLon maximum longitude value (in degrees); valid in [minLon : 180.0]
   */
  public LatLonBoundingBox(
      String name,
      final double minLat,
      final double minLon,
      final double maxLat,
      final double maxLon) {
    super(name, getType(2));
    setRangeValues(minLat, minLon, maxLat, maxLon);
  }

  /** set the field type */
  static FieldType getType(int geoDimensions) {
    FieldType ft = new FieldType();
    ft.setDimensions(geoDimensions * 2, BYTES);
    ft.freeze();
    return ft;
  }

  /**
   * Changes the values of the field
   *
   * @param minLat minimum latitude value (in degrees); valid in [-90.0 : 90.0]
   * @param minLon minimum longitude value (in degrees); valid in [-180.0 : 180.0]
   * @param maxLat maximum latitude value (in degrees); valid in [minLat : 90.0]
   * @param maxLon maximum longitude value (in degrees); valid in [minLon : 180.0]
   * @throws IllegalArgumentException if {@code min} or {@code max} is invalid
   */
  public void setRangeValues(double minLat, double minLon, double maxLat, double maxLon) {
    checkArgs(minLat, minLon, maxLat, maxLon);
    final byte[] bytes;
    if (fieldsData == null) {
      bytes = new byte[4 * BYTES];
      fieldsData = new BytesRef(bytes);
    } else {
      bytes = ((BytesRef) fieldsData).bytes;
    }
    encode(minLat, minLon, bytes, 0);
    encode(maxLat, maxLon, bytes, 2 * BYTES);
  }

  /** validate the two-dimension arguments */
  static void checkArgs(
      final double minLat, final double minLon, final double maxLat, final double maxLon) {
    // dateline crossing not supported
    if (minLon > maxLon) {
      throw new IllegalArgumentException(
          "cannot have minLon [" + minLon + "] exceed maxLon [" + maxLon + "].");
    }
    // pole crossing not supported
    if (minLat > maxLat) {
      throw new IllegalArgumentException(
          "cannot have minLat [" + minLat + "] exceed maxLat [" + maxLat + "].");
    }
  }

  /**
   * Create a new 2d query that finds all indexed 2d GeoBoundingBoxField values that intersect the
   * defined 3d bounding ranges
   *
   * @param field field name. must not be null
   * @param minLat minimum latitude value (in degrees); valid in [-90.0 : 90.0]
   * @param minLon minimum longitude value (in degrees); valid in [-180.0 : 180.0]
   * @param maxLat maximum latitude value (in degrees); valid in [minLat : 90.0]
   * @param maxLon maximum longitude value (in degrees); valid in [minLon : 180.0]
   * @return query for matching intersecting 2d bounding boxes
   */
  public static Query newIntersectsQuery(
      String field,
      final double minLat,
      final double minLon,
      final double maxLat,
      final double maxLon) {
    return newRangeQuery(
        field, minLat, minLon, maxLat, maxLon, RangeFieldQuery.QueryType.INTERSECTS);
  }

  /**
   * Create a new 2d query that finds all indexed 2d GeoBoundingBoxField values that are within the
   * defined 2d bounding box
   *
   * @param field field name. must not be null
   * @param minLat minimum latitude value (in degrees); valid in [-90.0 : 90.0]
   * @param minLon minimum longitude value (in degrees); valid in [-180.0 : 180.0]
   * @param maxLat maximum latitude value (in degrees); valid in [minLat : 90.0]
   * @param maxLon maximum longitude value (in degrees); valid in [minLon : 180.0]
   * @return query for matching 3d bounding boxes that are within the defined bounding box
   */
  public static Query newWithinQuery(
      String field,
      final double minLat,
      final double minLon,
      final double maxLat,
      final double maxLon) {
    return newRangeQuery(field, minLat, minLon, maxLat, maxLon, RangeFieldQuery.QueryType.WITHIN);
  }

  /**
   * Create a new 2d query that finds all indexed 2d GeoBoundingBoxField values that contain the
   * defined 2d bounding box
   *
   * @param field field name. must not be null
   * @param minLat minimum latitude value (in degrees); valid in [-90.0 : 90.0]
   * @param minLon minimum longitude value (in degrees); valid in [-180.0 : 180.0]
   * @param maxLat maximum latitude value (in degrees); valid in [minLat : 90.0]
   * @param maxLon maximum longitude value (in degrees); valid in [minLon : 180.0]
   * @return query for matching 2d bounding boxes that contain the defined bounding box
   */
  public static Query newContainsQuery(
      String field,
      final double minLat,
      final double minLon,
      final double maxLat,
      final double maxLon) {
    return newRangeQuery(field, minLat, minLon, maxLat, maxLon, RangeFieldQuery.QueryType.CONTAINS);
  }

  /**
   * Create a new 2d query that finds all indexed 2d GeoBoundingBoxField values that cross the
   * defined 3d bounding box
   *
   * @param field field name. must not be null
   * @param minLat minimum latitude value (in degrees); valid in [-90.0 : 90.0]
   * @param minLon minimum longitude value (in degrees); valid in [-180.0 : 180.0]
   * @param maxLat maximum latitude value (in degrees); valid in [minLat : 90.0]
   * @param maxLon maximum longitude value (in degrees); valid in [minLon : 180.0]
   * @return query for matching 2d bounding boxes that cross the defined bounding box
   */
  public static Query newCrossesQuery(
      String field,
      final double minLat,
      final double minLon,
      final double maxLat,
      final double maxLon) {
    return newRangeQuery(field, minLat, minLon, maxLat, maxLon, RangeFieldQuery.QueryType.CROSSES);
  }

  /** helper method to create a two-dimensional geospatial bounding box query */
  private static Query newRangeQuery(
      String field,
      final double minLat,
      final double minLon,
      final double maxLat,
      final double maxLon,
      final RangeFieldQuery.QueryType queryType) {
    checkArgs(minLat, minLon, maxLat, maxLon);
    return new RangeFieldQuery(field, encode(minLat, minLon, maxLat, maxLon), 2, queryType) {
      @Override
      protected String toString(byte[] ranges, int dimension) {
        return LatLonBoundingBox.toString(ranges, dimension);
      }
    };
  }

  /** encodes a two-dimensional geo bounding box into a byte array */
  static byte[] encode(double minLat, double minLon, double maxLat, double maxLon) {
    byte[] b = new byte[BYTES * 4];
    encode(minLat, minLon, b, 0);
    encode(maxLat, maxLon, b, BYTES * 2);
    return b;
  }

  /** encodes a two-dimensional geopoint (lat, lon) into a byte array */
  static void encode(double lat, double lon, byte[] result, int offset) {
    if (result == null) {
      result = new byte[BYTES * 4];
    }
    NumericUtils.intToSortableBytes(encodeLatitude(lat), result, offset);
    NumericUtils.intToSortableBytes(encodeLongitude(lon), result, offset + BYTES);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(" <");
    sb.append(name);
    sb.append(':');
    sb.append('[');
    byte[] b = ((BytesRef) fieldsData).bytes;
    sb.append(toString(b, 0));
    sb.append(',');
    sb.append(toString(b, 1));
    sb.append(']');
    sb.append('>');
    return sb.toString();
  }

  private static String toString(byte[] ranges, int dimension) {
    double lat, lon;
    switch (dimension) {
      case 0:
        lat = decodeLatitude(ranges, 0);
        lon = decodeLongitude(ranges, 4);
        break;
      case 1:
        lat = decodeLatitude(ranges, 8);
        lon = decodeLongitude(ranges, 12);
        break;
      default:
        throw new IllegalArgumentException("invalid dimension [" + dimension + "] in toString");
    }
    return lat + "," + lon;
  }
}
