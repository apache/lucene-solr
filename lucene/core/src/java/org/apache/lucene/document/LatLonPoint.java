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

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;

import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * An indexed location field.
 *
 * <p>Finding all documents within a range at search time is efficient. Multiple values for the same
 * field in one document is allowed.
 *
 * <p>This field defines static factory methods for common operations:
 *
 * <ul>
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching points within a bounding box.
 *   <li>{@link #newDistanceQuery newDistanceQuery()} for matching points within a specified
 *       distance.
 *   <li>{@link #newPolygonQuery newPolygonQuery()} for matching points within an arbitrary polygon.
 *   <li>{@link #newGeometryQuery newGeometryQuery()} for matching points within an arbitrary
 *       geometry collection.
 * </ul>
 *
 * <p>If you also need per-document operations such as sort by distance, add a separate {@link
 * LatLonDocValuesField} instance. If you also need to store the value, you should add a separate
 * {@link StoredField} instance.
 *
 * <p><b>WARNING</b>: Values are indexed with some loss of precision from the original {@code
 * double} values (4.190951585769653E-8 for the latitude component and 8.381903171539307E-8 for
 * longitude).
 *
 * @see PointValues
 * @see LatLonDocValuesField
 */
// TODO ^^^ that is very sandy and hurts the API, usage, and tests tremendously, because what the
// user passes
// to the field is not actually what gets indexed. Float would be 1E-5 error vs 1E-7, but it might
// be
// a better tradeoff? then it would be completely transparent to the user and lucene would be
// "lossless".
public class LatLonPoint extends Field {
  /** LatLonPoint is encoded as integer values so number of bytes is 4 */
  public static final int BYTES = Integer.BYTES;
  /**
   * Type for an indexed LatLonPoint
   *
   * <p>Each point stores two dimensions with 4 bytes per dimension.
   */
  public static final FieldType TYPE = new FieldType();

  static {
    TYPE.setDimensions(2, Integer.BYTES);
    TYPE.freeze();
  }

  /**
   * Change the values of this field
   *
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @throws IllegalArgumentException if latitude or longitude are out of bounds
   */
  public void setLocationValue(double latitude, double longitude) {
    final byte[] bytes;

    if (fieldsData == null) {
      bytes = new byte[8];
      fieldsData = new BytesRef(bytes);
    } else {
      bytes = ((BytesRef) fieldsData).bytes;
    }

    int latitudeEncoded = encodeLatitude(latitude);
    int longitudeEncoded = encodeLongitude(longitude);
    NumericUtils.intToSortableBytes(latitudeEncoded, bytes, 0);
    NumericUtils.intToSortableBytes(longitudeEncoded, bytes, Integer.BYTES);
  }

  /**
   * Creates a new LatLonPoint with the specified latitude and longitude
   *
   * @param name field name
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of
   *     bounds
   */
  public LatLonPoint(String name, double latitude, double longitude) {
    super(name, TYPE);
    setLocationValue(latitude, longitude);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    byte bytes[] = ((BytesRef) fieldsData).bytes;
    result.append(decodeLatitude(bytes, 0));
    result.append(',');
    result.append(decodeLongitude(bytes, Integer.BYTES));

    result.append('>');
    return result.toString();
  }

  /** sugar encodes a single point as a byte array */
  private static byte[] encode(double latitude, double longitude) {
    byte[] bytes = new byte[2 * Integer.BYTES];
    NumericUtils.intToSortableBytes(encodeLatitude(latitude), bytes, 0);
    NumericUtils.intToSortableBytes(encodeLongitude(longitude), bytes, Integer.BYTES);
    return bytes;
  }

  /** sugar encodes a single point as a byte array, rounding values up */
  private static byte[] encodeCeil(double latitude, double longitude) {
    byte[] bytes = new byte[2 * Integer.BYTES];
    NumericUtils.intToSortableBytes(encodeLatitudeCeil(latitude), bytes, 0);
    NumericUtils.intToSortableBytes(encodeLongitudeCeil(longitude), bytes, Integer.BYTES);
    return bytes;
  }

  /** helper: checks a fieldinfo and throws exception if its definitely not a LatLonPoint */
  static void checkCompatible(FieldInfo fieldInfo) {
    // point/dv properties could be "unset", if you e.g. used only StoredField with this same name
    // in the segment.
    if (fieldInfo.getPointDimensionCount() != 0
            && fieldInfo.getPointDimensionCount() != TYPE.pointDimensionCount()) {
      throw new IllegalArgumentException(
              "field=\""
                      + fieldInfo.name
                      + "\" was indexed with numDims="
                      + fieldInfo.getPointDimensionCount()
                      + " but this point type has numDims="
                      + TYPE.pointDimensionCount()
                      + ", is the field really a LatLonPoint?");
    }
    if (fieldInfo.getPointNumBytes() != 0 && fieldInfo.getPointNumBytes() != TYPE.pointNumBytes()) {
      throw new IllegalArgumentException(
              "field=\""
                      + fieldInfo.name
                      + "\" was indexed with bytesPerDim="
                      + fieldInfo.getPointNumBytes()
                      + " but this point type has bytesPerDim="
                      + TYPE.pointNumBytes()
                      + ", is the field really a LatLonPoint?");
    }
  }

  // static methods for generating queries

  /**
   * Create a query for matching a bounding box.
   *
   * <p>The box may cross over the dateline.
   *
   * @param field field name. must not be null.
   * @param minLatitude latitude lower bound: must be within standard +/-90 coordinate bounds.
   * @param maxLatitude latitude upper bound: must be within standard +/-90 coordinate bounds.
   * @param minLongitude longitude lower bound: must be within standard +/-180 coordinate bounds.
   * @param maxLongitude longitude upper bound: must be within standard +/-180 coordinate bounds.
   * @return query matching points within this box
   * @throws IllegalArgumentException if {@code field} is null, or the box has invalid coordinates.
   */
  public static Query newBoxQuery(
          String field,
          double minLatitude,
          double maxLatitude,
          double minLongitude,
          double maxLongitude) {
    // exact double values of lat=90.0D and lon=180.0D must be treated special as they are not
    // represented in the encoding
    // and should not drag in extra bogus junk! TODO: should encodeCeil just throw
    // ArithmeticException to be less trappy here?
    if (minLatitude == 90.0) {
      // range cannot match as 90.0 can never exist
      return new MatchNoDocsQuery("LatLonPoint.newBoxQuery with minLatitude=90.0");
    }
    if (minLongitude == 180.0) {
      if (maxLongitude == 180.0) {
        // range cannot match as 180.0 can never exist
        return new MatchNoDocsQuery("LatLonPoint.newBoxQuery with minLongitude=maxLongitude=180.0");
      } else if (maxLongitude < minLongitude) {
        // encodeCeil() with dateline wrapping!
        minLongitude = -180.0;
      }
    }
    byte[] lower = encodeCeil(minLatitude, minLongitude);
    byte[] upper = encode(maxLatitude, maxLongitude);
    // Crosses date line: we just rewrite into OR of two bboxes, with longitude as an open range:
    if (maxLongitude < minLongitude) {
      // Disable coord here because a multi-valued doc could match both rects and get unfairly
      // boosted:
      BooleanQuery.Builder q = new BooleanQuery.Builder();

      // E.g.: maxLon = -179, minLon = 179
      byte[] leftOpen = lower.clone();
      // leave longitude open
      NumericUtils.intToSortableBytes(Integer.MIN_VALUE, leftOpen, Integer.BYTES);
      Query left = newBoxInternal(field, leftOpen, upper);
      q.add(new BooleanClause(left, BooleanClause.Occur.SHOULD));

      byte[] rightOpen = upper.clone();
      // leave longitude open
      NumericUtils.intToSortableBytes(Integer.MAX_VALUE, rightOpen, Integer.BYTES);
      Query right = newBoxInternal(field, lower, rightOpen);
      q.add(new BooleanClause(right, BooleanClause.Occur.SHOULD));
      return new ConstantScoreQuery(q.build());
    } else {
      return newBoxInternal(field, lower, upper);
    }
  }

  private static Query newBoxInternal(String field, byte[] min, byte[] max) {
    return new PointRangeQuery(field, min, max, 2) {
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
   *
   * @param field field name. must not be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param radiusMeters maximum distance from the center in meters: must be non-negative and
   *     finite.
   * @return query matching points within this distance
   * @throws IllegalArgumentException if {@code field} is null, location has invalid coordinates, or
   *     radius is invalid.
   */
  public static Query newDistanceQuery(
          String field, double latitude, double longitude, double radiusMeters) {
    return new LatLonPointDistanceQuery(field, latitude, longitude, radiusMeters);
  }

  /**
   * Create a query for matching one or more polygons.
   *
   * @param field field name. must not be null.
   * @param polygons array of polygons. must not be null or empty
   * @return query matching points within this polygon
   * @throws IllegalArgumentException if {@code field} is null, {@code polygons} is null or empty
   * @see Polygon
   */
  public static Query newPolygonQuery(String field, Polygon... polygons) {
    return newGeometryQuery(field, ShapeField.QueryRelation.INTERSECTS, polygons);
  }

  /**
   * Create a query for matching one or more geometries against the provided {@link
   * ShapeField.QueryRelation}. Line geometries are not supported for WITHIN relationship.
   *
   * @param field field name. must not be null.
   * @param queryRelation The relation the points needs to satisfy with the provided geometries,
   *     must not be null.
   * @param latLonGeometries array of LatLonGeometries. must not be null or empty.
   * @return query matching points within at least one geometry.
   * @throws IllegalArgumentException if {@code field} is null, {@code queryRelation} is null,
   *     {@code latLonGeometries} is null, empty or contain a null.
   * @see LatLonGeometry
   */
  public static Query newGeometryQuery(
          String field, ShapeField.QueryRelation queryRelation, LatLonGeometry... latLonGeometries) {
    if (queryRelation == ShapeField.QueryRelation.INTERSECTS && latLonGeometries.length == 1) {
      if (latLonGeometries[0] instanceof Rectangle) {
        final Rectangle rect = (Rectangle) latLonGeometries[0];
        return newBoxQuery(field, rect.minLat, rect.maxLat, rect.minLon, rect.maxLon);
      }
      if (latLonGeometries[0] instanceof Circle) {
        final Circle circle = (Circle) latLonGeometries[0];
        return newDistanceQuery(field, circle.getLat(), circle.getLon(), circle.getRadius());
      }
    }
    if (queryRelation == ShapeField.QueryRelation.CONTAINS) {
      return makeContainsGeometryQuery(field, latLonGeometries);
    }
    return new LatLonPointQuery(field, queryRelation, latLonGeometries);
  }

  private static Query makeContainsGeometryQuery(String field, LatLonGeometry... latLonGeometries) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (LatLonGeometry geometry : latLonGeometries) {
      if ((geometry instanceof Point) == false) {
        return new MatchNoDocsQuery(
                "Contains LatLonPoint.newGeometryQuery with non-point geometries");
      }
      builder.add(
              new LatLonPointQuery(field, ShapeField.QueryRelation.CONTAINS, geometry),
              BooleanClause.Occur.MUST);
    }
    return new ConstantScoreQuery(builder.build());
  }

  /**
   * Given a field that indexes point values into a {@link LatLonPoint} and doc values into {@link
   * LatLonDocValuesField}, this returns a query that scores documents based on their haversine
   * distance in meters to {@code (originLat, originLon)}: {@code score = weight *
   * pivotDistanceMeters / (pivotDistanceMeters + distance)}, ie. score is in the {@code [0,
   * weight]} range, is equal to {@code weight} when the document's value is equal to {@code
   * (originLat, originLon)} and is equal to {@code weight/2} when the document's value is distant
   * of {@code pivotDistanceMeters} from {@code (originLat, originLon)}. In case of multi-valued
   * fields, only the closest point to {@code (originLat, originLon)} will be considered. This query
   * is typically useful to boost results based on distance by adding this query to a {@link
   * Occur#SHOULD} clause of a {@link BooleanQuery}.
   */
  public static Query newDistanceFeatureQuery(
          String field, float weight, double originLat, double originLon, double pivotDistanceMeters) {
    Query query =
            new LatLonPointDistanceFeatureQuery(field, originLat, originLon, pivotDistanceMeters);
    if (weight != 1f) {
      query = new BoostQuery(query, weight);
    }
    return query;
  }
}
