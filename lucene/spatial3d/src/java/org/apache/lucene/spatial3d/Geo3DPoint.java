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
package org.apache.lucene.spatial3d;

import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.GeoCircleFactory;
import org.apache.lucene.spatial3d.geom.GeoBBoxFactory;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.GeoPath;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * Add this to a document to index lat/lon or x/y/z point, indexed as a 3D point.
 * Multiple values are allowed: just add multiple Geo3DPoint to the document with the
 * same field name.
 * <p>
 * This field defines static factory methods for creating a shape query:
 * <ul>
 *   <li>{@link #newShapeQuery newShapeQuery()} for matching all points inside a specified shape
 * </ul>
 * @see PointValues
 *  @lucene.experimental */
public final class Geo3DPoint extends Field {

  /** Mean radius of the earth, in meters */
  protected final static double MEAN_EARTH_RADIUS_METERS = 6371008.7714;
  
  /** How many radians are in one earth surface meter */
  protected final static double RADIANS_PER_METER = 1.0 / MEAN_EARTH_RADIUS_METERS;
  
  /** Indexing {@link FieldType}. */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(3, Integer.BYTES);
    TYPE.freeze();
  }

  /** 
   * Creates a new Geo3DPoint field with the specified latitude, longitude (in degrees).
   *
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of bounds
   */
  public Geo3DPoint(String name, double latitude, double longitude) {
    super(name, TYPE);
    checkLatitude(latitude);
    checkLongitude(longitude);
    // Translate latitude/longitude to x,y,z:
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, fromDegrees(latitude), fromDegrees(longitude));
    fillFieldsData(point.x, point.y, point.z);
  }

  /** Converts degress to radians */
  protected static double fromDegrees(final double degrees) {
    return Math.toRadians(degrees);
  }
  
  /** Converts earth-surface meters to radians */
  protected static double fromMeters(final double meters) {
    return meters * RADIANS_PER_METER;
  }

  /**
   * Create a query for matching points within the specified distance of the supplied location.
   * @param field field name. must not be null.  Note that because
   * {@link PlanetModel#WGS84} is used, this query is approximate and may have up
   * to 0.5% error.
   *
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param radiusMeters maximum distance from the center in meters: must be non-negative and finite.
   * @return query matching points within this distance
   * @throws IllegalArgumentException if {@code field} is null, location has invalid coordinates, or radius is invalid.
   */
  public static Query newDistanceQuery(final String field, final double latitude, final double longitude, final double radiusMeters) {
    checkLatitude(latitude);
    checkLongitude(longitude);
    final GeoShape shape = GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, fromDegrees(latitude), fromDegrees(longitude), fromMeters(radiusMeters));
    return newShapeQuery(field, shape);
  }
  
  /**
   * Create a query for matching a box.
   * <p>
   * The box may cross over the dateline.
   * @param field field name. must not be null.
   * @param minLatitude latitude lower bound: must be within standard +/-90 coordinate bounds.
   * @param maxLatitude latitude upper bound: must be within standard +/-90 coordinate bounds.
   * @param minLongitude longitude lower bound: must be within standard +/-180 coordinate bounds.
   * @param maxLongitude longitude upper bound: must be within standard +/-180 coordinate bounds.
   * @return query matching points within this box
   * @throws IllegalArgumentException if {@code field} is null, or the box has invalid coordinates.
   */
  public static Query newBoxQuery(final String field, final double minLatitude, final double maxLatitude, final double minLongitude, final double maxLongitude) {
    checkLatitude(minLatitude);
    checkLongitude(minLongitude);
    checkLatitude(maxLatitude);
    checkLongitude(maxLongitude);
    final GeoShape shape = GeoBBoxFactory.makeGeoBBox(PlanetModel.WGS84, 
      fromDegrees(maxLatitude), fromDegrees(minLatitude), fromDegrees(minLongitude), fromDegrees(maxLongitude));
    return newShapeQuery(field, shape);
  }

  /** 
   * Create a query for matching a polygon.
   * <p>
   * The supplied {@code polyLatitudes}/{@code polyLongitudes} must be clockwise or counter-clockwise.
   * @param field field name. must not be null.
   * @param polyLatitudes latitude values for points of the polygon: must be within standard +/-90 coordinate bounds.
   * @param polyLongitudes longitude values for points of the polygon: must be within standard +/-180 coordinate bounds.
   * @return query matching points within this polygon
   */
  public static Query newPolygonQuery(final String field, final double[] polyLatitudes, final double[] polyLongitudes) {
    if (polyLatitudes.length != polyLongitudes.length) {
      throw new IllegalArgumentException("same number of latitudes and longitudes required");
    }
    if (polyLatitudes.length < 4) {
      throw new IllegalArgumentException("need three or more points");
    }
    if (polyLatitudes[0] != polyLatitudes[polyLatitudes.length-1] || polyLongitudes[0] != polyLongitudes[polyLongitudes.length-1]) {
      throw new IllegalArgumentException("last point must equal first point");
    }
    
    final List<GeoPoint> polyPoints = new ArrayList<>(polyLatitudes.length-1);
    for (int i = 0; i < polyLatitudes.length-1; i++) {
      final double latitude = polyLatitudes[i];
      final double longitude = polyLongitudes[i];
      checkLatitude(latitude);
      checkLongitude(longitude);
      polyPoints.add(new GeoPoint(PlanetModel.WGS84, fromDegrees(latitude), fromDegrees(longitude)));
    }
    // We don't know what the sense of the polygon is without providing the index of one vertex we know to be convex.
    // Since that doesn't fit with the "super-simple API" requirements, we just use the index of the first one, and people have to just
    // know to do it that way.
    final int convexPointIndex = 0;
    final GeoShape shape = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, polyPoints, convexPointIndex);
    return newShapeQuery(field, shape);
  }
  
  /** 
   * Create a query for matching a path.
   * <p>
   * @param field field name. must not be null.
   * @param pathLatitudes latitude values for points of the path: must be within standard +/-90 coordinate bounds.
   * @param pathLongitudes longitude values for points of the path: must be within standard +/-180 coordinate bounds.
   * @param pathWidthMeters width of the path in meters.
   * @return query matching points within this polygon
   */
  public static Query newPathQuery(final String field, final double[] pathLatitudes, final double[] pathLongitudes, final double pathWidthMeters) {
    if (pathLatitudes.length != pathLongitudes.length) {
      throw new IllegalArgumentException("same number of latitudes and longitudes required");
    }
    final GeoPoint[] points = new GeoPoint[pathLatitudes.length];
    for (int i = 0; i < pathLatitudes.length; i++) {
      final double latitude = pathLatitudes[i];
      final double longitude = pathLongitudes[i];
      checkLatitude(latitude);
      checkLongitude(longitude);
      points[i] = new GeoPoint(PlanetModel.WGS84, fromDegrees(latitude), fromDegrees(longitude));
    }
    final GeoShape shape = new GeoPath(PlanetModel.WGS84, fromMeters(pathWidthMeters), points);
    return newShapeQuery(field, shape);
  }
  
  /** 
   * Creates a new Geo3DPoint field with the specified x,y,z.
   *
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of bounds
   */
  public Geo3DPoint(String name, double x, double y, double z) {
    super(name, TYPE);
    fillFieldsData(x, y, z);
  }

  private void fillFieldsData(double x, double y, double z) {
    byte[] bytes = new byte[12];
    encodeDimension(x, bytes, 0);
    encodeDimension(y, bytes, Integer.BYTES);
    encodeDimension(z, bytes, 2*Integer.BYTES);
    fieldsData = new BytesRef(bytes);
  }

  // public helper methods (e.g. for queries)
  
  /** Encode single dimension */
  public static void encodeDimension(double value, byte bytes[], int offset) {
    NumericUtils.intToSortableBytes(Geo3DUtil.encodeValue(PlanetModel.WGS84.getMaximumMagnitude(), value), bytes, offset);
  }
  
  /** Decode single dimension */
  public static double decodeDimension(byte value[], int offset) {
    return Geo3DUtil.decodeValueCenter(PlanetModel.WGS84.getMaximumMagnitude(), NumericUtils.sortableBytesToInt(value, offset));
  }

  /** Returns a query matching all points inside the provided shape.
   * 
   * @param field field name. must not be {@code null}.
   * @param shape Which {@link GeoShape} to match
   */
  public static Query newShapeQuery(String field, GeoShape shape) {
    return new PointInGeo3DShapeQuery(field, shape);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    BytesRef bytes = (BytesRef) fieldsData;
    result.append(" x=" + decodeDimension(bytes.bytes, bytes.offset));
    result.append(" y=" + decodeDimension(bytes.bytes, bytes.offset + Integer.BYTES));
    result.append(" z=" + decodeDimension(bytes.bytes, bytes.offset + 2*Integer.BYTES));
    result.append('>');
    return result.toString();
  }

  // TODO LUCENE-7152: share this with GeoUtils.java from spatial module

  /** Minimum longitude value. */
  private static final double MIN_LON_INCL = -180.0D;

  /** Maximum longitude value. */
  private static final double MAX_LON_INCL = 180.0D;

  /** Minimum latitude value. */
  private static final double MIN_LAT_INCL = -90.0D;

  /** Maximum latitude value. */
  private static final double MAX_LAT_INCL = 90.0D;

  /** validates latitude value is within standard +/-90 coordinate bounds */
  private static void checkLatitude(double latitude) {
    if (Double.isNaN(latitude) || latitude < MIN_LAT_INCL || latitude > MAX_LAT_INCL) {
      throw new IllegalArgumentException("invalid latitude " +  latitude + "; must be between " + MIN_LAT_INCL + " and " + MAX_LAT_INCL);
    }
  }

  /** validates longitude value is within standard +/-180 coordinate bounds */
  private static void checkLongitude(double longitude) {
    if (Double.isNaN(longitude) || longitude < MIN_LON_INCL || longitude > MAX_LON_INCL) {
      throw new IllegalArgumentException("invalid longitude " +  longitude + "; must be between " + MIN_LON_INCL + " and " + MAX_LON_INCL);
    }
  }
}
