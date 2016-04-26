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
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.GeoCircleFactory;
import org.apache.lucene.spatial3d.geom.GeoBBoxFactory;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.GeoPathFactory;
import org.apache.lucene.spatial3d.geom.GeoCompositePolygon;
import org.apache.lucene.spatial3d.geom.GeoPolygon;
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

  /** How many radians are in one earth surface meter */
  public final static double RADIANS_PER_METER = 1.0 / PlanetModel.WGS84_MEAN;
  /** How many radians are in one degree */
  public final static double RADIANS_PER_DEGREE = Math.PI / 180.0;
  
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
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
    // Translate latitude/longitude to x,y,z:
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, fromDegrees(latitude), fromDegrees(longitude));
    fillFieldsData(point.x, point.y, point.z);
  }

  /** Converts degress to radians */
  private static double fromDegrees(final double degrees) {
    return degrees * RADIANS_PER_DEGREE;
  }
  
  /** Converts earth-surface meters to radians */
  private static double fromMeters(final double meters) {
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
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
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
    GeoUtils.checkLatitude(minLatitude);
    GeoUtils.checkLongitude(minLongitude);
    GeoUtils.checkLatitude(maxLatitude);
    GeoUtils.checkLongitude(maxLongitude);
    final GeoShape shape = GeoBBoxFactory.makeGeoBBox(PlanetModel.WGS84, 
      fromDegrees(maxLatitude), fromDegrees(minLatitude), fromDegrees(minLongitude), fromDegrees(maxLongitude));
    return newShapeQuery(field, shape);
  }

  /** 
   * Create a query for matching a polygon.
   * <p>
   * The supplied {@code polygons} must be clockwise on the outside level, counterclockwise on the next level in, etc.
   * @param field field name. must not be null.
   * @param polygons is the list of polygons to use to construct the query; must be at least one.
   * @return query matching points within this polygon
   */
  public static Query newPolygonQuery(final String field, final Polygon... polygons) {
    //System.err.println("Creating polygon...");
    if (polygons.length < 1) {
      throw new IllegalArgumentException("need at least one polygon");
    }
    final GeoShape shape;
    if (polygons.length == 1) {
      final GeoShape component = fromPolygon(polygons[0]);
      if (component == null) {
        // Polygon is degenerate
        shape = new GeoCompositePolygon();
      } else {
        shape = component;
      }
    } else {
      final GeoCompositePolygon poly = new GeoCompositePolygon();
      for (final Polygon p : polygons) {
        final GeoPolygon component = fromPolygon(p);
        if (component != null) {
          poly.addShape(component);
        }
      }
      shape = poly;
    }
    //System.err.println("...done");
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
      GeoUtils.checkLatitude(latitude);
      GeoUtils.checkLongitude(longitude);
      points[i] = new GeoPoint(PlanetModel.WGS84, fromDegrees(latitude), fromDegrees(longitude));
    }
    final GeoShape shape = GeoPathFactory.makeGeoPath(PlanetModel.WGS84, fromMeters(pathWidthMeters), points);
    return newShapeQuery(field, shape);
  }
  
  /**
    * Convert a Polygon object into a GeoPolygon.
    * This method uses
    * @param polygon is the Polygon object.
    * @return the GeoPolygon.
    */
  private static GeoPolygon fromPolygon(final Polygon polygon) {
    // First, assemble the "holes".  The geo3d convention is to use the same polygon sense on the inner ring as the
    // outer ring, so we process these recursively with reverseMe flipped.
    final Polygon[] theHoles = polygon.getHoles();
    final List<GeoPolygon> holeList = new ArrayList<>(theHoles.length);
    for (final Polygon hole : theHoles) {
      //System.out.println("Hole: "+hole);
      final GeoPolygon component = fromPolygon(hole);
      if (component != null) {
        holeList.add(component);
      }
    }
    
    // Now do the polygon itself
    final double[] polyLats = polygon.getPolyLats();
    final double[] polyLons = polygon.getPolyLons();
    
    // I presume the arguments have already been checked
    final List<GeoPoint> points = new ArrayList<>(polyLats.length-1);
    // We skip the last point anyway because the API requires it to be repeated, and geo3d doesn't repeat it.
    for (int i = 0; i < polyLats.length - 1; i++) {
      final int index = polyLats.length - 2 - i;
      points.add(new GeoPoint(PlanetModel.WGS84, fromDegrees(polyLats[index]), fromDegrees(polyLons[index])));
    }
    //System.err.println(" building polygon with "+points.size()+" points...");
    final GeoPolygon rval = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, points, holeList);
    //System.err.println(" ...done");
    return rval;
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
    NumericUtils.intToSortableBytes(Geo3DUtil.encodeValue(value), bytes, offset);
  }
  
  /** Decode single dimension */
  public static double decodeDimension(byte value[], int offset) {
    return Geo3DUtil.decodeValue(NumericUtils.sortableBytesToInt(value, offset));
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

}
