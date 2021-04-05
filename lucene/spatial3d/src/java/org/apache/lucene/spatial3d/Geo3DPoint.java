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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;
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

  /** Planet Model for this Geo3DPoint */
  protected final PlanetModel planetModel;

  /** Indexing {@link FieldType}. */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(3, Integer.BYTES);
    TYPE.freeze();
  }

  /**
   * Creates a new Geo3DPoint field with the specified latitude, longitude (in degrees), with default WGS84 PlanetModel.
   *
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of bounds
   */
  public Geo3DPoint(String name, double lat, double lon) {
    this(name, PlanetModel.WGS84, lat, lon);
  }

  /**
   * Creates a new Geo3DPoint field with the specified x,y,z, using default WGS84 planet model.
   *
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of bounds
   */
  public Geo3DPoint(String name, double x, double y, double z) {
    this(name, PlanetModel.WGS84, x, y, z);
  }

  /**
   * Creates a new Geo3DPoint field with the specified x,y,z, and given planet model.
   *
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of bounds
   */
  public Geo3DPoint(String name, PlanetModel planetModel, double x, double y, double z) {
    super(name, TYPE);
    this.planetModel = planetModel;
    fillFieldsData(planetModel, x, y, z);
  }

  /**
   * Creates a new Geo3DPoint field with the specified latitude, longitude (in degrees), given a planet model.
   *
   * @throws IllegalArgumentException if the field name is null or latitude or longitude are out of bounds
   */
  public Geo3DPoint(String name, PlanetModel planetModel, double latitude, double longitude) {
    super(name, TYPE);
    GeoUtils.checkLatitude(latitude);
    GeoUtils.checkLongitude(longitude);
    this.planetModel = planetModel;
    // Translate latitude/longitude to x,y,z:
    final GeoPoint point = new GeoPoint(planetModel, Geo3DUtil.fromDegrees(latitude), Geo3DUtil.fromDegrees(longitude));
    fillFieldsData(planetModel, point.x, point.y, point.z);
  }

  /**
   * Create a query for matching points within the specified distance of the supplied location.
   * @param field field name. must not be null.  Note that if
   * {@link PlanetModel#WGS84} is used, the query is approximate and may have up
   * to 0.5% error.
   *
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param radiusMeters maximum distance from the center in meters: must be non-negative and finite.
   * @return query matching points within this distance
   * @throws IllegalArgumentException if {@code field} is null, location has invalid coordinates, or radius is invalid.
   */
  public static Query newDistanceQuery(final String field, final PlanetModel planetModel, final double latitude, final double longitude, final double radiusMeters) {
    final GeoShape shape = Geo3DUtil.fromDistance(planetModel, latitude, longitude, radiusMeters);
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
  public static Query newBoxQuery(final String field, final PlanetModel planetModel, final double minLatitude, final double maxLatitude, final double minLongitude, final double maxLongitude) {
    final GeoShape shape = Geo3DUtil.fromBox(planetModel, minLatitude, maxLatitude, minLongitude, maxLongitude);
    return newShapeQuery(field, shape);
  }

  /** 
   * Create a query for matching a polygon.  The polygon should have a limited number of edges (less than 100) and be well-defined,
   * with well-separated vertices.
   * <p>
   * The supplied {@code polygons} must be clockwise on the outside level, counterclockwise on the next level in, etc.
   * @param field field name. must not be null.
   * @param polygons is the list of polygons to use to construct the query; must be at least one.
   * @return query matching points within this polygon
   */
  public static Query newPolygonQuery(final String field, final PlanetModel planetModel, final Polygon... polygons) {
    final GeoShape shape = Geo3DUtil.fromPolygon(planetModel, polygons);
    return newShapeQuery(field, shape);
  }

  /** 
   * Create a query for matching a large polygon.  This differs from the related newPolygonQuery in that it
   * does little or no legality checking and is optimized for very large numbers of polygon edges.
   * <p>
   * The supplied {@code polygons} must be clockwise on the outside level, counterclockwise on the next level in, etc.
   * @param field field name. must not be null.
   * @param polygons is the list of polygons to use to construct the query; must be at least one.
   * @return query matching points within this polygon
   */
  public static Query newLargePolygonQuery(final String field, PlanetModel planetModel, final Polygon... polygons) {
    final GeoShape shape = Geo3DUtil.fromLargePolygon(planetModel, polygons);
    return newShapeQuery(field, shape);
  }

  /** 
   * Create a query for matching a path.
   * @param field field name. must not be null.
   * @param pathLatitudes latitude values for points of the path: must be within standard +/-90 coordinate bounds.
   * @param pathLongitudes longitude values for points of the path: must be within standard +/-180 coordinate bounds.
   * @param pathWidthMeters width of the path in meters.
   * @return query matching points within this polygon
   */
  public static Query newPathQuery(final String field, final double[] pathLatitudes, final double[] pathLongitudes, final double pathWidthMeters, PlanetModel planetModel) {
    final GeoShape shape = Geo3DUtil.fromPath(planetModel, pathLatitudes, pathLongitudes, pathWidthMeters);
    return newShapeQuery(field, shape);
  }

  private void fillFieldsData(PlanetModel planetModel, double x, double y, double z) {
    byte[] bytes = new byte[12];
    encodeDimension(x, bytes, 0, planetModel);
    encodeDimension(y, bytes, Integer.BYTES, planetModel);
    encodeDimension(z, bytes, 2*Integer.BYTES, planetModel);
    fieldsData = new BytesRef(bytes);
  }

  // public helper methods (e.g. for queries)
  
  /** Encode single dimension */
  public static void encodeDimension(double value, byte bytes[], int offset, PlanetModel planetModel) {
    NumericUtils.intToSortableBytes(planetModel.encodeValue(value), bytes, offset);
  }
  
  /** Decode single dimension */
  public static double decodeDimension(byte value[], int offset, PlanetModel planetModel) {
    return planetModel.decodeValue(NumericUtils.sortableBytesToInt(value, offset));
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
    result.append(" x=").append(decodeDimension(bytes.bytes, bytes.offset, this.planetModel));
    result.append(" y=").append(decodeDimension(bytes.bytes, bytes.offset + Integer.BYTES, this.planetModel));
    result.append(" z=").append(decodeDimension(bytes.bytes, bytes.offset + 2 * Integer.BYTES, this.planetModel));
    result.append('>');
    return result.toString();
  }

}
