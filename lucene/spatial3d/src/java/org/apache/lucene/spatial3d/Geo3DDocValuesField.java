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

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.geo.Polygon;

import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoDistanceShape;
import org.apache.lucene.spatial3d.geom.GeoOutsideDistance;

/** 
 * An per-document 3D location field.
 * <p>
 * Sorting by distance is efficient. Multiple values for the same field in one document
 * is allowed. 
 * <p>
 * This field defines static factory methods for common operations:
 * <ul>
 *   <li>TBD
 * </ul>
 * <p>
 * If you also need query operations, you should add a separate {@link Geo3DPoint} instance.
 * <p>
 * <b>WARNING</b>: Values are indexed with some loss of precision from the
 * original {@code double} values (4.190951585769653E-8 for the latitude component
 * and 8.381903171539307E-8 for longitude).
 * @see Geo3DPoint
 */
public class Geo3DDocValuesField extends Field {
  private final PlanetModel planetModel;
  
  /**
   * Type for a Geo3DDocValuesField
   * <p>
   * Each value stores a 64-bit long where the three values (x, y, and z) are given
   * 21 bits each.  Each 21-bit value represents the maximum extent in that dimension
   * for the defined planet model.
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE.freeze();
  }
  
  /** 
   * Creates a new Geo3DDocValuesField with the specified x, y, and z
   * @param name field name
   * @param point is the point.
   * @throws IllegalArgumentException if the field name is null or the point is out of bounds
   */
  public Geo3DDocValuesField(final String name, final GeoPoint point, final PlanetModel planetModel) {
    this(name, TYPE, planetModel);
    setLocationValue(point);
  }

  /** 
   * Creates a new Geo3DDocValuesField with the specified x, y, and z
   * @param name field name
   * @param x is the x value for the point.
   * @param y is the y value for the point.
   * @param z is the z value for the point.
   * @throws IllegalArgumentException if the field name is null or x, y, or z are out of bounds
   */
  public Geo3DDocValuesField(final String name, final double x, final double y, final double z, final PlanetModel planetModel) {
    this(name, TYPE, planetModel);
    setLocationValue(x, y, z);
  }

  private Geo3DDocValuesField(final String name, final FieldType type, final PlanetModel planetModel) {
    super(name, TYPE);
    this.planetModel = planetModel;
  }

  /**
   * Change the values of this field
   * @param point is the point.
   * @throws IllegalArgumentException if the point is out of bounds
   */
  public void setLocationValue(final GeoPoint point) {
    fieldsData = Long.valueOf(planetModel.getDocValueEncoder().encodePoint(point));
  }

  /**
   * Change the values of this field
   * @param x is the x value for the point.
   * @param y is the y value for the point.
   * @param z is the z value for the point.
   * @throws IllegalArgumentException if x, y, or z are out of bounds
   */
  public void setLocationValue(final double x, final double y, final double z) {
    fieldsData = Long.valueOf(planetModel.getDocValueEncoder().encodePoint(x, y, z));
  }

  /** helper: checks a fieldinfo and throws exception if its definitely not a Geo3DDocValuesField */
  static void checkCompatible(FieldInfo fieldInfo) {
    // dv properties could be "unset", if you e.g. used only StoredField with this same name in the segment.
    if (fieldInfo.getDocValuesType() != DocValuesType.NONE && fieldInfo.getDocValuesType() != TYPE.docValuesType()) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with docValuesType=" + fieldInfo.getDocValuesType() + 
                                         " but this type has docValuesType=" + TYPE.docValuesType() + 
                                         ", is the field really a Geo3DDocValuesField?");
    }
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    long currentValue = (Long)fieldsData;
    
    result.append(planetModel.getDocValueEncoder().decodeXValue(currentValue));
    result.append(',');
    result.append(planetModel.getDocValueEncoder().decodeYValue(currentValue));
    result.append(',');
    result.append(planetModel.getDocValueEncoder().decodeZValue(currentValue));

    result.append('>');
    return result.toString();
  }

  /**
   * Creates a SortField for sorting by distance within a circle.
   * <p>
   * This sort orders documents by ascending distance from the location. The value returned in {@link FieldDoc} for
   * the hits contains a Double instance with the distance in meters.
   * <p>
   * If a document is missing the field, then by default it is treated as having {@link Double#POSITIVE_INFINITY} distance
   * (missing values sort last).
   * <p>
   * If a document contains multiple values for the field, the <i>closest</i> distance from the circle center is used.
   * 
   * @param field field name. must not be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param maxRadiusMeters is the maximum radius in meters.
   * @return SortField ordering documents by distance
   * @throws IllegalArgumentException if {@code field} is null or circle has invalid coordinates.
   */
  public static SortField newDistanceSort(final String field, final double latitude, final double longitude, final double maxRadiusMeters, final PlanetModel planetModel) {
    final GeoDistanceShape shape = Geo3DUtil.fromDistance(planetModel, latitude, longitude, maxRadiusMeters);
    return new Geo3DPointSortField(field, planetModel, shape);
  }

  /**
   * Creates a SortField for sorting by distance along a path.
   * <p>
   * This sort orders documents by ascending distance along the described path. The value returned in {@link FieldDoc} for
   * the hits contains a Double instance with the distance in meters.
   * <p>
   * If a document is missing the field, then by default it is treated as having {@link Double#POSITIVE_INFINITY} distance
   * (missing values sort last).
   * <p>
   * If a document contains multiple values for the field, the <i>closest</i> distance along the path is used.
   * 
   * @param field field name. must not be null.
   * @param pathLatitudes latitude values for points of the path: must be within standard +/-90 coordinate bounds.
   * @param pathLongitudes longitude values for points of the path: must be within standard +/-180 coordinate bounds.
   * @param pathWidthMeters width of the path in meters.
   * @return SortField ordering documents by distance
   * @throws IllegalArgumentException if {@code field} is null or path has invalid coordinates.
   */
  public static SortField newPathSort(final String field, final double[] pathLatitudes, final double[] pathLongitudes, final double pathWidthMeters, final PlanetModel planetModel) {
    final GeoDistanceShape shape = Geo3DUtil.fromPath(planetModel, pathLatitudes, pathLongitudes, pathWidthMeters);
    return new Geo3DPointSortField(field, planetModel, shape);
  }

  // Outside distances
  
  /**
   * Creates a SortField for sorting by outside distance from a circle.
   * <p>
   * This sort orders documents by ascending outside distance from the circle.  Points within the circle have distance 0.0.
   * The value returned in {@link FieldDoc} for
   * the hits contains a Double instance with the distance in meters.
   * <p>
   * If a document is missing the field, then by default it is treated as having {@link Double#POSITIVE_INFINITY} distance
   * (missing values sort last).
   * <p>
   * If a document contains multiple values for the field, the <i>closest</i> distance to the circle is used.
   * 
   * @param field field name. must not be null.
   * @param latitude latitude at the center: must be within standard +/-90 coordinate bounds.
   * @param longitude longitude at the center: must be within standard +/-180 coordinate bounds.
   * @param maxRadiusMeters is the maximum radius in meters.
   * @return SortField ordering documents by distance
   * @throws IllegalArgumentException if {@code field} is null or location has invalid coordinates.
   */
  public static SortField newOutsideDistanceSort(final String field, final double latitude, final double longitude, final double maxRadiusMeters, final PlanetModel planetModel) {
    final GeoOutsideDistance shape = Geo3DUtil.fromDistance(planetModel, latitude, longitude, maxRadiusMeters);
    return new Geo3DPointOutsideSortField(field, planetModel, shape);
  }

  /**
   * Creates a SortField for sorting by outside distance from a box.
   * <p>
   * This sort orders documents by ascending outside distance from the box.  Points within the box have distance 0.0.
   * The value returned in {@link FieldDoc} for
   * the hits contains a Double instance with the distance in meters.
   * <p>
   * If a document is missing the field, then by default it is treated as having {@link Double#POSITIVE_INFINITY} distance
   * (missing values sort last).
   * <p>
   * If a document contains multiple values for the field, the <i>closest</i> distance to the box is used.
   * 
   * @param field field name. must not be null.
   * @param minLatitude latitude lower bound: must be within standard +/-90 coordinate bounds.
   * @param maxLatitude latitude upper bound: must be within standard +/-90 coordinate bounds.
   * @param minLongitude longitude lower bound: must be within standard +/-180 coordinate bounds.
   * @param maxLongitude longitude upper bound: must be within standard +/-180 coordinate bounds.
   * @return SortField ordering documents by distance
   * @throws IllegalArgumentException if {@code field} is null or box has invalid coordinates.
   */
  public static SortField newOutsideBoxSort(final String field, final double minLatitude, final double maxLatitude, final double minLongitude, final double maxLongitude, final PlanetModel planetModel) {
    final GeoOutsideDistance shape = Geo3DUtil.fromBox(planetModel, minLatitude, maxLatitude, minLongitude, maxLongitude);
    return new Geo3DPointOutsideSortField(field, planetModel, shape);
  }

  /**
   * Creates a SortField for sorting by outside distance from a polygon.
   * <p>
   * This sort orders documents by ascending outside distance from the polygon.  Points within the polygon have distance 0.0.
   * The value returned in {@link FieldDoc} for
   * the hits contains a Double instance with the distance in meters.
   * <p>
   * If a document is missing the field, then by default it is treated as having {@link Double#POSITIVE_INFINITY} distance
   * (missing values sort last).
   * <p>
   * If a document contains multiple values for the field, the <i>closest</i> distance to the polygon is used.
   * 
   * @param field field name. must not be null.
   * @param polygons is the list of polygons to use to construct the query; must be at least one.
   * @return SortField ordering documents by distance
   * @throws IllegalArgumentException if {@code field} is null or polygon has invalid coordinates.
   */
  public static SortField newOutsidePolygonSort(final String field, final PlanetModel planetModel, final Polygon... polygons) {
    final GeoOutsideDistance shape = Geo3DUtil.fromPolygon(planetModel, polygons);
    return new Geo3DPointOutsideSortField(field, planetModel, shape);
  }

  /**
   * Creates a SortField for sorting by outside distance from a large polygon.  This differs from the related newOutsideLargePolygonSort in that it
   * does little or no legality checking and is optimized for very large numbers of polygon edges.
   * <p>
   * This sort orders documents by ascending outside distance from the polygon.  Points within the polygon have distance 0.0.
   * The value returned in {@link FieldDoc} for
   * the hits contains a Double instance with the distance in meters.
   * <p>
   * If a document is missing the field, then by default it is treated as having {@link Double#POSITIVE_INFINITY} distance
   * (missing values sort last).
   * <p>
   * If a document contains multiple values for the field, the <i>closest</i> distance to the polygon is used.
   * 
   * @param field field name. must not be null.
   * @param polygons is the list of polygons to use to construct the query; must be at least one.
   * @return SortField ordering documents by distance
   * @throws IllegalArgumentException if {@code field} is null or polygon has invalid coordinates.
   */
  public static SortField newOutsideLargePolygonSort(final String field, final PlanetModel planetModel, final Polygon... polygons) {
    final GeoOutsideDistance shape = Geo3DUtil.fromLargePolygon(planetModel, polygons);
    return new Geo3DPointOutsideSortField(field, planetModel, shape);
  }

  /**
   * Creates a SortField for sorting by outside distance from a path.
   * <p>
   * This sort orders documents by ascending outside distance from the described path. Points within the path
   * are given the distance of 0.0.  The value returned in {@link FieldDoc} for
   * the hits contains a Double instance with the distance in meters.
   * <p>
   * If a document is missing the field, then by default it is treated as having {@link Double#POSITIVE_INFINITY} distance
   * (missing values sort last).
   * <p>
   * If a document contains multiple values for the field, the <i>closest</i> distance from the path is used.
   * 
   * @param field field name. must not be null.
   * @param pathLatitudes latitude values for points of the path: must be within standard +/-90 coordinate bounds.
   * @param pathLongitudes longitude values for points of the path: must be within standard +/-180 coordinate bounds.
   * @param pathWidthMeters width of the path in meters.
   * @return SortField ordering documents by distance
   * @throws IllegalArgumentException if {@code field} is null or path has invalid coordinates.
   */
  public static SortField newOutsidePathSort(final String field, final double[] pathLatitudes, final double[] pathLongitudes, final double pathWidthMeters, final PlanetModel planetModel) {
    final GeoOutsideDistance shape = Geo3DUtil.fromPath(planetModel, pathLatitudes, pathLongitudes, pathWidthMeters);
    return new Geo3DPointOutsideSortField(field, planetModel, shape);
  }
}
