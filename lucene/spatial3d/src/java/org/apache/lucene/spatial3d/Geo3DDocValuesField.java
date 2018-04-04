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

  // These are the multiplicative constants we need to use to arrive at values that fit in 21 bits.
  // The formula we use to go from double to encoded value is:  Math.floor((value - minimum) * factor + 0.5)
  // If we plug in maximum for value, we should get 0x1FFFFF.
  // So, 0x1FFFFF = Math.floor((maximum - minimum) * factor + 0.5)
  // We factor out the 0.5 and Math.floor by stating instead:
  // 0x1FFFFF = (maximum - minimum) * factor
  // So, factor = 0x1FFFFF / (maximum - minimum)

  private final static double inverseMaximumValue = 1.0 / (double)(0x1FFFFF);
  
  private final static double inverseXFactor = (PlanetModel.WGS84.getMaximumXValue() - PlanetModel.WGS84.getMinimumXValue()) * inverseMaximumValue;
  private final static double inverseYFactor = (PlanetModel.WGS84.getMaximumYValue() - PlanetModel.WGS84.getMinimumYValue()) * inverseMaximumValue;
  private final static double inverseZFactor = (PlanetModel.WGS84.getMaximumZValue() - PlanetModel.WGS84.getMinimumZValue()) * inverseMaximumValue;
  
  private final static double xFactor = 1.0 / inverseXFactor;
  private final static double yFactor = 1.0 / inverseYFactor;
  private final static double zFactor = 1.0 / inverseZFactor;
  
  // Fudge factor for step adjustments.  This is here solely to handle inaccuracies in bounding boxes
  // that occur because of quantization.  For unknown reasons, the fudge factor needs to be
  // 10.0 rather than 1.0.  See LUCENE-7430.
  
  private final static double STEP_FUDGE = 10.0;
  
  // These values are the delta between a value and the next value in each specific dimension
  
  private final static double xStep = inverseXFactor * STEP_FUDGE;
  private final static double yStep = inverseYFactor * STEP_FUDGE;
  private final static double zStep = inverseZFactor * STEP_FUDGE;
  
  /**
   * Type for a Geo3DDocValuesField
   * <p>
   * Each value stores a 64-bit long where the three values (x, y, and z) are given
   * 21 bits each.  Each 21-bit value represents the maximum extent in that dimension
   * for the WGS84 planet model.
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
  public Geo3DDocValuesField(final String name, final GeoPoint point) {
    super(name, TYPE);
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
  public Geo3DDocValuesField(final String name, final double x, final double y, final double z) {
    super(name, TYPE);
    setLocationValue(x, y, z);
  }
  
  /**
   * Change the values of this field
   * @param point is the point.
   * @throws IllegalArgumentException if the point is out of bounds
   */
  public void setLocationValue(final GeoPoint point) {
    fieldsData = Long.valueOf(encodePoint(point));
  }

  /**
   * Change the values of this field
   * @param x is the x value for the point.
   * @param y is the y value for the point.
   * @param z is the z value for the point.
   * @throws IllegalArgumentException if x, y, or z are out of bounds
   */
  public void setLocationValue(final double x, final double y, final double z) {
    fieldsData = Long.valueOf(encodePoint(x, y, z));
  }
  
  /** Encode a point.
   * @param point is the point
   * @return the encoded long
   */
  public static long encodePoint(final GeoPoint point) {
    return encodePoint(point.x, point.y, point.z);
  }

  /** Encode a point.
   * @param x is the x value
   * @param y is the y value
   * @param z is the z value
   * @return the encoded long
   */
  public static long encodePoint(final double x, final double y, final double z) {
    int XEncoded = encodeX(x);
    int YEncoded = encodeY(y);
    int ZEncoded = encodeZ(z);
    return
      (((long)(XEncoded & 0x1FFFFF)) << 42) |
      (((long)(YEncoded & 0x1FFFFF)) << 21) |
      ((long)(ZEncoded & 0x1FFFFF));
  }

  /** Decode GeoPoint value from long docvalues value.
   * @param docValue is the doc values value.
   * @return the GeoPoint.
   */
  public static GeoPoint decodePoint(final long docValue) {
    return new GeoPoint(decodeX(((int)(docValue >> 42)) & 0x1FFFFF),
      decodeY(((int)(docValue >> 21)) & 0x1FFFFF),
      decodeZ(((int)(docValue)) & 0x1FFFFF));
  }
  
  /** Decode X value from long docvalues value.
   * @param docValue is the doc values value.
   * @return the x value.
   */
  public static double decodeXValue(final long docValue) {
    return decodeX(((int)(docValue >> 42)) & 0x1FFFFF);
  }
  
  /** Decode Y value from long docvalues value.
   * @param docValue is the doc values value.
   * @return the y value.
   */
  public static double decodeYValue(final long docValue) {
    return decodeY(((int)(docValue >> 21)) & 0x1FFFFF);
  }
  
  /** Decode Z value from long docvalues value.
   * @param docValue is the doc values value.
   * @return the z value.
   */
  public static double decodeZValue(final long docValue) {
    return decodeZ(((int)(docValue)) & 0x1FFFFF);
  }

  /** Round the provided X value down, by encoding it, decrementing it, and unencoding it.
   * @param startValue is the starting value.
   * @return the rounded value.
   */
  public static double roundDownX(final double startValue) {
    return startValue - xStep;
  }

  /** Round the provided X value up, by encoding it, incrementing it, and unencoding it.
   * @param startValue is the starting value.
   * @return the rounded value.
   */
  public static double roundUpX(final double startValue) {
    return startValue + xStep;
  }

  /** Round the provided Y value down, by encoding it, decrementing it, and unencoding it.
   * @param startValue is the starting value.
   * @return the rounded value.
   */
  public static double roundDownY(final double startValue) {
    return startValue - yStep;
  }

  /** Round the provided Y value up, by encoding it, incrementing it, and unencoding it.
   * @param startValue is the starting value.
   * @return the rounded value.
   */
  public static double roundUpY(final double startValue) {
    return startValue + yStep;
  }
  
  /** Round the provided Z value down, by encoding it, decrementing it, and unencoding it.
   * @param startValue is the starting value.
   * @return the rounded value.
   */
  public static double roundDownZ(final double startValue) {
    return startValue - zStep;
  }

  /** Round the provided Z value up, by encoding it, incrementing it, and unencoding it.
   * @param startValue is the starting value.
   * @return the rounded value.
   */
  public static double roundUpZ(final double startValue) {
    return startValue + zStep;
  }

  // For encoding/decoding, we generally want the following behavior:
  // (1) If you encode the maximum value or the minimum value, the resulting int fits in 21 bits.
  // (2) If you decode an encoded value, you get back the original value for both the minimum and maximum planet model values.
  // (3) Rounding occurs such that a small delta from the minimum and maximum planet model values still returns the same
  // values -- that is, these are in the center of the range of input values that should return the minimum or maximum when decoded
  
  private static int encodeX(final double x) {
    if (x > PlanetModel.WGS84.getMaximumXValue()) {
      throw new IllegalArgumentException("x value exceeds WGS84 maximum");
    } else if (x < PlanetModel.WGS84.getMinimumXValue()) {
      throw new IllegalArgumentException("x value less than WGS84 minimum");
    }
    return (int)Math.floor((x - PlanetModel.WGS84.getMinimumXValue()) * xFactor + 0.5);
  }
  
  private static double decodeX(final int x) {
    return x * inverseXFactor + PlanetModel.WGS84.getMinimumXValue();
  }

  private static int encodeY(final double y) {
    if (y > PlanetModel.WGS84.getMaximumYValue()) {
      throw new IllegalArgumentException("y value exceeds WGS84 maximum");
    } else if (y < PlanetModel.WGS84.getMinimumYValue()) {
      throw new IllegalArgumentException("y value less than WGS84 minimum");
    }
    return (int)Math.floor((y - PlanetModel.WGS84.getMinimumYValue()) * yFactor + 0.5);
  }

  private static double decodeY(final int y) {
    return y * inverseYFactor + PlanetModel.WGS84.getMinimumYValue();
  }

  private static int encodeZ(final double z) {
    if (z > PlanetModel.WGS84.getMaximumZValue()) {
      throw new IllegalArgumentException("z value exceeds WGS84 maximum");
    } else if (z < PlanetModel.WGS84.getMinimumZValue()) {
      throw new IllegalArgumentException("z value less than WGS84 minimum");
    }
    return (int)Math.floor((z - PlanetModel.WGS84.getMinimumZValue()) * zFactor + 0.5);
  }

  private static double decodeZ(final int z) {
    return z * inverseZFactor + PlanetModel.WGS84.getMinimumZValue();
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
    
    result.append(decodeXValue(currentValue));
    result.append(',');
    result.append(decodeYValue(currentValue));
    result.append(',');
    result.append(decodeZValue(currentValue));

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
  public static SortField newDistanceSort(final String field, final double latitude, final double longitude, final double maxRadiusMeters) {
    final GeoDistanceShape shape = Geo3DUtil.fromDistance(latitude, longitude, maxRadiusMeters);
    return new Geo3DPointSortField(field, shape);
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
  public static SortField newPathSort(final String field, final double[] pathLatitudes, final double[] pathLongitudes, final double pathWidthMeters) {
    final GeoDistanceShape shape = Geo3DUtil.fromPath(pathLatitudes, pathLongitudes, pathWidthMeters);
    return new Geo3DPointSortField(field, shape);
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
  public static SortField newOutsideDistanceSort(final String field, final double latitude, final double longitude, final double maxRadiusMeters) {
    final GeoOutsideDistance shape = Geo3DUtil.fromDistance(latitude, longitude, maxRadiusMeters);
    return new Geo3DPointOutsideSortField(field, shape);
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
  public static SortField newOutsideBoxSort(final String field, final double minLatitude, final double maxLatitude, final double minLongitude, final double maxLongitude) {
    final GeoOutsideDistance shape = Geo3DUtil.fromBox(minLatitude, maxLatitude, minLongitude, maxLongitude);
    return new Geo3DPointOutsideSortField(field, shape);
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
  public static SortField newOutsidePolygonSort(final String field, final Polygon... polygons) {
    final GeoOutsideDistance shape = Geo3DUtil.fromPolygon(polygons);
    return new Geo3DPointOutsideSortField(field, shape);
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
  public static SortField newOutsideLargePolygonSort(final String field, final Polygon... polygons) {
    final GeoOutsideDistance shape = Geo3DUtil.fromLargePolygon(polygons);
    return new Geo3DPointOutsideSortField(field, shape);
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
  public static SortField newOutsidePathSort(final String field, final double[] pathLatitudes, final double[] pathLongitudes, final double pathWidthMeters) {
    final GeoOutsideDistance shape = Geo3DUtil.fromPath(pathLatitudes, pathLongitudes, pathWidthMeters);
    return new Geo3DPointOutsideSortField(field, shape);
  }

}
