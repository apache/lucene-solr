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

import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;

/**
 * An per-document location field.
 *
 * <p>Sorting by distance is efficient. Multiple values for the same field in one document is
 * allowed.
 *
 * <p>This field defines static factory methods for common operations:
 *
 * <ul>
 *   <li>{@link #newSlowBoxQuery newSlowBoxQuery()} for matching points within a bounding box.
 *   <li>{@link #newSlowDistanceQuery newSlowDistanceQuery()} for matching points within a specified
 *       distance.
 *   <li>{@link #newSlowPolygonQuery newSlowPolygonQuery()} for matching points within an arbitrary
 *       polygon.
 *   <li>{@link #newSlowGeometryQuery newSlowGeometryQuery()} for matching points within an
 *       arbitrary geometry.
 *   <li>{@link #newDistanceSort newDistanceSort()} for ordering documents by distance from a
 *       specified location.
 * </ul>
 *
 * <p>If you also need query operations, you should add a separate {@link XYPointField} instance. If
 * you also need to store the value, you should add a separate {@link StoredField} instance.
 *
 * @see XYPointField
 */
public class XYDocValuesField extends Field {

  /**
   * Type for a XYDocValuesField
   *
   * <p>Each value stores a 64-bit long where the upper 32 bits are the encoded x value, and the
   * lower 32 bits are the encoded y value.
   *
   * @see org.apache.lucene.geo.XYEncodingUtils#decode(int)
   */
  public static final FieldType TYPE = new FieldType();

  static {
    TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    TYPE.freeze();
  }

  /**
   * Creates a new XYDocValuesField with the specified x and y
   *
   * @param name field name
   * @param x x value.
   * @param y y values.
   * @throws IllegalArgumentException if the field name is null or x or y are infinite or NaN.
   */
  public XYDocValuesField(String name, float x, float y) {
    super(name, TYPE);
    setLocationValue(x, y);
  }

  /**
   * Change the values of this field
   *
   * @param x x value.
   * @param y y value.
   * @throws IllegalArgumentException if x or y are infinite or NaN.
   */
  public void setLocationValue(float x, float y) {
    int xEncoded = XYEncodingUtils.encode(x);
    int yEncoded = XYEncodingUtils.encode(y);
    fieldsData = Long.valueOf((((long) xEncoded) << 32) | (yEncoded & 0xFFFFFFFFL));
  }

  /** helper: checks a fieldinfo and throws exception if its definitely not a XYDocValuesField */
  static void checkCompatible(FieldInfo fieldInfo) {
    // dv properties could be "unset", if you e.g. used only StoredField with this same name in the
    // segment.
    if (fieldInfo.getDocValuesType() != DocValuesType.NONE
        && fieldInfo.getDocValuesType() != TYPE.docValuesType()) {
      throw new IllegalArgumentException(
          "field=\""
              + fieldInfo.name
              + "\" was indexed with docValuesType="
              + fieldInfo.getDocValuesType()
              + " but this type has docValuesType="
              + TYPE.docValuesType()
              + ", is the field really a XYDocValuesField?");
    }
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    long currentValue = (Long) fieldsData;
    result.append(XYEncodingUtils.decode((int) (currentValue >> 32)));
    result.append(',');
    result.append(XYEncodingUtils.decode((int) (currentValue & 0xFFFFFFFF)));

    result.append('>');
    return result.toString();
  }

  /**
   * Creates a SortField for sorting by distance from a location.
   *
   * <p>This sort orders documents by ascending distance from the location. The value returned in
   * {@link FieldDoc} for the hits contains a Double instance with the distance in meters.
   *
   * <p>If a document is missing the field, then by default it is treated as having {@link
   * Double#POSITIVE_INFINITY} distance (missing values sort last).
   *
   * <p>If a document contains multiple values for the field, the <i>closest</i> distance to the
   * location is used.
   *
   * @param field field name. must not be null.
   * @param x x at the center.
   * @param y y at the center.
   * @return SortField ordering documents by distance
   * @throws IllegalArgumentException if {@code field} is null or location has invalid coordinates.
   */
  public static SortField newDistanceSort(String field, float x, float y) {
    return new XYPointSortField(field, x, y);
  }

  /**
   * Create a query for matching a bounding box using doc values. This query is usually slow as it
   * does not use an index structure and needs to verify documents one-by-one in order to know
   * whether they match. It is best used wrapped in an {@link IndexOrDocValuesQuery} alongside a
   * {@link XYPointField#newBoxQuery}.
   */
  public static Query newSlowBoxQuery(
      String field, float minX, float maxX, float minY, float maxY) {
    XYRectangle rectangle = new XYRectangle(minX, maxX, minY, maxY);
    return new XYDocValuesPointInGeometryQuery(field, rectangle);
  }

  /**
   * Create a query for matching points within the specified distance of the supplied location. This
   * query is usually slow as it does not use an index structure and needs to verify documents
   * one-by-one in order to know whether they match. It is best used wrapped in an {@link
   * IndexOrDocValuesQuery} alongside a {@link XYPointField#newDistanceQuery}.
   *
   * @param field field name. must not be null.
   * @param x x at the center.
   * @param y y at the center: must be within standard +/-180 coordinate bounds.
   * @param radius maximum distance from the center in cartesian distance: must be non-negative and
   *     finite.
   * @return query matching points within this distance
   * @throws IllegalArgumentException if {@code field} is null, location has invalid coordinates, or
   *     radius is invalid.
   */
  public static Query newSlowDistanceQuery(String field, float x, float y, float radius) {
    XYCircle circle = new XYCircle(x, y, radius);
    return new XYDocValuesPointInGeometryQuery(field, circle);
  }

  /**
   * Create a query for matching points within the supplied polygons. This query is usually slow as
   * it does not use an index structure and needs to verify documents one-by-one in order to know
   * whether they match. It is best used wrapped in an {@link IndexOrDocValuesQuery} alongside a
   * {@link XYPointField#newPolygonQuery(String, XYPolygon...)}.
   *
   * @param field field name. must not be null.
   * @param polygons array of polygons. must not be null or empty.
   * @return query matching points within the given polygons.
   * @throws IllegalArgumentException if {@code field} is null or polygons is empty or contain a
   *     null polygon.
   */
  public static Query newSlowPolygonQuery(String field, XYPolygon... polygons) {
    return newSlowGeometryQuery(field, polygons);
  }

  /**
   * Create a query for matching points within the supplied geometries. XYLine geometries are not
   * supported. This query is usually slow as it does not use an index structure and needs to verify
   * documents one-by-one in order to know whether they match. It is best used wrapped in an {@link
   * IndexOrDocValuesQuery} alongside a {@link XYPointField#newGeometryQuery(String,
   * XYGeometry...)}.
   *
   * @param field field name. must not be null.
   * @param geometries array of XY geometries. must not be null or empty.
   * @return query matching points within the given geometries.
   * @throws IllegalArgumentException if {@code field} is null, {@code polygons} is null, empty or
   *     contains a null or XYLine geometry.
   */
  public static Query newSlowGeometryQuery(String field, XYGeometry... geometries) {
    return new XYDocValuesPointInGeometryQuery(field, geometries);
  }
}
