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

import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;


/** 
 * An indexed XY position field.
 * <p>
 * Finding all documents within a range at search time is
 * efficient.  Multiple values for the same field in one document
 * is allowed. 
 * <p>
 * This field defines static factory methods for common operations:
 * <ul>
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching points within a bounding box.
 *   <li>{@link #newDistanceQuery newDistanceQuery()} for matching points within a specified distance.
 *   <li>{@link #newPolygonQuery newPolygonQuery()} for matching points within an arbitrary polygon.
 *   <li>{@link #newGeometryQuery newGeometryQuery()} for matching points within an arbitrary geometry collection.
 * </ul>
 * <p>
 * If you also need per-document operations such as sort by distance, add a separate {@link XYDocValuesField} instance.
 * If you also need to store the value, you should add a separate {@link StoredField} instance.
 *
 * @see PointValues
 * @see XYDocValuesField
 */

public class XYPointField extends Field {
  /** XYPoint is encoded as integer values so number of bytes is 4 */
  public static final int BYTES = Integer.BYTES;
  /**
   * Type for an indexed XYPoint
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
   * @param x x value.
   * @param y y value.
   */
  public void setLocationValue(float x, float y) {
    final byte[] bytes;
    if (fieldsData == null) {
      bytes = new byte[8];
      fieldsData = new BytesRef(bytes);
    } else {
      bytes = ((BytesRef) fieldsData).bytes;
    }
    int xEncoded = XYEncodingUtils.encode(x);
    int yEncoded = XYEncodingUtils.encode(y);
    NumericUtils.intToSortableBytes(xEncoded, bytes, 0);
    NumericUtils.intToSortableBytes(yEncoded, bytes, Integer.BYTES);
  }

  /**
   * Creates a new XYPoint with the specified x and y
   * @param name field name
   * @param x x value.
   * @param y y value.
   */
  public XYPointField(String name, float x, float y) {
    super(name, TYPE);
    setLocationValue(x, y);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(getClass().getSimpleName());
    result.append(" <");
    result.append(name);
    result.append(':');

    byte bytes[] = ((BytesRef) fieldsData).bytes;
    result.append(XYEncodingUtils.decode(bytes, 0));
    result.append(',');
    result.append(XYEncodingUtils.decode(bytes, Integer.BYTES));

    result.append('>');
    return result.toString();
  }


  /** helper: checks a fieldinfo and throws exception if its definitely not a XYPoint */
  static void checkCompatible(FieldInfo fieldInfo) {
    // point/dv properties could be "unset", if you e.g. used only StoredField with this same name in the segment.
    if (fieldInfo.getPointDimensionCount() != 0 && fieldInfo.getPointDimensionCount() != TYPE.pointDimensionCount()) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with numDims=" + fieldInfo.getPointDimensionCount() +
          " but this point type has numDims=" + TYPE.pointDimensionCount() +
                                         ", is the field really a XYPoint?");
    }
    if (fieldInfo.getPointNumBytes() != 0 && fieldInfo.getPointNumBytes() != TYPE.pointNumBytes()) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with bytesPerDim=" + fieldInfo.getPointNumBytes() +
                                         " but this point type has bytesPerDim=" + TYPE.pointNumBytes() +
                                         ", is the field really a XYPoint?");
    }
  }

  // static methods for generating queries

  /**
   * Create a query for matching a bounding box.
   * @param field field name. must not be null.
   * @param minX x lower bound.
   * @param maxX x upper bound.
   * @param minY y lower bound.
   * @param maxY y upper bound.
   * @return query matching points within this box
   * @throws IllegalArgumentException if {@code field} is null, or the box has invalid coordinates.
   */
  public static Query newBoxQuery(String field, float minX, float maxX, float minY, float maxY) {
    XYRectangle rectangle = new XYRectangle(minX, maxX, minY, maxY);
    return new XYPointInGeometryQuery(field, rectangle);
  }

  /**
   * Create a query for matching points within the specified distance of the supplied location.
   * @param field field name. must not be null.
   * @param x x at the center.
   * @param y y at the center.
   * @param radius maximum distance from the center in cartesian units: must be non-negative and finite.
   * @return query matching points within this distance
   * @throws IllegalArgumentException if {@code field} is null, location has invalid coordinates, or radius is invalid.
   */
  public static Query newDistanceQuery(String field, float x, float y, float radius) {
    XYCircle circle = new XYCircle(x, y, radius);
    return new XYPointInGeometryQuery(field, circle);
  }
  
  /** 
   * Create a query for matching one or more polygons.
   * @param field field name. must not be null.
   * @param polygons array of polygons. must not be null or empty
   * @return query matching points within this polygon
   * @throws IllegalArgumentException if {@code field} is null, {@code polygons} is null or empty
   * @see Polygon
   */
  public static Query newPolygonQuery(String field, XYPolygon... polygons) {
    return newGeometryQuery(field, polygons);
  }

  /** create a query to find all indexed shapes that intersect a provided geometry collection. XYLine geometries are not supported.
   * @param field field name. must not be null.
   * @param xyGeometries array of geometries. must not be null or empty.
   * @return query matching points within this geometry collection.
   * @throws IllegalArgumentException if {@code field} is null, {@code polygons} is null, empty or contains a null or XYLine geometry.
   * @see XYGeometry
   **/
  public static Query newGeometryQuery(String field, XYGeometry... xyGeometries) {
    return new XYPointInGeometryQuery(field, xyGeometries);
  }
}
