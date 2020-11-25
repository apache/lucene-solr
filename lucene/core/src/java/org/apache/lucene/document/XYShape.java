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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.ShapeField.QueryRelation; // javadoc
import org.apache.lucene.document.ShapeField.Triangle;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYGeometry;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYRectangle;
import org.apache.lucene.index.PointValues; // javadoc
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;

import static org.apache.lucene.geo.XYEncodingUtils.encode;

/**
 * A cartesian shape utility class for indexing and searching geometries whose vertices are unitless x, y values.
 * <p>
 * This class defines seven static factory methods for common indexing and search operations:
 * <ul>
 *   <li>{@link #createIndexableFields(String, XYPolygon)} for indexing a cartesian polygon.
 *   <li>{@link #createIndexableFields(String, XYLine)} for indexing a cartesian linestring.
 *   <li>{@link #createIndexableFields(String, float, float)} for indexing a x, y cartesian point.
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching cartesian shapes that have some {@link QueryRelation} with a bounding box.
 *   <li>{@link #newBoxQuery newLineQuery()} for matching cartesian shapes that have some {@link QueryRelation} with a linestring.
 *   <li>{@link #newBoxQuery newPolygonQuery()} for matching cartesian shapes that have some {@link QueryRelation} with a polygon.
 *   <li>{@link #newGeometryQuery newGeometryQuery()} for matching cartesian shapes that have some {@link QueryRelation}
 *   with one or more {@link XYGeometry}.
 * </ul>

 * <b>WARNING</b>: Like {@link LatLonPoint}, vertex values are indexed with some loss of precision from the
 * original {@code double} values.
 * @see PointValues
 * @see LatLonDocValuesField
 */
public class XYShape {

  // no instance:
  private XYShape() {
  }

  /** create indexable fields for cartesian polygon geometry */
  public static Field[] createIndexableFields(String fieldName, XYPolygon polygon) {

    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    List<Triangle> fields = new ArrayList<>(tessellation.size());
    for (Tessellator.Triangle t : tessellation) {
      fields.add(new Triangle(fieldName, t));
    }
    return fields.toArray(new Field[fields.size()]);
  }

  /** create indexable fields for cartesian line geometry */
  public static Field[] createIndexableFields(String fieldName, XYLine line) {
    int numPoints = line.numPoints();
    Field[] fields = new Field[numPoints - 1];
    // create "flat" triangles
    for (int i = 0, j = 1; j < numPoints; ++i, ++j) {
      fields[i] = new Triangle(fieldName,
          encode(line.getX(i)), encode(line.getY(i)),
          encode(line.getX(j)), encode(line.getY(j)),
          encode(line.getX(i)), encode(line.getY(i)));
    }
    return fields;
  }

  /** create indexable fields for cartesian point geometry */
  public static Field[] createIndexableFields(String fieldName, float x, float y) {
    return new Field[] {new Triangle(fieldName,
        encode(x), encode(y), encode(x), encode(y), encode(x), encode(y))};
  }

  /** create a query to find all cartesian shapes that intersect a defined bounding box **/
  public static Query newBoxQuery(String field, QueryRelation queryRelation, float minX, float maxX, float minY, float maxY) {
    XYRectangle rectangle = new XYRectangle(minX, maxX, minY, maxY);
    return newGeometryQuery(field, queryRelation, rectangle);
  }

  /** create a query to find all cartesian shapes that intersect a provided linestring (or array of linestrings) **/
  public static Query newLineQuery(String field, QueryRelation queryRelation, XYLine... lines) {
    return newGeometryQuery(field, queryRelation, lines);
  }

  /** create a query to find all cartesian shapes that intersect a provided polygon (or array of polygons) **/
  public static Query newPolygonQuery(String field, QueryRelation queryRelation, XYPolygon... polygons) {
    return newGeometryQuery(field, queryRelation, polygons);
  }

  /** create a query to find all indexed shapes that comply the {@link QueryRelation} with the provided point
   **/
  public static Query newPointQuery(String field, QueryRelation queryRelation, float[]... points) {
    XYPoint[] pointArray = new XYPoint[points.length];
    for (int i =0; i < points.length; i++) {
      pointArray[i] = new XYPoint(points[i][0], points[i][1]);
    }
    return newGeometryQuery(field, queryRelation, pointArray);
  }

  /** create a query to find all cartesian shapes that intersect a provided circle (or arrays of circles) **/
  public static Query newDistanceQuery(String field, QueryRelation queryRelation, XYCircle... circle) {
    return newGeometryQuery(field, queryRelation, circle);
  }

  /** create a query to find all indexed geo shapes that intersect a provided geometry collection
   *  note: Components do not support dateline crossing
   **/
  public static Query newGeometryQuery(String field, QueryRelation queryRelation, XYGeometry... xyGeometries) {
    if (queryRelation == QueryRelation.CONTAINS && xyGeometries.length > 1) {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      for (int i = 0; i < xyGeometries.length; i++) {
        builder.add(newGeometryQuery(field, queryRelation, xyGeometries[i]), BooleanClause.Occur.MUST);
      }
      return new ConstantScoreQuery(builder.build());
    }
    return new XYShapeQuery(field, queryRelation, xyGeometries);
  }
}
