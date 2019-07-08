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
import org.apache.lucene.index.PointValues; // javadoc
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.search.Query;

import static org.apache.lucene.geo.XYEncodingUtils.encode;

/**
 * A cartesian shape utility class for indexing and searching geometries whose vertices are unitless x, y values.
 * <p>
 * This class defines six static factory methods for common indexing and search operations:
 * <ul>
 *   <li>{@link #createIndexableFields(String, XYPolygon)} for indexing a cartesian polygon.
 *   <li>{@link #createIndexableFields(String, XYLine)} for indexing a cartesian linestring.
 *   <li>{@link #createIndexableFields(String, float, float)} for indexing a x, y cartesian point.
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching cartesian shapes that have some {@link QueryRelation} with a bounding box.
 *   <li>{@link #newBoxQuery newLineQuery()} for matching cartesian shapes that have some {@link QueryRelation} with a linestring.
 *   <li>{@link #newBoxQuery newPolygonQuery()} for matching cartesian shapes that have some {@link QueryRelation} with a polygon.
 * </ul>

 * <b>WARNING</b>: Like {@link LatLonPoint}, vertex values are indexed with some loss of precision from the
 * original {@code double} values.
 * @see PointValues
 * @see LatLonDocValuesField
 *
 * @lucene.experimental
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
    return new XYShapeBoundingBoxQuery(field, queryRelation, minX, maxX, minY, maxY);
  }

  /** create a query to find all cartesian shapes that intersect a provided linestring (or array of linestrings) **/
  public static Query newLineQuery(String field, QueryRelation queryRelation, XYLine... lines) {
    return new XYShapeLineQuery(field, queryRelation, lines);
  }

  /** create a query to find all cartesian shapes that intersect a provided polygon (or array of polygons) **/
  public static Query newPolygonQuery(String field, QueryRelation queryRelation, XYPolygon... polygons) {
    return new XYShapePolygonQuery(field, queryRelation, polygons);
  }
}
