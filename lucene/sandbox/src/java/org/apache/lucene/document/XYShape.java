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

import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.search.Query;

import static org.apache.lucene.geo.XYEncodingUtils.encode;

public class XYShape extends ShapeField {

  // no instance:
  private XYShape() {
  }

  /** create indexable fields for polygon geometry */
  public static Field[] createIndexableFields(String fieldName, XYPolygon polygon) {

    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    List<Triangle> fields = new ArrayList<>(tessellation.size());
    for (Tessellator.Triangle t : tessellation) {
      fields.add(new Triangle(fieldName, t));
    }
    return fields.toArray(new Field[fields.size()]);
  }

  /** create indexable fields for line geometry */
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

  /** create indexable fields for point geometry */
  public static Field[] createIndexableFields(String fieldName, double x, double y) {
    return new Field[] {new Triangle(fieldName,
        encode(x), encode(y), encode(x), encode(y), encode(x), encode(y))};
  }

  /** create a query to find all polygons that intersect a defined bounding box
   **/
  public static Query newBoxQuery(String field, QueryRelation queryRelation, double minX, double maxX, double minY, double maxY) {
    return new XYShapeBoundingBoxQuery(field, queryRelation, minX, maxX, minY, maxY);
  }

  /** create a query to find all polygons that intersect a provided linestring (or array of linestrings)
   **/
  public static Query newLineQuery(String field, QueryRelation queryRelation, XYLine... lines) {
    return new XYShapeLineQuery(field, queryRelation, lines);
  }

  /** create a query to find all polygons that intersect a provided polygon (or array of polygons)
   **/
  public static Query newPolygonQuery(String field, QueryRelation queryRelation, XYPolygon... polygons) {
    return new XYShapePolygonQuery(field, queryRelation, polygons);
  }
}
