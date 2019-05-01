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

import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.Query;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

/**
 * An indexed shape utility class.
 * <p>
 * {@link Polygon}'s are decomposed into a triangular mesh using the {@link Tessellator} utility class
 * Each {@link Triangle} is encoded and indexed as a multi-value field.
 * <p>
 * Finding all shapes that intersect a range (e.g., bounding box) at search time is efficient.
 * <p>
 * This class defines static factory methods for common operations:
 * <ul>
 *   <li>{@link #createIndexableFields(String, Polygon)} for matching polygons that intersect a bounding box.
 *   <li>{@link #newBoxQuery newBoxQuery()} for matching polygons that intersect a bounding box.
 * </ul>

 * <b>WARNING</b>: Like {@link LatLonPoint}, vertex values are indexed with some loss of precision from the
 * original {@code double} values (4.190951585769653E-8 for the latitude component
 * and 8.381903171539307E-8 for longitude).
 * @see PointValues
 * @see LatLonDocValuesField
 *
 * @lucene.experimental
 */
public class LatLonShape extends ShapeField {

  // no instance:
  private LatLonShape() {
  }

  /** create indexable fields for polygon geometry */
  public static Field[] createIndexableFields(String fieldName, Polygon polygon) {
    // the lionshare of the indexing is done by the tessellator
    List<Tessellator.Triangle> tessellation = Tessellator.tessellate(polygon);
    List<Triangle> fields = new ArrayList<>();
    for (Tessellator.Triangle t : tessellation) {
      fields.add(new Triangle(fieldName, t));
    }
    return fields.toArray(new Field[fields.size()]);
  }

  /** create indexable fields for line geometry */
  public static Field[] createIndexableFields(String fieldName, Line line) {
    int numPoints = line.numPoints();
    Field[] fields = new Field[numPoints - 1];
    // create "flat" triangles
    for (int i = 0, j = 1; j < numPoints; ++i, ++j) {
      fields[i] = new Triangle(fieldName,
          encodeLongitude(line.getLon(i)), encodeLatitude(line.getLat(i)),
          encodeLongitude(line.getLon(j)), encodeLatitude(line.getLat(j)),
          encodeLongitude(line.getLon(i)), encodeLatitude(line.getLat(i)));
    }
    return fields;
  }

  /** create indexable fields for point geometry */
  public static Field[] createIndexableFields(String fieldName, double lat, double lon) {
    return new Field[] {new Triangle(fieldName,
        encodeLongitude(lon), encodeLatitude(lat),
        encodeLongitude(lon), encodeLatitude(lat),
        encodeLongitude(lon), encodeLatitude(lat))};
  }

  /** create a query to find all polygons that intersect a defined bounding box
   **/
  public static Query newBoxQuery(String field, QueryRelation queryRelation, double minLatitude, double maxLatitude, double minLongitude, double maxLongitude) {
    return new LatLonShapeBoundingBoxQuery(field, queryRelation, minLatitude, maxLatitude, minLongitude, maxLongitude);
  }

  /** create a query to find all polygons that intersect a provided linestring (or array of linestrings)
   *  note: does not support dateline crossing
   **/
  public static Query newLineQuery(String field, QueryRelation queryRelation, Line... lines) {
    return new LatLonShapeLineQuery(field, queryRelation, lines);
  }

  /** create a query to find all polygons that intersect a provided polygon (or array of polygons)
   *  note: does not support dateline crossing
   **/
  public static Query newPolygonQuery(String field, QueryRelation queryRelation, Polygon... polygons) {
    return new LatLonShapePolygonQuery(field, queryRelation, polygons);
  }
}
